# scraper_logic.py
import os
import re
import sys
import calendar
import tempfile
import time
import json
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import gspread
from gspread_formatting import CellFormat, TextFormat, format_cell_range
from google.oauth2.service_account import Credentials

from pydrive2.auth  import GoogleAuth
from pydrive2.drive import GoogleDrive
from oauth2client.service_account import ServiceAccountCredentials

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# ── CONFIG ─────────────────────────────────────────────────────────────
creds = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"])

SCOPES = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive.readonly"]

SPREADSHEET_URL        = "https://docs.google.com/spreadsheets/d/1tFrTn7iNN9IdQVONgjJgHVZkFwQo4GOIe3EPTH289hM"
MASTER_SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1NO5KnRCNV0oV_Qly757Ca9IA8_-pzbZ06CNfuuFkOCc"
MASTER_TAB_NAME        = "ALL TV GDS ARRAZ"

DRIVE_FOLDER_ID = "1YipBkHKjTYjUk6NJIcLDusJa54IiKsgM"

TARGET_CHANNELS  = ["RCTI", "MNCTV", "GTV", "INEWS"]
GSHEET_SHEET_MAP = {"INEWS": "INews", "RCTI": "RCTI", "MNCTV": "MNCTV", "GTV": "GTV"}
RAW_SKIPROWS = 3

BASE_URL = "https://rundek-00.mncplus.com/project/Conviva/job/show/"
JOB_IDS  = [
    "800d5efe-2a09-48a8-a7ed-c4a255f69829",
    "58922b76-06b2-48de-9279-2ec48b2360fb",
    "f8fd07c2-f5d8-47d6-8c1f-6cb7ffbb52ab",
    "d8d7cc54-d77f-4f6c-9ad7-c4a12d1574e8",
]
USERNAME = "rnd"
PASSWORD = "rnd!@#"

# ── HELPER UTILITIES ────────────────────────────────────────────────────
def extract_dates_from_filename(name: str):
    name = name.upper()
    r = re.search(r"(\d{1,2})-(\d{1,2})\s+([A-Z]+)\s+(\d{4})", name)
    if r:
        d1,d2,mon,yr = r.groups()
        m = list(calendar.month_abbr).index(mon[:3].title())
        return [datetime(int(yr),m,int(d1)).date(), datetime(int(yr),m,int(d2)).date()]
    r = re.search(r"(\d{1,2})\s+([A-Z]+)\s+(\d{4})", name)
    if r:
        d,mon,yr = r.groups()
        m = list(calendar.month_abbr).index(mon[:3].title())
        return [datetime(int(yr),m,int(d)).date()]
    return []

parse_time_to_int_safe = lambda s: (
    int(re.sub(r"[^\d]", "", str(s))) if pd.notna(s) and re.sub(r"[^\d]", "", str(s)).isdigit() else None)

def normalize_time_string(s):
    if pd.isna(s):
        return None
    try:
        parts = str(s).strip().split(":")
        if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
            return None
        h = int(parts[0]) % 24
        m = int(parts[1])
        return f"{h:02}:{m:02}"
    except:
        return None

def split_genre(desc):
    if pd.isna(desc): return "", ""
    p = str(desc).split(":", 1)
    return p[0].strip(), (p[1].strip() if len(p) > 1 else "")

def to_two_decimal_string(v):
    try:
        if pd.isna(v) or str(v).strip().lower() in ['nan','']: return ''
        if isinstance(v, str): v = v.replace(',', '.')
        return f"{float(v):.2f}"
    except: return ''

def clean_dataframe(df: pd.DataFrame, ch: str):
    if df.empty: return df.copy()

    def minutes(hhmm):
        if pd.isna(hhmm) or ":" not in str(hhmm): return 0
        h, m = map(int, hhmm.split(":")); return h * 60 + m

    keep = [abs(minutes(r["End Time"]) - minutes(r["Start Time"])) > 5 for _, r in df.iterrows()]
    df = df.loc[keep].reset_index(drop=True)

    rows = []
    for _, r in df.iterrows():
        if rows:
            prev = rows[-1]
            dup = str(r["Prog"]).strip().upper() == str(prev["Prog"]).strip().upper()
            if dup and not (ch.upper() == "RCTI" and str(r["Prog"]).strip().upper() == "SINEMA"):
                prev["End Time"] = r["End Time"]
                continue
        rows.append(r.copy())
    return pd.DataFrame(rows)

def download_drive_excels(folder_id):
    gauth = GoogleAuth()
    gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name(
        SERVICE_ACCOUNT_FILE, scopes=["https://www.googleapis.com/auth/drive.readonly"])
    drive = GoogleDrive(gauth)
    lst = drive.ListFile({'q': f"'{folder_id}' in parents and trashed=false"}).GetList()
    lst = [f for f in lst if f['title'].lower().endswith('.xlsx')]
    lst.sort(key=lambda f: f['modifiedDate'])
    out = []
    for f in lst:
        temp_path = Path(tempfile.gettempdir()) / f["title"]
        f.GetContentFile(str(temp_path))
        out.append((temp_path, f["title"]))
    return out

def upload_channel_to_gsheet(df,sheet_title,date_obj):
    df=df.copy()
    df['s']=df['Start Time'].apply(parse_time_to_int_safe)
    df['e']=df['End Time'  ].apply(parse_time_to_int_safe)
    df=df[df['s'].notna() & df['e'].notna() & (df['e']>=df['s']) & (df['e']<=2359)]
    if df.empty: return
    rows=[[date_obj.strftime('%Y-%m-%d')]*2 + [s,e] for s,e in zip(df['Start Time'],df['End Time'])]
    ws=gspread.authorize(
        Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE,scopes=SCOPES)
    ).open_by_url(SPREADSHEET_URL).worksheet(sheet_title)
    ws.batch_clear(['A2:D'])
    ws.update(rows, 'A2')
    format_cell_range(ws, f"A2:D{len(rows)+1}",
        CellFormat(textFormat=TextFormat(fontFamily="Arial",fontSize=10),
                   horizontalAlignment="RIGHT"))

def run_conviva_jobs(headless=True):
    opt = webdriver.ChromeOptions()
    if headless:
        opt.add_argument('--headless=new')
    opt.add_argument('--no-sandbox')
    opt.add_argument('--disable-dev-shm-usage')
    opt.add_argument('--disable-gpu')
    
    drv = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opt)
    wait = WebDriverWait(drv, 15)
    try:
        drv.get('https://rundek-00.mncplus.com')
        wait.until(EC.visibility_of_element_located((By.ID, 'login'))).send_keys(USERNAME)
        drv.find_element(By.ID, 'password').send_keys(PASSWORD)
        drv.find_element(By.ID, 'btn-login').click()
        wait.until(EC.url_contains('/menu'))
        for jid in JOB_IDS:
            drv.get(BASE_URL + jid)
            wait.until(EC.element_to_be_clickable((By.ID, 'execFormRunButton'))).click()
            time.sleep(1.0)
    finally:
        drv.quit()

    print("⏳ Waiting 20 seconds for Output sheet to be filled…")
    time.sleep(20)


# ── MERGE 4 KOL OUTPUT ──────────────────────────────────────────────────
def merge_output_cols(df, out_rows):
    # Siapkan kolom output
    df[["E_out", "F_out", "G_out", "H_out"]] = ""

    # Konversi jam ke format angka string seperti 2300
    def jam_to_str(s):
        if pd.isna(s): return None
        s = str(s)
        digits = re.sub(r"[^\d]", "", s)
        return digits.zfill(4) if digits.isdigit() else None

    # Buat dictionary dari (jam1, jam2) → [plays, uniq, ccu, min_ud]
    out_map = {}
    for row in out_rows:
        if len(row) < 6:
            continue
        jam1 = jam_to_str(row[0])
        jam2 = jam_to_str(row[1])
        if not jam1 or not jam2:
            continue
        try:
            plays   = float(str(row[2]).replace(',', ''))
            uniq    = float(str(row[3]).replace(',', ''))
            ccu     = float(str(row[4]).replace(',', ''))
            min_ud  = float(str(row[5]).replace(',', '.'))  # Gsheet kadang pakai koma decimal
        except:
            continue  # skip kalau parsing gagal
        out_map[(jam1, jam2)] = [plays, uniq, ccu, min_ud]

    match_count = 0

    # Cocokkan jam di df dengan out_map
    for i, r in df.iterrows():
        jam1 = jam_to_str(r["Start Time"])
        jam2 = jam_to_str(r["End Time"])
        key = (jam1, jam2)
        if key in out_map:
            plays, uniq, ccu, min_ud = out_map[key]
            df.loc[i, ["E_out", "F_out", "G_out", "H_out"]] = [plays, uniq, ccu, min_ud]
            match_count += 1

    # Pastikan semua kolom hasil adalah numerik (bukan string)
    df[["E_out", "F_out", "G_out", "H_out"]] = df[["E_out", "F_out", "G_out", "H_out"]].apply(pd.to_numeric, errors='coerce')
    print(f"✅ {match_count} baris berhasil dicocokkan berdasarkan jam.")


# ── MAIN PIPELINE ───────────────────────────────────────────────────────

def filter_and_group_rows(df: pd.DataFrame, mode: str):
    # Konversi kolom C (Start Time) ke bentuk integer, misal "04:28" → 428
    df["t_int"] = df.iloc[:, 2].apply(parse_time_to_int_safe)

    # Siapkan struktur data untuk masing-masing channel
    grouped = {ch: [] for ch in TARGET_CHANNELS}

    for _, row in df.iterrows():
        ch = str(row.iloc[0]).strip().upper()
        if ch not in TARGET_CHANNELS:
            continue

        t_int = row["t_int"]
        if t_int is None:
            continue

        # Koreksi logika — hanya pilih baris yang sesuai dengan tanggal logis
        if mode == "yesterday":
            # HANYA ambil yang lewat tengah malam (misalnya 2401 atau 0000 yang tidak valid sebenarnya)
            if t_int > 2359:
                grouped[ch].append(row.iloc[2:6].tolist())
        elif mode == "today":
            # Ambil semua tayangan sampai jam 23:59
            if t_int <= 2359:
                grouped[ch].append(row.iloc[2:6].tolist())

    return grouped

def run_scraper(headless=True):
    files = download_drive_excels(DRIVE_FOLDER_ID)
    if not files: raise RuntimeError("No Excel in Drive folder")
    today_path,today_name = files[-1]
    dates = extract_dates_from_filename(today_name)
    if not dates: raise RuntimeError(f"Bad filename: {today_name}")
    today     = max(dates)
    yesterday = today - timedelta(days=1)
    y_tuple   = next((t for t in files if yesterday in extract_dates_from_filename(t[1])), None)
    if not y_tuple: raise RuntimeError(f"Missing file for {yesterday:%d %b %Y}")
    y_path,_  = y_tuple

    df_today = pd.read_excel(today_path, sheet_name=f"{today.day:02}",
                             skiprows=RAW_SKIPROWS, dtype=str)
    df_yest  = pd.read_excel(y_path,   sheet_name=f"{yesterday.day:02}",
                             skiprows=RAW_SKIPROWS, dtype=str)

    grouped_today = filter_and_group_rows(df_today, "today")
    grouped_yest  = filter_and_group_rows(df_yest,  "yesterday")
    combined={ch:grouped_yest[ch]+grouped_today[ch] for ch in TARGET_CHANNELS}

    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE,scopes=SCOPES)
    sh_raw   = gspread.authorize(creds).open_by_url(SPREADSHEET_URL)
    sh_final = gspread.authorize(creds).open_by_url(MASTER_SPREADSHEET_URL)
    try:
        ws_final = sh_final.worksheet(MASTER_TAB_NAME)
    except gspread.WorksheetNotFound:
        ws_final = sh_final.add_worksheet(title=MASTER_TAB_NAME,rows=2000,cols=20)

    channel_dfs={}
    for ch in TARGET_CHANNELS:
        base=pd.DataFrame(combined[ch],columns=['Start Time','End Time','Prog','Desc'])
        base['Start Time']=base['Start Time'].apply(normalize_time_string)
        base['End Time']  =base['End Time' ].apply(normalize_time_string)
        base = clean_dataframe(base,ch)
        if base.empty: continue

        base[['Genre1','Genre2']] = base['Desc'].apply(lambda x: pd.Series(split_genre(x)))
        base.insert(0,'Date',f"{today.day:02}/{today.month:02}/{today.year}")
        base.insert(0,'Channel',ch)
        channel_dfs[ch]=base

        upload_channel_to_gsheet(
            base[['Start Time','End Time']],
            f"Input_{GSHEET_SHEET_MAP[ch]}_Live_TV",
            today
        )

    # Trigger Rundeck & tunggu
    run_conviva_jobs(headless=headless)

    # Gabungkan output
    for ch,df in channel_dfs.items():
        try:
            out_rows = sh_raw.worksheet(f"Output_{GSHEET_SHEET_MAP[ch]}_Live_TV").get("C2:H")
        except gspread.WorksheetNotFound:
            out_rows=[]
        merge_output_cols(df,out_rows)

        final_cols=['Channel','Date','Start Time','End Time','Prog',
                    'E_out','F_out','G_out','H_out','Genre1','Genre2']
        df=df.reindex(columns=final_cols).fillna('')

        start_row=len(ws_final.col_values(1))+1
        ws_final.update(df.values.tolist(), f"A{start_row}")

        end_row=start_row+len(df)-1
        # rataan dasar = kanan
        format_cell_range(ws_final,f"A{start_row}:K{end_row}",
                          CellFormat(horizontalAlignment="RIGHT"))
        
        # Format kolom angka agar pakai pemisah ribuan (,)
        for col in ['E', 'F', 'G']:
            format_cell_range(ws_final, f"{col}{start_row}:{col}{end_row}",
                            CellFormat(numberFormat={'type': 'NUMBER', 'pattern': '#,##0'},
                                        horizontalAlignment="RIGHT"))
        # Kolom H (min/ud) tetap angka desimal dengan koma ribuan
        format_cell_range(ws_final, f"H{start_row}:H{end_row}",
                            CellFormat(numberFormat={'type': 'NUMBER', 'pattern': '#,##0.00'},
                                        horizontalAlignment="RIGHT"))
        # rata‑kiri kolom A,E,J,K
        for col in ['A','E','J','K']:
            format_cell_range(ws_final,f"{col}{start_row}:{col}{end_row}",
                              CellFormat(horizontalAlignment="LEFT"))
        # format tanggal kolom B
        format_cell_range(ws_final,f"B{start_row}:B{end_row}",
                          CellFormat(numberFormat={'type':'DATE','pattern':'dd/mm/yyyy'},
                                     horizontalAlignment="RIGHT"))

    print("✅ Pipeline complete.")

# ── CLI ─────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        run_scraper(headless=True)
    except Exception as e:
        print("❌ An error occurred:", e)
        input("Press Enter to exit...")

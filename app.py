import streamlit as st
import os, tempfile
from scraper_logic import run_scraper

st.set_page_config(page_title="Report Scraper", layout="centered")

st.title("Report Scraper")
st.write("Ekstrak dari Google Drive → Proses file Excel → Unggah ke Google Sheet (Input) → Jalankan job di Rundeck → Ambil hasil dari Sheet Output → Gabungkan ke Sheet Master")

if st.button("Run Report Pipeline"):
    with st.spinner("Running pipeline..."):
        try:
            run_scraper(headless=True)
            st.success("Done! Google Sheets and Rundeck have been updated.")
        except Exception as e:
            st.error(f"Error: {e}")
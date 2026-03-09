# news_calender_fxverify.py

import logging
from datetime import datetime, date, timedelta, timezone
import requests
from bs4 import BeautifulSoup
import re
import time
import os
import json
import hashlib 

# Import Selenium
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException
from config import config

logger = logging.getLogger(__name__)

# Pengaturan Caching
FXVERIFY_CACHE_FILE = "fxverify_calendar_cache.json"
CACHE_TTL_SECONDS = 3600 * 2 # 2 jam TTL untuk kalender (bisa disesuaikan)

def scrape_fxverify_calendar_selenium():
    """
    Melakukan scraping kalender ekonomi dari FXVerify.com menggunakan Selenium.
    Menggunakan mekanisme caching.
    """
    url = "https://fxverify.com/tools/economic-calendar"
    
    events_list = []
    driver = None

    # --- Coba Baca dari Cache Terlebih Dahulu ---
    if os.path.exists(FXVERIFY_CACHE_FILE):
        try:
            with open(FXVERIFY_CACHE_FILE, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            cache_timestamp = datetime.fromisoformat(cache_data['timestamp']).replace(tzinfo=timezone.utc)
            
            if (datetime.now(timezone.utc) - cache_timestamp).total_seconds() < CACHE_TTL_SECONDS:
                logger.info(f"Menggunakan data kalender dari cache (segar, {len(cache_data['events'])} event) untuk FXVerify.")
                for item in cache_data['events']:
                    if item.get('event_time_utc') and isinstance(item['event_time_utc'], str):
                        item['event_time_utc'] = datetime.fromisoformat(item['event_time_utc']).replace(tzinfo=timezone.utc)
                return cache_data['events']
            else:
                logger.info(f"Cache FXVerify kedaluwarsa ({CACHE_TTL_SECONDS / 3600} jam). Akan men-scrape ulang.")
        except Exception as e:
            logger.warning(f"Gagal membaca atau memvalidasi cache FXVerify: {e}. Akan men-scrape ulang.")

    logger.info(f"Mulai scraping kalender dari: {url} menggunakan Selenium.")

    try:
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--disable-gpu")

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        
        driver.get(url)
        
        logger.info("Menunggu tabel kalender dan setidaknya satu baris event dimuat...")
        
        WebDriverWait(driver, 45).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#eventDate_table_body tr.ec-fx-table-event-row"))
        )
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        calendar_table = soup.find('table', id='table_detail') 
        if not calendar_table:
            logger.error("Tabel kalender dengan ID 'table_detail' tidak ditemukan setelah rendering Selenium.")
            return []

        tbody = calendar_table.find('tbody', id='eventDate_table_body')
        if not tbody:
            logger.error("Tbody dengan ID 'eventDate_table_body' tidak ditemukan dalam tabel kalender.")
            return []

        rows = tbody.find_all('tr', class_='ec-fx-table-event-row')
        
        if not rows:
            logger.warning("Tidak ada baris event ('ec-fx-table-event-row') ditemukan dalam tbody, meskipun ditunggu.")
            return []

        for row in rows:
            event_datetime_utc = None
            # --- MODIFIKASI DIMULAI DI SINI ---
            # Pastikan event_timestamp_unix selalu ada dan valid
            event_timestamp_unix_str = row.get('time')
            event_timestamp_unix = 0
            if event_timestamp_unix_str:
                try:
                    event_timestamp_unix = int(event_timestamp_unix_str)
                except ValueError:
                    logger.warning(f"FxVerify: Gagal mengonversi timestamp '{event_timestamp_unix_str}' ke integer. Menggunakan 0.")
                    event_timestamp_unix = 0

            if event_timestamp_unix != 0:
                try: 
                    event_datetime_utc = datetime.fromtimestamp(event_timestamp_unix, tz=timezone.utc)
                except ValueError:
                    logger.warning(f"FxVerify: Timestamp UNIX '{event_timestamp_unix}' tidak valid. Menggunakan waktu saat ini sebagai fallback.")
                    event_datetime_utc = datetime.now(timezone.utc)
            else:
                logger.warning(f"FxVerify: Atribut 'time' tidak valid atau 0 untuk baris event. Menggunakan waktu saat ini sebagai fallback.")
                event_datetime_utc = datetime.now(timezone.utc)
            # --- MODIFIKASI BERAKHIR DI SINI ---

            cells = row.find_all('td')
            
            if len(cells) < 11:
                logger.debug(f"FxVerify: Melewatkan baris karena jumlah sel tidak cukup ({len(cells)}): {row.prettify()[:100]}")
                continue

            currency_div = cells[4].find('div')
            currency = currency_div.text.strip() if currency_div else "" 
            # --- MODIFIKASI DIMULAI DI SINI ---
            if not currency: # Pastikan currency tidak kosong
                logger.warning(f"FxVerify: Mata uang kosong untuk event '{event_name}'. Menggunakan 'N/A' sebagai fallback.")
                currency = "N/A"
            # --- MODIFIKASI BERAKHIR DI SINI ---

            impact_div = cells[5].find('div', class_=re.compile(r'ec-fx-impact'))
            impact = "Low"
            if impact_div:
                if 'high' in impact_div.get('class', []):
                    impact = "High"
                elif 'medium' in impact_div.get('class', []):
                    impact = "Medium"

            event_name_link = cells[6].find('a', class_='event-name')
            event_name = event_name_link.text.strip() if event_name_link else ""
            # --- MODIFIKASI DIMULAI DI SINI ---
            if not event_name: # Pastikan event_name tidak kosong
                logger.warning(f"FxVerify: Nama event kosong. Menggunakan 'Unknown Event' sebagai fallback.")
                event_name = "Unknown Event"
            # --- MODIFIKASI BERAKHIR DI SINI ---

            actual_value = cells[8].text.strip()
            actual_value = actual_value.replace('\n', '').replace('\t', '').replace('\r', '').replace('&nbsp;', '').strip()
            if not actual_value: actual_value = None

            forecast_value = cells[9].text.strip()
            forecast_value = forecast_value.replace('\n', '').replace('\t', '').replace('\r', '').replace('&nbsp;', '').strip()
            if not forecast_value: forecast_value = None

            previous_value = cells[10].text.strip()
            previous_value = previous_value.replace('\n', '').replace('\t', '').replace('\r', '').replace('&nbsp;', '').strip()
            if not previous_value: previous_value = None

            # Buat event_id yang unik dan robust menggunakan SHA256 hash
            combined_string_for_hash = f"{event_name}_{currency}_{event_datetime_utc.strftime('%Y%m%d%H%M%S')}_{actual_value}_{forecast_value}_{previous_value}_{impact}"
            event_id_final = hashlib.sha256(combined_string_for_hash.encode('utf-8')).hexdigest()

            events_list.append({
                "event_id": event_id_final, 
                "name": event_name, "currency": currency,
                "impact": impact, "event_time_utc": event_datetime_utc, 
                "actual_value": actual_value, "forecast_value": forecast_value, "previous_value": previous_value,
                "smart_analysis": None
            })
            
    except TimeoutException:
        logger.error("Timeout saat menunggu elemen kalender dimuat. Koneksi lambat atau elemen tidak muncul.")
    except WebDriverException as wde:
        logger.error(f"Kesalahan WebDriver (pastikan Chrome terinstal dan kompatibel dengan ChromeDriver): {wde}", exc_info=True)
    except requests.exceptions.RequestException as e:
        logger.error(f"Gagal mengakses halaman kalender FxVerify: {e}")
    except Exception as e:
        logger.error(f"Terjadi kesalahan tak terduga saat scraping dengan Selenium: {e}", exc_info=True)
        
    finally:
        if driver:
            driver.quit()
    
    logger.info(f"Selesai scraping FxVerify dengan Selenium. Ditemukan {len(events_list)} event.")

    # --- Simpan Hasil ke Cache Jika Berhasil ---
    if events_list:
        try:
            cache_data_to_save = {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'events': []
            }
            for item in events_list:
                item_copy = item.copy()
                if item_copy.get('event_time_utc') and isinstance(item_copy['event_time_utc'], datetime):
                    item_copy['event_time_utc'] = item_copy['event_time_utc'].isoformat()
                cache_data_to_save['events'].append(item_copy)

            with open(FXVERIFY_CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(cache_data_to_save, f, indent=4)
            logger.info(f"Data kalender FXVerify berhasil disimpan ke cache: {FXVERIFY_CACHE_FILE}")
        except Exception as e:
            logger.error(f"Gagal menulis data kalender ke cache: {e}")
        
    return events_list
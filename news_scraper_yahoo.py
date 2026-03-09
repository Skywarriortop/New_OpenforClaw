# news_scraper_yahoo.py

import logging
from datetime import datetime, timedelta, timezone
import requests
from bs4 import BeautifulSoup
import re
import time
import os
import json 


logger = logging.getLogger(__name__)

# Pengaturan Caching
YAHOO_CACHE_FILE = "yahoo_news_cache.json"
CACHE_TTL_SECONDS = 3600 # 1 jam TTL untuk berita Yahoo (bisa disesuaikan)

def scrape_yahoo_finance_headlines_direct(days_past=1):
    """
    Melakukan scraping langsung berita headline dari halaman utama Yahoo Finance
    menggunakan requests dan BeautifulSoup. Menggunakan mekanisme caching.
    """
    news_items_list = []

    urls_to_scrape = [
        "https://finance.yahoo.com/",
        "https://finance.yahoo.com/news/"
    ]

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
        'Referer': 'https://www.yahoo.com/'
    }

    # --- Coba Baca dari Cache Terlebih Dahulu ---
    if os.path.exists(YAHOO_CACHE_FILE):
        try:
            with open(YAHOO_CACHE_FILE, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            cache_timestamp = datetime.fromisoformat(cache_data['timestamp']).replace(tzinfo=timezone.utc)
            
            if (datetime.now(timezone.utc) - cache_timestamp).total_seconds() < CACHE_TTL_SECONDS:
                logger.info(f"Menggunakan data berita dari cache (segar, {len(cache_data['news'])} berita) untuk Yahoo Finance.")
                for item in cache_data['news']:
                    if item.get('published_time_utc') and isinstance(item['published_time_utc'], str):
                        item['published_time_utc'] = datetime.fromisoformat(item['published_time_utc']).replace(tzinfo=timezone.utc)
                return cache_data['news']
            else:
                logger.info(f"Cache Yahoo Finance kedaluwarsa ({CACHE_TTL_SECONDS / 3600} jam). Akan men-scrape ulang.")
        except Exception as e:
            logger.warning(f"Gagal membaca atau memvalidasi cache Yahoo Finance: {e}. Akan men-scrape ulang.")

    logger.info(f"[{__name__}] Mulai scraping berita headline langsung dari Yahoo Finance (Pure Requests).")

    try:
        now_utc = datetime.now(timezone.utc)
        start_ts_filter = (now_utc - timedelta(days=days_past)).timestamp()

        for url in urls_to_scrape:
            logger.debug(f"[{__name__}] Mengambil HTML dari URL: {url}")
            try:
                response = requests.get(url, headers=headers, timeout=15)
                response.raise_for_status()
                time.sleep(1)
            except requests.exceptions.RequestException as e:
                logger.error(f"[{__name__}] Gagal mengakses halaman Yahoo Finance ({url}): {e}")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')

            story_list = soup.find('ul', class_='story-items')

            story_items = []
            if story_list:
                story_items = story_list.find_all('li', class_='story-item')
            else:
                # Ini adalah pola regex yang bisa cocok dengan 'story-item' dan 'story-item-<suffix>'
                story_items = soup.find_all('li', class_=re.compile(r'story-item'))
                if not story_items:
                    # Tambahan selektor untuk memastikan kita menangkap semua berita
                    story_items = soup.find_all('section', attrs={"data-testid": "storyitem"})
                    # Coba juga selektor yang lebih umum yang mungkin mengandung tautan berita
                    if not story_items:
                        story_items = soup.find_all('div', attrs={"data-test-id": re.compile(r'StreamItem|FullStreamItem')})


            if not story_items:
                logger.warning(f"[{__name__}] Tidak ada item berita ditemukan di {url} dengan selektor YF.")
                continue

            for item in story_items:
                title_tag = item.find('a', class_=re.compile(r'titles|Card-titleLink|Fz-16px')) # Menambahkan pola Fz-16px
                title = title_tag.find('h3').text.strip() if title_tag and title_tag.find('h3') else None
                article_url_raw = title_tag.get('href') if title_tag else None

                # --- MODIFIKASI DIMULAI DI SINI: Validasi dan Penugasan ID ---
                if not title or not article_url_raw:
                    logger.debug(f"[{__name__}] Melewatkan item tanpa judul atau URL mentah: {item.prettify()[:100]}")
                    continue

                # Pastikan URL absolut
                final_article_url = requests.compat.urljoin(url, article_url_raw)
                if not final_article_url.startswith('http'):
                    logger.warning(f"[{__name__}] Melewatkan link dengan URL tidak absolut setelah join: {final_article_url}. URL mentah: {article_url_raw}")
                    continue
                
                news_id = final_article_url # Gunakan URL yang sudah divalidasi sebagai ID
                # --- MODIFIKASI BERAKHIR DI SINI ---

                # Dapatkan ringkasan, jika ada. Fallback ke string kosong jika tidak ditemukan.
                summary_tag = item.find('p', class_='fz-s lh-16') # Contoh class untuk ringkasan di YF
                summary_text = summary_tag.text.strip() if summary_tag else ""


                published_time = None
                time_tag = item.find('time')
                if time_tag and time_tag.get('datetime'):
                    try: published_time = datetime.fromisoformat(time_tag.get('datetime').replace('Z', '+00:00'))
                    except ValueError: 
                        logger.warning(f"[{__name__}] Gagal parsing datetime dari {time_tag.get('datetime')}. Mencoba 'time ago' atau fallback.")
                        pass # published_time akan tetap None, ditangani di bawah
                
                # Fallback untuk waktu publikasi jika datetime gagal
                if published_time is None:
                    # Coba parsing dari teks "X ago"
                    publishing_div = item.find('div', class_='publishing')
                    publisher_text = publishing_div.text.strip() if publishing_div else ""
                    time_ago_match = re.search(r'(\d+)\s+(minute|hour|day)s?\s+ago', publisher_text, re.IGNORECASE)
                    if time_ago_match:
                        num = int(time_ago_match.group(1))
                        unit = time_ago_match.group(2).lower()
                        if 'min' in unit: published_time = now_utc - timedelta(minutes=num)
                        elif 'hour' in unit: published_time = now_utc - timedelta(hours=num)
                        elif 'day' in unit: published_time = now_utc - timedelta(days=num)
                    else:
                        published_time = now_utc # Fallback terakhir jika tidak bisa parse waktu

                if published_time.timestamp() < start_ts_filter:
                    logger.debug(f"[{__name__}] Berita YF (direct) '{title[:50]}...' dilewatkan: terlalu lama.")
                    continue

                news_items_list.append({
                    "id": news_id, 
                    "source": "Yahoo Finance (Direct)",
                    "title": title,
                    "summary": summary_text,
                    "url": final_article_url, 
                    "published_time_utc": published_time, 
                    "relevance_score": 1
                })

    except Exception as e:
        logger.error(f"[{__name__}] Terjadi kesalahan saat scraping langsung Yahoo Finance: {e}", exc_info=True)

    logger.info(f"[{__name__}] Selesai scraping berita langsung dari Yahoo Finance. Ditemukan {len(news_items_list)} berita.")
    
    # --- Simpan Hasil ke Cache Jika Berhasil ---
    if news_items_list:
        try:
            cache_data_to_save = {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'news': []
            }
            for item in news_items_list:
                item_copy = item.copy()
                if item_copy.get('published_time_utc') and isinstance(item_copy['published_time_utc'], datetime):
                    item_copy['published_time_utc'] = item_copy['published_time_utc'].isoformat()
                cache_data_to_save['news'].append(item_copy)

            with open(YAHOO_CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(cache_data_to_save, f, indent=4)
            logger.info(f"Data berita Yahoo Finance berhasil disimpan ke cache: {YAHOO_CACHE_FILE}")
        except Exception as e:
            logger.error(f"Gagal menulis data berita Yahoo Finance ke cache: {e}")

    return news_items_list
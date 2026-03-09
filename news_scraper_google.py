# news_scraper_google.py

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
GOOGLE_CACHE_FILE = "google_news_cache.json"
CACHE_TTL_SECONDS = 3600 # 1 jam TTL untuk berita Google (bisa disesuaikan)

def scrape_google_news(query_topics=None, days_past=1):
    """
    Melakukan scraping berita dari Google News tanpa filter kata kunci di URL,
    dan mencoba selektor yang lebih umum. Menggunakan mekanisme caching.
    """
    news_items_list = []
    
    base_url = "https://news.google.com/search"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
        'Referer': 'https://news.google.com/'
    }

    search_query_for_url = "+OR+".join(query_topics) if query_topics else "forex+gold+markets"

    params = {'q': search_query_for_url, 'hl': 'en-US', 'gl': 'US', 'ceid': 'US:EN', 'tbm': 'nws'}

    # --- Coba Baca dari Cache Terlebih Dahulu ---
    if os.path.exists(GOOGLE_CACHE_FILE):
        try:
            with open(GOOGLE_CACHE_FILE, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            cache_timestamp = datetime.fromisoformat(cache_data['timestamp']).replace(tzinfo=timezone.utc)
            
            if (datetime.now(timezone.utc) - cache_timestamp).total_seconds() < CACHE_TTL_SECONDS:
                logger.info(f"Menggunakan data berita dari cache (segar, {len(cache_data['news'])} berita) untuk Google News.")
                for item in cache_data['news']:
                    if item.get('published_time_utc') and isinstance(item['published_time_utc'], str):
                        item['published_time_utc'] = datetime.fromisoformat(item['published_time_utc']).replace(tzinfo=timezone.utc)
                return cache_data['news']
            else:
                logger.info(f"Cache Google News kedaluwarsa ({CACHE_TTL_SECONDS / 3600} jam). Akan men-scrape ulang.")
        except Exception as e:
            logger.warning(f"Gagal membaca atau memvalidasi cache Google News: {e}. Akan men-scrape ulang.")

    logger.info(f"[{__name__}] Mulai scraping berita dari Google News untuk query: '{search_query_for_url}'")

    try:
        now_utc = datetime.now(timezone.utc)
        start_ts_filter = (now_utc - timedelta(days=days_past)).timestamp()

        response = requests.get(base_url, headers=headers, params=params, timeout=20)
        response.raise_for_status()
        time.sleep(1)
            
        soup = BeautifulSoup(response.text, 'html.parser')
        
        news_links = soup.find_all('a', class_=re.compile(r'DY5T1d|RZIKme'))
        
        if not news_links:
            logger.warning(f"Tidak ada link berita ('a.DY5T1d') ditemukan di Google News untuk query '{search_query_for_url}'. Struktur HTML mungkin berubah. Mencoba selektor yang sangat umum.")
            news_links = soup.find_all('a', href=re.compile(r'/articles/|/read/CBMi'))

        if not news_links:
            logger.warning(f"Tidak ada link berita ditemukan sama sekali di Google News untuk query '{search_query_for_url}'.")
            return news_items_list


        for link_tag in news_links:
            title = link_tag.text.strip()
            article_url_raw = link_tag.get('href')

            # --- MODIFIKASI DIMULAI DI SINI: Validasi dan Penugasan ID ---
            if not title or not article_url_raw:
                logger.debug(f"[{__name__}] Melewatkan link tanpa judul atau URL: {link_tag.prettify()[:50]}")
                continue
            
            # Ubah URL relatif ke absolut (Google News sering menggunakan URL relatif)
            final_article_url = ""
            if article_url_raw.startswith('./read/'):
                final_article_url = "https://news.google.com" + article_url_raw.lstrip('.')
            elif article_url_raw.startswith('/articles/'):
                final_article_url = "https://news.google.com" + article_url_raw
            else:
                final_article_url = article_url_raw

            # Final check untuk URL: harus dimulai dengan http dan tidak kosong
            if not final_article_url or not final_article_url.startswith('http'):
                logger.warning(f"[{__name__}] Melewatkan link dengan URL tidak valid atau tidak absolut: {final_article_url}. URL mentah: {article_url_raw}")
                continue # Lewati berita ini jika URL tidak valid
            
            news_id = final_article_url # Gunakan URL yang sudah divalidasi sebagai ID
            # --- MODIFIKASI BERAKHIR DI SINI ---

            published_time = None
            summary = title # Default summary adalah judul
            source_name = "Google News" # Default

            time_tag = link_tag.find_next_sibling('time')
            if not time_tag:
                parent_article = link_tag.find_parent('article')
                if parent_article:
                    time_tag = parent_article.find('time')
            
            if time_tag and time_tag.get('datetime'):
                try:
                    published_time = datetime.fromisoformat(time_tag.get('datetime').replace('Z', '+00:00'))
                except ValueError:
                    logger.warning(f"[{__name__}] Gagal parsing datetime dari {time_tag.get('datetime')}. Menggunakan waktu saat ini sebagai fallback.")
                    pass # published_time akan tetap None, ditangani di bawah

            # Fallback untuk waktu publikasi
            if published_time is None:
                published_time = now_utc # Fallback terakhir jika tidak bisa parse waktu

            if published_time.timestamp() < start_ts_filter:
                logger.debug(f"Berita Google News '{title[:50]}...' dilewatkan karena terlalu lama.")
                continue

            news_items_list.append({
                "id": news_id, 
                "source": source_name,
                "title": title,
                "summary": summary,
                "url": final_article_url, 
                "published_time_utc": published_time, 
                "relevance_score": 1 
            })
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Gagal mengakses halaman Google News: {e}")
    except Exception as e:
        logger.error(f"Terjadi kesalahan tak terduga saat scraping Google News: {e}", exc_info=True)
        
    logger.info(f"[{__name__}] Selesai scraping Google News. Ditemukan {len(news_items_list)} berita.")

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

            with open(GOOGLE_CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(cache_data_to_save, f, indent=4)
            logger.info(f"Data berita Google News berhasil disimpan ke cache: {GOOGLE_CACHE_FILE}")
        except Exception as e:
            logger.error(f"Gagal menulis data berita Google News ke cache: {e}")
            
    return news_items_list
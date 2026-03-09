# news_scraper_myfxbook.py

import logging
from datetime import datetime, timedelta, timezone
import requests
from bs4 import BeautifulSoup
import re
import time 
import json 
import os   
import hashlib 

logger = logging.getLogger(__name__)

# Pengaturan Caching
MYFXBOOK_CACHE_FILE = "myfxbook_news_cache.json"
CACHE_TTL_SECONDS = 3600 # 1 jam TTL untuk berita Myfxbook (bisa disesuaikan)

# Fungsi bantu untuk mem-parsing string cookie
def _parse_cookie_string(cookie_string):
    """Mengurai string cookie (misal dari header browser) menjadi dictionary."""
    cookies = {}
    for part in cookie_string.split(';'):
        if '=' in part:
            name, value = part.strip().split('=', 1)
            cookies[name] = value
    return cookies

def scrape_myfxbook_news(cookie_string=None, days_past=1):
    """
    Melakukan scraping berita dari bagian "News / Hottest Stories" Myfxbook.
    Menggunakan requests.Session() dan cookies, serta memperbaiki parsing waktu.
    Menggunakan mekanisme caching.
    """
    news_items_list = []
    
    url = "https://www.myfxbook.com/news"
    cache_file_path = MYFXBOOK_CACHE_FILE 

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
        'Referer': 'https://www.myfxbook.com/'
    }

    # --- 1. Cek Cache ---
    if os.path.exists(cache_file_path):
        try:
            with open(cache_file_path, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            cache_timestamp = datetime.fromisoformat(cache_data['timestamp']).replace(tzinfo=timezone.utc)
            
            if (datetime.now(timezone.utc) - cache_timestamp).total_seconds() < CACHE_TTL_SECONDS:
                logger.info(f"Menggunakan data berita dari cache (segar, {len(cache_data['news'])} berita) untuk Myfxbook.")
                for item in cache_data['news']:
                    if item.get('published_time_utc') and isinstance(item['published_time_utc'], str):
                        item['published_time_utc'] = datetime.fromisoformat(item['published_time_utc']).replace(tzinfo=timezone.utc)
                return cache_data['news']
            else:
                logger.info(f"Cache Myfxbook kedaluwarsa ({CACHE_TTL_SECONDS / 3600} jam). Akan men-scrape ulang.")
        except Exception as e:
            logger.warning(f"Gagal membaca atau memvalidasi cache Myfxbook: {e}. Akan men-scrape ulang.")

    # --- 2. Lakukan Scraping ---
    logger.info(f"Mulai scraping berita dari Myfxbook.com/news (dengan perbaikan waktu dan cookies).")
    
    with requests.Session() as session:
        session.headers.update(headers)

        if cookie_string:
            parsed_cookies = _parse_cookie_string(cookie_string)
            session.cookies.update(parsed_cookies)
            logger.debug(f"Menambahkan {len(parsed_cookies)} cookie yang disediakan ke sesi.")

        try:
            response = session.get(url, timeout=15)
            response.raise_for_status()
            time.sleep(1) 
                
            soup = BeautifulSoup(response.text, 'html.parser')
            
            main_news_container = soup.find('div', class_='row display-flex flex-wrap-wrap main-news forex-news')
            
            if not main_news_container:
                logger.warning(f"Tidak menemukan div 'main-news forex-news' di {url}. Struktur HTML mungkin berubah.")
                return news_items_list

            articles = main_news_container.find_all('article', class_='col-xs-12 col-md-4')
            
            if not articles:
                logger.warning(f"Tidak ada tag <article> berita ditemukan di {url} dengan selektor 'col-xs-12 col-md-4'.")
                return news_items_list

            now_utc = datetime.now(timezone.utc)
            start_ts_filter = (now_utc - timedelta(days=days_past)).timestamp()

            for article in articles:
                title = None
                article_url_relative = None
                summary = ""
                source_name = "Myfxbook"
                time_text_raw = ""
                news_details_div = article.find('div', class_='news-details')
                if news_details_div:
                    news_separator_span = news_details_div.find('span', class_='news-separator')
                    if news_separator_span and news_separator_span.next_sibling:
                        time_text_raw = news_separator_span.next_sibling.strip()
                    # Tambahan: Coba cari elemen <time> di dalam news_details_div juga
                    if not time_text_raw:
                        time_tag_in_details = news_details_div.find('time')
                        if time_tag_in_details and time_tag_in_details.get('datetime'):
                            time_text_raw = time_tag_in_details.get('datetime')

                published_time = None 

                h2_title = article.find('h2', class_='font-weight-700')
                if h2_title:
                    link_tag = h2_title.find('a', href=True)
                    if link_tag:
                        title = link_tag.text.strip()
                        article_url_relative = link_tag.get('href') # URL bisa relatif

                news_description_div = article.find('div', class_='news-description')
                if news_description_div:
                    summary = news_description_div.text.strip()
                
                news_details_div = article.find('div', class_='news-details')
                if news_details_div:
                    news_separator_span = news_details_div.find('span', class_='news-separator')
                    if news_separator_span and news_separator_span.next_sibling:
                        time_text_raw = news_separator_span.next_sibling.strip()
                    else:
                        time_text_raw = ""
                    
                    # --- Perbaikan Parsing Waktu ---
                try:
                    match_ago = re.search(r'(\d+)\s+(day|hour|min)s?\s+ago', time_text_raw, re.IGNORECASE)
                    if match_ago:
                        num = int(match_ago.group(1))
                        unit = match_ago.group(2).lower()
                        if 'min' in unit: published_time = now_utc - timedelta(minutes=num)
                        elif 'hour' in unit: published_time = now_utc - timedelta(hours=num)
                        elif 'day' in unit: published_time = now_utc - timedelta(days=num)
                    else: # Fallback untuk format tanggal absolut
                        # Daftar format yang lebih lengkap
                        for fmt in ['%b %d, %Y %I:%M %p', '%b %d, %Y', '%B %d, %Y %I:%M %p', '%B %d, %Y', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M']:
                            try:
                                # Perhatikan: jika timezone tidak ada di string, asumsikan UTC.
                                # Menambahkan .replace(tzinfo=timezone.utc) jika format tidak menyertakan TZ.
                                dt_obj = datetime.strptime(time_text_raw, fmt)
                                if dt_obj.tzinfo is None:
                                    published_time = dt_obj.replace(tzinfo=timezone.utc)
                                else:
                                    published_obj = dt_obj
                                break
                            except ValueError:
                                continue
                except Exception as parse_e:
                    logger.debug(f"Myfxbook: Gagal parse waktu dari '{time_text_raw}': {parse_e}")
                    published_time = None # Set None jika gagal parse

                # --- MODIFIKASI DIMULAI DI SINI: Validasi dan Penugasan ID ---
                # Validasi minimum: Judul atau URL harus ada.
                if not title and not article_url_relative:
                    logger.debug(f"[{__name__}] Melewatkan item tanpa judul atau URL (semuanya kosong).")
                    continue
                
                # Jadikan URL absolut, pastikan tidak kosong
                final_article_url = requests.compat.urljoin(url, article_url_relative or "") # Tangani article_url_relative = None
                if not final_article_url.startswith('http'):
                    logger.warning(f"[{__name__}] Melewatkan link dengan URL tidak absolut setelah join: {final_article_url}. URL mentah: {article_url_relative}.")
                    continue # Lewati berita ini jika URL tidak valid

                # Fallback terakhir untuk published_time jika masih None
                if published_time is None:
                    published_time = now_utc
                    logger.warning(f"[{__name__}] published_time_utc masih None untuk '{title[:50]}...'. Menggunakan waktu saat ini.")

                # Gunakan URL yang sudah divalidasi sebagai ID. Jika URL masih kosong/None (meskipun sudah divalidasi),
                # buat ID fallback yang sangat unik.
                news_id = final_article_url
                if not news_id:
                    # Ini seharusnya jarang terjadi dengan validasi di atas, tapi untuk jaga-jaga
                    news_id = f"FALLBACK_MYFXBOOK_{hashlib.sha256((title + summary + str(published_time)).encode('utf-8')).hexdigest()}"
                    logger.critical(f"[{__name__}] URL artikel kosong bahkan setelah join untuk '{title[:50]}...'. Menggunakan ID fallback hash.")

                if published_time.timestamp() < start_ts_filter:
                    logger.debug(f"Berita Myfxbook '{title[:50]}...' dilewatkan karena terlalu lama.")
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
                # --- MODIFIKASI BERAKHIR DI SINI ---

        except requests.exceptions.RequestException as e:
            logger.error(f"Terjadi kesalahan saat scraping Myfxbook News: {e}. Tidak akan menulis ke cache.")
            return []
        except Exception as e:
            logger.error(f"Terjadi kesalahan tak terduga saat scraping Myfxbook News: {e}", exc_info=True)
            return []

    # --- 3. Jika Scraping Berhasil, Simpan ke Cache ---
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

            with open(cache_file_path, 'w', encoding='utf-8') as f:
                json.dump(cache_data_to_save, f, indent=4)
            logger.info(f"Data berita Myfxbook berhasil disimpan ke cache: {cache_file_path}")
        except Exception as e:
            logger.error(f"Gagal menulis data berita ke cache: {e}")
        
    logger.info(f"Selesai scraping Myfxbook News. Ditemukan {len(news_items_list)} berita.")
    return news_items_list
# news_scalper_cnbc.py

import logging
from datetime import datetime, timedelta, timezone
import requests
from bs4 import BeautifulSoup
import re
import time
import os
import json # Pastikan ini diimpor

logger = logging.getLogger(__name__)

# Pengaturan Caching
CNBC_CACHE_FILE = "cnbc_news_cache.json"
CACHE_TTL_SECONDS = 3600 # 1 jam TTL untuk berita CNBC (bisa disesuaikan)

def scrape_cnbc_news(query_topics=None, days_past=1):
    """
    Melakukan scraping berita dari CNBC.com menggunakan requests dan BeautifulSoup.
    Fokus pada bagian halaman utama yang berisi berita unggulan dan sekunder.
    Menggunakan mekanisme caching.
    """
    news_items_list = []
    
    base_urls = [
        "https://www.cnbc.com/finance/",
        "https://www.cnbc.com/economy/",
        "https://www.cnbc.com/world-markets/",
        "https://www.cnbc.com/business/"
    ]
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
        'Referer': 'https://www.cnbc.com/'
    }

    keywords = [
        "gold", "emas", "forex", "dollar", "usd", "federal reserve", "fed",
        "interest rates", "suku bunga", "inflation", "inflasi",
        "war", "geopolitics", "konflik", "ukraine", "russia", "middle east",
        "bank sentral", "central bank", "market", "economy", "recession", "stimulus",
        "commodities", "yields", "bonds", "gdp", "jobs"
    ]

    # --- Coba Baca dari Cache Terlebih Dahulu ---
    if os.path.exists(CNBC_CACHE_FILE):
        try:
            with open(CNBC_CACHE_FILE, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)
            
            cache_timestamp = datetime.fromisoformat(cache_data['timestamp']).replace(tzinfo=timezone.utc)
            
            if (datetime.now(timezone.utc) - cache_timestamp).total_seconds() < CACHE_TTL_SECONDS:
                logger.info(f"Menggunakan data berita dari cache (segar, {len(cache_data['news'])} berita) untuk CNBC.")
                for item in cache_data['news']:
                    if item.get('published_time_utc') and isinstance(item['published_time_utc'], str):
                        item['published_time_utc'] = datetime.fromisoformat(item['published_time_utc']).replace(tzinfo=timezone.utc)
                return cache_data['news']
            else:
                logger.info(f"Cache CNBC kedaluwarsa ({CACHE_TTL_SECONDS / 3600} jam). Akan men-scrape ulang.")
        except Exception as e:
            logger.warning(f"Gagal membaca atau memvalidasi cache CNBC: {e}. Akan men-scrape ulang.")


    logger.info(f"[{__name__}] Mulai scraping berita dari CNBC untuk topik: {query_topics if query_topics else 'Semua'}")

    try:
        now_utc = datetime.now(timezone.utc)
        start_ts_filter = (now_utc - timedelta(days=days_past)).timestamp()

        for url in base_urls:
            logger.debug(f"[{__name__}] Mengambil berita dari CNBC URL: {url}")
            try:
                response = requests.get(url, headers=headers, timeout=15)
                response.raise_for_status()
                time.sleep(1)
            except requests.exceptions.RequestException as e:
                logger.error(f"[{__name__}] Gagal mengakses halaman CNBC ({url}): {e}")
                continue
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # --- Selektor Berita CNBC yang Ditargetkan ---
            # 1. Berita Unggulan (Featured News Hero)
            featured_headlines = soup.find_all('h2', class_='FeaturedCard-packagedCardTitle')
            for headline_h2 in featured_headlines:
                link_tag = headline_h2.find('a')
                if not link_tag: continue
                title = link_tag.text.strip()
                article_url_raw = link_tag.get('href')
                
                # JAMINAN title, URL dan waktu non-NULL
                if not title or not article_url_raw:
                    logger.debug(f"[{__name__}] Melewatkan item unggulan tanpa judul/URL valid: {headline_h2.prettify()[:100]}")
                    continue

                # --- MODIFIKASI DIMULAI DI SINI: Validasi dan Penugasan ID ---
                # Pastikan URL adalah absolut dan valid
                final_article_url = requests.compat.urljoin(url, article_url_raw) # Menggunakan url base saat ini
                if not final_article_url.startswith('http'):
                    logger.warning(f"[{__name__}] Melewatkan link dengan URL tidak absolut setelah join: {final_article_url}. URL mentah: {article_url_raw}.")
                    continue
                
                # Jika URL valid, gunakan sebagai ID
                news_id = final_article_url
                # --- MODIFIKASI BERAKHIR DI SINI ---

                summary = link_tag.get('title', '')
                published_time = None # Inisialisasi sebagai None

                # Fallback untuk waktu publikasi
                if published_time is None:
                    published_time = now_utc # Fallback terakhir jika tidak bisa parse waktu

                if published_time.timestamp() < start_ts_filter:
                    logger.debug(f"Berita CNBC (Featured) '{title[:50]}...' dilewatkan karena terlalu lama.")
                    continue

                article_text = title + " " + summary
                
                is_relevant = False
                for kw in keywords:
                    if kw.lower() in article_text.lower():
                        is_relevant = True
                        break
                if query_topics and not any(topic.lower() in article_text.lower() for topic in query_topics):
                    is_relevant = False
                
                if is_relevant:
                    news_items_list.append({
                        "id": news_id, # <--- Gunakan ID yang sudah divalidasi
                        "source": "CNBC (Featured)",
                        "name": title,
                        "summary": summary,
                        "url": final_article_url, # Pastikan ini juga final_article_url
                        "published_time_utc": published_time, 
                        "relevance_score": 1
                    })
            
            # 2. Berita Sekunder/Daftar
            secondary_headlines = soup.find_all('div', class_='SecondaryCard-headline')
            for headline_div in secondary_headlines:
                link_tag = headline_div.find('a')
                if not link_tag: continue
                title = link_tag.text.strip()
                article_url_raw = link_tag.get('href')

                if not title or not article_url_raw:
                    logger.debug(f"[{__name__}] Melewatkan item sekunder tanpa judul/URL valid: {headline_div.prettify()[:100]}")
                    continue
                
                # --- MODIFIKASI DIMULAI DI SINI: Validasi dan Penugasan ID ---
                final_article_url = requests.compat.urljoin(url, article_url_raw)
                if not final_article_url.startswith('http'):
                    logger.warning(f"[{__name__}] Melewatkan link dengan URL tidak absolut setelah join: {final_article_url}. URL mentah: {article_url_raw}.")
                    continue
                news_id = final_article_url
                # --- MODIFIKASI BERAKHIR DI SINI ---

                summary = link_tag.get('title', '')
                published_time = None # Inisialisasi sebagai None

                # Fallback untuk waktu publikasi
                if published_time is None:
                    published_time = now_utc # Fallback terakhir

                if published_time.timestamp() < start_ts_filter:
                    logger.debug(f"Berita CNBC (Secondary) '{title[:50]}...' dilewatkan karena terlalu lama.")
                    continue

                article_text = title + " " + summary
                
                is_relevant = False
                for kw in keywords:
                    if kw.lower() in article_text.lower():
                        is_relevant = True
                        break
                if query_topics and not any(topic.lower() in article_text.lower() for topic in query_topics):
                    is_relevant = False
                
                if is_relevant:
                    news_items_list.append({
                        "id": news_id, # <--- Gunakan ID yang sudah divalidasi
                        "source": "CNBC (Secondary)",
                        "name": title,
                        "summary": summary,
                        "url": final_article_url, # Pastikan ini juga final_article_url
                        "published_time_utc": published_time, 
                        "relevance_score": 1
                    })
            
            # Tambahan: Pola lain yang umum di CNBC untuk daftar berita (River-item, Card-outerContent)
            other_article_containers = soup.find_all('div', class_=re.compile(r'River-item|Card-outerContent|ArticleListItem-container'))
            for item_container in other_article_containers:
                title_tag = item_container.find('a', class_=re.compile(r'Card-titleLink|River-titleLink|ArticleListItem-titleLink'))
                if not title_tag: continue

                title = title_tag.text.strip()
                article_url_raw = title_tag.get('href')
                if not title or not article_url_raw:
                    logger.debug(f"[{__name__}] Melewatkan item umum tanpa judul/URL valid: {item_container.prettify()[:100]}")
                    continue
                
                # --- MODIFIKASI DIMULAI DI SINI: Validasi dan Penugasan ID ---
                final_article_url = requests.compat.urljoin(url, article_url_raw)
                if not final_article_url.startswith('http'):
                    logger.warning(f"[{__name__}] Melewatkan link dengan URL tidak absolut setelah join: {final_article_url}. URL mentah: {article_url_raw}.")
                    continue
                news_id = final_article_url
                # --- MODIFIKASI BERAKHIR DI SINI ---

                summary_tag = item_container.find('p', class_=re.compile(r'Card-summary|River-description|ArticleListItem-description'))
                summary = summary_tag.text.strip() if summary_tag else ""
                
                published_time = None
                time_tag = item_container.find('time')
                if time_tag and time_tag.get('datetime'):
                    try:
                        published_time = datetime.fromisoformat(time_tag.get('datetime').replace('Z', '+00:00'))
                    except ValueError:
                        pass
                
                if published_time is None:
                    published_time = now_utc # Fallback terakhir

                if published_time.timestamp() < start_ts_filter:
                    logger.debug(f"Berita CNBC (Other) '{title[:50]}...' dilewatkan karena terlalu lama.")
                    continue

                article_text = title + " " + summary

                is_relevant = False
                for kw in keywords:
                    if kw.lower() in article_text.lower():
                        is_relevant = True
                        break
                if query_topics and not any(topic.lower() in article_text.lower() for topic in query_topics):
                    is_relevant = False

                if is_relevant:
                    news_items_list.append({
                        "id": news_id, # <--- Gunakan ID yang sudah divalidasi
                        "source": "CNBC (Other)",
                        "name": title,
                        "summary": summary,
                        "url": final_article_url, 
                        "published_time_utc": published_time, 
                        "relevance_score": 1
                    })
            
            # --- MODIFIKASI DIMULAI DI SINI: Penanganan Fallback Links yang lebih robust ---
            # Fallback ini biasanya tidak memiliki summary, jadi pastikan summary kosong.
            # Pastikan juga URL absolut dan unik.
            fallback_links = soup.find_all('a', href=re.compile(r'/202\d/\d{2}/\d{2}/'))
            if not news_items_list and fallback_links: # Hanya gunakan fallback jika tidak ada berita lain yang ditemukan
                for link_tag in fallback_links:
                    title = link_tag.text.strip()
                    article_url_raw = link_tag.get('href')
                    
                    if not title or not article_url_raw:
                        logger.debug(f"[{__name__}] Melewatkan fallback link tanpa judul/URL: {link_tag.prettify()[:100]}")
                        continue

                    final_article_url = requests.compat.urljoin(url, article_url_raw)
                    if not final_article_url.startswith('http'):
                        logger.warning(f"[{__name__}] Melewatkan fallback link dengan URL tidak absolut setelah join: {final_article_url}. URL mentah: {article_url_raw}.")
                        continue
                    news_id = final_article_url

                    article_text = title
                    is_relevant = False
                    for kw in keywords:
                        if kw.lower() in article_text.lower():
                            is_relevant = True
                            break
                    if query_topics and not any(topic.lower() in article_text.lower() for topic in query_topics):
                        is_relevant = False

                    if is_relevant:
                        news_items_list.append({
                            "id": news_id, # <--- Gunakan ID yang sudah divalidasi
                            "source": "CNBC (Fallback)",
                            "title": title,
                            "summary": "", # Tidak ada ringkasan dari fallback
                            "url": final_article_url, 
                            "published_time_utc": now_utc, # Asumsi waktu saat ini jika tidak ada info waktu
                            "relevance_score": 1
                        })
            # --- MODIFIKASI BERAKHIR DI SINI ---
                
    except requests.exceptions.RequestException as e:
        logger.error(f"Gagal mengakses halaman CNBC (requests): {e}")
    except Exception as e:
        logger.error(f"Terjadi kesalahan tak terduga saat scraping CNBC News: {e}", exc_info=True)
        
    logger.info(f"[{__name__}] Selesai scraping CNBC News. Ditemukan {len(news_items_list)} berita relevan.")

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

            with open(CNBC_CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(cache_data_to_save, f, indent=4)
            logger.info(f"Data berita CNBC berhasil disimpan ke cache: {CNBC_CACHE_FILE}")
        except Exception as e:
            logger.error(f"Gagal menulis data berita CNBC ke cache: {e}")

    return news_items_list
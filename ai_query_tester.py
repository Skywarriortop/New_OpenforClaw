# ai_query_tester.py

import logging
import logging.handlers
import sys
import os
import time
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import json # Tambahkan import json

# Pastikan path ini benar jika file config.py tidak langsung di root
# Tambahkan path induk ke sys.path jika ai_query_tester.py berada di subdirektori
# Contoh: jika ai_query_tester.py ada di folder 'tests/', maka:
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# Untuk saat ini, asumsikan semua file di direktori yang sama.

import config
import database_manager
import mt5_connector
import notification_service
import ai_database_agent
import trading_strategy_agent
# --- Konfigurasi Logging untuk Tester ---
# Atur logger agar tidak terlalu verbose di konsol, tapi tetap detail di file log.
logger = logging.getLogger()
# Hapus handler yang mungkin sudah ditambahkan oleh modul lain (jika ai_query_tester dijalankan secara independen)
for handler in logger.handlers[:]:
    logger.removeHandler(handler)
logger.propagate = False # Penting untuk mencegah duplikasi log jika root logger sudah dikonfigurasi

# Handler konsol: Hanya INFO ke atas
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)

# Handler file: DEBUG ke atas
LOG_FILE_PATH = "ai_query_tester.log"
file_handler = logging.handlers.RotatingFileHandler(
    LOG_FILE_PATH,
    maxBytes=10 * 1024 * 1024, # 10 MB
    backupCount=3,
    encoding='utf-8'
)
file_handler.setLevel(logging.DEBUG)
file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)

logger.setLevel(logging.DEBUG) # Level default untuk logger ini
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
logging.getLogger('sqlalchemy.pool').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('requests').setLevel(logging.WARNING)
logging.getLogger('ai_database_agent').setLevel(logging.DEBUG)
logging.getLogger('mt5_connector').setLevel(logging.DEBUG) # Tambahkan ini untuk melihat log dari MT5 connector


# Inisialisasi konfigurasi aplikasi
my_app_config = config.Config()
my_app_config.validate_config()

def initialize_tester_components():
    logger.info("Memulai inisialisasi komponen tester...")
    try:
        db_url_to_use = my_app_config.Database.URL
        if db_url_to_use is None:
             raise Exception("DATABASE_URL tidak dapat diakses atau None. Harap setel di file .env Anda.")

        logger.info("Menginisialisasi koneksi database...")
        if not database_manager.init_db_connection(db_url_to_use):
            raise Exception("Gagal menginisialisasi koneksi database.")
        
        logger.info("Menginisialisasi database writer thread...")
        if not database_manager.init_db_writer():
            logger.warning("Database writer thread mungkin sudah berjalan atau gagal memulai.")

        logger.info("Menginisialisasi klien OpenAI/LM Studio...")
        ai_database_agent.init_openai_client(my_app_config.APIKeys.OPENAI_API_KEY)

        logger.info("Menginisialisasi koneksi MT5 (opsional untuk data) ...")
        if mt5_connector.initialize_mt5():
            logger.info("Koneksi MT5 berhasil. Akan mencoba memperbarui data perdagangan MT5.")
            trading_symbol = my_app_config.TRADING_SYMBOL
            try:
                database_manager.update_mt5_trade_data_periodically(trading_symbol)
                logger.info(f"Data perdagangan MT5 untuk {trading_symbol} berhasil diperbarui (satu kali).")
            except Exception as e:
                logger.error(f"Gagal memperbarui data perdagangan MT5: {e}", exc_info=True)
            finally:
                mt5_connector.shutdown_mt5()
        else:
            logger.warning("Koneksi MT5 gagal diinisialisasi. Database mungkin tidak memiliki data MT5 real-time.")
            logger.warning("Pastikan MT5.exe berjalan dan login dengan kredensial yang benar.")

        logger.info("Inisialisasi komponen tester selesai.")
        return True
    except Exception as e:
        logger.critical(f"Gagal menginisialisasi tester: {e}", exc_info=True)
        return False

def run_ai_query_tests():
    logger.info("Memulai tes kueri AI database... [Fokus pada pengujian otak trading baru]")

    # Mengurangi pertanyaan lain agar fokus pada kueri LLM yang dimodifikasi
    test_questions = [
        # "Berapa saldo akun MT5 saya saat ini?",
        # "Tampilkan 3 posisi buy aktif XAUUSD yang paling baru.",
        # "Apa 2 berita ekonomi terbaru tentang USD dengan dampak High?",
        # "Berapa harga penutupan candle D1 terbaru untuk XAUUSD?",
        # "Berapa rata-rata harga pembukaan harian untuk XAUUSD di bulan ini?",
        # "Berikan saya 5 FVG yang belum terisi di timeframe M15 untuk XAUUSD.",
        # "Daftar semua Order Block Bullish di timeframe H1 untuk XAUUSD yang belum dimitigasi.",
        # "Tunjukkan 5 level Support Resistance terkuat di H4 untuk XAUUSD.",
        "Apa rekomendasi trading saat ini untuk XAUUSD di timeframe H1 berdasarkan analisis pasar?",
    ]

    for i, question in enumerate(test_questions):
        logger.info(f"\n--- Tes Kueri {i+1}/{len(test_questions)} ---")
        logger.info(f"Pertanyaan: '{question}'")
        
        start_time = time.time()
        
        response = {}

        if question == "Apa rekomendasi trading saat ini untuk XAUUSD di timeframe H1 berdasarkan analisis pasar?":
            logger.info("Memicu analisis dan proposal trading...")
            analysis_result = trading_strategy_agent.analyze_and_propose_trade(symbol="XAUUSD", timeframe="H1")
            
            if analysis_result["status"] == "success":
                response = {
                    "status": "success",
                    "question": question,
                    "sql_query": "N/A (Analisis Gabungan)", # Ini bukan kueri SQL langsung
                    "raw_results": analysis_result, # Data lengkap dari trading_strategy_agent
                    "interpreted_results": analysis_result.get("ai_summary", "Tidak ada ringkasan AI tersedia."),
                    "recommendation": analysis_result.get("recommendation"),
                    "proposed_entry": analysis_result.get("proposed_entry"),
                    "proposed_sl": analysis_result.get("proposed_sl"),
                    "proposed_tp": analysis_result.get("proposed_tp"),
                    "ai_reasoning": analysis_result.get("ai_reasoning", "Tidak ada penalaran AI tersedia.")
                }
            else:
                # Perbaikan di sini: Ambil pesan dari 'ai_reasoning' atau fallback ke pesan generik
                error_message_from_analysis = analysis_result.get("ai_reasoning", "Analisis LLM gagal atau tidak dapat divalidasi.")
                response = {
                    "status": "error",
                    "message": error_message_from_analysis, # Mengambil pesan dari kunci yang benar
                    "raw_results": analysis_result # Jika gagal, tetap sertakan raw_results
                }
        else:
            # Fungsi process_user_data_query sudah menghasilkan format respons yang berbeda
            # Jadi, kita tidak perlu menyesuaikannya di sini, biarkan saja jika ada tes lain
            response = ai_database_agent.process_user_data_query(question)
        
        end_time = time.time()
        duration = end_time - start_time
        
        if response.get("status") == "success":
            logger.info(f"Status: BERHASIL ✅")
            logger.info(f"Rekomendasi Trading: {response.get('recommendation', 'N/A')}")
            # Pastikan ini menampilkan nilai float yang diformat
            logger.info(f"Entry: {response.get('proposed_entry', 'N/A')}, SL: {response.get('proposed_sl', 'N/A')}, TP: {response.get('proposed_tp', 'N/A')}")
            logger.info(f"Ringkasan AI:\n{response.get('interpreted_results')}")
            logger.info(f"Penalaran AI:\n{response.get('ai_reasoning')}")
            # Gunakan default=str untuk menangani objek Decimal/datetime di json.dumps
            logger.debug(f"Hasil Lengkap (raw_results):\n{json.dumps(response.get('raw_results'), indent=2, default=str)}")
        else:
            logger.error(f"Status: GAGAL ❌")
            logger.error(f"Pesan Error: {response.get('message')}") # Ini sekarang akan mengambil dari 'message' yang diperbaiki
            if response.get('sql_query'): # Kueri SQL hanya ada jika error dari process_user_data_query
                logger.error(f"Kueri SQL yang gagal: {response.get('sql_query')}")
            logger.debug(f"Hasil Lengkap (raw_results) saat GAGAL:\n{json.dumps(response.get('raw_results'), indent=2, default=str)}") # Tambahkan logging detail untuk kasus gagal
            
        logger.info(f"Durasi proses: {duration:.2f} detik.")
        time.sleep(2)

    logger.info("Tes kueri AI database selesai.")

def cleanup_tester_components():
    logger.info("Memulai cleanup komponen tester...")
    try:
        database_manager.stop_db_writer()
        logger.info("Menunggu semua tugas database writer selesai saat cleanup...")
        
        max_wait_time = 30
        start_wait_time = time.time()
        while not database_manager._db_write_queue.empty() and (time.time() - start_wait_time) < max_wait_time:
            time.sleep(0.1)
        
        if not database_manager._db_write_queue.empty():
            logger.warning("Database write queue tidak kosong saat shutdown tester. Mungkin ada tugas yang tidak sempat diproses.")
        
        logger.info("Cleanup komponen tester selesai.")
    except Exception as e:
        logger.error(f"Error saat cleanup tester: {e}", exc_info=True)


if __name__ == '__main__':
    if initialize_tester_components():
        run_ai_query_tests()
    
    cleanup_tester_components()
    logger.info("ai_query_tester.py selesai dijalankan.")
    sys.exit(0)
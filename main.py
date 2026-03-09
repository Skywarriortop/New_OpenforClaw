# main.py

import logging
import sys
import os
import threading
import time
from datetime import datetime, timezone, timedelta
import pandas as pd
from api_routes import DashboardLogHandler, create_app # create_app juga diimpor di sini
from decimal import Decimal # <--- TAMBAHKAN BARIS INI
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv
basedir = os.path.abspath(os.path.dirname(__file__))

# Impor modul lain yang dibutuhkan
import config # Ini mengimpor modul config.py
import database_manager #
import notification_service #
import mt5_connector #
import scheduler # Diperlukan untuk mengakses _feature_backfill_completed
import market_data_processor #
import fundamental_data_service #
import ai_analyzer #
import ai_consensus_manager #
import api_routes # <--- Pastikan ini diimpor untuk mengakses fungsi emitter
import detector_monitoring # <--- TAMBAHKAN INI

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

load_dotenv(os.path.join(basedir, '.env'))


# PENTING: INSTANSIASI OBJEK CONFIG SEGERA SETELAH IMPOR MODULNYA
# Ini memastikan objek `my_app_config` (instance dari kelas Config) tersedia
# segera setelah modul `config` diimpor dan kode di dalamnya dieksekusi.
my_app_config = config.Config()
my_app_config.validate_config()

# Variabel global untuk melacak status inisialisasi DB
_db_initialized_event = threading.Event()

# Pastikan output konsol mendukung UTF-8
if sys.stdout.encoding != 'utf-8':
    sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf-8', buffering=1)
if sys.stderr.encoding != 'utf-s':
    sys.stderr = open(sys.stderr.fileno(), mode='w', encoding='utf-8', buffering=1)


# --- Konfigurasi Logging yang Disempurnakan ---
logger = logging.getLogger()
for handler in logger.handlers[:]:
    logger.removeHandler(handler)
logger.propagate = False

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
# --- KOREKSI TYPO: Tambahkan tanda kurung () pada logging.Formatter ---
console_formatter = logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)

LOG_FILE_PATH = "bot_trading.log"
MAX_LOG_SIZE_MB = 100
BACKUP_COUNT = 5
max_bytes = MAX_LOG_SIZE_MB * 1024 * 1024
file_handler = RotatingFileHandler(
    LOG_FILE_PATH,
    maxBytes=max_bytes,
    backupCount=BACKUP_COUNT,
    encoding='utf-8'
)
file_handler.setLevel(logging.DEBUG)
file_formatter = logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)

dashboard_handler = DashboardLogHandler()
dashboard_handler.setLevel(logging.INFO)
dashboard_formatter = logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')
dashboard_handler.setFormatter(dashboard_formatter)
logger.addHandler(dashboard_handler)

logger.setLevel(logging.DEBUG)
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
logging.getLogger('sqlalchemy.pool').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('selenium').setLevel(logging.WARNING)
logging.getLogger('webdriver_manager').setLevel(logging.WARNING)
logging.getLogger('werkzeug').setLevel(logging.WARNING)
logging.getLogger('httpcore').setLevel(logging.WARNING)
logging.getLogger('asyncio').setLevel(logging.WARNING)
logging.getLogger('rule_based_signal_generator').setLevel(logging.DEBUG)
logging.getLogger('market_data_processor').setLevel(logging.DEBUG)
logging.getLogger('database_manager').setLevel(logging.DEBUG)
logging.getLogger('notification_service').setLevel(logging.DEBUG)
logging.getLogger('telegram.ext').setLevel(logging.DEBUG)
logging.getLogger('httpx').setLevel(logging.DEBUG)
logging.getLogger('telegram').setLevel(logging.DEBUG)
logging.getLogger('ai_consensus_manager').setLevel(logging.DEBUG)
logging.getLogger('detector_monitoring').setLevel(logging.DEBUG) # <--- TAMBAHKAN INI
logging.getLogger('mt5_connector').setLevel(logging.DEBUG)
# --- FUNGSI UTAMA UNTUK INISIALISASI APLIKASI ---

def initialize_application_components():
    logger.info("Memulai proses inisialisasi aplikasi di background thread...")
    try:
        # AKSES KONFIGURASI MELALUI INSTANCE `my_app_config`
        db_url_to_use = my_app_config.Database.URL
        if db_url_to_use is None:
             raise Exception("DATABASE_URL tidak dapat diakses atau None. Harap setel di file .env Anda.")

        # 1. Start Notification Service
        if not notification_service.start_notification_service():
            raise Exception("Layanan notifikasi Telegram gagal dimulai.")

        # 2. Inisialisasi MT5
        logger.info("Menginisialisasi koneksi MetaTrader 5...")
        # Akses path MT5 dari objek `my_app_config`
        if not mt5_connector.initialize_mt5():
            raise Exception(f"Koneksi MT5 gagal diinisialisasi. Error: {mt5_connector.mt5.last_error()}")
        mt5_connector.print_mt5_status()

        # --- MULAI MODIFIKASI LENGKAP UNTUK DETEKSI DATA BROKER DAN PENGATURAN FLAG ---
        logger.info("Memverifikasi ketersediaan data volume spesifik dari broker...")

        # Inisialisasi flag di config.py ke False (default) sebelum deteksi
        # Penting: Mengakses my_app_config.Trading karena ini adalah instance dari Config.Trading
        my_app_config.Trading.HAS_REAL_VOLUME_DATA_FROM_BROKER = False
        my_app_config.Trading.HAS_TICK_VOLUME_DATA_FROM_BROKER = False
        my_app_config.Trading.HAS_REALTIME_TICK_VOLUME_FROM_BROKER = False

        # 1. Ambil Sampel Candle D1
        sample_candles_d1 = mt5_connector.get_historical_candles(
            my_app_config.TRADING_SYMBOL, "D1", num_candles=10
        )
        if sample_candles_d1 and len(sample_candles_d1) > 0:
            first_candle_d1 = sample_candles_d1[0]
            # Cek real_volume untuk D1
            if 'real_volume' in first_candle_d1 and first_candle_d1['real_volume'] is not None and first_candle_d1['real_volume'] > Decimal('0.0'):
                my_app_config.Trading.HAS_REAL_VOLUME_DATA_FROM_BROKER = True
                logger.info(f"  Real Volume terdeteksi di D1 (contoh: {float(first_candle_d1['real_volume']):.2f}).")
            # Cek tick_volume untuk D1
            if 'tick_volume' in first_candle_d1 and first_candle_d1['tick_volume'] is not None and first_candle_d1['tick_volume'] > Decimal('0.0'):
                my_app_config.Trading.HAS_TICK_VOLUME_DATA_FROM_BROKER = True
                logger.info(f"  Tick Volume terdeteksi di D1 (contoh: {float(first_candle_d1['tick_volume']):.2f}).")
        else:
            logger.warning("  Tidak ada candle D1 sampel yang cukup untuk deteksi volume.")
        
        # 2. Ambil Sampel Candle M5 (untuk cross-check)
        sample_candles_m5 = mt5_connector.get_historical_candles(
            my_app_config.TRADING_SYMBOL, "M5", num_candles=10
        )
        if sample_candles_m5 and len(sample_candles_m5) > 0:
            first_candle_m5 = sample_candles_m5[0]
            # Cek real_volume untuk M5 (set True jika terdeteksi di M5)
            if 'real_volume' in first_candle_m5 and first_candle_m5['real_volume'] is not None and first_candle_m5['real_volume'] > Decimal('0.0'):
                my_app_config.Trading.HAS_REAL_VOLUME_DATA_FROM_BROKER = True 
                logger.info(f"  Real Volume terdeteksi di M5 (contoh: {float(first_candle_m5['real_volume']):.2f}).")
            # Cek tick_volume untuk M5 (set True jika terdeteksi di M5)
            if 'tick_volume' in first_candle_m5 and first_candle_m5['tick_volume'] is not None and first_candle_m5['tick_volume'] > Decimal('0.0'):
                my_app_config.Trading.HAS_TICK_VOLUME_DATA_FROM_BROKER = True
                logger.info(f"  Tick Volume terdeteksi di M5 (contoh: {float(first_candle_m5['tick_volume']):.2f}).")
        else:
            logger.warning("  Tidak ada candle M5 sampel yang cukup untuk deteksi volume.")


        # 3. Ambil Sampel Tick Real-Time
        sample_tick = mt5_connector.get_current_tick_info(my_app_config.TRADING_SYMBOL)
        if sample_tick:
            # Periksa 'volume_real' atau 'volume' pada tick real-time
            if ('volume_real' in sample_tick and sample_tick['volume_real'] is not None and sample_tick['volume_real'] > Decimal('0.0')) or \
               ('volume' in sample_tick and sample_tick['volume'] is not None and sample_tick['volume'] > Decimal('0.0')):
                my_app_config.Trading.HAS_REALTIME_TICK_VOLUME_FROM_BROKER = True
                logger.info(f"  Realtime Tick Volume terdeteksi (contoh: {float(sample_tick.get('volume_real') or sample_tick.get('volume')):.2f}).")
            else:
                logger.warning("  Realtime Tick Volume TIDAK terdeteksi atau bernilai nol.")
        else:
            logger.warning("  Gagal mendapatkan sampel tick real-time untuk deteksi volume.")
        
        logger.info(f"Status Data Volume Broker {my_app_config.TRADING_SYMBOL}:")
        logger.info(f"  Real Volume (Candle Hist.): {my_app_config.Trading.HAS_REAL_VOLUME_DATA_FROM_BROKER}")
        logger.info(f"  Tick Volume (Candle Hist.): {my_app_config.Trading.HAS_TICK_VOLUME_DATA_FROM_BROKER}")
        logger.info(f"  Realtime Tick Volume: {my_app_config.Trading.HAS_REALTIME_TICK_VOLUME_FROM_BROKER}")

        # --- Mulai Penyesuaian Konfigurasi Berdasarkan Deteksi ---
        if not my_app_config.Trading.HAS_REAL_VOLUME_DATA_FROM_BROKER:
            # Jika real_volume tidak tersedia, nonaktifkan/sesuaikan fitur yang sangat bergantung padanya
            my_app_config.MarketData.ENABLE_VOLUME_PROFILE_DETECTION = False
            
            # Dan setel faktor volume di RuleBasedStrategy menjadi 0.0 (jika kita tidak ingin tick_volume memengaruhinya)
            # ATAU: Anda bisa memutuskan untuk menggunakan tick_volume sebagai pengganti dengan multiplier tertentu.
            # Untuk saat ini, kita setel ke nol karena real_volume yang ideal tidak ada.
            my_app_config.RuleBasedStrategy.OB_VOLUME_FACTOR_MULTIPLIER = Decimal('0.0')
            my_app_config.RuleBasedStrategy.FVG_VOLUME_FACTOR_FOR_STRENGTH = Decimal('0.0')
            
            logger.warning("Deteksi Volume Profile dan faktor volume di OB/FVG dinonaktifkan/disetel ke nol: Real Volume data tidak tersedia dari broker.")
            notification_service.send_telegram_message(
                f"⚠️ *Peringatan Inisialisasi:*\nFitur Volume Profile & detektor berbasis volume dinonaktifkan: Real Volume data tidak tersedia dari broker ({my_app_config.TRADING_SYMBOL})."
            )
        else:
            logger.info("Real Volume data tersedia. Fitur berbasis volume diaktifkan penuh.")

        if not my_app_config.Trading.HAS_TICK_VOLUME_DATA_FROM_BROKER:
            logger.warning("Tick Volume dari candle historis juga TIDAK tersedia. Detektor yang mengandalkan volume (bahkan tick_volume) mungkin tidak akan akurat sama sekali.")
            # Ini adalah skenario yang lebih ekstrem, di mana tidak ada bentuk volume dari candle.
            # Anda mungkin ingin menonaktifkan lebih banyak detektor di sini jika mereka tidak bisa berfungsi tanpa tick_volume.
            notification_service.send_telegram_message(
                f"⚠️ *Peringatan Inisialisasi:*\nFitur Volume Profile & detektor berbasis volume dinonaktifkan: Real Volume data tidak tersedia dari broker ({my_app_config.TRADING_SYMBOL})."
            )
        
        # --- AKHIR MODIFIKASI LENGKAP UNTUK DETEKSI DATA BROKER DAN PENGATURAN FLAG ---


        # 3. Inisialisasi Database (PostgreSQL)
        logger.info("Menginisialisasi koneksi database...")
        if not database_manager.init_db_connection(db_url_to_use):
            raise Exception("Gagal menginisialisasi koneksi database.")

        # 4. Buat Tabel Database jika belum ada
        logger.info("Membuat/memverifikasi tabel database...")
        if not database_manager.create_all_tables():
            raise Exception("Gagal membuat atau memeriksa tabel database.")

        # --- PERUBAHAN: Memulai Database Writer Thread di sini ---
        # 5. Inisialisasi Database Writer Thread
        logger.info("Menginisialisasi database writer thread...")
        if not database_manager.init_db_writer():
            raise Exception("Gagal menginisialisasi database writer thread.")
        # --- AKHIR PERUBAHAN ---

        # 6. Pastikan metadata sesi (Asia, Eropa, New York) ada (sekarang langkah 6)
        logger.info("Memastikan entri metadata sesi pasar ada...")
        if not database_manager.ensure_session_metadata_exists():
            raise Exception("Gagal memastikan metadata sesi pasar ada.")

        # 7. Inisialisasi AI Client (sekarang langkah 7)
        logger.info("Menginisialisasi klien OpenAI...")
        ai_analyzer.init_openai_client(my_app_config.APIKeys.OPENAI_API_KEY)

        # --- PEMBARUAN DATA AWAL & BACKFILL FITUR (Synchronous) ---
        # Ini akan memblokir startup aplikasi sampai semua data historis dan fitur terdeteksi.
        logger.info("Memulai pembaruan data awal (synchronous)...")

        logger.info("Update historical candles awal...")
        market_data_processor.update_all_historical_candles()

        logger.info("Update daily open prices awal...")
        market_data_processor.update_daily_open_prices_logic(my_app_config.TRADING_SYMBOL)

        logger.info("Memberikan jeda singkat agar MT5 stabil sebelum mendapatkan tick...")
        time.sleep(10)

        logger.info("Update session data awal...")
        current_mt5_datetime_for_session = None
        now_minus_24h = datetime.now(timezone.utc) - timedelta(hours=24)

        # Prioritaskan mendapatkan waktu dari tick terbaru di DB (jika ada)
        latest_db_tick = database_manager.get_latest_price_tick(my_app_config.TRADING_SYMBOL)
        if latest_db_tick and latest_db_tick.get('time_utc_datetime') is not None:
            try:
                temp_dt = latest_db_tick['time_utc_datetime']
                if temp_dt > now_minus_24h:
                    current_mt5_datetime_for_session = temp_dt
                    logger.info(f"Menggunakan waktu dari tick DB terbaru untuk data sesi awal: {current_mt5_datetime_for_session.isoformat()}")
                else:
                    logger.warning(f"Waktu tick DB terlalu lama ({temp_dt.isoformat()}). Mencoba dari MT5 langsung.")
            except ValueError:
                logger.warning("Waktu tick DB terbaru tidak dalam format ISO yang valid. Mencoba dari MT5 langsung.")

        # Jika belum berhasil (atau tick DB terlalu lama), coba dapatkan dari MT5 langsung dengan retry
        if current_mt5_datetime_for_session is None:
            max_tick_retries_mt5 = 10
            for attempt in range(max_tick_retries_mt5):
                current_mt5_tick_info = mt5_connector.get_current_tick_info(my_app_config.TRADING_SYMBOL)
                if current_mt5_tick_info and \
                   current_mt5_tick_info.get('time') and \
                   current_mt5_tick_info.get('bid') and current_mt5_tick_info['bid'] > Decimal('0.0'):
                    
                    temp_dt = current_mt5_tick_info['time']

                    if temp_dt > now_minus_24h:
                        current_mt5_datetime_for_session = temp_dt
                        logger.info(f"Berhasil mengambil waktu tick dari MT5 langsung untuk data sesi awal: {current_mt5_datetime_for_session.isoformat()}")
                        break
                    else:
                        logger.warning(f"Tick dari MT5 memiliki waktu yang tidak masuk akal ({temp_dt.isoformat()}). Percobaan {attempt+1}/{max_tick_retries_mt5}. Mencoba lagi dalam 1 detik...")
                else:
                    logger.warning(f"Gagal mengambil tick dari MT5 langsung (percobaan {attempt+1}/{max_tick_retries_mt5}). Tick info: {current_mt5_tick_info}. Mencoba lagi dalam 1 detik...")
                time.sleep(1)
                
        # Jika masih belum ada waktu yang valid setelah semua percobaan, gunakan waktu sistem sebagai fallback terakhir
        if current_mt5_datetime_for_session is None:
            current_mt5_datetime_for_session = datetime.now(timezone.utc)
            logger.warning("Tidak dapat mengambil waktu tick yang valid setelah semua percobaan. Menggunakan waktu sistem UTC sebagai fallback untuk data sesi awal.")

        market_data_processor.calculate_and_update_session_data(my_app_config.TRADING_SYMBOL, current_mt5_datetime_for_session)


        logger.info("Update MT5 trade data awal...")
        database_manager.update_mt5_trade_data_periodically(my_app_config.TRADING_SYMBOL)

        # --- PENTING: Backfill Fitur Historis LENGKAP di sini, SYNCHRONOUSLY ---
        # Ini akan memastikan semua FVG, OB, SR, dll., sudah terdeteksi untuk semua data historis.
        # Loop scheduler yang bergantung pada backfill ini akan menunggu hingga baris ini selesai.
        logger.info("Memulai backfill fitur historis LENGKAP (membutuhkan waktu lama!)...")
        market_data_processor.backfill_historical_features(my_app_config.TRADING_SYMBOL)
        logger.info("Backfill fitur historis LENGKAP selesai.")
        notification_service.send_telegram_message(
            f"✅ *Backfill Historis Lengkap {my_app_config.TRADING_SYMBOL} Selesai!*\n"
            f"Semua data historis dan fitur telah diproses. Bot akan memulai operasi penuh."
        )

        logger.info("Pembaruan data awal (synchronous) selesai.")

    except Exception as e:
        logger.critical(f"Gagal menginisialisasi aplikasi: {e}", exc_info=True)
        notification_service.notify_error(f"Gagal menginisialisasi aplikasi: {e}", "App Init Failure")
        return

    _db_initialized_event.set()
    

    logger.info("Inisialisasi aplikasi selesai di background thread.")
    
    # --- TAMBAHKAN BARIS INI (Setel bendera di scheduler setelah backfill selesai) ---
    scheduler._feature_backfill_completed = True
    logger.info("Flag _feature_backfill_completed diatur ke True. Loop bergantung akan mulai bekerja.")
    # --- AKHIR PENAMBAHAN ---

    logger.info("Inisialisasi aplikasi selesai di background thread.")


# --- BAGIAN MAIN APPLICATION EXECUTION ---
if __name__ == '__main__':
    # Initial Telegram notification about app start
    notification_service.notify_app_start()

    # Create Flask app and SocketIO instance
    app, socketio = api_routes.create_app(my_app_config)

    # Start app initialization in a separate thread
    app_init_thread = threading.Thread(target=initialize_application_components, name="AppInitThread")
    app_init_thread.daemon = True # Daemon thread will exit when main program exits
    app_init_thread.start()

    # Wait for the initialization thread to complete
    logger.info("Menunggu inisialisasi database dan backfill fitur awal selesai...")
    INIT_TIMEOUT_SECONDS = 3600 * 24 * 2 # Meningkatkan timeout menjadi 4 jam (14400 detik) untuk backfill yang panjang
    if not _db_initialized_event.wait(timeout=INIT_TIMEOUT_SECONDS):
        logger.critical(f"Inisialisasi database dan backfill fitur awal gagal dalam batas waktu ({INIT_TIMEOUT_SECONDS} detik). Aplikasi akan keluar.")
        notification_service.notify_error(f"Inisialisasi database timeout. Aplikasi berhenti.", "DB Init Timeout")
        sys.exit(1)

    logger.info("Inisialisasi database dan backfill fitur awal selesai. Melanjutkan startup scheduler dan API.")

    # Mulai emitter progress WebSocket
    api_routes.start_backfill_progress_emitter()

    # Start all periodic scheduler threads
    scheduler._start_data_update_threads(initial_run=True)

    try:
        # Start the Flask API and SocketIO server
        logger.info("Memulai server API dan SocketIO...")
        api_routes.socketio.run(app, host='0.0.0.0', port=5000)

    except KeyboardInterrupt:
        logger.info("Aplikasi dihentikan secara manual (Ctrl+C).")
    except Exception as e:
        logger.critical(f"Kesalahan fatal pada aplikasi: {e}", exc_info=True)
        notification_service.notify_error(f"Kesalahan fatal pada aplikasi: {e}", "Fatal App Error")
    finally:
        logger.info("Melakukan cleanup aplikasi...")
        scheduler._stop_data_update_threads()
        mt5_connector.shutdown_mt5()
        notification_service.stop_notification_service()
        # Hentikan emitter progress WebSocket
        api_routes.stop_backfill_progress_emitter()
        # --- PERUBAHAN: Pastikan DB Writer Thread dihentikan dengan rapi ---
        logger.info("Menunggu semua tugas database writer selesai...")
        # Memberikan waktu yang cukup bagi writer untuk menyelesaikan antrean
        database_manager._db_write_queue.join(timeout=120)
        if not database_manager._db_write_queue.empty():
            logger.warning("Database write queue tidak kosong saat shutdown. Mungkin ada tugas yang tidak sempat diproses! Meningkatkan timeout join jika ini sering terjadi.")
        database_manager.stop_db_writer()
        # --- AKHIR PERUBAHAN ---
        notification_service.notify_app_stop()
        logger.info("Aplikasi telah dihentikan dengan bersih.")
        sys.exit(0)
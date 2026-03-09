import logging
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta # Import ini untuk perhitungan bulan yang akurat

# Konfigurasi logging dasar untuk script ini
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# --- KONFIGURASI LOGGER SPESIFIK INI ---
logging.getLogger('market_data_processor').setLevel(logging.DEBUG)
logging.getLogger('database_manager').setLevel(logging.DEBUG)
logging.getLogger('mt5_connector').setLevel(logging.DEBUG)
# --- AKHIR KONFIGURASI LOGGER SPESIFIK ---

# Inisialisasi logger untuk modul ini
logger = logging.getLogger(__name__)

# Impor modul yang dibutuhkan
from config import config
import database_manager
import market_data_processor
import mt5_connector


if __name__ == "__main__":
    symbol = config.TRADING_SYMBOL

    # --- INISIALISASI DATABASE ---
    try:
        logger.info("Menginisialisasi koneksi database untuk proses backfill...")
        if not database_manager.init_db_connection(config.Database.URL):
            logger.critical("Gagal menginisialisasi koneksi database. Backfill tidak dapat berjalan.")
            exit(1)

        if not database_manager.init_db_writer():
            logger.critical("Gagal memulai database writer thread. Backfill tidak dapat berjalan.")
            exit(1)

        logger.info("Koneksi database dan DB writer thread berhasil diinisialisasi untuk backfill.")

        # Opsional: Pastikan tabel ada
        # database_manager.create_all_tables()

    except Exception as e:
        logger.critical(f"Kesalahan KRITIS saat startup database untuk backfill: {e}", exc_info=True)
        exit(1)
    # --- AKHIR INISIALISASI DATABASE ---
    logger.info("Menghapus status backfill sebelumnya untuk semua timeframe...")
    timeframes_to_reset = list(config.MarketData.ENABLED_TIMEFRAMES.keys())
    for tf_name in timeframes_to_reset:
        database_manager.delete_feature_backfill_status(symbol, tf_name)
        # --- TAMBAHKAN LOG VERIFIKASI INI ---
        check_status_after_delete = database_manager.get_last_feature_backfill_time(symbol, tf_name)
        logger.debug(f"Status backfill untuk {symbol} {tf_name} setelah upaya penghapusan: {check_status_after_delete}")
        # --- AKHIR LOG VERIFIKASI ---
    logger.info(f"Status backfill berhasil dihapus untuk timeframes: {timeframes_to_reset}.")
    # --- AKHIR HAPUS STATUS BACKFILL ---
    # --- INISIALISASI MT5 (untuk _initialize_symbol_point_value di market_data_processor) ---
    # Panggil initialize_mt5 tanpa argumen jika fungsi Anda tidak menerimanya
    try:
        logger.info("Menginisialisasi koneksi MT5 untuk proses backfill...")
        mt5_connector.initialize_mt5()
        logger.info("Koneksi MT5 berhasil diinisialisasi untuk backfill.")
    except Exception as e:
        logger.error(f"Gagal menginisialisasi MT5 untuk backfill: {e}. Pastikan terminal MT5 berjalan.", exc_info=True)
        # Jangan exit, karena backfill mungkin masih bisa berjalan dari data DB yang sudah ada
        # Tapi beberapa detektor mungkin tidak bekerja optimal tanpa koneksi MT5.

    # --- Panggil fungsi backfill historis ---
    logger.info(f"Memulai proses backfill fitur historis untuk {symbol}...")

    # --- MODIFIKASI KRITIKAL DI SINI: MENENTUKAN RENTANG TANGGAL 7 BULAN ---
    end_date_custom = datetime.now(timezone.utc) # Waktu saat ini (UTC)
    start_date_custom = end_date_custom - relativedelta(months=7) # 7 bulan ke belakang

    logger.info(f"Backfill akan berjalan untuk rentang kustom: {start_date_custom.isoformat()} hingga {end_date_custom.isoformat()}.")

    market_data_processor.backfill_historical_features(
        symbol,
        start_date=start_date_custom,
        end_date=end_date_custom
    )
    # --- AKHIR MODIFIKASI KRITIKAL ---

    logger.info("\nProses backfill historis selesai. Periksa log dan database Anda.")

    # --- SHUTDOWN MT5 DAN DB WRITER ---
    try:
        mt5_connector.shutdown_mt5()
        logger.info("Koneksi MT5 dimatikan setelah backfill.")
    except Exception as e:
        logger.error(f"Error saat mematikan koneksi MT5 setelah backfill: {e}", exc_info=True)

    try:
        database_manager.stop_db_writer()
        logger.info("DB writer thread dihentikan setelah backfill.")
    except Exception as e:
        logger.error(f"Error saat menghentikan DB writer thread setelah backfill: {e}", exc_info=True)
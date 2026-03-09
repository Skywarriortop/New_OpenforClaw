# mt5_data_verifier.py

import sys
import os
import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal

# Pastikan jalur ke direktori proyek Anda ada di Python path
# Jika mt5_connector.py dan config.py ada di direktori yang sama dengan script ini,
# baris ini mungkin tidak diperlukan, tetapi lebih aman untuk memastikan.
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# Mengatur logging agar output terlihat di konsol
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO) # Atur level info untuk output konsol

# Menonaktifkan logging dari modul lain yang terlalu verbose
logging.getLogger('MetaTrader5').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('selenium').setLevel(logging.WARNING)
logging.getLogger('webdriver_manager').setLevel(logging.WARNING)
logging.getLogger('werkzeug').setLevel(logging.WARNING)
logging.getLogger('httpcore').setLevel(logging.WARNING)
logging.getLogger('asyncio').setLevel(logging.WARNING)

# Impor modul Anda
try:
    import mt5_connector
    import config
    # Memastikan config diinisialisasi
    my_app_config = config.Config()
    my_app_config.validate_config()
except ImportError as e:
    logger.error(f"Gagal mengimpor modul bot. Pastikan mt5_connector.py dan config.py ada di jalur yang benar. Error: {e}")
    sys.exit(1)
except Exception as e:
    logger.error(f"Gagal menginisialisasi konfigurasi bot: {e}")
    sys.exit(1)


def verify_mt5_data_capabilities(symbol: str):
    """
    Menginisialisasi MT5, mengambil sampel data, dan memverifikasi ketersediaan
    kolom-kolom kunci seperti volume real dan tick volume.
    """
    logger.info(f"--- Memulai verifikasi data MT5 untuk simbol: {symbol} ---")

    # 1. Inisialisasi Koneksi MT5
    logger.info("Mencoba menginisialisasi koneksi MetaTrader 5...")
    if not mt5_connector.initialize_mt5():
        logger.error("Gagal menginisialisasi MT5. Pastikan terminal MT5 berjalan dan jalur di config.py benar.")
        return False
    logger.info("Koneksi MT5 berhasil diinisialisasi.")
    mt5_connector.print_mt5_status()

    # 2. Periksa Informasi Simbol (Point Value, dll.)
    logger.info("\n--- Memeriksa Informasi Simbol ---")
    symbol_info_raw = mt5_connector.mt5.symbol_info(symbol) # Mengakses langsung mt5.symbol_info
    if symbol_info_raw:
        symbol_dict = symbol_info_raw._asdict()
        logger.info(f"Informasi dasar simbol {symbol}:")
        for key, value in symbol_dict.items():
            if isinstance(value, float):
                logger.info(f"  {key}: {value:.5f}")
            else:
                logger.info(f"  {key}: {value}")
        logger.info(f"Nilai Point: {symbol_dict.get('point')}")
        logger.info(f"Ukuran Kontrak: {symbol_dict.get('trade_contract_size')}")
    else:
        logger.warning(f"Gagal mendapatkan informasi simbol untuk {symbol}.")

    # 3. Ambil dan Periksa Data Tick Real-Time
    logger.info("\n--- Memeriksa Data Tick Real-Time ---")
    current_tick = mt5_connector.get_current_tick_info(symbol)
    if current_tick:
        logger.info("Data tick real-time berhasil diambil:")
        for key, value in current_tick.items():
            if isinstance(value, Decimal):
                logger.info(f"  {key}: {float(value):.5f}")
            elif isinstance(value, datetime):
                logger.info(f"  {key}: {value.isoformat()}")
            else:
                logger.info(f"  {key}: {value}")

        # Verifikasi keberadaan volume_real pada tick
        if 'volume_real' in current_tick and current_tick['volume_real'] is not None and current_tick['volume_real'] > Decimal('0.0'):
            logger.info(f"✅ Volume Realtime Tick: TERSEDIA (volume_real={float(current_tick['volume_real']):.2f})")
        else:
            logger.warning("❌ Volume Realtime Tick: TIDAK TERSEDIA atau bernilai nol.")
        
        # Verifikasi keberadaan volume pada tick (jika bukan volume_real)
        if 'volume' in current_tick and current_tick['volume'] is not None and current_tick['volume'] > Decimal('0.0'):
            logger.info(f"✅ Volume Tick (standar): TERSEDIA (volume={float(current_tick['volume']):.2f})")
        else:
            logger.warning("❌ Volume Tick (standar): TIDAK TERSEDIA atau bernilai nol.")

    else:
        logger.warning(f"Gagal mendapatkan data tick real-time untuk {symbol}.")

    # 4. Ambil dan Periksa Data Candle Historis (D1)
    logger.info("\n--- Memeriksa Data Candle Historis (D1) ---")
    # Ambil beberapa candle D1 untuk memeriksa struktur kolom
    sample_candles_d1 = mt5_connector.get_historical_candles(symbol, "D1", num_candles=5)
    if sample_candles_d1:
        logger.info(f"Berhasil mengambil {len(sample_candles_d1)} candle D1. Periksa kolom candle:")
        first_candle_d1 = sample_candles_d1[0]
        logger.info(f"Kolom yang tersedia: {first_candle_d1.keys()}")

        if 'real_volume' in first_candle_d1 and first_candle_d1['real_volume'] is not None and first_candle_d1['real_volume'] > Decimal('0.0'):
            logger.info(f"✅ Real Volume (Candle D1): TERSEDIA (contoh: {float(first_candle_d1['real_volume']):.2f})")
        else:
            logger.warning("❌ Real Volume (Candle D1): TIDAK TERSEDIA atau bernilai nol.")

        if 'tick_volume' in first_candle_d1 and first_candle_d1['tick_volume'] is not None and first_candle_d1['tick_volume'] > Decimal('0.0'):
            logger.info(f"✅ Tick Volume (Candle D1): TERSEDIA (contoh: {float(first_candle_d1['tick_volume']):.2f})")
        else:
            logger.warning("❌ Tick Volume (Candle D1): TIDAK TERSEDIA atau bernilai nol.")

        logger.info("Contoh data candle D1 (terbaru):")
        for i, candle in enumerate(reversed(sample_candles_d1)): # Tampilkan yang terbaru dulu
            if i >= 2: break # Cukup 2 candle terbaru
            logger.info(f"  Candle D1 ({candle['open_time_utc'].isoformat().split('T')[0]}):")
            logger.info(f"    Open: {float(candle['open_price']):.5f}, High: {float(candle['high_price']):.5f}, Low: {float(candle['low_price']):.5f}, Close: {float(candle['close_price']):.5f}")
            logger.info(f"    Tick Vol: {float(candle['tick_volume']):.0f}, Real Vol: {float(candle['real_volume'] if 'real_volume' in candle else 0):.0f}, Spread: {float(candle['spread']):.0f}")
    else:
        logger.warning(f"Gagal mendapatkan data candle historis D1 untuk {symbol}.")
    
    # 5. Ambil dan Periksa Data Candle Historis (M5)
    logger.info("\n--- Memeriksa Data Candle Historis (M5) ---")
    # Ambil beberapa candle M5 untuk memeriksa struktur kolom
    sample_candles_m5 = mt5_connector.get_historical_candles(symbol, "M5", num_candles=5)
    if sample_candles_m5:
        logger.info(f"Berhasil mengambil {len(sample_candles_m5)} candle M5. Periksa kolom candle:")
        first_candle_m5 = sample_candles_m5[0]
        logger.info(f"Kolom yang tersedia: {first_candle_m5.keys()}")

        if 'real_volume' in first_candle_m5 and first_candle_m5['real_volume'] is not None and first_candle_m5['real_volume'] > Decimal('0.0'):
            logger.info(f"✅ Real Volume (Candle M5): TERSEDIA (contoh: {float(first_candle_m5['real_volume']):.2f})")
        else:
            logger.warning("❌ Real Volume (Candle M5): TIDAK TERSEDIA atau bernilai nol.")

        if 'tick_volume' in first_candle_m5 and first_candle_m5['tick_volume'] is not None and first_candle_m5['tick_volume'] > Decimal('0.0'):
            logger.info(f"✅ Tick Volume (Candle M5): TERSEDIA (contoh: {float(first_candle_m5['tick_volume']):.2f})")
        else:
            logger.warning("❌ Tick Volume (Candle M5): TIDAK TERSEDIA atau bernilai nol.")
        
        logger.info("Contoh data candle M5 (terbaru):")
        for i, candle in enumerate(reversed(sample_candles_m5)): # Tampilkan yang terbaru dulu
            if i >= 2: break # Cukup 2 candle terbaru
            logger.info(f"  Candle M5 ({candle['open_time_utc'].isoformat().split('.')[0]}):")
            logger.info(f"    Open: {float(candle['open_price']):.5f}, High: {float(candle['high_price']):.5f}, Low: {float(candle['low_price']):.5f}, Close: {float(candle['close_price']):.5f}")
            logger.info(f"    Tick Vol: {float(candle['tick_volume']):.0f}, Real Vol: {float(candle['real_volume'] if 'real_volume' in candle else 0):.0f}, Spread: {float(candle['spread']):.0f}")
    else:
        logger.warning(f"Gagal mendapatkan data candle historis M5 untuk {symbol}.")

    logger.info("\n--- Verifikasi data MT5 selesai. ---")
    return True


if __name__ == "__main__":
    # Tentukan simbol trading yang ingin Anda periksa (ambil dari config.py)
    TARGET_SYMBOL = my_app_config.TRADING_SYMBOL

    try:
        verify_mt5_data_capabilities(TARGET_SYMBOL)
    except Exception as e:
        logger.critical(f"Terjadi kesalahan fatal saat verifikasi data: {e}", exc_info=True)
    finally:
        # Penting: Pastikan koneksi MT5 di-shutdown di akhir
        mt5_connector.shutdown_mt5()
        logger.info("Koneksi MT5 dimatikan.")
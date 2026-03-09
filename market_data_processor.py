# market_data_processor.py
import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import json
import pandas as pd
import talib
from sqlalchemy.exc import OperationalError
import time
from collections import defaultdict
import threading
import queue
from enum import Enum
import os
import mt5_connector
from config import config
import database_manager
import utils
from utils import to_float_or_none, to_iso_format_or_none , to_decimal_or_none, to_int_or_none, to_bool_or_none, _get_scalar_from_possibly_ndarray , _get_pandas_freq_alias , DistributionData 
import detector_monitoring
import numpy as np
import math
from copy import deepcopy


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s') # Komentar atau hapus jika sudah di main.py
logger_mp = logging.getLogger('market_data_processor')
logger_mp.setLevel(logging.DEBUG)

print(">>> market_data_processor.py: MODUL DIMUAT (VERSI DEBUG) <<<")

# --- Variabel Global Baru untuk Status Backfill ---
_backfill_overall_progress_status = defaultdict(dict)
_backfill_progress_lock = threading.Lock()

# --- Variabel global di tingkat modul untuk menyimpan batas harga dinamis ---
_dynamic_price_min_for_analysis = None
_dynamic_price_max_for_analysis = None
_dynamic_price_lock = threading.Lock() # Untuk keamanan thread saat update variabel global

_SYMBOL_POINT_VALUE = None
_test_db_manager_override = None

def _get_db_manager():
    """Mengembalikan instance database_manager yang digunakan, atau override untuk pengujian."""
    global _test_db_manager_override
    if _test_db_manager_override:
        return _test_db_manager_override
    return database_manager

def _calculate_rsi(ohlc_df: pd.DataFrame, period: int) -> pd.Series:
    """
    Menghitung Relative Strength Index (RSI) menggunakan TA-Lib.
    Args:
        ohlc_df (pd.DataFrame): DataFrame yang berisi kolom 'close' dalam format float atau Decimal.
                                Fungsi ini akan mengonversinya ke float jika diperlukan.
        period (int): Periode RSI.
    Returns:
        pd.Series: Series berisi nilai RSI (float), atau Series kosong jika data tidak cukup.
    """
    logger.debug(f"RSI_DEBUG: Memulai _calculate_rsi. Panjang input DataFrame: {len(ohlc_df)}, periode: {period}.") # Log baru
    
    if ohlc_df.empty:
        logger.warning(f"DataFrame input untuk RSI kosong atau tidak memiliki kolom 'close'.") # Modifikasi log
        return pd.Series(dtype='float64') # Mengembalikan Series kosong jika tidak cukup data

    # Pastikan kolom 'close' ada dan bertipe float untuk TA-Lib
    if 'close' not in ohlc_df.columns:
        logger.error("DataFrame input untuk RSI harus memiliki kolom 'close'.")
        return pd.Series(dtype='float64')
    
    # Konversi kolom 'close' ke float secara aman menggunakan to_float_or_none
    # Ini penting karena data mungkin datang sebagai Decimal dari DB
    close_prices_float = ohlc_df['close'].apply(to_float_or_none).values
    logger.debug(f"RSI_DEBUG: Input close_prices_float to TA-Lib (head): {close_prices_float[:5]}, (tail): {close_prices_float[-5:]}") # Log baru
    
    # Periksa apakah ada nilai NaN setelah konversi
    if np.isnan(close_prices_float).all():
        logger.warning("Kolom 'close' hanya berisi NaN setelah konversi ke float. Tidak dapat menghitung RSI.")
        return pd.Series(dtype='float64')

    if len(ohlc_df) < period: # Pindahkan pengecekan panjang di sini setelah konversi
        logger.warning(f"Tidak cukup data ({len(ohlc_df)} bar) untuk menghitung RSI (periode {period}).")
        # Perhitungan RSI menghasilkan seri kosong atau semua NaN untuk periode 14. Panjang input: 10. Mengembalikan seri NaN dengan indeks yang benar.
        # Jika tidak cukup data, TA-Lib akan mengembalikan NaN. Kita bisa langsung mengembalikan Series NaN di sini.
        return pd.Series(np.nan, index=ohlc_df.index, dtype='float64') # Mengembalikan Series NaN

    # --- WORKAROUND UNTUK HARGA KONSTAN ---
    # Periksa apakah semua nilai non-NaN dalam close_prices_float adalah sama (harga konstan)
    non_nan_prices = close_prices_float[~np.isnan(close_prices_float)]
    if len(non_nan_prices) > 0 and np.all(non_nan_prices == non_nan_prices[0]):
        logger.debug(f"RSI_DEBUG: Terdeteksi harga konstan. Mengembalikan RSI 50.0 untuk semua data.") # Log baru
        rsi_values = np.full_like(close_prices_float, 50.0) # Isi dengan 50.0
    else:
        # TA-Lib memerlukan numpy array, jadi konversi Series Pandas ke numpy array
        rsi_values = talib.RSI(close_prices_float, timeperiod=period)
    
    logger.debug(f"RSI_DEBUG: Output rsi_values from TA-Lib (atau workaround) (head): {rsi_values[:5]}, (tail): {rsi_values[-5:]}") # Log baru

    # Konversi kembali numpy array ke Pandas Series dengan indeks yang sama
    rsi_series = pd.Series(rsi_values, index=ohlc_df.index)
    
    logger.debug(f"RSI_DEBUG: Selesai _calculate_rsi. Panjang seri hasil: {len(rsi_series)}, NaN count: {rsi_series.isnull().sum()}.") # Log baru
    return rsi_series
# --- AKHIR FUNGSI PERHITUNGAN RSI ---
def _calculate_atr(ohlc_df_float: pd.DataFrame, period: int) -> pd.Series:
    """
    Menghitung Average True Range (ATR) menggunakan TA-Lib.
    Mengharapkan df_ohlc_float memiliki kolom 'high', 'low', 'close' (float) sebagai input.
    """
    if ohlc_df_float.empty or len(ohlc_df_float) < period:
        logger.warning(f"Tidak cukup data ({len(ohlc_df_float)} bar) untuk menghitung ATR (periode {period}).")
        return pd.Series(np.nan, index=ohlc_df_float.index, dtype='float64')

    # Pastikan kolom-kolom ini ada dan bertipe float
    # MODIFIKASI/KONFIRMASI INI: Pastikan kolom 'high', 'low', 'close' sudah ada
    # dan tipe datanya sudah float.
    # Jika df_ohlc_float berasal dari data_updater, mungkin masih ada kolom '_price'
    # Pastikan pemanggilnya sudah me-rename kolom ini menjadi 'high', 'low', 'close'.
    # ATAU, lakukan renaming dan konversi di sini jika Anda yakin ini adalah tempat terbaik.

    # Saya akan tambahkan langkah penjaminan di sini, ini duplikasi tapi lebih aman.
    temp_df = ohlc_df_float.copy()
    temp_df = temp_df.rename(columns={
        'high_price': 'high', 'low_price': 'low', 'close_price': 'close' # Hanya jika belum di-rename
    })
    for col in ['high', 'low', 'close']:
        if col not in temp_df.columns:
            logger.error(f"DataFrame input untuk ATR harus memiliki kolom '{col}'.")
            return pd.Series(np.nan, index=ohlc_df_float.index, dtype='float64')
        # Pastikan kolom adalah float untuk TA-Lib
        temp_df[col] = temp_df[col].apply(utils.to_float_or_none)

    high_prices = temp_df['high'].values
    low_prices = temp_df['low'].values
    close_prices = temp_df['close'].values
    
    # Periksa apakah ada NaN di data setelah konversi
    if np.isnan(high_prices).all() or np.isnan(low_prices).all() or np.isnan(close_prices).all():
        logger.warning("Kolom 'high', 'low', atau 'close' hanya berisi NaN setelah konversi ke float. Tidak dapat menghitung ATR.")
        return pd.Series(np.nan, index=ohlc_df_float.index, dtype='float64')

    # Hitung ATR menggunakan TA-Lib
    atr_values = talib.ATR(high_prices, low_prices, close_prices, timeperiod=period)

    # Konversi kembali numpy array ke Pandas Series dengan indeks yang sama
    atr_series = pd.Series(atr_values, index=ohlc_df_float.index)
    return atr_series

def _calculate_macd(df_ohlc_float: pd.DataFrame, fast_period: int, slow_period: int, signal_period: int):
    """
    Menghitung MACD menggunakan TA-Lib dan MACD sebagai persentase harga.
    Mengharapkan df_ohlc_float memiliki kolom 'close' (float atau dapat dikonversi) sebagai input.
    """
    if df_ohlc_float.empty or 'close' not in df_ohlc_float.columns:
        logger.warning("DataFrame kosong atau tidak memiliki kolom 'close' untuk menghitung MACD.")
        empty_series = pd.Series(np.nan, index=df_ohlc_float.index)
        return {"macd_line": empty_series, "signal_line": empty_series, "histogram": empty_series, "macd_pcent": empty_series}

    # Konversi kolom 'close' ke float secara aman menggunakan to_float_or_none
    close_prices_float = df_ohlc_float['close'].apply(to_float_or_none).values

    min_required = max(fast_period, slow_period, signal_period) + 1
    if len(close_prices_float) < min_required:
        logger.warning(f"Tidak cukup data ({len(close_prices_float)} bar) untuk menghitung MACD (periode min {min_required}).")
        empty_series = pd.Series(np.nan, index=df_ohlc_float.index)
        return {"macd_line": empty_series, "signal_line": empty_series, "histogram": empty_series, "macd_pcent": empty_series}

    # Hitung MACD menggunakan TA-Lib
    macd, macd_signal, macd_hist = talib.MACD(
        close_prices_float,
        fastperiod=fast_period,
        slowperiod=slow_period,
        signalperiod=signal_period
    )

    # --- KOREKSI KRUSIAL: Pastikan nilai awal adalah NaN secara eksplisit ---
    # TA-Lib terkadang mengembalikan 0.0 atau nilai kecil lain alih-alih NaN untuk periode awal
    # jika inputnya konstan atau sangat datar. Kita ingin mereka menjadi NaN eksplisit.
    min_nan_period = slow_period + signal_period - 1 # Ini adalah jumlah NaN yang diharapkan
    
    # Inisialisasi series dengan NaN
    macd_series = pd.Series(np.nan, index=df_ohlc_float.index)
    signal_series = pd.Series(np.nan, index=df_ohlc_float.index)
    histogram_series = pd.Series(np.nan, index=df_ohlc_float.index)

    # Isi nilai dari TA-Lib mulai dari indeks pertama non-NaN
    if len(macd) > min_nan_period:
        macd_series.iloc[min_nan_period:] = macd[min_nan_period:]
        signal_series.iloc[min_nan_period:] = macd_signal[min_nan_period:]
        histogram_series.iloc[min_nan_period:] = macd_hist[min_nan_period:]
    # Jika tidak ada cukup data untuk TA-Lib menghasilkan nilai non-NaN, biarkan semua NaN.

    logger.debug(f"MACD_DEBUG: Calculated min_nan_period based on expected_hist: {min_nan_period}")
    logger.debug(f"MACD_DEBUG: Output macd_values from TA-Lib (or workaround) (head): {macd_series.head().values}, (tail): {macd_series.tail().values}")
    logger.debug(f"MACD_DEBUG: Selesai _calculate_macd. Panjang seri hasil: {len(macd_series)}, NaN count: {macd_series.isnull().sum()}.")
    # --- AKHIR KOREKSI KRUSIAL ---

    # HITUNG MACD SEBAGAI PERSENTASE HARGA
    current_close_prices_float_for_pcent = df_ohlc_float['close'].apply(to_float_or_none)
    macd_pcent_series = pd.Series(np.where(
        current_close_prices_float_for_pcent != 0, # Hindari pembagian dengan nol
        (macd_series / current_close_prices_float_for_pcent) * 100, # Kali 100 untuk persentase
        np.nan # Jika harga nol, hasilnya NaN
    ), index=df_ohlc_float.index)
    # Pastikan macd_pcent juga NaN di awal periode
    if len(macd_pcent_series) > min_nan_period:
        macd_pcent_series.iloc[:min_nan_period] = np.nan

    return {
        "macd_line": macd_series,
        "signal_line": signal_series,
        "histogram": histogram_series,
        "macd_pcent": macd_pcent_series
    }

def calculate_sma(prices, period):
    """
    Menghitung Simple Moving Average (SMA).
    Mengharapkan 'prices' adalah list atau Series numerik (Decimal atau float).
    """
    if not isinstance(prices, pd.Series):
        prices = pd.Series(prices)

    # Tambahkan validasi periode
    if not isinstance(period, int) or period <= 0:
        logger.warning(f"Periode SMA tidak valid ({period}). Harus berupa integer positif.")
        return None

    # Pastikan prices adalah numerik dan konversi ke Decimal untuk perhitungan presisi
    prices_decimal = prices.apply(lambda x: utils.to_decimal_or_none(x)).dropna()

    if len(prices_decimal) < period:
        logger.warning(f"Tidak cukup data ({len(prices_decimal)}) untuk menghitung SMA (periode {period}). Diperlukan {period} harga.")
        return None

    # Perhitungan SMA menggunakan Decimal
    sma_value = prices_decimal.iloc[-period:].sum() / Decimal(str(period))
    return sma_value

def _calculate_ema_internal(df_close_prices: pd.Series, period: int) -> pd.Series:
    if df_close_prices.empty or len(df_close_prices) < period:
        logger.warning(f"Tidak cukup data ({len(df_close_prices)} bar) untuk menghitung EMA (periode {period}).")
        return pd.Series(np.nan, index=df_close_prices.index, dtype=float)

    prices_float = df_close_prices.apply(utils.to_float_or_none)
    
    original_nans_mask = prices_float.isnull()

    temp_prices_for_ema = prices_float.ffill() 
    temp_prices_for_ema = temp_prices_for_ema.bfill() 

    if temp_prices_for_ema.isnull().all():
        logger.warning(f"EMA Calc: Semua harga adalah NaN setelah imputasi sementara. Mengembalikan semua NaN.")
        return pd.Series(np.nan, index=df_close_prices.index, dtype=float)

    ema_series_calc = temp_prices_for_ema.ewm(span=period, adjust=False).mean()
    
    ema_series = pd.Series(ema_series_calc, index=df_close_prices.index, dtype=float)
    ema_series[original_nans_mask] = np.nan 
    
    return ema_series


def _calculate_sessions_internal(
    ohlc: pd.DataFrame,
    timeframe_str: str,
    session_name: str,
    start_time_str: str = "",
    end_time_str: str = "",
    time_zone: str = "UTC",
) -> pd.DataFrame:
    """
    Menghitung sesi pasar (Asia, Eropa, New York, kustom) dan data high/low yang terakumulasi
    untuk setiap lilin dalam DataFrame OHLC.
    Args:
        ohlc (pd.DataFrame): DataFrame yang berisi data OHLC (open, high, low, close)
                             dengan indeks datetime (UTC aware). Kolom harga diasumsikan sudah float (atau np.nan).
        timeframe_str (str): Timeframe yang sedang diproses (misal "H1").
        session_name (str): Nama sesi yang akan dihitung (misal "Tokyo", "London", "Custom").
        start_time_str (str, optional): Waktu mulai sesi kustom dalam format "HH:MM" (UTC).
        end_time_str (str, optional): Waktu akhir sesi kustom dalam format "HH:MM" (UTC).
        time_zone (str, optional): Zona waktu untuk sesi. Saat ini hanya mendukung "UTC".
    Returns:
        pd.DataFrame: DataFrame dengan indeks yang sama seperti input, berisi kolom 'Active'
                      (1 jika sesi aktif, 0 jika tidak), 'High' (high sesi terakumulasi),
                      dan 'Low' (low sesi terakumulasi).
    """
    logger.debug(f"Menjalankan _calculate_sessions_internal untuk timeframe {timeframe_str} sesi '{session_name}' dengan time_zone={time_zone} pada {len(ohlc)} lilin.")

    if session_name == "Custom" and (start_time_str == "" or end_time_str == ""):
        logger.error("Sesi kustom membutuhkan waktu mulai dan akhir.")
        return pd.DataFrame(index=ohlc.index, columns=['Active', 'High', 'Low'])

    default_sessions = {
        "Sydney": {"start": "21:00", "end": "06:00"},
        "Tokyo": {"start": "00:00", "end": "09:00"},
        "London": {"start": "07:00", "end": "16:00"},
        "New York": {"start": "13:00", "end": "22:00"},
        "Asian kill zone": {"start": "00:00", "end": "04:00"},
        "London open kill zone": {"start": "06:00", "end": "09:00"},
        "New York kill zone": {"start": "11:00", "end": "14:00"},
        "london close kill zone": {"start": "14:00", "end": "16:00"},
        "Custom": {"start": start_time_str, "end": end_time_str},
    }

    if ohlc.empty:
        logger.debug("DataFrame ohlc kosong untuk _calculate_sessions_internal.")
        return pd.DataFrame(index=ohlc.index, columns=['Active', 'High', 'Low'])

    # Pastikan indeks DataFrame adalah datetime timezone-aware UTC.
    if ohlc.index.tz is None:
        ohlc.index = ohlc.index.tz_localize(timezone.utc)
    else:
        ohlc.index = ohlc.index.tz_convert(timezone.utc)

    # --- FASE 1 REVISI AKHIR - KOREKSI NAMERROR: DEFINISIKAN start_time_obj DAN end_time_obj DI SINI ---
    start_time_obj = datetime.strptime(default_sessions[session_name]["start"], "%H:%M").time()
    end_time_obj = datetime.strptime(default_sessions[session_name]["end"], "%H:%M").time()

    # Inisialisasi array NumPy untuk kinerja
    active = np.zeros(len(ohlc), dtype=np.int32)
    high = np.full(len(ohlc), np.nan, dtype=np.float32)
    low = np.full(len(ohlc), np.nan, dtype=np.float32)

    for i in range(len(ohlc)):
        current_candle_time_utc = ohlc.index[i].time()

        is_active_session = False
        if start_time_obj < end_time_obj:
            # Sesi tidak melewati tengah malam
            if start_time_obj <= current_candle_time_utc < end_time_obj:
                is_active_session = True
        else:
            # Sesi melewati tengah malam
            if start_time_obj <= current_candle_time_utc or current_candle_time_utc < end_time_obj:
                is_active_session = True

        if is_active_session:
            active[i] = 1
            
            # FASE 1 REVISI AKHIR - PERBAIKAN: Pastikan ini selalu float atau np.nan
            # Menggunakan utils.to_float_or_none adalah lapisan pertahanan terakhir
            current_candle_high = utils.to_float_or_none(ohlc["high"].iloc[i])
            current_candle_low = utils.to_float_or_none(ohlc["low"].iloc[i])

            # Periksa jika setelah konversi, nilainya masih None (sangat jarang jika sudah di-dropna dengan baik,
            # tetapi ini lapisan pertahanan terakhir sebelum NumPy)
            if current_candle_high is None: current_candle_high = np.nan
            if current_candle_low is None: current_candle_low = np.nan

            # Mengakumulasi high dan low sesi.
            # np.nanmax/np.nanmin dapat menangani np.nan dengan baik.
            if i > 0 and active[i-1] == 1: # Jika sesi aktif di lilin sebelumnya juga
                prev_session_high = high[i-1] 
                prev_session_low = low[i-1]

                high[i] = np.nanmax([current_candle_high, prev_session_high])
                low[i] = np.nanmin([current_candle_low, prev_session_low])
            else: # Ini adalah lilin pertama sesi yang aktif, atau setelah sesi sempat tidak aktif
                high[i] = current_candle_high
                low[i] = current_candle_low
        else:
            # Jika sesi tidak aktif, high/low sesi menjadi NaN
            high[i] = np.nan
            low[i] = np.nan

    # Menggabungkan hasil ke dalam DataFrame
    active_series = pd.Series(active, index=ohlc.index, name="Active")
    high_series = pd.Series(high, index=ohlc.index, name="High")
    low_series = pd.Series(low, index=ohlc.index, name="Low")

    return pd.concat([active_series, high_series, low_series], axis=1)

def calculate_and_update_session_data(
    symbol_param: str,
    current_mt5_datetime: datetime = None,
    target_timeframe_str: str = "H1",
    target_session_name: str = None
):
    """
    Menghitung dan memperbarui data sesi (Asia, Eropa, New York) dan menyimpannya
    menggunakan logika sesi internal.
    Ini termasuk sesi lilin dan data ayunan.
    Dapat menerima waktu, timeframe, dan sesi target untuk pengujian atau kasus penggunaan spesifik.
    Args:
        symbol_param (str): Simbol trading.
        current_mt5_datetime (datetime, optional): Waktu referensi yang akan digunakan.
                                                    Jika None, datetime.now(timezone.utc) akan digunakan.
        target_timeframe_str (str, optional): Timeframe yang akan digunakan untuk mengambil lilin. Default "H1".
        target_session_name (str, optional): Nama sesi spesifik untuk diproses (misal "London").
                                               Jika None, semua sesi yang dikonfigurasi akan diproses.
    """
    effective_current_datetime = current_mt5_datetime or datetime.now(timezone.utc)

    logger.info(f"Menghitung dan memperbarui data sesi untuk {symbol_param} pada {effective_current_datetime.strftime('%Y-%m-%d %H:%M:%S UTC')} (TF: {target_timeframe_str}) menggunakan logika sesi internal...")

    current_date = effective_current_datetime.date()

    candles_for_session_calc = database_manager.get_historical_candles_from_db(
        symbol_param, target_timeframe_str, limit=72, end_time_utc=effective_current_datetime
    )

    # --- DEBUGGING BARU: Periksa input awal dari DB ---
    logger.debug(f"SESSION_DEBUG_DATA: candles_for_session_calc type: {type(candles_for_session_calc)}")
    if isinstance(candles_for_session_calc, list):
        logger.debug(f"SESSION_DEBUG_DATA: candles_for_session_calc (first 2 records): {candles_for_session_calc[:2]}")
        if candles_for_session_calc:
            logger.debug(f"SESSION_DEBUG_DATA: Keys in first record: {candles_for_session_calc[0].keys()}")
    # --- AKHIR DEBUGGING BARU ---

    if not candles_for_session_calc:
        logger.warning(f"Tidak ada data candle untuk {symbol_param} ({target_timeframe_str}) untuk menghitung sesi. Tidak dapat mengupdate data sesi.")
        with config.MarketData._market_status_data_lock:
            config.MarketData.market_status_data = {
                "session_status": [],
                "overlap_status": "No Candles for Session Calc",
                "current_utc_time": effective_current_datetime.isoformat(),
                "detailed_sessions": []
            }
        return

    # Persiapan DataFrame untuk _calculate_sessions_internal
    ohlc_df_for_calc = pd.DataFrame(candles_for_session_calc)

    # --- DEBUGGING BARU: Periksa DataFrame setelah pembuatan awal ---
    logger.debug(f"SESSION_DEBUG_DF_INIT: Columns after initial DataFrame creation: {ohlc_df_for_calc.columns.tolist()}")
    logger.debug(f"SESSION_DEBUG_DF_INIT: Head of ohlc_df_for_calc (initial):\n{ohlc_df_for_calc.head()}")
    # --- AKHIR DEBUGGING BARU ---

    # --- MODIFIKASI: Penanganan kolom 'open_time_utc' dan konversi ke Decimal ---
    # Pastikan 'open_time_utc' ada sebagai kolom sebelum mencoba mengaksesnya
    if 'open_time_utc' not in ohlc_df_for_calc.columns:
        logger.error(f"SESSION_DEBUG_ERROR: 'open_time_utc' column not found in DataFrame. Available columns: {ohlc_df_for_calc.columns.tolist()}")
        with config.MarketData._market_status_data_lock:
            config.MarketData.market_status_data = {
                "session_status": [],
                "overlap_status": "Error: Missing 'open_time_utc'",
                "current_utc_time": effective_current_datetime.isoformat(),
                "detailed_sessions": []
            }
        return # Keluar dari fungsi jika kolom krusial hilang

    ohlc_df_for_calc['open_time_utc'] = pd.to_datetime(ohlc_df_for_calc['open_time_utc'], utc=True, errors='coerce')
    ohlc_df_for_calc = ohlc_df_for_calc.set_index('open_time_utc').sort_index()

    # --- DEBUGGING BARU: Periksa DataFrame setelah set_index ---
    logger.debug(f"SESSION_DEBUG_DF_INDEX: Columns after set_index: {ohlc_df_for_calc.columns.tolist()}")
    logger.debug(f"SESSION_DEBUG_DF_INDEX: Index type: {type(ohlc_df_for_calc.index)}")
    logger.debug(f"SESSION_DEBUG_DF_INDEX: Head of ohlc_df_for_calc (after index set):\n{ohlc_df_for_calc.head()}")
    # --- AKHIR DEBUGGING BARU ---

    # Rename kolom-kolom harga dan volume untuk konsistensi
    ohlc_df_for_calc = ohlc_df_for_calc.rename(columns={
        'open_price': 'open', 'high_price': 'high', 'low_price': 'low',
        'close_price': 'close', 'tick_volume': 'volume', 'real_volume': 'real_volume', 'spread': 'spread'
    })

    # Konversi kolom-kolom harga dan volume ke Decimal
    price_and_volume_cols = ['open', 'high', 'low', 'close', 'volume', 'real_volume', 'spread']
    for col in price_and_volume_cols:
        if col in ohlc_df_for_calc.columns:
            ohlc_df_for_calc[col] = ohlc_df_for_calc[col].apply(utils.to_decimal_or_none)

    # --- DEBUGGING BARU: Periksa DataFrame setelah konversi Decimal ---
    logger.debug(f"SESSION_DEBUG_DF_DECIMAL: Head of ohlc_df_for_calc (after Decimal conversion):\n{ohlc_df_for_calc.head()}")
    logger.debug(f"SESSION_DEBUG_DF_DECIMAL: dtypes after Decimal conversion:\n{ohlc_df_for_calc.dtypes}")
    # --- AKHIR DEBUGGING BARU ---

    # --- Perbaikan: Pastikan tidak ada NaN di kolom harga kritis setelah konversi ke Decimal ---
    critical_price_cols = ['open', 'high', 'low', 'close']
    initial_len_before_dropna_decimal = len(ohlc_df_for_calc)
    # Ganti Decimal('NaN') yang mungkin muncul dari to_decimal_or_none jika input bukan numerik
    for col in critical_price_cols:
        if col in ohlc_df_for_calc.columns:
            # Mengganti Decimal('NaN') atau None dengan np.nan untuk penanganan dropna yang tepat
            ohlc_df_for_calc[col] = ohlc_df_for_calc[col].apply(lambda x: x if not isinstance(x, Decimal) or not x.is_nan() else np.nan)

    ohlc_df_for_calc.dropna(subset=critical_price_cols, inplace=True)
    if len(ohlc_df_for_calc) < initial_len_before_dropna_decimal:
        logger.warning(f"Dihapus {initial_len_before_dropna_decimal - len(ohlc_df_for_calc)} baris dengan NaN di kolom harga kritis setelah konversi ke Decimal untuk sesi.")

    if ohlc_df_for_calc.empty:
        logger.warning(f"DataFrame OHLC kosong setelah pembersihan NaN (Decimal) untuk perhitungan sesi. Tidak dapat mengupdate data sesi.")
        with config.MarketData._market_status_data_lock:
            config.MarketData.market_status_data = {
                "session_status": [],
                "overlap_status": "No Valid Candles for Session Calc",
                "current_utc_time": effective_current_datetime.isoformat(),
                "detailed_sessions": []
            }
        return # Keluar lebih awal jika tidak ada data valid setelah pembersihan

    # --- Buat salinan DataFrame untuk `_calculate_sessions_internal` dengan tipe float ---
    ohlc_df_for_smc_calc_float = ohlc_df_for_calc.copy()
    for col in critical_price_cols: # Hanya kolom harga yang perlu float untuk SMC internal
        if col in ohlc_df_for_smc_calc_float.columns:
            ohlc_df_for_smc_calc_float[col] = ohlc_df_for_smc_calc_float[col].apply(utils.to_float_or_none)

    # Pastikan tidak ada NaN setelah konversi ke float juga
    initial_len_before_dropna_float = len(ohlc_df_for_smc_calc_float)
    ohlc_df_for_smc_calc_float.dropna(subset=critical_price_cols, inplace=True)
    if len(ohlc_df_for_smc_calc_float) < initial_len_before_dropna_float:
        logger.warning(f"Dihapus {initial_len_before_dropna_float - len(ohlc_df_for_smc_calc_float)} baris dengan NaN di kolom harga kritis setelah konversi ke float untuk sesi internal.")


    if ohlc_df_for_smc_calc_float.empty:
        logger.warning(f"DataFrame OHLC (float) kosong setelah pembersihan NaN untuk perhitungan sesi internal. Tidak dapat mengupdate data sesi.")
        with config.MarketData._market_status_data_lock:
            config.MarketData.market_status_data = {
                "session_status": [],
                "overlap_status": "No Valid Candles for Session Calc (Float)",
                "current_utc_time": effective_current_datetime.isoformat(),
                "detailed_sessions": []
            }
        return

    # --- DEBUGGING BARU: Periksa DataFrame Float sebelum passing ke _calculate_sessions_internal ---
    logger.debug(f"SESSION_DEBUG_DF_FLOAT_FINAL: Head of ohlc_df_for_smc_calc_float (final for _calculate_sessions_internal):\n{ohlc_df_for_smc_calc_float.head()}")
    logger.debug(f"SESSION_DEBUG_DF_FLOAT_FINAL: dtypes of ohlc_df_for_smc_calc_float:\n{ohlc_df_for_smc_calc_float.dtypes}")
    # --- AKHIR DEBUGGING BARU ---


    active_sessions_names = []
    detailed_sessions = []
    session_metadata_from_db = database_manager.get_session_metadata()

    if not session_metadata_from_db:
        logger.warning("Metadata sesi kosong dari database_manager. Tidak dapat menghitung status sesi.")
        with config.MarketData._market_status_data_lock:
            config.MarketData.market_status_data = {
                "session_status": [],
                "overlap_status": "Error: No Session Meta",
                "current_utc_time": effective_current_datetime.isoformat(),
                "detailed_sessions": []
            }
        return

    for session_meta in session_metadata_from_db:
        session_id = session_meta['id']
        session_name = session_meta['session_name']
        start_hour_utc = session_meta['utc_start_hour']
        end_hour_utc = session_meta['utc_end_hour']

        if target_session_name is not None and session_name != target_session_name:
            continue

        start_time_str_param = f"{start_hour_utc:02d}:00"
        end_time_str_param = f"{end_hour_utc:02d}:00"

        logger.debug(f"Menghitung sesi: {session_name} ({start_time_str_param}-{end_time_str_param} UTC)")

        smc_session_map = {
            "Asia": "Tokyo",
            "Europe": "London",
            "New York": "New York"
        }
        smc_internal_session_name = smc_session_map.get(session_name, "Custom")

        if smc_internal_session_name == "Custom":
            session_results_df = _calculate_sessions_internal(
                ohlc_df_for_smc_calc_float, # Gunakan DataFrame float yang sudah siap
                target_timeframe_str,
                session_name="Custom",
                start_time_str=start_time_str_param,
                end_time_str=end_time_str_param,
                time_zone="UTC"
            )
        else:
            session_results_df = _calculate_sessions_internal(
                ohlc_df_for_smc_calc_float, # Gunakan DataFrame float yang sudah siap
                target_timeframe_str,
                session_name=smc_internal_session_name,
                time_zone="UTC"
            )

        if session_results_df.empty:
            logger.warning(f"Hasil perhitungan sesi kosong untuk {session_name}. Melewatkan.")
            continue

        try:
            # Pastikan ini mengambil lilin terakhir yang <= effective_current_datetime
            relevant_candle_for_status = session_results_df.loc[session_results_df.index <= effective_current_datetime].iloc[-1]
        except IndexError:
            logger.warning(f"Tidak ada lilin yang relevan di session_results_df untuk waktu {effective_current_datetime.isoformat()} dan sesi {session_name}. Melewatkan.")
            continue

        is_active = int(relevant_candle_for_status['Active']) == 1

        # session_high dan session_low dari session_results_df adalah float, konversi ke Decimal untuk akurasi.
        session_high = utils.to_decimal_or_none(relevant_candle_for_status['High'])
        session_low = utils.to_decimal_or_none(relevant_candle_for_status['Low'])

        if is_active:
            active_sessions_names.append(session_name)

        detailed_sessions.append({
            "name": session_name,
            "status": "Open" if is_active else "Closed",
            "start_time_utc": start_time_str_param,
            "end_time_utc": end_time_str_param,
            "session_high": utils.to_float_or_none(session_high),
            "session_low": utils.to_float_or_none(session_low)
        })

        # Penentuan effective start/end time sesi (handle sesi lintas hari)
        session_start_dt_today = effective_current_datetime.replace(hour=start_hour_utc, minute=0, second=0, microsecond=0)
        session_end_dt_today = effective_current_datetime.replace(hour=end_hour_utc, minute=0, second=0, microsecond=0)
        session_start_dt_effective = session_start_dt_today
        session_end_dt_effective = session_end_dt_today

        if start_hour_utc >= end_hour_utc: # Sesi melewati tengah malam
            if effective_current_datetime.hour >= start_hour_utc:
                session_end_dt_effective = session_end_dt_effective + timedelta(days=1)
            else:
                session_start_dt_effective = session_start_dt_effective - timedelta(days=1)

        # Batasi waktu akhir pengambilan tick data hingga waktu saat ini atau akhir sesi (ditambah buffer)
        effective_end_time_for_ticks = min(effective_current_datetime, session_end_dt_effective + timedelta(minutes=config.Trading.MARKET_CLOSE_BUFFER_MINUTES))

        # Cek jika sesi sudah berakhir dan melewati buffer penutupan
        if effective_current_datetime > session_end_dt_effective + timedelta(minutes=config.Trading.MARKET_CLOSE_BUFFER_MINUTES):
             logger.debug(f"Sesi {session_name} sudah berakhir dan buffer waktu penutupannya terlampaui. Melewatkan pembaruan data sesi candle/swing untuk hari ini.")
             continue

        # Hanya proses jika waktu saat ini berada dalam jendela sesi atau buffer setelah sesi
        if session_start_dt_effective <= effective_current_datetime <= (session_end_dt_effective + timedelta(minutes=config.Trading.MARKET_CLOSE_BUFFER_MINUTES)):
            session_ticks = database_manager.get_price_history(
                symbol_param,
                start_time_utc=session_start_dt_effective,
                end_time_utc=effective_end_time_for_ticks
            )

            if not session_ticks:
                logger.warning(f"Tidak ada data tick untuk sesi {session_name} ({session_start_dt_effective} - {effective_end_time_for_ticks}).")
            else:
                session_open_price = utils.to_decimal_or_none(session_ticks[0]['last_price'])
                session_close_price = utils.to_decimal_or_none(session_ticks[-1]['last_price'])
                session_actual_high = max(utils.to_decimal_or_none(t['last_price']) for t in session_ticks if t.get('last_price') is not None)
                session_actual_low = min(utils.to_decimal_or_none(t['last_price']) for t in session_ticks if t.get('last_price') is not None)

                # Cek jika ada harga yang None setelah konversi
                if any(p is None for p in [session_open_price, session_close_price, session_actual_high, session_actual_low]):
                    logger.warning(f"Harga tick tidak valid (None/NaN) untuk sesi {session_name}. Melewatkan penyimpanan candle/swing sesi.")
                    continue

                database_manager.save_session_candle_data(
                    session_id=session_id,
                    trade_date=current_date.isoformat(),
                    candle_time_utc=effective_current_datetime,
                    open_price=session_open_price,
                    high_price=session_actual_high,
                    low_price=session_actual_low,
                    close_price=session_close_price,
                    tick_volume=len(session_ticks),
                    spread=0,
                    real_volume=0,
                    base_candle_name=f"{session_name}_session_ohlc",
                    symbol=symbol_param
                )
                logger.info(f"Candle sesi {session_name} berhasil dikirim ke antrean untuk disimpan untuk {current_date} ({symbol_param}).")

                database_manager.save_session_swing_data(
                    session_id=session_id,
                    trade_date=current_date.isoformat(),
                    swing_high=session_actual_high,
                    swing_high_timestamp_utc=effective_current_datetime,
                    swing_low=session_actual_low,
                    swing_low_timestamp_utc=effective_current_datetime,
                    symbol=symbol_param
                )
                logger.info(f"Data swing sesi {session_name} berhasil dikirim ke antrean untuk disimpan untuk {current_date} ({symbol_param}).")
        else:
            logger.debug(f"Sesi {session_name} saat ini tidak dalam jendela aktif atau buffer penutupan untuk pengumpulan tick data.")


    # Fungsi helper untuk cek overlap
    def is_in_overlap(current_h, current_m, overlap_start_h, overlap_start_m, overlap_end_h, overlap_end_m):
        start_time_total_minutes = overlap_start_h * 60 + overlap_start_m
        end_time_total_minutes = overlap_end_h * 60 + overlap_end_m
        current_time_total_minutes = current_h * 60 + current_m

        if start_time_total_minutes < end_time_total_minutes:
            return start_time_total_minutes <= current_time_total_minutes < end_time_total_minutes
        else: # Overlap melewati tengah malam
            return start_time_total_minutes <= current_time_total_minutes or current_time_total_minutes < end_time_total_minutes

    overlap_status_text = "No Overlap"
    is_asia_open = "Asia" in active_sessions_names
    is_europe_open = "Europe" in active_sessions_names
    is_ny_open = "New York" in active_sessions_names

    asia_start = config.Sessions.ASIA_SESSION_START_HOUR_UTC
    asia_end = config.Sessions.ASIA_SESSION_END_HOUR_UTC
    europe_start = config.Sessions.EUROPE_SESSION_START_HOUR_UTC
    europe_end = config.Sessions.EUROPE_SESSION_END_HOUR_UTC
    ny_start = config.Sessions.NEWYORK_SESSION_START_HOUR_UTC
    ny_end = config.Sessions.NEWYORK_SESSION_END_HOUR_UTC

    current_hour = effective_current_datetime.hour
    current_minute = effective_current_datetime.minute

    if is_asia_open and is_europe_open and \
       is_in_overlap(current_hour, current_minute, europe_start, 0, asia_end, 0):
        overlap_status_text = "Asia-Europe Overlap"
    elif is_europe_open and is_ny_open and \
         is_in_overlap(current_hour, current_minute, ny_start, 0, europe_end, 0):
        overlap_status_text = "Europe-New York Overlap"

    with config.MarketData._market_status_data_lock:
        config.MarketData.market_status_data = {
            "session_status": active_sessions_names,
            "overlap_status": overlap_status_text,
            "current_utc_time": effective_current_datetime.isoformat(),
            "detailed_sessions": detailed_sessions
        }
    logger.info(f"Status Pasar Diperbarui: Sesi Aktif={active_sessions_names}, Overlap={overlap_status_text}")


def _calculate_previous_high_low_internal(ohlc_df_float: pd.DataFrame, higher_timeframe_for_phl: str) -> pd.DataFrame:
    """
    Menghitung Previous High dan Previous Low berdasarkan timeframe yang lebih tinggi.
    Args:
        ohlc_df_float (pd.DataFrame): DataFrame candle (sudah dalam format float, dengan kolom 'open', 'high', 'low', 'close').
        higher_timeframe_for_phl (str): Timeframe yang lebih tinggi untuk menghitung Previous High/Low (misalnya "D1", "H4").
    Returns:
        pd.DataFrame: DataFrame dengan kolom 'PreviousHigh', 'PreviousLow', 'BrokenHigh', 'BrokenLow'.
    """
    logger.debug(f"Menghitung Previous High/Low internal untuk timeframe {higher_timeframe_for_phl} pada {len(ohlc_df_float)} lilin.")

    if ohlc_df_float.empty or len(ohlc_df_float) < 2:
        logger.debug("DataFrame kosong atau tidak cukup lilin untuk menghitung Previous High/Low.")
        return pd.DataFrame(index=ohlc_df_float.index, columns=['PreviousHigh', 'PreviousLow', 'BrokenHigh', 'BrokenLow'])

    # Ambil data dari DataFrame input
    ohlc_len = len(ohlc_df_float)
    _high = ohlc_df_float['high'].values
    _low = ohlc_df_float['low'].values
    _close = ohlc_df_float['close'].values
    _index = ohlc_df_float.index

    # Inisialisasi kolom hasil
    previous_high_values = np.full(ohlc_len, np.nan)
    previous_low_values = np.full(ohlc_len, np.nan)
    broken_high = np.zeros(ohlc_len, dtype=int)
    broken_low = np.zeros(ohlc_len, dtype=int)

    # --- MULAI KODE MODIFIKASI UNTUK MENGAMBIL DAN MENGHITUNG PHL DARI TIMEFRAME LEBIH TINGGI ---

    # Tentukan rentang waktu untuk query timeframe yang lebih tinggi
    # Ambil sedikit lebih banyak data di awal untuk memastikan kita mendapatkan candle "sebelumnya"
    start_time_query = ohlc_df_float.index.min().to_pydatetime() - timedelta(days=7) # Buffer 7 hari
    end_time_query = ohlc_df_float.index.max().to_pydatetime() + timedelta(days=1) # Sedikit buffer di akhir

    logger.debug(f"Mengambil candle historis untuk TF lebih tinggi ({higher_timeframe_for_phl}) dari {start_time_query} hingga {end_time_query}.")
    higher_tf_candles_raw = database_manager.get_historical_candles_from_db(
        ohlc_df_float.symbol.iloc[0], # Ambil simbol dari DataFrame
        higher_timeframe_for_phl,
        start_time_utc=start_time_query,
        end_time_utc=end_time_query,
        order_asc=True,
        limit=None # Ambil semua dalam rentang
    )

    if not higher_tf_candles_raw:
        logger.warning(f"Tidak ada data candle untuk timeframe lebih tinggi ({higher_timeframe_for_phl}) dalam rentang yang diminta. Previous High/Low tidak dapat dihitung.")
        # Kembalikan DataFrame kosong dengan kolom yang diharapkan
        return pd.DataFrame(index=_index, columns=['PreviousHigh', 'PreviousLow', 'BrokenHigh', 'BrokenLow'])

    higher_tf_df = pd.DataFrame(higher_tf_candles_raw)
    higher_tf_df['open_time_utc'] = pd.to_datetime(higher_tf_df['open_time_utc'])
    higher_tf_df = higher_tf_df.set_index('open_time_utc').sort_index()
    
    # Pastikan kolom-kolom di higher_tf_df juga di-rename dan dikonversi ke float
    higher_tf_df = higher_tf_df.rename(columns={
        'open_price': 'open', 'high_price': 'high', 'low_price': 'low',
        'close_price': 'close', 'tick_volume': 'volume'
    })
    for col in ['open', 'high', 'low', 'close', 'volume']:
        if col in higher_tf_df.columns:
            higher_tf_df[col] = higher_tf_df[col].apply(to_float_or_none)

    # Pastikan higher_tf_df tidak kosong setelah pemrosesan
    if higher_tf_df.empty:
        logger.warning(f"DataFrame timeframe lebih tinggi ({higher_timeframe_for_phl}) kosong setelah pemrosesan. Previous High/Low tidak dapat dihitung.")
        return pd.DataFrame(index=_index, columns=['PreviousHigh', 'PreviousLow', 'BrokenHigh', 'BrokenLow'])

    # Gabungkan ohlc_df_float dengan higher_tf_df untuk mendapatkan high/low sebelumnya dari TF yang lebih tinggi
    if ohlc_df_float.index.name is None:
        ohlc_df_float.index.name = 'open_time_utc'
    if higher_tf_df.index.name is None:
        higher_tf_df.index.name = 'open_time_utc'

    merged_df = pd.merge_asof(
        ohlc_df_float, # Tidak perlu reset_index() jika indeks sudah bernama
        higher_tf_df[['high', 'low']].rename(columns={'high': 'prev_higher_tf_high', 'low': 'prev_higher_tf_low'}), # Tidak perlu reset_index()
        left_index=True, # Gunakan indeks kiri sebagai kunci merge
        right_index=True, # Gunakan indeks kanan sebagai kunci merge
        direction='backward' 
    )

    # Isi previous_high_values dan previous_low_values
    # Jika tidak ada candle TF lebih tinggi sebelumnya, akan ada NaN.
    previous_high_values = merged_df['prev_higher_tf_high'].values
    previous_low_values = merged_df['prev_higher_tf_low'].values

    # --- AKHIR KODE MODIFIKASI UNTUK MENGAMBIL DAN MENGHITUNG PHL DARI TIMEFRAME LEBIH TINGGI ---

    # --- Deteksi Broken High/Low ---
    for i in range(ohlc_len): 
        current_high = _high[i]
        current_low = _low[i]
        current_close = _close[i] # Diperlukan untuk konfirmasi break
        
        prev_high = previous_high_values[i] # Gunakan nilai dari TF lebih tinggi
        prev_low = previous_low_values[i]   # Gunakan nilai dari TF lebih tinggi

        # Pastikan prev_high/prev_low bukan NaN sebelum perbandingan
        if np.isnan(prev_high) or np.isnan(prev_low):
            continue # Lewati jika tidak ada Previous High/Low dari TF lebih tinggi

        # Deteksi Broken High
        # Jika current_high menembus prev_high DAN current_close di atas prev_high (konfirmasi)
        if current_high > prev_high and current_close > prev_high: 
            broken_high[i] = 1 
        
        # Deteksi Broken Low
        # Jika current_low menembus prev_low DAN current_close di bawah prev_low (konfirmasi)
        if current_low < prev_low and current_close < prev_low:
            broken_low[i] = 1 

    # Buat DataFrame hasil
    results_df = pd.DataFrame({
        'PreviousHigh': previous_high_values, # Gunakan nilai dari TF lebih tinggi
        'PreviousLow': previous_low_values,   # Gunakan nilai dari TF lebih tinggi
        'BrokenHigh': broken_high,
        'BrokenLow': broken_low
    }, index=_index)

    return results_df






def _calculate_swing_highs_lows_internal(ohlc_df_float: pd.DataFrame, swing_length: int = 5) -> pd.DataFrame:
    """
    Menghitung Swing Highs dan Swing Lows berdasarkan pola "fractal" (harga tertinggi/terendah diapit oleh lilin-lilin
    yang lebih rendah/tinggi di kedua sisinya). Digunakan sebagai dasar untuk mendeteksi Market Structure dan level lainnya.
    Args:
        ohlc_df_float (pd.DataFrame): DataFrame candle yang sudah diformat ke float,
                                      dengan kolom 'high' dan 'low', dan indeks datetime.
        swing_length (int): Jumlah lilin di setiap sisi yang lebih rendah/tinggi dari lilin pusat
                            untuk mengkonfirmasi Swing High/Low.
    Returns:
        pd.DataFrame: DataFrame dengan indeks yang sama seperti input, berisi kolom 'HighLow' (1 untuk Swing High,
                      -1 untuk Swing Low, NaN jika bukan swing) dan 'Level' (harga swing).
    """
    logger.debug(f"SWING_DIAG: Menghitung Swing Highs/Low internal dengan swing_length={swing_length} pada {len(ohlc_df_float)} lilin.")

    # Pastikan swing_length adalah integer yang valid
    swing_length = int(swing_length)
    # Penting: swing_length harus setidaknya 1 agar ada lilin di setiap sisi
    if swing_length <= 0:
        logger.warning(f"SWING_DIAG: swing_length harus lebih besar dari 0. Menggunakan nilai default 1.")
        swing_length = 1

    # Lakukan pembersihan NaN pada DataFrame input.
    initial_len_input = len(ohlc_df_float)
    ohlc_df_float_cleaned = ohlc_df_float.dropna(subset=['high', 'low', 'open', 'close']).copy()
    if len(ohlc_df_float_cleaned) < initial_len_input:
        logger.warning(f"SWING_DIAG: Dihapus {initial_len_input - len(ohlc_df_float_cleaned)} baris dengan NaN di kolom OHLC. Sisa lilin: {len(ohlc_df_float_cleaned)}.")

    # Pastikan ada cukup data setelah pembersihan untuk perhitungan swing.
    # Membutuhkan setidaknya 2 * swing_length + 1 lilin untuk jendela lengkap.
    if ohlc_df_float_cleaned.empty or len(ohlc_df_float_cleaned) < (2 * swing_length + 1):
        logger.debug(f"SWING_DIAG: DataFrame kosong atau tidak cukup lilin ({len(ohlc_df_float_cleaned)}) setelah pembersihan NaN untuk menghitung Swing Highs/Lows dengan swing_length {swing_length}. Mengembalikan DataFrame kosong.")
        return pd.DataFrame(index=ohlc_df_float.index, columns=['HighLow', 'Level'])

    # Ekstrak data harga sebagai array NumPy dari DataFrame yang sudah dibersihkan.
    _high = ohlc_df_float_cleaned['high'].values
    _low = ohlc_df_float_cleaned['low'].values
    _index = ohlc_df_float_cleaned.index
    ohlc_len = len(ohlc_df_float_cleaned)

    logger.debug(f"SWING_DIAG: Input _high head: {_high[:5]}, tail: {_high[-5:]}")
    logger.debug(f"SWING_DIAG: Input _low head: {_low[:5]}, tail: {_low[-5:]}")

    # Inisialisasi array untuk menyimpan hasil swing (1/-1) dan level harga.
    swing_highs_lows_values = np.full(ohlc_len, np.nan, dtype=float)
    level_values = np.full(ohlc_len, np.nan, dtype=float)

    # MODIFIKASI KRITIS FINAL: Perbaiki operator perbandingan untuk deteksi swing fractal.
    # Lilin pusat (i) harus LEBIH TINGGI KETAT / RENDAH KETAT dari tetangga.
    for i in range(swing_length, ohlc_len - swing_length):
        is_sh_candidate = True
        is_sl_candidate = True

        # Cek untuk Swing High: high[i] harus lebih tinggi KETAT dari swing_length lilin di kiri dan kanan
        for j in range(1, swing_length + 1):
            if _high[i] <= _high[i - j] or _high[i] <= _high[i + j]: # <-- PERBAIKAN: Ini tetap <=
                                                                      # Artinya, jika high[i] TIDAK lebih besar ketat dari tetangga, maka dibatalkan
                is_sh_candidate = False
                break

        # Cek untuk Swing Low: low[i] harus lebih rendah KETAT dari swing_length lilin di kiri dan kanan
        for j in range(1, swing_length + 1):
            if _low[i] >= _low[i - j] or _low[i] >= _low[i + j]: # <-- PERBAIKAN: Ini tetap >=
                                                                  # Artinya, jika low[i] TIDAK lebih kecil ketat dari tetangga, maka dibatalkan
                is_sl_candidate = False
                break

        # Terapkan mutual exclusivity: sebuah lilin hanya bisa menjadi SH atau SL, bukan keduanya.
        if is_sh_candidate and not is_sl_candidate:
            swing_highs_lows_values[i] = 1
            level_values[i] = _high[i]
        elif is_sl_candidate and not is_sh_candidate:
            swing_highs_lows_values[i] = -1
            level_values[i] = _low[i]

    swing_series_candidates = pd.Series(swing_highs_lows_values, index=_index, name="HighLow")
    level_series_candidates = pd.Series(level_values, index=_index, name="Level")
    
    swing_points_candidates_df = pd.concat([swing_series_candidates, level_series_candidates], axis=1).dropna(subset=['HighLow', 'Level'])

    logger.debug(f"SWING_DIAG: Total initial swing candidates: {len(swing_points_candidates_df)}")
    
    global _SYMBOL_POINT_VALUE
    symbol_point_value_for_calc = _SYMBOL_POINT_VALUE

    if symbol_point_value_for_calc is None or (isinstance(symbol_point_value_for_calc, Decimal) and symbol_point_value_for_calc == Decimal('0.0')):
        logger.error(f"SWING_DIAG: _SYMBOL_POINT_VALUE tidak valid ({symbol_point_value_for_calc}). Menggunakan fallback Decimal('0.001').")
        symbol_point_value_for_calc = Decimal('0.001')

    min_price_movement_for_swing = float(symbol_point_value_for_calc) * 2.0
    
    meaningful_swings = []

    if not swing_points_candidates_df.empty:
        for idx, row in swing_points_candidates_df.iterrows():
            loc_result = _index.get_loc(idx)
            if isinstance(loc_result, (int, np.integer)):
                current_idx_pos = int(loc_result)
            elif isinstance(loc_result, np.ndarray) and loc_result.size == 1:
                current_idx_pos = int(loc_result.item())
            else:
                logger.warning(f"SWING_DIAG: get_loc untuk indeks {idx} mengembalikan tipe tidak terduga: {type(loc_result)}. Melewatkan lilin ini.")
                continue

            check_start = max(0, current_idx_pos - swing_length)
            check_end = min(ohlc_len, current_idx_pos + swing_length + 1)
            
            if check_start >= check_end:
                logger.debug(f"SWING_DIAG: Jendela pemeriksaan tidak valid untuk idx {idx}.")
                continue

            window_high = np.max(_high[check_start:check_end])
            window_low = np.min(_low[check_start:check_end])
            
            if (window_high - window_low) <= min_price_movement_for_swing:
                logger.debug(f"SWING_DIAG: Kandidat swing {idx} ({row['HighLow']}, Level: {row['Level']}) dilewati. Rentang jendela ({window_high - window_low:.5f}) terlalu kecil (min: {min_price_movement_for_swing:.5f}).")
                continue
            
            meaningful_swings.append({'index': idx, 'type': row['HighLow'], 'level': row['Level']})
    
    logger.debug(f"SWING_DIAG: Total meaningful swing points after initial filter: {len(meaningful_swings)}")

    final_swing_highs_lows = np.full(ohlc_len, np.nan, dtype=float)
    final_level = np.full(ohlc_len, np.nan, dtype=float)

    if meaningful_swings:
        filtered_alternating_swings = []
        last_type = None
        last_level = None

        for sp in meaningful_swings:
            current_type = sp['type']
            current_level = sp['level']
            current_idx = sp['index']

            if last_type is None:
                filtered_alternating_swings.append({'index': current_idx, 'type': current_type, 'level': current_level})
            elif current_type != last_type:
                filtered_alternating_swings.append({'index': current_idx, 'type': current_type, 'level': current_level})
            else:
                if current_type == 1:
                    if current_level > last_level:
                        filtered_alternating_swings[-1] = {'index': current_idx, 'type': current_type, 'level': current_level}
                        last_level = current_level
                else:
                    if current_level < last_level:
                        filtered_alternating_swings[-1] = {'index': current_idx, 'type': current_type, 'level': current_level}
                        last_level = current_level
            
            if filtered_alternating_swings:
                last_type = filtered_alternating_swings[-1]['type']
                last_level = filtered_alternating_swings[-1]['level']

        logger.debug(f"SWING_DIAG: filtered_alternating_swings after alternating logic (first 5): {filtered_alternating_swings[:5]}")
        logger.debug(f"SWING_DIAG: Total swing points after alternating logic: {len(filtered_alternating_swings)}")

        for sp in filtered_alternating_swings:
            try:
                original_idx_pos = _index.get_loc(sp['index'])
                if isinstance(original_idx_pos, np.ndarray) and original_idx_pos.size == 1:
                    original_idx_pos = original_idx_pos.item()
                
                final_swing_highs_lows[original_idx_pos] = sp['type']
                final_level[original_idx_pos] = sp['level']
            except KeyError:
                logger.warning(f"SWING_DIAG: Indeks {sp['index']} dari filtered_alternating_swings tidak ditemukan di ohlc_df_float_cleaned.index. Melewatkan.")
                continue

    results_df = pd.concat(
        [
            pd.Series(final_swing_highs_lows, index=_index, name="HighLow"),
            pd.Series(final_level, index=_index, name="Level"),
        ],
        axis=1,
    )
    logger.debug(f"SWING_DIAG: Final Swing Results DF (head):\n{results_df.head()}")
    logger.debug(f"SWING_DIAG: Final Swing Results DF (tail):\n{results_df.tail()}")
    logger.debug(f"SWING_DIAG: Total final swing points (non-NaN): {len(results_df.dropna())}")
    
    return results_df.reindex(ohlc_df_float.index)


def _calculate_retracements_internal(ohlc_df_decimal: pd.DataFrame, swing_highs_lows_df: pd.DataFrame) -> pd.DataFrame:
    logger.debug(f"RETRACEMENT_DEBUG_VERSION_CHECK: Running _calculate_retracements_internal - Version 20250715_FixRetracementCalc.")

    if ohlc_df_decimal.empty or swing_highs_lows_df.empty:
        logger.debug("DataFrame candle atau swing highs/lows kosong, tidak dapat menghitung retracement.")
        return pd.DataFrame(index=ohlc_df_decimal.index, columns=['Direction', 'CurrentRetracement%', 'DeepestRetracement%', 'HighRef', 'LowRef', 'StartRefTime', 'EndRefTime'])

    # Pastikan kolom high dan low di ohlc_df_decimal diubah ke float untuk perhitungan NumPy/rasio
    _high = ohlc_df_decimal["high"].apply(utils.to_float_or_none).values
    _low = ohlc_df_decimal["low"].apply(utils.to_float_or_none).values
    _index = ohlc_df_decimal.index

    # Ambil nilai dari DataFrame swing
    swing_hl_values = swing_highs_lows_df["HighLow"].values
    swing_level_values = swing_highs_lows_df["Level"].values

    ohlc_len_int = len(ohlc_df_decimal)
    
    if not isinstance(ohlc_len_int, int):
        try:
            ohlc_len_int = int(ohlc_len_int)
            logger.warning(f"RETRACEMENT_DEBUG: ohlc_len_int dikonversi dari {type(ohlc_len_int)} ke int: {ohlc_len_int}")
        except ValueError:
            logger.error(f"RETRACEMENT_DEBUG: Gagal mengonversi panjang DataFrame ke integer yang valid: {ohlc_len_int}. Melewatkan perhitungan retracement.", exc_info=True)
            return pd.DataFrame(index=ohlc_df_decimal.index, columns=['Direction', 'CurrentRetracement%', 'DeepestRetracement%', 'HighRef', 'LowRef', 'StartRefTime', 'EndRefTime'])

    if ohlc_len_int <= 0:
        logger.warning(f"RETRACEMENT_DEBUG: Panjang ohlc_df_decimal adalah {ohlc_len_int}. Melewatkan perhitungan retracement.")
        return pd.DataFrame(index=ohlc_df_decimal.index, columns=['Direction', 'CurrentRetracement%', 'DeepestRetracement%', 'HighRef', 'LowRef', 'StartRefTime', 'EndRefTime'])

    # MODIFIKASI: Inisialisasi array hasil dengan NaN (sesuai ekspektasi tes)
    direction = np.full(ohlc_len_int, np.nan, dtype=np.float64)
    current_retracement = np.full(ohlc_len_int, np.nan, dtype=np.float64)
    deepest_retracement = np.full(ohlc_len_int, np.nan, dtype=np.float64)

    # Inisialisasi variabel untuk melacak swing pair yang aktif
    last_processed_sh_time = pd.NaT
    last_processed_sh_level = np.nan
    last_processed_sl_time = pd.NaT
    last_processed_sl_level = np.nan

    # Ini adalah variabel yang akan digunakan untuk perhitungan retracement pada setiap lilin
    current_leg_high_ref = np.nan
    current_leg_low_ref = np.nan
    current_leg_start_time = pd.NaT
    current_leg_end_time = pd.NaT
    current_leg_direction = 0 # 1 for bullish leg (SL->SH), -1 for bearish leg (SH->SL)


    start_time_ref_values = np.full(ohlc_len_int, np.datetime64('NaT'), dtype='datetime64[ns]')
    end_time_ref_values = np.full(ohlc_len_int, np.datetime64('NaT'), dtype='datetime64[ns]')
    active_top_series_values = np.full(ohlc_len_int, np.nan, dtype=np.float64)
    active_bottom_series_values = np.full(ohlc_len_int, np.nan, dtype=np.float64)


    for i in range(ohlc_len_int):
        current_candle_time = ohlc_df_decimal.index[i]
        
        # Update last detected swing points
        if pd.notna(swing_hl_values[i]):
            swing_type = swing_hl_values[i]
            swing_level = swing_level_values[i]
            swing_time = ohlc_df_decimal.index[i]

            if swing_type == 1: # Current bar is a Swing High
                last_processed_sh_level = swing_level
                last_processed_sh_time = swing_time
            elif swing_type == -1: # Current bar is a Swing Low
                last_processed_sl_level = swing_level
                last_processed_sl_time = swing_time
        
        # Determine the current active leg for retracement calculation
        # A leg is defined by two alternating swings.
        # We look for the most recent valid SH-SL or SL-SH pair.

        # Case 1: Bullish leg (price moving up from SL to SH)
        # Requires: A valid SL, followed by a valid SH, where SL time < SH time
        if pd.notna(last_processed_sl_level) and pd.notna(last_processed_sh_level) and \
           last_processed_sl_time < last_processed_sh_time:
            current_leg_high_ref = last_processed_sh_level
            current_leg_low_ref = last_processed_sl_level
            current_leg_start_time = last_processed_sl_time
            current_leg_end_time = last_processed_sh_time
            current_leg_direction = 1 # Bullish leg

        # Case 2: Bearish leg (price moving down from SH to SL)
        # Requires: A valid SH, followed by a valid SL, where SH time < SL time
        elif pd.notna(last_processed_sh_level) and pd.notna(last_processed_sl_level) and \
             last_processed_sh_time < last_processed_sl_time:
            current_leg_high_ref = last_processed_sh_level
            current_leg_low_ref = last_processed_sl_level
            current_leg_start_time = last_processed_sh_time
            current_leg_end_time = last_processed_sl_time
            current_leg_direction = -1 # Bearish leg
        else: # No valid active leg yet, or swings are not alternating correctly
            current_leg_high_ref = np.nan
            current_leg_low_ref = np.nan
            current_leg_start_time = pd.NaT
            current_leg_end_time = pd.NaT
            current_leg_direction = 0

        # Store the leg info for the current bar (even if NaN for calculation)
        # MODIFIKASI: Direction diatur ke NaN jika tidak ada active leg
        if current_leg_direction == 0:
            direction[i] = np.nan # Jika tidak ada active leg, set Direction ke NaN
        else:
            direction[i] = current_leg_direction

        start_time_ref_values[i] = pd.to_datetime(current_leg_start_time, utc=True, errors='coerce').to_numpy() # Pastikan ini datetime
        end_time_ref_values[i] = pd.to_datetime(current_leg_end_time, utc=True, errors='coerce').to_numpy() # Pastikan ini datetime
        active_top_series_values[i] = current_leg_high_ref
        active_bottom_series_values[i] = current_leg_low_ref

        # If no active leg, or zero range, skip retracement calculation
        if pd.isna(current_leg_high_ref) or pd.isna(current_leg_low_ref) or current_leg_high_ref == current_leg_low_ref:
            current_retracement[i] = np.nan
            deepest_retracement[i] = np.nan
            continue # Continue to next bar, no retracement calc for this bar

        # Perform retracement calculation using current_leg_high_ref and current_leg_low_ref
        top_float = float(current_leg_high_ref)
        bottom_float = float(current_leg_low_ref)
        total_swing_range = top_float - bottom_float

        if total_swing_range == 0: # Defensive check for division by zero
            current_retracement[i] = np.nan
            deepest_retracement[i] = np.nan
            continue

        # Re-evaluate CurrentRetracement% based on current_leg_direction
        current_candle_high_float = utils.to_float_or_none(ohlc_df_decimal['high'].iloc[i])
        current_candle_low_float = utils.to_float_or_none(ohlc_df_decimal['low'].iloc[i])

        # Pastikan harga lilin saat ini valid
        if current_candle_high_float is None or current_candle_low_float is None:
            current_retracement[i] = np.nan
            deepest_retracement[i] = np.nan
            continue

        if current_leg_direction == 1: # Bullish leg (SL->SH), price retracing downwards from SH
            # Retracement = 100 - ((current_low - SL) / (SH - SL) * 100)
            raw_ratio = (current_candle_low_float - bottom_float) / total_swing_range
            current_retracement[i] = round(float(100 - (raw_ratio * 100)), 1)
            # Update deepest_retracement for bullish leg (max of previous and current)
            prev_deepest_val = deepest_retracement[i - 1] if i > 0 and direction[i-1] == current_leg_direction else np.nan
            deepest_retracement[i] = max(prev_deepest_val, current_retracement[i]) if pd.notna(prev_deepest_val) else current_retracement[i]

        elif current_leg_direction == -1: # Bearish leg (SH->SL), price retracing upwards from SL
            # Retracement = ((current_high - SL) / (SH - SL) * 100)
            raw_ratio = (current_candle_high_float - bottom_float) / total_swing_range
            current_retracement[i] = round(float(raw_ratio * 100), 1)
            # Update deepest_retracement for bearish leg (max of previous and current)
            prev_deepest_val = deepest_retracement[i - 1] if i > 0 and direction[i-1] == current_leg_direction else np.nan
            deepest_retracement[i] = max(prev_deepest_val, current_retracement[i]) if pd.notna(prev_deepest_val) else current_retracement[i]
        else: # current_leg_direction is 0 or invalid, should already be handled by `if pd.isna(current_leg_high_ref)`
            current_retracement[i] = np.nan
            deepest_retracement[i] = np.nan


    # shift the arrays by 1 (ini untuk menyelaraskan dengan logika SMC asli)
    # MODIFIKASI: HAPUS np.roll()
    # if ohlc_len_int > 0:
    #     current_retracement = np.roll(current_retracement, 1)
    #     deepest_retracement = np.roll(deepest_retracement, 1)
    #     direction = np.roll(direction, 1)
    #     active_top_series_values = np.roll(active_top_series_values, 1)
    #     active_bottom_series_values = np.roll(active_bottom_series_values, 1)
    #     start_time_ref_values = np.roll(start_time_ref_values, 1)
    #     end_time_ref_values = np.roll(end_time_ref_values, 1)


    # Buat DataFrame hasil
    results_df = pd.concat([
        pd.Series(direction, index=_index, name="Direction"),
        pd.Series(current_retracement, index=_index, name="CurrentRetracement%"),
        pd.Series(deepest_retracement, index=_index, name="DeepestRetracement%"),
        pd.Series(active_top_series_values, index=_index, name="HighRef"),
        pd.Series(active_bottom_series_values, index=_index, name="LowRef"),
        pd.Series(start_time_ref_values, index=_index, name="StartRefTime"),
        pd.Series(end_time_ref_values, index=_index, name="EndRefTime")
    ], axis=1)

    return results_df




def _calculate_volume_profile_internal(df_interval_ohlcv: pd.DataFrame, df_interval_ticks: pd.DataFrame | None, row_height: float, distribution: DistributionData) -> tuple:
    """
    Menghitung Volume Profile untuk satu interval, mengadaptasi logika dari VolumeProfile._create_vp.
    Mengembalikan tuple dari DataFrames: (normal_df, buy_sell_df, delta_df).
    """
    logger.debug(f"DEBUG _calculate_volume_profile_internal: Memulai dengan row_height={row_height} (Type: {type(row_height)})")

    # Handle empty OHLCV DataFrame early
    if df_interval_ohlcv.empty:
        logger.warning("DEBUG _calculate_volume_profile_internal: DataFrame OHLCV input kosong.")
        empty_df_columns = ['vp_datetime', 'vp_prices', 'vp_normal', 'vp_normal_total'] # Example columns
        return (pd.DataFrame(columns=empty_df_columns),
                pd.DataFrame(columns=[]), # Buy/Sell
                pd.DataFrame(columns=[])) # Delta

    # Ensure OHLCV columns are Decimal and fill any NaNs with 0.0 for calculations
    # Kolom 'volume' seharusnya sudah ada di sini sebagai Decimal karena disiapkan oleh pemanggil (update_volume_profiles)
    for col in ['open', 'high', 'low', 'close', 'volume']: # <-- Pastikan 'volume' ada di loop ini
        if col in df_interval_ohlcv.columns:
            df_interval_ohlcv[col] = df_interval_ohlcv[col].apply(utils.to_decimal_or_none)
            df_interval_ohlcv[col] = df_interval_ohlcv[col].fillna(Decimal('0.0'))
        else:
            # Ini hanya sebagai fallback, idealnya 'volume' sudah ada
            df_interval_ohlcv[col] = Decimal('0.0')
            if col == 'volume':
                logger.error(f"DEBUG _calculate_volume_profile_internal: Kolom 'volume' tidak ditemukan di df_interval_ohlcv. Menggunakan volume nol. Periksa pemanggil.")


    interval_lowest = df_interval_ohlcv['low'].min()
    interval_highest = df_interval_ohlcv['high'].max()
    interval_open = df_interval_ohlcv['open'].iat[0]
    logger.debug(f"DEBUG _calculate_volume_profile_internal: interval_lowest={interval_lowest} (Type: {type(interval_lowest)})")
    logger.debug(f"DEBUG _calculate_volume_profile_internal: interval_highest={interval_highest} (Type: {type(interval_highest)})")
    logger.debug(f"DEBUG _calculate_volume_profile_internal: interval_open={interval_open} (Type: {type(interval_open)})")

    interval_segments = []
    
    decimal_row_height = utils.to_decimal_or_none(row_height)
    if decimal_row_height is None or decimal_row_height == Decimal('0'):
        logger.error("DEBUG _calculate_volume_profile_internal: row_height invalid or zero. Returning empty DataFrames.")
        empty_df_columns = ['vp_datetime', 'vp_prices', 'vp_normal', 'vp_normal_total']
        return (pd.DataFrame(columns=empty_df_columns),
                pd.DataFrame(columns=[]),
                pd.DataFrame(columns=[]))

    prev_segment = interval_open
    while prev_segment >= (interval_lowest - decimal_row_height): 
        interval_segments.append(prev_segment)
        prev_segment = (prev_segment - decimal_row_height) 
    prev_segment = interval_open
    while prev_segment <= (interval_highest + decimal_row_height):
        interval_segments.append(prev_segment)
        prev_segment = (prev_segment + decimal_row_height)
    
    interval_segments = sorted(list(set(interval_segments)))

    interval_segments_float = [float(d) for d in interval_segments]
    logger.debug(f"DEBUG _calculate_volume_profile_internal: interval_segments_float[0]={interval_segments_float[0]} (Type: {type(interval_segments_float[0])})")
    logger.debug(f"DEBUG _calculate_volume_profile_internal: len(interval_segments_float)={len(interval_segments_float)}")


    normal = {'datetime': [], 'prices': [], 'values': [], 'total_value': Decimal('0.0')} 
    buy_sell = {'datetime': [], 'prices': [], 'vp_buy': [], 'vp_sell': [],
                'value_buy': Decimal('0.0'), 'value_sell': Decimal('0.0'),
                'value_sum': Decimal('0.0'), 'value_subtract': Decimal('0.0'), 'value_divide': Decimal('0.0')} 
    delta = {'datetime': [], 'prices': [], 'values': [],
             'total_delta': Decimal('0.0'), 'min_delta': Decimal('0.0'), 'max_delta': Decimal('0.0')} 
    normal['datetime'] = [df_interval_ohlcv.index.min()] * len(interval_segments)
    normal['prices'] = interval_segments
    buy_sell['datetime'] = [df_interval_ohlcv.index.min()] * len(interval_segments)
    buy_sell['prices'] = interval_segments
    delta['datetime'] = [df_interval_ohlcv.index.min()] * len(interval_segments)
    delta['prices'] = interval_segments
    
    normal['values'] = [Decimal('0.0')] * len(interval_segments)
    buy_sell['vp_buy'] = [Decimal('0.0')] * len(interval_segments)
    buy_sell['vp_sell'] = [Decimal('0.0')] * len(interval_segments)
    delta['values'] = [Decimal('0.0')] * len(interval_segments)
    logger.debug(f"DEBUG _calculate_volume_profile_internal: Initial normal['values'][0]={normal['values'][0]} (Type: {type(normal['values'][0])})")


    def _add_volume(index: int, volume_i: Decimal, is_up_i: bool):
        logger.debug(f"DEBUG _add_volume: Adding volume_i={volume_i} (Type: {type(volume_i)}) to index {index}")
        normal['values'][index] = normal['values'][index] + volume_i
        logger.debug(f"DEBUG _add_volume: normal['values'][{index}] after add={normal['values'][index]} (Type: {type(normal['values'][index])})")

        buy_sell['vp_buy' if is_up_i else 'vp_sell'][index] = \
            buy_sell['vp_buy' if is_up_i else 'vp_sell'][index] + volume_i
        logger.debug(f"DEBUG _add_volume: buy_sell['vp_buy/sell'][{index}] after add={buy_sell['vp_buy' if is_up_i else 'vp_sell'][index]} (Type: {type(buy_sell['vp_buy' if is_up_i else 'vp_sell'][index])})")


        buy = buy_sell['vp_buy'][index]
        sell = buy_sell['vp_sell'][index]
        
        delta['values'][index] = buy - sell 
        logger.debug(f"DEBUG _add_volume: delta['values'][{index}] after add={delta['values'][index]} (Type: {type(delta['values'][index])})")

        if delta['values'][index] < delta['min_delta']: 
            delta['min_delta'] = delta['values'][index]
            logger.debug(f"DEBUG _add_volume: Updated min_delta={delta['min_delta']}")
        if delta['values'][index] > delta['max_delta']: 
            delta['max_delta'] = delta['values'][index]
            logger.debug(f"DEBUG _add_volume: Updated max_delta={delta['max_delta']}")
    
    # Tick_By_Tick logic
    if df_interval_ticks is not None and not df_interval_ticks.empty and distribution == utils.DistributionData.Tick_By_Tick:
        calculate_len = len(df_interval_ticks)
        for i in range(calculate_len):
            tick = utils.to_decimal_or_none(df_interval_ticks['close'].iat[i])
            prev_tick = utils.to_decimal_or_none(df_interval_ticks['close'].iat[i - 1]) if i > 0 else tick 
            
            if tick is None or prev_tick is None: 
                continue
            
            logger.debug(f"DEBUG _calculate_volume_profile_internal: Tick {i}: tick={tick} (Type: {type(tick)}), prev_tick={prev_tick} (Type: {type(prev_tick)})")

            idx = np.searchsorted(interval_segments_float, float(tick), side='left')
            if idx == len(interval_segments_float):
                idx = len(interval_segments_float) - 1
            elif idx > 0 and abs(interval_segments_float[idx] - float(tick)) > abs(interval_segments_float[idx-1] - float(tick)):
                idx = idx -1

            if idx < 0 or idx >= len(interval_segments):
                logger.warning(f"Tick price {tick} out of segment range. Skipping.") # Updated log message
                continue

            _add_volume(idx, Decimal('1.0'), tick > prev_tick) # Tick volume is usually 1 for each tick

            buy = buy_sell['vp_buy'][idx]
            sell = buy_sell['vp_sell'][idx]
            delta['values'][idx] = buy - sell 

            if delta['values'][idx] < delta['min_delta']:
                delta['min_delta'] = delta['values'][idx]
            if delta['values'][idx] > delta['max_delta']:
                delta['max_delta'] = delta['values'][idx]
    # This block handles OHLCV data distribution
    else: 
        calculate_len = len(df_interval_ohlcv)
        for i in range(calculate_len):
            open_p = df_interval_ohlcv['open'].iat[i]
            high_p = df_interval_ohlcv['high'].iat[i]
            low_p = df_interval_ohlcv['low'].iat[i]
            close_p = df_interval_ohlcv['close'].iat[i]
            volume = df_interval_ohlcv['volume'].iat[i] # <-- AKSES KOLOM 'volume' YANG SUDAH TERPILIH
            
            if None in [open_p, high_p, low_p, close_p, volume]: 
                continue

            is_up = close_p >= open_p
            logger.debug(f"DEBUG _calculate_volume_profile_internal: OHLCV Bar {i}: volume={volume} (Type: {type(volume)})")
            
            price_range_candle = (high_p - low_p)
            
            # Define avg_vol and vol_per_bin_for_ohlc_no_avg before distribution checks
            avg_vol = Decimal('0.0') 
            if price_range_candle == Decimal('0'): 
                avg_vol = volume
            else:
                avg_vol = volume / (price_range_candle / decimal_row_height)

            num_bins_in_candle = Decimal('0')
            if decimal_row_height > Decimal('0'):
                num_bins_in_candle = (high_p - low_p) / decimal_row_height
            
            vol_per_bin_for_ohlc_no_avg = Decimal('0.0') 
            if num_bins_in_candle == Decimal('0'):
                vol_per_bin_for_ohlc_no_avg = volume
            else:
                vol_per_bin_for_ohlc_no_avg = volume / num_bins_in_candle


            if distribution == utils.DistributionData.OHLC or distribution == utils.DistributionData.OHLC_No_Avg:
                logger.debug(f"DEBUG _calculate_volume_profile_internal: Distribution OHLC/OHLC_No_Avg: avg_vol={avg_vol} (Type: {type(avg_vol)})")
                
                if distribution == utils.DistributionData.OHLC_No_Avg:
                    # FIX: Correctly handle zero-range candle for OHLC_No_Avg
                    if price_range_candle == Decimal('0'): 
                        idx = np.searchsorted(interval_segments_float, float(close_p), side='left')
                        if idx == len(interval_segments_float): idx = len(interval_segments_float) - 1
                        elif idx > 0 and abs(interval_segments_float[idx] - float(close_p)) > abs(interval_segments_float[idx-1] - float(close_p)): idx = idx -1
                        if idx >= 0 and idx < len(interval_segments):
                            _add_volume(idx, volume, is_up) # Add full volume to this single bin
                    else: # Original OHLC_No_Avg logic for non-zero range
                        for idx in range(len(interval_segments)):
                            row_price = interval_segments[idx]
                            bin_bottom = row_price - (decimal_row_height / Decimal('2'))
                            bin_top = row_price + (decimal_row_height / Decimal('2'))

                            if (max(low_p, bin_bottom) < min(high_p, bin_top)):
                                 _add_volume(idx, vol_per_bin_for_ohlc_no_avg, is_up)


                else: # DistributionData.OHLC
                    # FIX: Correctly handle zero-range candle for OHLC
                    if price_range_candle == Decimal('0'): 
                        idx_for_zero_range = np.searchsorted(interval_segments_float, float(close_p), side='left')
                        if idx_for_zero_range == len(interval_segments_float): idx_for_zero_range = len(interval_segments_float) - 1
                        elif idx_for_zero_range > 0 and abs(interval_segments_float[idx_for_zero_range] - float(close_p)) > abs(interval_segments_float[idx_for_zero_range-1] - float(close_p)): idx_for_zero_range = idx_for_zero_range -1
                        if idx_for_zero_range >= 0 and idx_for_zero_range < len(interval_segments):
                            _add_volume(idx_for_zero_range, volume, is_up) # Add full volume to this single bin
                    else: # Original OHLC logic for non-zero range
                        for idx in range(len(interval_segments)):
                            row_price = interval_segments[idx]
                            bin_bottom_price = row_price - (decimal_row_height / Decimal('2'))
                            bin_top_price = row_price + (decimal_row_height / Decimal('2'))

                            overlap_bottom = max(low_p, bin_bottom_price)
                            overlap_top = min(high_p, bin_top_price)
                            
                            overlap_range = Decimal('0')
                            if overlap_top > overlap_bottom:
                                overlap_range = overlap_top - overlap_bottom
                            if price_range_candle > Decimal('0'):
                                proportional_volume = volume * (overlap_range / price_range_candle)
                                _add_volume(idx, proportional_volume, is_up)
                            # else: This case is already covered by the specific zero-range handling above


            elif distribution == utils.DistributionData.Open or distribution == utils.DistributionData.High or \
                    distribution == utils.DistributionData.Low or distribution == utils.DistributionData.Close:
                target_price = None

                if distribution == utils.DistributionData.Open:
                    target_price = open_p
                elif distribution == utils.DistributionData.High:
                    target_price = high_p
                elif distribution == utils.DistributionData.Low:
                    target_price = low_p
                else: 
                    target_price = close_p
                
                if target_price is not None:
                    idx = np.searchsorted(interval_segments_float, float(target_price), side='left')
                    if idx == len(interval_segments_float):
                        idx = len(interval_segments_float) - 1
                    elif idx > 0 and abs(interval_segments_float[idx] - float(target_price)) > abs(interval_segments_float[idx-1] - float(target_price)):
                        idx = idx -1

                    if idx >= 0 and idx < len(interval_segments):
                        _add_volume(idx, volume, is_up)

            elif distribution == utils.DistributionData.Uniform_Distribution:
                hl = high_p - low_p
                if hl == Decimal('0'):
                    idx = np.searchsorted(interval_segments_float, float(close_p), side='left')
                    if idx == len(interval_segments_float): idx = len(interval_segments_float) - 1
                    elif idx > 0 and abs(interval_segments_float[idx] - float(close_p)) > abs(interval_segments_float[idx-1] - float(close_p)): idx = idx -1
                    if idx >= 0 and idx < len(interval_segments):
                        _add_volume(idx, volume, is_up)
                else:
                    for idx in range(len(interval_segments)):
                        row_price = interval_segments[idx]
                        bin_bottom = row_price - (decimal_row_height / Decimal('2'))
                        bin_top = row_price + (decimal_row_height / Decimal('2'))
                        
                        if max(low_p, bin_bottom) < min(high_p, bin_top):
                            num_bins_touched = hl / decimal_row_height
                            if num_bins_touched == Decimal('0'): 
                                vol_per_bin_calc = volume
                            else:
                                vol_per_bin_calc = volume / num_bins_touched
                            _add_volume(idx, vol_per_bin_calc, is_up)

            elif distribution == utils.DistributionData.Uniform_Presence:
                uni_presence = Decimal('1.0') 
                logger.debug(f"DEBUG _calculate_volume_profile_internal: Distribution Uniform_Presence: uni_presence={uni_presence} (Type: {type(uni_presence)})")
                for idx in range(len(interval_segments)):
                    row_price = interval_segments[idx]
                    bin_bottom = row_price - (decimal_row_height / Decimal('2'))
                    bin_top = row_price + (decimal_row_height / Decimal('2'))

                    if max(low_p, bin_bottom) < min(high_p, bin_top):
                        _add_volume(idx, uni_presence, is_up)
            elif distribution == utils.DistributionData.Parabolic_Distribution:
                hl = high_p - low_p
                if hl == Decimal('0'): 
                    parabolic_vol = Decimal('0')
                else:
                    hl2 = hl / Decimal('2')
                    one_step = hl2 * (hl - hl2) / Decimal('2')
                    second_step = (hl - hl2) * hl / Decimal('2')
                    final = one_step + second_step
                    parabolic_vol = volume / parabolic_vol if parabolic_vol != Decimal('0') else Decimal('0')
                logger.debug(f"DEBUG _calculate_volume_profile_internal: Distribution Parabolic: parabolic_vol={parabolic_vol} (Type: {type(parabolic_vol)})")

                for idx in range(len(interval_segments)):
                    row_price = interval_segments[idx]
                    bin_bottom = row_price - (decimal_row_height / Decimal('2'))
                    bin_top = row_price + (decimal_row_height / Decimal('2'))
                    if max(low_p, bin_bottom) < min(high_p, bin_top):
                        _add_volume(idx, parabolic_vol, is_up)
            elif distribution == utils.DistributionData.Triangular_Distribution:
                hl = high_p - low_p
                if hl == Decimal('0'): 
                    triangular_volume = Decimal('0')
                else:
                    hl2 = hl / Decimal('2')
                    one_step = hl2 * (hl - hl2) / Decimal('2')
                    second_step = (hl - hl2) * hl / Decimal('2')
                    final = one_step + second_step
                    triangular_volume = volume / final if final > Decimal('0') else Decimal('0')
                logger.debug(f"DEBUG _calculate_volume_profile_internal: Distribution Triangular: triangular_volume={triangular_volume} (Type: {type(triangular_volume)})")

                for idx in range(len(interval_segments)):
                    row_price = interval_segments[idx]
                    bin_bottom = row_price - (decimal_row_height / Decimal('2'))
                    bin_top = row_price + (decimal_row_height / Decimal('2'))
                    if max(low_p, bin_bottom) < min(high_p, bin_top):
                        _add_volume(idx, triangular_volume, is_up)


    normal['total_value'] = sum(normal['values']) 
    logger.debug(f"DEBUG _calculate_volume_profile_internal: Sum of normal['values']={sum(normal['values'])} (Type: {type(sum(normal['values']))})")
    normal['total_value'] = normal['total_value'].quantize(Decimal('0.00001'))
    logger.debug(f"DEBUG _calculate_volume_profile_internal: normal['total_value'] after quantize={normal['total_value']} (Type: {type(normal['total_value'])})")


    buy_sell['value_buy'] = sum(buy_sell['vp_buy']) 
    buy_sell['value_sell'] = sum(buy_sell['vp_sell']) 
    buy_sell['value_sum'] = buy_sell['value_buy'] + buy_sell['value_sell']
    buy_sell['value_subtract'] = buy_sell['value_buy'] - buy_sell['value_sell']
    if buy_sell['value_buy'] != Decimal('0.0') and buy_sell['value_sell'] != Decimal('0.0'): 
        buy_sell['value_divide'] = buy_sell['value_buy'] / buy_sell['value_sell']
    else:
        buy_sell['value_divide'] = Decimal('0.0')

    delta['total_delta'] = sum(delta['values']) 
    delta['total_delta'] = delta['total_delta'].quantize(Decimal('0.00001'))

    normal_df = pd.DataFrame(normal)
    normal_df.rename(columns={'datetime': 'vp_datetime',
                              'prices': 'vp_prices',
                              'values': 'vp_normal',
                              'total_value': 'vp_normal_total'}, inplace=True)

    buy_sell_df = pd.DataFrame(buy_sell)
    buy_sell_df.rename(columns={'datetime': 'vp_datetime',
                                'prices': 'vp_prices',
                                'vp_buy': 'vp_buy_values', 
                                'vp_sell': 'vp_sell_values', 
                                'value_buy': 'vp_buy_total', 
                                'value_sell': 'vp_sell_total', 
                                'value_sum': 'vp_bs_sum',
                                'value_subtract': 'vp_bs_subtract',
                                'value_divide': 'vp_bs_divide'}, inplace=True)

    delta_df = pd.DataFrame(delta)
    delta_df.rename(columns={'datetime': 'vp_datetime',
                              'prices': 'vp_prices',
                              'values': 'vp_delta',
                              'total_delta': 'vp_delta_total',
                              'min_delta': 'vp_delta_min',
                              'max_delta': 'vp_delta_max'}, inplace=True)

    for _df in [normal_df, buy_sell_df, delta_df]:
        if not _df.empty:
            if len(_df) >= 2: 
                _df.drop(_df.head(1).index, inplace=True)
                _df.drop(_df.tail(1).index, inplace=True) 

    logger.debug(f"_calculate_volume_profile_internal selesai. Mengembalikan DataFrames Volume Profile.")
    return [normal_df, buy_sell_df, delta_df]


def update_volume_profiles(symbol_param: str, timeframe_str: str, candles_df: pd.DataFrame) -> int:
    """
    Menghitung dan menyimpan Volume Profile.
    Ini mengimplementasikan deteksi POC, VAH, dan VAL nyata (dasar).
    Args:
        symbol_param (str): Simbol trading.
        timeframe_str (str): Timeframe yang akan diproses (misal "H4", "D1").
        candles_df (pd.DataFrame): DataFrame candle historis yang akan digunakan (sudah difilter dan disiapkan).
    Returns:
        int: Jumlah Volume Profiles baru yang berhasil dideteksi dan dikirim ke antrean DB.
    """
    logger.info(f"Menghitung Volume Profiles untuk {symbol_param} {timeframe_str}...")
    processed_count = 0 

    _initialize_symbol_point_value()
    if _SYMBOL_POINT_VALUE is None:
        logger.error(f"Nilai POINT untuk {symbol_param} tidak tersedia. Melewatkan perhitungan Volume Profile.")
        return 0

    price_level_granularity_points = 10 
    value_area_percentage = Decimal('0.70')
    distribution_method = utils.DistributionData.OHLC_No_Avg 

    price_granularity_value = price_level_granularity_points * _SYMBOL_POINT_VALUE # Ini adalah row_height dalam unit harga
    if price_granularity_value == Decimal('0'):
        logger.warning(f"Price granularity value is 0 for {symbol_param}. Skipping Volume Profile calculation.")
        return 0

    if candles_df.empty or len(candles_df) < 20: 
        logger.warning(f"Tidak ada candle cukup ({len(candles_df)}) untuk {symbol_param} {timeframe_str} untuk menghitung Volume Profile. Diperlukan minimal 20 lilin.")
        return 0

    try:
        df_processed_candles = candles_df.copy()
        # --- START PERBAIKAN: Ganti nama kolom untuk konsistensi ---
        df_processed_candles = df_processed_candles.rename(columns={
            'open_price': 'open',
            'high_price': 'high',
            'low_price': 'low',
            'close_price': 'close',
            'tick_volume': 'volume', # Juga ganti nama 'tick_volume' menjadi 'volume'
            'real_volume': 'real_volume', # Tambahkan ini jika ada
            'spread': 'spread' # Tambahkan ini jika ada
        })
        # --- AKHIR PERBAIKAN ---

        # KOREKSI: HANYA konversi kolom harga dan volume ke Decimal
        for col in ['open', 'high', 'low', 'close', 'volume', 'real_volume', 'spread']: # <--- PASTIKAN LIST KOLOM INI
            if col in df_processed_candles.columns: # Pastikan kolom ada
                df_processed_candles[col] = df_processed_candles[col].apply(lambda x: Decimal(str(x)) if pd.notna(x) else np.nan) # Gunakan np.nan untuk nilai yang hilang
        
        if not isinstance(df_processed_candles.index, pd.DatetimeIndex):
            df_processed_candles['open_time_utc'] = pd.to_datetime(df_processed_candles.index)
            df_processed_candles = df_processed_candles.set_index('open_time_utc').sort_index()

        period_start = df_processed_candles.index.min().to_pydatetime()
        period_end = df_processed_candles.index.max().to_pydatetime()
        
        ticks_start_time = df_processed_candles.index.min().to_pydatetime()
        ticks_end_time = df_processed_candles.index.max().to_pydatetime()

        raw_ticks_data = _get_db_manager().get_price_history( 
            symbol_param,
            start_time_utc=ticks_start_time,
            end_time_utc=ticks_end_time
        )
        
        df_ticks = None
        if raw_ticks_data:
            df_ticks = pd.DataFrame(raw_ticks_data)
            if 'time_msc' in df_ticks.columns:
                df_ticks['datetime'] = pd.to_datetime(df_ticks['time_msc'], unit='ms', utc=True)
                df_ticks.set_index('datetime', inplace=True)
            elif 'datetime' in df_ticks.columns: 
                df_ticks['datetime'] = pd.to_datetime(df_ticks['datetime'], utc=True)
                df_ticks.set_index('datetime', inplace=True)
            else:
                logger.warning("Kolom waktu (time_msc/datetime) tidak ditemukan di df_ticks, mencoba menggunakan indeks DataFrame sebagai datetime.")
                if not isinstance(df_ticks.index, pd.DatetimeIndex):
                    df_ticks.index = pd.to_datetime(df_ticks.index, utc=True) 

            if 'close' not in df_ticks.columns: 
                 if 'last_price' in df_ticks.columns:
                     df_ticks['close'] = df_ticks['last_price']
                 elif 'bid_price' in df_ticks.columns and 'ask_price' in df_ticks.columns:
                    df_ticks['close'] = (df_ticks['bid_price'] + df_ticks['ask_price']) / 2
                 else:
                    df_ticks['close'] = np.nan 
                    logger.warning("Tidak ada kolom harga yang cocok (close, last_price, bid_price/ask_price) ditemukan di df_ticks. Menggunakan NaN untuk harga tick.")
            
            df_ticks['close'] = df_ticks['close'].apply(lambda x: Decimal(str(x)) if pd.notna(x) else np.nan)


        row_height_for_vp_calc = float(price_granularity_value) 

        normal_profile_df, buy_sell_profile_df, delta_profile_df = _calculate_volume_profile_internal(
            df_interval_ohlcv=df_processed_candles, 
            df_interval_ticks=df_ticks, 
            row_height=row_height_for_vp_calc,
            distribution=distribution_method
        )
        
        poc_price = Decimal('0')
        vah_price = Decimal('0')
        val_price = Decimal('0')
        total_volume = Decimal('0')
        
        profile_data_list = [] 

        if not normal_profile_df.empty and not normal_profile_df['vp_normal'].isnull().all():
            try:
                max_volume_idx = normal_profile_df['vp_normal'].idxmax()
                if pd.notna(max_volume_idx):
                    poc_price = normal_profile_df.loc[max_volume_idx, 'vp_prices']
                else:
                    poc_price = Decimal('0') 

                if not normal_profile_df['vp_prices'].empty and not normal_profile_df['vp_prices'].isnull().all():
                    vah_price = normal_profile_df['vp_prices'].max()
                    val_price = normal_profile_df['vp_prices'].min()
                else:
                    vah_price = Decimal('0')
                    val_price = Decimal('0')

                if not normal_profile_df['vp_normal_total'].empty and not normal_profile_df['vp_normal_total'].isnull().all():
                    total_volume = normal_profile_df['vp_normal_total'].sum()
                else:
                    total_volume = Decimal('0')

                poc_price = to_decimal_or_none(poc_price) or Decimal('0')
                vah_price = to_decimal_or_none(vah_price) or Decimal('0')
                val_price = to_decimal_or_none(val_price) or Decimal('0')
                total_volume = to_decimal_or_none(total_volume) or Decimal('0')

                for idx, row in normal_profile_df.iterrows():
                    price_val = to_decimal_or_none(row['vp_prices']) or Decimal('0')
                    volume_val = to_decimal_or_none(row['vp_normal']) or Decimal('0')
                    profile_data_list.append({
                        "price": float(price_val), 
                        "volume": float(volume_val)
                    })
            except Exception as e:
                logger.error(f"Error saat mengekstrak VP metrics dari normal_profile_df: {e}", exc_info=True)
                poc_price = Decimal('0')
                vah_price = Decimal('0')
                val_price = Decimal('0')
                total_volume = Decimal('0')
                profile_data_list = [] 

        profile_data_json = json.dumps(profile_data_list)
        
        existing_vp_keys = set()
        try:
            existing_vps_db = _get_db_manager().get_volume_profiles( 
                symbol=symbol_param,
                timeframe=timeframe_str,
                start_time_utc=period_start - timedelta(minutes=1), 
                end_time_utc=period_end + timedelta(minutes=1)
            )
            for vp in existing_vps_db:
                vp_period_start_dt = vp['period_start_utc']
                existing_vp_keys.add((vp['symbol'], vp['timeframe'], vp_period_start_dt.replace(microsecond=0).isoformat()))
            logger.debug(f"Ditemukan {len(existing_vp_keys)} Volume Profiles yang sudah ada di DB untuk {timeframe_str}.")
        except Exception as e:
            logger.error(f"Gagal mengambil existing Volume Profiles dari DB untuk {timeframe_str} (untuk pre-filtering): {e}", exc_info=True)
            pass 

        BATCH_SAVE_THRESHOLD_VP = 100 
        detected_vps_to_save_in_batch = []
        
        vp_keys_in_current_chunk = set()

        if total_volume == Decimal('0') and not profile_data_list: 
             logger.debug(f"VP_DEBUG: Total volume 0 dan profile_data_list kosong. Melewatkan penyimpanan VP.")
             return 0 

        new_vp_key = (symbol_param, timeframe_str, period_start.replace(microsecond=0).isoformat())

        if new_vp_key not in existing_vp_keys:
            new_vp_data = {
                "symbol": symbol_param, "timeframe": timeframe_str,
                "period_start_utc": period_start, "period_end_utc": period_end,
                "poc_price": poc_price, "vah_price": vah_price, "val_price": val_price,
                "total_volume": total_volume, "profile_data_json": profile_data_json,
                "row_height_value": price_granularity_value
            }
            detected_vps_to_save_in_batch.append(new_vp_data)
            
            if len(detected_vps_to_save_in_batch) >= BATCH_SAVE_THRESHOLD_VP:
                try:
                    _get_db_manager().save_volume_profiles_batch(detected_vps_to_save_in_batch)
                    processed_count += len(detected_vps_to_save_in_batch) 
                    logger.info(f"BACKFILL WORKER: Auto-saved {len(detected_vps_to_save_in_batch)} new VPs for {timeframe_str}.")
                    detected_vps_to_save_in_batch = [] 
                except Exception as e:
                    logger.error(f"BACKFILL WORKER: Gagal auto-save VPs batch: {e}", exc_info=True)
                    detected_vps_to_save_in_batch = [] 
        
        if detected_vps_to_save_in_batch:
            try:
                _get_db_manager().save_volume_profiles_batch(detected_vps_to_save_in_batch)
                processed_count += len(detected_vps_to_save_in_batch) 
                logger.info(f"BACKFILL WORKER: Final auto-saved {len(detected_vps_to_save_in_batch)} remaining new VPs for {timeframe_str}.")
            except Exception as e:
                logger.error(f"BACKFILL WORKER: Gagal final auto-save VPs batch: {e}", exc_info=True)

    except Exception as e:
        logger.error(f"Gagal menghitung atau menyimpan Volume Profile untuk {timeframe_str}: {e}", exc_info=True)
        return processed_count 
    logger.info(f"Selesai menghitung Volume Profiles untuk {symbol_param} {timeframe_str}.")
    return processed_count 


def _calculate_liquidity_zones_internal(ohlc_df: pd.DataFrame, swing_highs_lows_df: pd.DataFrame, range_percent: float = 0.01) -> pd.DataFrame:
    """
    Mendeteksi Liquidity Zones (Equal Highs/Lows) berdasarkan logika smc.py.
    Likuiditas terjadi ketika ada beberapa high dalam kisaran kecil satu sama lain,
    atau beberapa low dalam kisaran kecil satu sama lain.

    Parameters:
    ohlc_df: DataFrame - DataFrame dengan kolom 'open', 'high', 'low', 'close' (sudah float) dan indeks datetime.
    swing_highs_lows_df: DataFrame - DataFrame dari hasil _calculate_swing_highs_lows_internal.
    range_percent: float - Persentase dari total rentang harga (max high - min low) untuk menentukan kisaran likuiditas.

    Returns:
    DataFrame dengan kolom 'Liquidity' (1 jika bullish/Equal Highs, -1 jika bearish/Equal Lows),
    'Level', 'End' (indeks akhir kelompok likuiditas), dan 'Swept' (indeks lilin yang menyapu likuiditas).
    """
    logger.debug(f"Menjalankan _calculate_liquidity_zones_internal dengan range_percent={range_percent} pada {len(ohlc_df)} lilin.")

    ohlc_len = len(ohlc_df)
    # KOREKSI: Pastikan hanya kolom harga yang dikonversi ke float untuk operasi NumPy
    _ohlc_high = ohlc_df["high"].apply(to_float_or_none).values
    _ohlc_low = ohlc_df["low"].apply(to_float_or_none).values
    
    # Buat salinan untuk dimodifikasi internal (menandai kandidat yang sudah digunakan)
    _shl_HighLow = swing_highs_lows_df["HighLow"].values.copy()
    _shl_Level = swing_highs_lows_df["Level"].values.copy()

    # Hitung rentang pip berdasarkan keseluruhan rentang high-low DataFrame
    # Menggunakan to_float_or_none karena max()/min() di Pandas bisa mengembalikan Decimal
    # dan `range_percent` adalah float. Konversi ke float untuk perhitungan ini.
    ohlc_high_max = ohlc_df["high"].apply(to_float_or_none).max()
    ohlc_low_min = ohlc_df["low"].apply(to_float_or_none).min()
    
    if ohlc_high_max is None or ohlc_low_min is None or np.isnan(ohlc_high_max) or np.isnan(ohlc_low_min):
        logger.warning("Tidak dapat menghitung rentang harga untuk Liquidity Zones karena nilai high/low tidak valid.")
        pip_range_val = 0.0 # Fallback
    else:
        pip_range_val = (ohlc_high_max - ohlc_low_min) * range_percent
    
    # Inisialisasi array output dengan NaN
    liquidity = np.full(ohlc_len, np.nan, dtype=np.float32)
    liquidity_level = np.full(ohlc_len, np.nan, dtype=np.float32)
    liquidity_end = np.full(ohlc_len, np.nan, dtype=np.float32)
    liquidity_swept = np.full(ohlc_len, np.nan, dtype=np.float32)

    # Dapatkan indeks swing high dan swing low
    bull_indices = np.flatnonzero(_shl_HighLow == 1) # Indeks Swing High
    bear_indices = np.flatnonzero(_shl_HighLow == -1) # Indeks Swing Low

    # --- Proses likuiditas bullish (Equal Highs) ---
    for i in bull_indices:
        # Lewati jika kandidat ini sudah digunakan (ditandai 0 di _shl_HighLow)
        if _shl_HighLow[i] != 1:
            continue
        
        high_level_i = _shl_Level[i]
        range_low_bound = high_level_i - pip_range_val
        range_high_bound = high_level_i + pip_range_val
        
        group_levels = [high_level_i] # Mulai kelompok dengan level swing saat ini
        group_end_idx = i # Indeks terakhir dalam kelompok likuiditas

        # Deteksi Swept Index: Candle pertama setelah 'i' yang high-nya menembus range_high_bound
        swept_idx = 0 # Default 0 (belum tersapu)
        search_start_idx = i + 1
        if search_start_idx < ohlc_len:
            # np.argmax mengembalikan indeks pertama yang True. Tambahkan `search_start_idx` untuk indeks absolut.
            cond_swept = _ohlc_high[search_start_idx:] >= range_high_bound
            if np.any(cond_swept):
                swept_idx = search_start_idx + np.argmax(cond_swept)
        
        # Iterasi kandidat swing high setelah 'i'
        for j in bull_indices:
            if j <= i: # Hanya lihat kandidat setelah 'i'
                continue
            if swept_idx and j >= swept_idx: # Jika sudah tersapu, berhenti mencari kandidat
                break
            
            # Jika kandidat 'j' adalah Swing High yang belum digunakan dan berada dalam kisaran likuiditas
            if _shl_HighLow[j] == 1 and (range_low_bound <= _shl_Level[j] <= range_high_bound):
                group_levels.append(_shl_Level[j])
                group_end_idx = j
                _shl_HighLow[j] = 0 # Tandai kandidat ini sudah digunakan

        # Rekam likuiditas hanya jika ada lebih dari satu kandidat yang dikelompokkan
        if len(group_levels) > 1:
            avg_level = sum(group_levels) / len(group_levels)
            liquidity[i] = 1 # Bullish Liquidity / Equal Highs
            liquidity_level[i] = avg_level
            liquidity_end[i] = group_end_idx
            liquidity_swept[i] = swept_idx


    # --- Proses likuiditas bearish (Equal Lows) ---
    for i in bear_indices:
        if _shl_HighLow[i] != -1:
            continue
        
        low_level_i = _shl_Level[i]
        range_low_bound = low_level_i - pip_range_val
        range_high_bound = low_level_i + pip_range_val
        
        group_levels = [low_level_i]
        group_end_idx = i

        # Deteksi Swept Index: Candle pertama setelah 'i' yang low-nya menembus range_low_bound
        swept_idx = 0
        search_start_idx = i + 1
        if search_start_idx < ohlc_len:
            cond_swept = _ohlc_low[search_start_idx:] <= range_low_bound
            if np.any(cond_swept):
                swept_idx = search_start_idx + np.argmax(cond_swept)
        
        for j in bear_indices:
            if j <= i:
                continue
            if swept_idx and j >= swept_idx:
                break
            
            if _shl_HighLow[j] == -1 and (range_low_bound <= _shl_Level[j] <= range_high_bound):
                group_levels.append(_shl_Level[j])
                group_end_idx = j
                _shl_HighLow[j] = 0
            
        if len(group_levels) > 1:
            avg_level = sum(group_levels) / len(group_levels)
            liquidity[i] = -1 # Bearish Liquidity / Equal Lows
            liquidity_level[i] = avg_level
            liquidity_end[i] = group_end_idx
            liquidity_swept[i] = swept_idx

    # Gabungkan hasil ke DataFrame
    liquidity_results_df = pd.DataFrame({
        "Liquidity": liquidity,
        "Level": liquidity_level,
        "End": liquidity_end, # Indeks akhir kelompok (belum tentu sama dengan Swept)
        "Swept": liquidity_swept
    }, index=ohlc_df.index)

    logger.debug(f"_calculate_liquidity_zones_internal selesai. Ditemukan {len(liquidity_results_df.dropna(subset=['Liquidity']))} Liquidity Zones.")
    return liquidity_results_df


def _calculate_order_blocks_internal(ohlc_df_float: pd.DataFrame, shoulder_length: int, swing_highs_lows_df: pd.DataFrame, consolidation_tolerance_points: Decimal, break_tolerance_value: Decimal) -> pd.DataFrame:
    """
    Menghitung Order Blocks (OB) berdasarkan definisi SMC/ICT dari smc.py:
    Lilin terakhir yang berlawanan sebelum pergerakan impulsif yang signifikan.
    Order Block adalah zona harga di mana 'jejak' aktivitas order institusional terlihat,
    biasanya lilin terakhir yang berlawanan sebelum pergerakan tren yang kuat.
    Args:
        ohlc_df_float (pd.DataFrame): DataFrame candle yang sudah diformat ke float,
                                      dengan kolom 'open', 'high', 'low', 'close', 'volume',
                                      dan indeks datetime.
        shoulder_length (int): Jumlah lilin di setiap sisi yang membentuk "bahu" OB (konfirmasi awal).
        swing_highs_lows_df (pd.DataFrame): DataFrame hasil dari fungsi _calculate_swing_highs_lows_internal,
                                           berisi 'HighLow' (1/-1) dan 'Level' (harga swing).
        consolidation_tolerance_points (Decimal): Toleransi dalam poin untuk mengidentifikasi konsolidasi.
        break_tolerance_value (Decimal): Toleransi harga dalam unit mata uang untuk konfirmasi penembusan (break).
    Returns:
        pd.DataFrame: DataFrame dengan kolom 'OB' (1=Bullish, -1=Bearish), 'Top', 'Bottom', 'OBVolume',
                      'MitigatedIndex' (indeks lilin mitigasi), dan 'Percentage'.
                      Indeks DataFrame ini akan cocok dengan ohlc_df_float asli.
    """
    logger.debug(f"OB_CALC_DIAG: Menjalankan _calculate_order_blocks_internal (SMC Aligned) pada {len(ohlc_df_float)} lilin.")

    # Validasi awal: Pastikan DataFrame input tidak kosong dan memiliki kolom OHLCV yang diperlukan.
    if ohlc_df_float.empty or not all(col in ohlc_df_float.columns for col in ['open', 'high', 'low', 'close', 'volume']):
        logger.debug("OB_CALC_DIAG: DataFrame input ohlc_df_float tidak memiliki kolom OHLCV yang diperlukan.")
        return pd.DataFrame(index=ohlc_df_float.index, columns=['OB', 'Top', 'Bottom', 'OBVolume', 'MitigatedIndex', 'Percentage'])
    
    # PERBAIKAN PENTING DI SINI: Bersihkan NaN dari DataFrame lilin dan swing.
    initial_ohlc_len = len(ohlc_df_float)
    ohlc_df_float_cleaned = ohlc_df_float.dropna(subset=['open', 'high', 'low', 'close', 'volume']).copy()
    if len(ohlc_df_float_cleaned) < initial_ohlc_len:
        logger.warning(f"OB_CALC_DIAG: Dihapus {initial_ohlc_len - len(ohlc_df_float_cleaned)} baris dengan NaN di kolom OHLCV pada ohlc_df_float sebelum perhitungan OB.")
    
    initial_swing_len = len(swing_highs_lows_df)
    swing_highs_lows_df_cleaned = swing_highs_lows_df.dropna(subset=['HighLow', 'Level']).copy()
    if len(swing_highs_lows_df_cleaned) < initial_swing_len:
        logger.warning(f"OB_CALC_DIAG: Dihapus {initial_swing_len - len(swing_highs_lows_df_cleaned)} baris dengan NaN di kolom Swing HighLow/Level.")

    # Validasi setelah pembersihan: Pastikan masih ada cukup data yang tersisa untuk perhitungan.
    if ohlc_df_float_cleaned.empty or len(ohlc_df_float_cleaned) < shoulder_length + 2:
        logger.debug(f"OB_CALC_DIAG: DataFrame ohlc_df_float_cleaned kosong atau tidak cukup lilin ({len(ohlc_df_float_cleaned)}) setelah pembersihan NaN untuk menghitung Order Blocks.")
        return pd.DataFrame(index=ohlc_df_float.index, columns=['OB', 'Top', 'Bottom', 'OBVolume', 'MitigatedIndex', 'Percentage'])

    # Ekstrak data harga dan volume sebagai array NumPy dari DataFrame yang sudah dibersihkan.
    _open = ohlc_df_float_cleaned['open'].values
    _high = ohlc_df_float_cleaned['high'].values
    _low = ohlc_df_float_cleaned['low'].values
    _close = ohlc_df_float_cleaned['close'].values
    _volume = ohlc_df_float_cleaned['volume'].values
    _index = ohlc_df_float_cleaned.index
    ohlc_len = len(ohlc_df_float_cleaned)

    logger.debug(f"OB_CALC_DIAG: Input _high head: {_high[:5]}, tail: {_high[-5:]}")
    logger.debug(f"OB_CALC_DIAG: Input _low head: {_low[:5]}, tail: {_low[-5:]}")
    logger.debug(f"OB_CALC_DIAG: Input _volume head: {_volume[:5]}, tail: {_volume[-5:]}")

    # Inisialisasi array hasil dengan NaN untuk memastikan setiap posisi memiliki nilai default.
    ob = np.full(ohlc_len, np.nan, dtype=float)
    top_arr = np.full(ohlc_len, np.nan, dtype=float)
    bottom_arr = np.full(ohlc_len, np.nan, dtype=float)
    obVolume = np.full(ohlc_len, np.nan, dtype=float)
    mitigated_index = np.full(ohlc_len, np.nan, dtype=float)
    percentage = np.full(ohlc_len, np.nan, dtype=float)

    # Ambil swing points dari DataFrame yang sudah dibersihkan.
    swing_hl = swing_highs_lows_df_cleaned["HighLow"].values
    swing_level = swing_highs_lows_df_cleaned["Level"].values

    # Pre-komputasi indeks swing high dan swing low untuk pencarian yang efisien.
    swing_high_indices = np.flatnonzero(swing_hl == 1)
    swing_low_indices = np.flatnonzero(swing_hl == -1)

    # Inisialisasi array untuk melacak swing yang sudah "crossed" (ditembus).
    crossed = np.full(ohlc_len, False, dtype=bool)

    # Konversi toleransi penembusan ke float untuk perbandingan numerik.
    tolerance_float = float(break_tolerance_value)

    active_bullish_obs = []  # Melacak Order Blocks Bullish yang masih aktif (belum dimitigasi).
    active_bearish_obs = [] # Melacak Order Blocks Bearish yang masih aktif.

    # Iterasi melalui setiap lilin dalam data yang sudah dibersihkan.
    for i in range(ohlc_len): 
        current_candle_idx = i

        # --- Update OB yang sudah ada (Mitigasi) ---
        # Periksa apakah OB Bullish yang aktif telah dimitigasi oleh pergerakan harga saat ini.
        for ob_idx in active_bullish_obs.copy(): # Gunakan salinan untuk modifikasi saat iterasi
            # OB Bullish dimitigasi jika harga low saat ini menembus di bawah dasar OB.
            if _low[current_candle_idx] < bottom_arr[ob_idx] - tolerance_float:
                mitigated_index[ob_idx] = current_candle_idx # Catat lilin mitigasi
                active_bullish_obs.remove(ob_idx) # Hapus dari daftar aktif
                logger.debug(f"OB_CALC_DIAG: Bullish OB at index {ob_idx} MITIGATED by candle {current_candle_idx} (Low: {_low[current_candle_idx]:.5f} < OB_Bottom: {bottom_arr[ob_idx]:.5f}).")
        
        # Periksa apakah OB Bearish yang aktif telah dimitigasi.
        for ob_idx in active_bearish_obs.copy():
            # OB Bearish dimitigasi jika harga high saat ini menembus di atas puncak OB.
            if _high[current_candle_idx] > top_arr[ob_idx] + tolerance_float:
                mitigated_index[ob_idx] = current_candle_idx
                active_bearish_obs.remove(ob_idx)
                logger.debug(f"OB_CALC_DIAG: Bearish OB at index {ob_idx} MITIGATED by candle {current_candle_idx} (High: {_high[current_candle_idx]:.5f} > OB_Top: {top_arr[ob_idx]:.5f}).")

        # --- Deteksi Order Block Baru ---
        # Cari swing high/low terakhir yang relevan *sebelum* current_candle_idx.
        last_swing_high_idx_found = None
        for sh_idx in reversed(swing_high_indices):
            if sh_idx < current_candle_idx: # Hanya swing yang terjadi sebelum lilin saat ini
                last_swing_high_idx_found = sh_idx
                break
        
        last_swing_low_idx_found = None
        for sl_idx in reversed(swing_low_indices):
            if sl_idx < current_candle_idx: # Hanya swing yang terjadi sebelum lilin saat ini
                last_swing_low_idx_found = sl_idx
                break
        
        # --- Deteksi Bullish Order Block ---
        if last_swing_high_idx_found is not None: 
            if _high[current_candle_idx] > _high[last_swing_high_idx_found] + tolerance_float and \
               _close[current_candle_idx] > (_high[last_swing_high_idx_found] - tolerance_float * 0.1): 
                
                ob_candidate_idx = -1
                for j in range(current_candle_idx - 1, max(-1, current_candle_idx - shoulder_length - 1), -1):
                    if _close[j] < _open[j]: # Lilin bearish (close < open)
                        ob_candidate_idx = j
                        break
                
                if ob_candidate_idx != -1: 
                    is_impulsive_move = False
                    if ob_candidate_idx + 1 <= current_candle_idx:
                        candles_after_ob_candidate = ohlc_df_float_cleaned.iloc[ob_candidate_idx + 1 : current_candle_idx + 1]
                        
                        if not candles_after_ob_candidate.empty:
                            highest_after_ob = np.max(candles_after_ob_candidate['high'].values)
                            # Ini mengakses config.RuleBasedStrategy.OB_MIN_IMPULSIVE_MOVE_MULTIPLIER
                            # Asumsi config sudah diimpor dan variabel ini tersedia.
                            ob_min_impulsive_move_multiplier_float = float(config.RuleBasedStrategy.OB_MIN_IMPULSIVE_MOVE_MULTIPLIER)
                            if (_close[current_candle_idx] - _low[ob_candidate_idx]) > (ob_min_impulsive_move_multiplier_float * tolerance_float) or \
                               (_high[current_candle_idx] - _low[ob_candidate_idx]) > (ob_min_impulsive_move_multiplier_float * tolerance_float) or \
                               (highest_after_ob - _low[ob_candidate_idx]) > (ob_min_impulsive_move_multiplier_float * tolerance_float):
                                is_impulsive_move = True
                                logger.debug(f"OB_CALC_DIAG: Bullish OB Impulsive Move Confirmed (after OB candle) for index {ob_candidate_idx}.")
                        else:
                            logger.debug(f"OB_CALC_DIAG: Tidak ada lilin setelah OB candidate {ob_candidate_idx} untuk konfirmasi impulsif (Bullish).")
                    
                    if is_impulsive_move:
                        # Tandai lilin sebagai OB Bullish dan simpan level serta volumenya.
                        ob[ob_candidate_idx] = 1 # Bullish OB
                        top_arr[ob_candidate_idx] = _high[ob_candidate_idx]
                        bottom_arr[ob_candidate_idx] = _low[ob_candidate_idx]
                        
                        vol_ob_candle = np.nan_to_num(_volume[ob_candidate_idx], nan=0.0)
                        obVolume[ob_candidate_idx] = vol_ob_candle # Volume lilin OB saja
                        
                        # MODIFIKASI: Hanya tambahkan ke daftar aktif jika belum ada
                        if ob_candidate_idx not in active_bullish_obs: 
                            active_bullish_obs.append(ob_candidate_idx) # Tambahkan ke daftar OB aktif
                            logger.debug(f"OB_CALC_DIAG: BULLISH OB DETECTED at index {ob_candidate_idx}. Top:{top_arr[ob_candidate_idx]:.5f}, Bottom:{bottom_arr[ob_candidate_idx]:.5f}, Volume:{obVolume[ob_candidate_idx]:.2f}")
        
        # --- Deteksi Bearish Order Block ---
        if last_swing_low_idx_found is not None:
            if _low[current_candle_idx] < _low[last_swing_low_idx_found] - tolerance_float and \
               _close[current_candle_idx] < (_low[last_swing_low_idx_found] + tolerance_float * 0.1): 
                
                ob_candidate_idx = -1
                for j in range(current_candle_idx - 1, max(-1, current_candle_idx - shoulder_length - 1), -1):
                    if _close[j] > _open[j]: # Lilin bullish (close > open)
                        ob_candidate_idx = j
                        break
                
                if ob_candidate_idx != -1: 
                    is_impulsive_move = False
                    if ob_candidate_idx + 1 <= current_candle_idx:
                        candles_after_ob_candidate = ohlc_df_float_cleaned.iloc[ob_candidate_idx + 1 : current_candle_idx + 1]
                        
                        if not candles_after_ob_candidate.empty:
                            highest_after_ob = np.max(candles_after_ob_candidate['high'].values)
                            lowest_after_ob = np.min(candles_after_ob_candidate['low'].values)
                            # Ini mengakses config.RuleBasedStrategy.OB_MIN_IMPULSIVE_MOVE_MULTIPLIER
                            ob_min_impulsive_move_multiplier_float = float(config.RuleBasedStrategy.OB_MIN_IMPULSIVE_MOVE_MULTIPLIER)
                            if (_high[ob_candidate_idx] - _close[current_candle_idx]) > (ob_min_impulsive_move_multiplier_float * tolerance_float) or \
                               (_high[ob_candidate_idx] - _low[current_candle_idx]) > (ob_min_impulsive_move_multiplier_float * tolerance_float) or \
                               (_high[ob_candidate_idx] - lowest_after_ob) > (ob_min_impulsive_move_multiplier_float * tolerance_float):
                                is_impulsive_move = True
                                logger.debug(f"OB_CALC_DIAG: Bearish OB Impulsive Move Confirmed (after OB candle) for index {ob_candidate_idx}.")
                        else:
                            logger.debug(f"OB_CALC_DIAG: Tidak ada lilin setelah OB candidate {ob_candidate_idx} untuk konfirmasi impulsif (Bearish).")

                    if is_impulsive_move:
                        # Tandai lilin sebagai OB Bearish dan simpan level serta volumenya.
                        ob[ob_candidate_idx] = -1 # Bearish OB
                        top_arr[ob_candidate_idx] = _high[ob_candidate_idx]
                        bottom_arr[ob_candidate_idx] = _low[ob_candidate_idx]
                        
                        vol_ob_candle = np.nan_to_num(_volume[ob_candidate_idx], nan=0.0)
                        obVolume[ob_candidate_idx] = vol_ob_candle # Volume lilin OB saja
                        
                        # MODIFIKASI: Hanya tambahkan ke daftar aktif jika belum ada
                        if ob_candidate_idx not in active_bearish_obs: 
                            active_bearish_obs.append(ob_candidate_idx) # Tambahkan ke daftar OB aktif
                            logger.debug(f"OB_CALC_DIAG: BEARISH OB DETECTED at index {ob_candidate_idx}. Top:{top_arr[ob_candidate_idx]:.5f}, Bottom:{bottom_arr[ob_candidate_idx]:.5f}, Volume:{obVolume[ob_candidate_idx]:.2f}")

    logger.debug(f"OB_CALC_DIAG: Active Bullish OBs: {active_bullish_obs}")
    logger.debug(f"OB_CALC_DIAG: Active Bearish OBs: {active_bearish_obs}")

    # Finalisasi array: Set nilai NaN yang tidak digunakan.
    final_ob_mask = ~np.isnan(ob)
    ob = np.where(final_ob_mask, ob, np.nan)
    top_arr = np.where(final_ob_mask, top_arr, np.nan)
    bottom_arr = np.where(final_ob_mask, bottom_arr, np.nan)
    obVolume = np.where(final_ob_mask, obVolume, np.nan)
    mitigated_index = np.where(final_ob_mask, mitigated_index, np.nan)
    percentage = np.where(final_ob_mask, percentage, np.nan)

    # Buat DataFrame hasil dengan indeks dari DataFrame yang sudah dibersihkan.
    results_df = pd.concat(
        [
            pd.Series(ob, index=_index, name="OB"),
            pd.Series(top_arr, index=_index, name="Top"),
            pd.Series(bottom_arr, index=_index, name="Bottom"),
            pd.Series(obVolume, index=_index, name="OBVolume"),
            pd.Series(mitigated_index, index=_index, name="MitigatedIndex"),
            pd.Series(percentage, index=_index, name="Percentage"),
        ],
        axis=1,
    )
    return results_df.reindex(ohlc_df_float.index)





def _calculate_fvg_internal(ohlc_df: pd.DataFrame, join_consecutive: bool = False) -> pd.DataFrame:
    """
    Menghitung Fair Value Gaps (FVG) berdasarkan logika smc.py.
    FVG terjadi ketika high candle sebelumnya lebih rendah dari low candle berikutnya (bullish FVG),
    atau ketika low candle sebelumnya lebih tinggi dari high candle berikutnya (bearish FVG).

    Parameters:
    ohlc_df: DataFrame - DataFrame dengan kolom 'open', 'high', 'low', 'close' (sudah float) dan indeks datetime.
    join_consecutive: bool - Jika True, FVG berurutan akan digabungkan menjadi satu.

    Returns:
    DataFrame dengan kolom 'FVG' (1 jika bullish, -1 jika bearish), 'Top', 'Bottom', dan 'MitigatedIndex'.
    """
    logger.debug(f"Menjalankan _calculate_fvg_internal dengan join_consecutive={join_consecutive} pada {len(ohlc_df)} lilin.")

    # KOREKSI: Pastikan hanya kolom harga yang dikonversi ke NumPy array
    _open = ohlc_df["open"].apply(to_float_or_none).values
    _high = ohlc_df["high"].apply(to_float_or_none).values
    _low = ohlc_df["low"].apply(to_float_or_none).values
    _close = ohlc_df["close"].apply(to_float_or_none).values
    ohlc_len = len(ohlc_df)

    # Inisialisasi array hasil
    fvg = np.full(ohlc_len, np.nan, dtype=np.float32)
    top_arr = np.full(ohlc_len, np.nan, dtype=np.float32)
    bottom_arr = np.full(ohlc_len, np.nan, dtype=np.float32)
    mitigated_index = np.full(ohlc_len, np.nan, dtype=np.float32) # Diinisialisasi dengan NaN

    # Logika deteksi FVG
    # Bullish FVG: (prev_high < next_low) DAN (current_close > current_open)
    # Bearish FVG: (prev_low > next_high) DAN (current_close < current_open)
    
    # Kondisi Bullish FVG
    is_bullish_fvg_candidate = (_high[:-2] < _low[2:]) & (_close[1:-1] > _open[1:-1])
    fvg[1:-1][is_bullish_fvg_candidate] = 1 # Tandai FVG di candle tengah (indeks 1 dari slice 0:3)

    # Kondisi Bearish FVG
    is_bearish_fvg_candidate = (_low[:-2] > _high[2:]) & (_close[1:-1] < _open[1:-1])
    fvg[1:-1][is_bearish_fvg_candidate] = -1 # Tandai FVG di candle tengah (indeks 1 dari slice 0:3)

    # Mengisi Top dan Bottom FVG
    # Jika bullish FVG (fvg==1), Top adalah next_low, Bottom adalah prev_high
    # Jika bearish FVG (fvg==-1), Top adalah prev_low, Bottom adalah next_high
    top_arr[1:-1] = np.where(
        fvg[1:-1] == 1,
        _low[2:],  # next_low for bullish FVG
        np.where(
            fvg[1:-1] == -1,
            _low[:-2], # prev_low for bearish FVG
            np.nan
        )
    )

    bottom_arr[1:-1] = np.where(
        fvg[1:-1] == 1,
        _high[:-2], # prev_high for bullish FVG
        np.where(
            fvg[1:-1] == -1,
            _high[2:], # next_high for bearish FVG
            np.nan
        )
    )

    # Logic for joining consecutive FVG (jika diaktifkan)
    if join_consecutive:
        for i in range(ohlc_len - 1):
            if not np.isnan(fvg[i]) and fvg[i] == fvg[i+1]:
                top_arr[i+1] = max(top_arr[i], top_arr[i+1])
                bottom_arr[i+1] = min(bottom_arr[i], bottom_arr[i+1])
                fvg[i] = top_arr[i] = bottom_arr[i] = np.nan

    # Logika deteksi mitigasi FVG
    # Mitigasi hanya dicatat jika harga menembus FVG sepenuhnya setelah formasinya.
    for i in np.where(~np.isnan(fvg))[0]:
        found_mitigation_idx_for_this_fvg = np.nan # Default ke NaN
        
        # FVG terdeteksi pada lilin indeks 'i'.
        # Lilin yang menyelesaikan pola FVG adalah lilin di indeks 'i+1'.
        # Jadi, kita harus mulai mencari mitigasi dari lilin *setelah* lilin 'i+1', yaitu dari indeks 'i+2'.
        # Ini konsisten dengan logika smc.py asli yang mencari mitigasi dari `i + 2 :`
        
        start_check_idx_for_mitigation_candle = i + 2 
        
        # Tambahkan pengecekan untuk memastikan _SYMBOL_POINT_VALUE sudah diinisialisasi
        if _SYMBOL_POINT_VALUE is None:
            globals().get('_initialize_symbol_point_value')() # Panggil inisialisasi jika belum
            if _SYMBOL_POINT_VALUE is None: # Jika masih None setelah upaya inisialisasi
                logger.error("Nilai POINT simbol tidak tersedia. Melewatkan deteksi mitigasi FVG.")
                mitigated_index[i] = np.nan # Pastikan FVG ini tidak ditandai mitigated
                continue

        if start_check_idx_for_mitigation_candle < ohlc_len:
            # Toleransi untuk perbandingan float dengan Decimal
            tolerance_decimal = config.RuleBasedStrategy.RULE_SR_TOLERANCE_POINTS * _SYMBOL_POINT_VALUE
            tolerance_val_float = float(tolerance_decimal)

            current_fvg_top = top_arr[i] # ini sudah float (dari top_arr)
            current_fvg_bottom = bottom_arr[i] # ini sudah float (dari bottom_arr)
            
            cond_mitigated_slice = np.array([], dtype=np.bool_) # Inisialisasi default

            if fvg[i] == 1: # Bullish FVG (Top=low[i+1], Bottom=high[i-1])
                # Mitigasi jika low lilin menembus FVG bottom_price (yaitu high[i-1])
                # Periksa lilin dari start_check_idx_for_mitigation_candle dan seterusnya
                cond_mitigated_slice = ohlc_df["low"].apply(to_float_or_none).values[start_check_idx_for_mitigation_candle:] <= current_fvg_bottom - tolerance_val_float
            elif fvg[i] == -1: # Bearish FVG (Top=low[i-1], Bottom=high[i+1])
                # Mitigasi jika high lilin menembus FVG top_price (yaitu low[i-1])
                # Periksa lilin dari start_check_idx_for_mitigation_candle dan seterusnya
                cond_mitigated_slice = ohlc_df["high"].apply(to_float_or_none).values[start_check_idx_for_mitigation_candle:] >= current_fvg_top + tolerance_val_float
            
            # Hanya jika ada elemen True dalam kondisi dan indeksnya valid
            if np.any(cond_mitigated_slice):
                relative_idx = np.argmax(cond_mitigated_slice)
                absolute_mitigation_idx = start_check_idx_for_mitigation_candle + relative_idx
                
                # Pastikan indeks yang ditemukan berada dalam batas array lilin yang sedang diperiksa
                if absolute_mitigation_idx < ohlc_len:
                    found_mitigation_idx_for_this_fvg = absolute_mitigation_idx
        
        # Hanya isi mitigated_index[i] jika indeks mitigasi yang valid ditemukan.
        if not np.isnan(found_mitigation_idx_for_this_fvg):
            mitigated_index[i] = found_mitigation_idx_for_this_fvg

    # Gabungkan hasil ke DataFrame
    fvg_results_df = pd.DataFrame({
        "FVG": pd.Series(fvg, index=ohlc_df.index),
        "Top": pd.Series(top_arr, index=ohlc_df.index),
        "Bottom": pd.Series(bottom_arr, index=ohlc_df.index),
        "MitigatedIndex": pd.Series(mitigated_index, index=ohlc_df.index)
    })

    logger.debug(f"_calculate_fvg_internal selesai. Ditemukan {len(fvg_results_df.dropna(subset=['FVG']))} FVG.")
    return fvg_results_df


def _initialize_symbol_point_value():
    """
    Menginisialisasi atau memperbarui nilai point untuk simbol trading.
    Menggunakan nilai dari config.py sebagai fallback jika MT5 tidak bisa memberikan.
    """
    global _SYMBOL_POINT_VALUE
    logger.debug(f"DEBUG: _initialize_symbol_point_value() dipanggil. Current _SYMBOL_POINT_VALUE: {_SYMBOL_POINT_VALUE}")

    # Ambil nilai dari MT5 hanya jika kita benar-benar membutuhkannya (mis. belum ada nilai valid)
    retrieved_point_value = None
    # Jika _SYMBOL_POINT_VALUE belum diinisialisasi atau masih menggunakan default dari config.py
    # (yang berarti belum ada nilai aktual dari MT5)
    if _SYMBOL_POINT_VALUE is None or (_SYMBOL_POINT_VALUE == config.TRADING_SYMBOL_POINT_VALUE and config.TRADING_SYMBOL_POINT_VALUE != Decimal('0.0')): # Added check for non-zero default
        try:
            mt5_connector.initialize_mt5() # Pastikan MT5 terinisialisasi
            retrieved_point_value = mt5_connector.get_symbol_point_value(config.TRADING_SYMBOL)
            mt5_connector.shutdown_mt5() # Matikan MT5
            logger.debug(f"DEBUG: Retrieved point value from MT5: {retrieved_point_value}")
        except Exception as e:
            logger.warning(f"Gagal mengambil nilai POINT dari MT5: {e}. Menggunakan nilai dari config.", exc_info=True)


    # Kondisi pertama untuk inisialisasi jika masih None
    if _SYMBOL_POINT_VALUE is None or _SYMBOL_POINT_VALUE == Decimal('0.0'): # Juga cek jika 0.0
        if retrieved_point_value is None or retrieved_point_value == 0.0:
            # Gunakan nilai dari config.TRADING_SYMBOL_POINT_VALUE sebagai fallback
            _SYMBOL_POINT_VALUE = config.TRADING_SYMBOL_POINT_VALUE
            logger.error(f"Gagal mendapatkan nilai POINT untuk {config.TRADING_SYMBOL}. Menggunakan nilai default dari config: {_SYMBOL_POINT_VALUE}.")
        else:
            _SYMBOL_POINT_VALUE = utils.to_decimal_or_none(retrieved_point_value)  # Pastikan Decimal
            logger.info(f"Nilai POINT untuk {config.TRADING_SYMBOL} berhasil diinisialisasi: {_SYMBOL_POINT_VALUE}")

    # Kondisi kedua untuk memperbarui jika sebelumnya pakai fallback dan sekarang ada nilai asli
    elif _SYMBOL_POINT_VALUE == config.TRADING_SYMBOL_POINT_VALUE and retrieved_point_value is not None and retrieved_point_value != 0.0:
        _SYMBOL_POINT_VALUE = utils.to_decimal_or_none(retrieved_point_value)
        logger.info(f"Nilai POINT untuk {config.TRADING_SYMBOL} berhasil diperbarui: {_SYMBOL_POINT_VALUE}")

    logger.debug(f"DEBUG: _initialize_symbol_point_value() selesai. Final _SYMBOL_POINT_VALUE: {_SYMBOL_POINT_VALUE}")


def _detect_ma_crossover_signals_historically(symbol_param: str, timeframe_str: str, candles_df: pd.DataFrame) -> int:
    """
    Mendeteksi sinyal MA Crossover untuk symbol_param timeframe_str.
    Menyimpan sinyal baru ke database secara batch dengan deduplikasi.
    Args:
        symbol_param (str): Simbol trading.
        timeframe_str (str): Timeframe.
        candles_df (pd.DataFrame): DataFrame candle historis.
    Returns:
        int: Jumlah sinyal MA Crossover baru yang berhasil dideteksi dan dikirim ke antrean DB.
    """
    logger.info(f"Mendeteksi sinyal MA Crossover untuk {symbol_param} {timeframe_str}...")
    processed_count = 0 

    # Ambil periode MA yang ingin Anda gunakan untuk sinyal crossover dari config
    short_period_sma = int(config.MarketData.MA_PERIODS_TO_CALCULATE[0]) if config.MarketData.MA_PERIODS_TO_CALCULATE else 50
    long_period_sma = int(config.MarketData.MA_PERIODS_TO_CALCULATE[1]) if len(config.MarketData.MA_PERIODS_TO_CALCULATE) > 1 else 100
    short_period_ema = int(config.AIAnalysts.RSI_PERIOD * 3)
    long_period_ema = int(config.AIAnalysts.RSI_PERIOD * 18)

    if candles_df.empty or not isinstance(candles_df.index, pd.DatetimeIndex):
        logger.warning(f"DataFrame candle kosong atau indeks tidak valid untuk deteksi MA Crossover di {timeframe_str}.")
        return 0

    min_required_candles = max(short_period_sma, long_period_sma, short_period_ema, long_period_ema)
    if len(candles_df) < min_required_candles:
        logger.debug(f"Tidak cukup lilin ({len(candles_df)}) untuk deteksi MA Crossover di {timeframe_str}. Diperlukan data yang cukup untuk periode terpanjang ({min_required_candles}).")
        return 0 

    df_processed = candles_df.copy()
    for col in ['open_price', 'high_price', 'low_price', 'close_price', 'tick_volume']:
        if col in df_processed.columns: 
            df_processed[col] = df_processed[col].apply(utils.to_float_or_none) # Konversi ke float
    
    # Blok renaming ini TETAP DIPERLUKAN karena fungsi ini sendiri yang melakukan renaming
    df_processed = df_processed.rename(columns={
        'open_price': 'open', 'high_price': 'high', 'low_price': 'low',
        'close_price': 'close', 'tick_volume': 'volume'
    })

    signals_to_save_batch = []

    existing_signal_keys = set()
    try:
        existing_signals_db = database_manager.get_market_structure_events(
            symbol=symbol_param,
            timeframe=timeframe_str,
            event_type=['SMA Buy Crossover', 'SMA Sell Crossover', 'EMA Buy Crossover', 'EMA Sell Crossover'],
            start_time_utc=candles_df.index.min().to_pydatetime()
        )
        for sig in existing_signals_db:
            sig_time_dt = utils.to_utc_datetime_or_none(sig['event_time_utc'])
            if sig_time_dt:
                existing_signal_keys.add((sig['symbol'], sig['timeframe'], sig_time_dt.isoformat(), sig['event_type']))
    except Exception as e:
        logger.error(f"Gagal mengambil existing MA Crossover signals dari DB untuk pre-filtering: {e}", exc_info=True)
        pass

    for i in range(1, len(df_processed)):
        current_index = df_processed.index[i]
        prev_index = df_processed.index[i-1]
        event_time = current_index.to_pydatetime()

        logger.debug(f"MA_CROSSOVER_DEBUG: Memeriksa lilin {current_index.isoformat()} (sebelumnya: {prev_index.isoformat()})")

        # --- PERBAIKAN DI SINI ---
        # Ganti `candles_df.loc[current_index, 'close_price']`
        # menjadi `df_processed.loc[current_index, 'close']`
        current_close_price_dec = utils.to_decimal_or_none(df_processed.loc[current_index, 'close'])
        # --- AKHIR PERBAIKAN ---

        if current_close_price_dec is None:
            logger.debug(f"MA_CROSSOVER_DEBUG: Harga penutupan tidak valid untuk {current_index}. Melewatkan.")
            continue

        # Ambil nilai SMA
        ma_short_sma_curr_obj = database_manager.get_moving_averages(symbol_param, timeframe_str, "SMA", short_period_sma, limit=1, end_time_utc=current_index.to_pydatetime())
        ma_long_sma_curr_obj = database_manager.get_moving_averages(symbol_param, timeframe_str, "SMA", long_period_sma, limit=1, end_time_utc=current_index.to_pydatetime())
        ma_short_sma_prev_obj = database_manager.get_moving_averages(symbol_param, timeframe_str, "SMA", short_period_sma, limit=1, end_time_utc=prev_index.to_pydatetime())
        ma_long_sma_prev_obj = database_manager.get_moving_averages(symbol_param, timeframe_str, "SMA", long_period_sma, limit=1, end_time_utc=prev_index.to_pydatetime())

        short_sma_val = utils.to_decimal_or_none(ma_short_sma_curr_obj[0]['value']) if ma_short_sma_curr_obj else None
        long_sma_val = utils.to_decimal_or_none(ma_long_sma_curr_obj[0]['value']) if ma_long_sma_curr_obj else None
        prev_short_sma_val = utils.to_decimal_or_none(ma_short_sma_prev_obj[0]['value']) if ma_short_sma_prev_obj else None
        prev_long_sma_val = utils.to_decimal_or_none(ma_long_sma_prev_obj[0]['value']) if ma_long_sma_prev_obj else None

        logger.debug(f"MA_CROSSOVER_DEBUG: SMA - curr_short={short_sma_val}, curr_long={long_sma_val}, prev_short={prev_short_sma_val}, prev_long={prev_long_sma_val}")
        
        # Ambil nilai EMA
        ma_short_ema_curr_obj = database_manager.get_moving_averages(symbol_param, timeframe_str, "EMA", short_period_ema, limit=1, end_time_utc=current_index.to_pydatetime())
        ma_long_ema_curr_obj = database_manager.get_moving_averages(symbol_param, timeframe_str, "EMA", long_period_ema, limit=1, end_time_utc=current_index.to_pydatetime())
        ma_short_ema_prev_obj = database_manager.get_moving_averages(symbol_param, timeframe_str, "EMA", short_period_ema, limit=1, end_time_utc=prev_index.to_pydatetime())
        ma_long_ema_prev_obj = database_manager.get_moving_averages(symbol_param, timeframe_str, "EMA", long_period_ema, limit=1, end_time_utc=prev_index.to_pydatetime())

        short_ema_val = utils.to_decimal_or_none(ma_short_ema_curr_obj[0]['value']) if ma_short_ema_curr_obj else None
        long_ema_val = utils.to_decimal_or_none(ma_long_ema_curr_obj[0]['value']) if ma_long_ema_curr_obj else None
        prev_short_ema_val = utils.to_decimal_or_none(ma_short_ema_prev_obj[0]['value']) if ma_short_ema_prev_obj else None
        prev_long_ema_val = utils.to_decimal_or_none(ma_long_ema_prev_obj[0]['value']) if ma_long_ema_prev_obj else None

        logger.debug(f"MA_CROSSOVER_DEBUG: EMA - curr_short={short_ema_val}, curr_long={long_ema_val}, prev_short={prev_short_ema_val}, prev_long={prev_long_ema_val}")

        # Deteksi Sinyal SMA Crossover
        if all(v is not None for v in [short_sma_val, long_sma_val, prev_short_sma_val, prev_long_sma_val]):
            if (short_sma_val > long_sma_val and
                prev_short_sma_val <= prev_long_sma_val):
                event_type = "SMA Buy Crossover"
                direction = "Bullish"
                
                new_key = (symbol_param, timeframe_str, event_time.isoformat(), event_type)
                if new_key not in existing_signal_keys:
                    signals_to_save_batch.append({
                        "symbol": symbol_param, "timeframe": timeframe_str, "event_type": event_type,
                        "direction": direction, "price_level": current_close_price_dec, "event_time_utc": event_time,
                        "indicator_value_at_event": short_sma_val 
                    })
                    existing_signal_keys.add(new_key)
                    logger.debug(f"MA_CROSSOVER_DEBUG: SMA Buy Crossover terdeteksi di {timeframe_str} pada {event_time}.")

            elif (short_sma_val < long_sma_val and
                  prev_short_sma_val >= prev_long_sma_val):
                event_type = "SMA Sell Crossover"
                direction = "Bearish"
                
                new_key = (symbol_param, timeframe_str, event_time.isoformat(), event_type)
                if new_key not in existing_signal_keys:
                    signals_to_save_batch.append({
                        "symbol": symbol_param, "timeframe": timeframe_str, "event_type": event_type,
                        "direction": direction, "price_level": current_close_price_dec, "event_time_utc": event_time,
                        "indicator_value_at_event": short_sma_val 
                    })
                    existing_signal_keys.add(new_key)
                    logger.debug(f"MA_CROSSOVER_DEBUG: SMA Sell Crossover terdeteksi di {timeframe_str} pada {event_time}.")
        else:
            logger.debug(f"MA_CROSSOVER_DEBUG: Data SMA tidak lengkap untuk deteksi crossover pada {current_index}. Melewatkan.")

        # Deteksi Sinyal EMA Crossover
        if all(v is not None for v in [short_ema_val, long_ema_val, prev_short_ema_val, prev_long_ema_val]):
            if (short_ema_val > long_ema_val and
                prev_short_ema_val <= prev_long_ema_val):
                event_type = "EMA Buy Crossover"
                direction = "Bullish"
                
                new_key = (symbol_param, timeframe_str, event_time.isoformat(), event_type)
                if new_key not in existing_signal_keys:
                    signals_to_save_batch.append({
                        "symbol": symbol_param, "timeframe": timeframe_str, "event_type": event_type,
                        "direction": direction, "price_level": current_close_price_dec, "event_time_utc": event_time,
                        "indicator_value_at_event": short_ema_val 
                    })
                    existing_signal_keys.add(new_key)
                    logger.debug(f"MA_CROSSOVER_DEBUG: EMA Buy Crossover terdeteksi di {timeframe_str} pada {event_time}.")

            elif (short_ema_val < long_ema_val and
                  prev_short_ema_val >= prev_long_ema_val):
                event_type = "EMA Sell Crossover"
                direction = "Bearish"
                
                new_key = (symbol_param, timeframe_str, event_time.isoformat(), event_type)
                if new_key not in existing_signal_keys:
                    signals_to_save_batch.append({
                        "symbol": symbol_param, "timeframe": timeframe_str, "event_type": event_type,
                        "direction": direction, "price_level": current_close_price_dec, "event_time_utc": event_time,
                        "indicator_value_at_event": short_ema_val 
                    })
                    existing_signal_keys.add(new_key)
                    logger.debug(f"MA_CROSSOVER_DEBUG: EMA Sell Crossover terdeteksi di {timeframe_str} pada {event_time}.")
        else:
            logger.debug(f"MA_CROSSOVER_DEBUG: Data EMA tidak lengkap untuk deteksi crossover pada {current_index}. Melewatkan.")

    if signals_to_save_batch:
        try:
            database_manager.save_market_structure_events_batch(signals_to_save_batch)
            processed_count += len(signals_to_save_batch) 
            logger.info(f"BACKFILL WORKER: Berhasil menyimpan {len(signals_to_save_batch)} sinyal MA Crossover unik untuk {symbol_param} {timeframe_str}.")
        except Exception as e:
            logger.error(f"BACKFILL WORKER: Gagal menyimpan sinyal MA Crossover batch: {e}", exc_info=True)

    return processed_count


def get_trend_context_from_ma(symbol_param: str, timeframe_str: str) -> dict:
    logger.info(f"MODIFIKASI: Debugging Tambahan dan Robustness -- Memulai analisis konteks tren dari MA untuk {symbol_param} melintasi multi-timeframe.")
    
    # Pastikan _SYMBOL_POINT_VALUE diinisialisasi jika diperlukan oleh fungsi lain
    _initialize_symbol_point_value_callable = globals().get('_initialize_symbol_point_value')
    if _initialize_symbol_point_value_callable:
        _initialize_symbol_point_value_callable()

    # Inisialisasi default output, ini akan diisi atau ditimpa
    result_context = {
        "overall_trend": "Undefined",
        "reason": "Initial state, no trend determined yet.",
        "ema_short": float(np.nan),
        "ema_medium": float(np.nan),
        "ema_long": float(np.nan),
        "current_price": float(np.nan),
        "ema_crossover_signal": "No Signal"
    }

    # Inisialisasi skor di awal fungsi
    bullish_score = 0
    bearish_score = 0
    neutral_score = 0

    # PERBAIKAN: Validasi konfigurasi MA_TREND_PERIODS di awal
    if not hasattr(config.MarketData, 'MA_TREND_PERIODS') or not isinstance(config.MarketData.MA_TREND_PERIODS, list) or len(config.MarketData.MA_TREND_PERIODS) < 2:
        reason_msg = f"Konfigurasi periode MA tidak cukup ({config.MarketData.MA_TREND_PERIODS}). Diperlukan minimal 2 periode MA untuk menentukan tren."
        logger.warning(f"DEBUG_TREND_CONFIG: {reason_msg}")
        result_context.update({"overall_trend": "Undefined", "reason": reason_msg})
        return result_context

    # Ambil data historis
    candles_raw_data = database_manager.get_historical_candles_from_db(
        symbol=symbol_param, # <--- Baris ini penyebab error jika definisi di database_manager.py belum disesuaikan
        timeframe=timeframe_str,
        limit=config.AIAnalysts.TREND_MA_LONG_PERIOD + 10
    )
    # PERBAIKAN PENTING: Pastikan candles_df selalu Pandas DataFrame yang valid
    if not isinstance(candles_raw_data, list):
        reason_msg = f"Data mentah candle bukan list yang diharapkan: {type(candles_raw_data)}. Mengembalikan error."
        logger.error(f"DEBUG_TREND_DATA: {reason_msg}")
        result_context.update({"overall_trend": "Error", "reason": reason_msg})
        return result_context

    if not candles_raw_data: # Jika list kosong
        candles_df = pd.DataFrame()
    else:
        candles_df = pd.DataFrame(candles_raw_data)
        
        # Pastikan kolom 'open_time_utc' ada dan diatur sebagai indeks
        if 'open_time_utc' not in candles_df.columns:
            reason_msg = f"Kolom 'open_time_utc' tidak ditemukan di data candle dari DB untuk {timeframe_str}. Melewatkan."
            logger.error(f"DEBUG_TREND_DATA: {reason_msg}")
            result_context.update({"overall_trend": "Error", "reason": reason_msg})
            return result_context

        candles_df['open_time_utc'] = pd.to_datetime(candles_df['open_time_utc'], utc=True, errors='coerce')
        candles_df.dropna(subset=['open_time_utc'], inplace=True)
        candles_df = candles_df.set_index('open_time_utc').sort_index()

        # Rename kolom harga dan konversi ke Decimal (lalu ke float saat digunakan oleh EMA)
        candles_df = candles_df.rename(columns={
            'open_price': 'open', 'high_price': 'high', 'low_price': 'low', 'close_price': 'close'
        })
        for col in ['open', 'high', 'low', 'close']:
            if col in candles_df.columns:
                # Perbaikan: Pastikan konversi ke Decimal aman, tangani NaN dengan None
                candles_df[col] = candles_df[col].apply(lambda x: Decimal(str(x)) if pd.notna(x) else None) 
    
    logger.debug(f"DEBUG_TREND_PRICE_SOURCE: candles_df is None: {candles_df is None}, candles_df empty: {candles_df.empty}")

    # **PERBAIKAN 1 & 2**: Perbaiki pesan "Tidak cukup data"
    if candles_df.empty or len(candles_df) < config.AIAnalysts.TREND_MA_LONG_PERIOD:
        reason_msg = f"Tidak cukup data valid ({len(candles_df)} bar) untuk {timeframe_str} untuk menentukan tren MA. Diperlukan minimal {config.AIAnalysts.TREND_MA_LONG_PERIOD} lilin."
        logger.warning(reason_msg)
        result_context.update({"overall_trend": "Undefined", "reason": reason_msg})
        return result_context

    # Ambil harga saat ini (dari tick terbaru atau fallback D1 candle)
    current_price = None
    logger.debug("DEBUG_TREND_PRICE: Mengambil harga saat ini dari latest tick atau last D1 candle.")
    try:
        latest_tick_data = database_manager.get_latest_price_tick(symbol_param)
        if latest_tick_data and latest_tick_data.get('last_price') is not None:
            current_price = latest_tick_data['last_price']
            logger.debug(f"DEBUG_TREND_PRICE: Harga dari latest tick: {current_price}")
    except Exception as e:
        logger.error(f"Error saat mengambil latest price tick: {e}. Mencoba fallback.", exc_info=True)
        # Lanjutkan ke fallback jika ada error pada tick
    
    if current_price is None or current_price == Decimal('0.0'):
        logger.warning("DEBUG_TREND_PRICE: Latest tick tidak valid atau nol, mencoba fallback ke D1 candle terakhir.")
        try:
            # Menggunakan get_historical_candles_from_db untuk D1 sebagai fallback
            # Pastikan ini mengembalikan list of dicts yang bisa diakses
            latest_d1_candle_data = database_manager.get_historical_candles_from_db(symbol_param, "D1", limit=1)
            if latest_d1_candle_data and isinstance(latest_d1_candle_data, list) and latest_d1_candle_data:
                if latest_d1_candle_data[0].get('close_price') is not None:
                    current_price = latest_d1_candle_data[0]['close_price']
                    logger.debug(f"DEBUG_TREND_PRICE: Harga dari last D1 candle: {current_price}")
        except Exception as e:
            logger.error(f"Error saat mengambil D1 candle terakhir untuk harga: {e}. Harga saat ini tidak tersedia.", exc_info=True)
            
    if current_price is None or current_price == Decimal('0.0'):
        reason_msg = "Gagal mendapatkan harga saat ini dari semua sumber."
        logger.error(f"DEBUG_TREND_PRICE: {reason_msg}")
        result_context.update({"overall_trend": "Undefined", "reason": reason_msg})
        return result_context

    current_price_val = float(current_price) # Pastikan dalam float untuk perbandingan
    result_context['current_price'] = current_price_val

    # Hitung EMA series
    ema_short = globals().get('_calculate_ema_internal')(candles_df['close'], config.AIAnalysts.TREND_MA_SHORT_PERIOD)
    ema_medium = globals().get('_calculate_ema_internal')(candles_df['close'], config.AIAnalysts.TREND_MA_MEDIUM_PERIOD)
    ema_long = globals().get('_calculate_ema_internal')(candles_df['close'], config.AIAnalysts.TREND_MA_LONG_PERIOD)

    logger.debug(f"DEBUG_EMA_SERIES_TAIL: Short EMA tail: {ema_short.tail().to_dict()}, Medium EMA tail: {ema_medium.tail().to_dict()}, Long EMA tail: {ema_long.tail().to_dict()}")
    logger.debug(f"DEBUG_EMA_SERIES_NAN_COUNT: Short NaN: {ema_short.isnull().sum()}, Medium NaN: {ema_medium.isnull().sum()}, Long NaN: {ema_long.isnull().sum()}")

    # Ekstrak nilai scalar terakhir yang valid untuk perbandingan
    ema_short_val = ema_short.dropna().iloc[-1] if not ema_short.dropna().empty else np.nan
    ema_medium_val = ema_medium.dropna().iloc[-1] if not ema_medium.dropna().empty else np.nan
    ema_long_val = ema_long.dropna().iloc[-1] if not ema_long.dropna().empty else np.nan

    result_context['ema_short'] = float(ema_short_val)
    result_context['ema_medium'] = float(ema_medium_val)
    result_context['ema_long'] = float(ema_long_val)

    logger.debug(f"DEBUG_EMA_SCALAR_VALUES: Short: {ema_short_val:.2f}, Medium: {ema_medium_val:.2f}, Long: {ema_long_val:.2f}")
    logger.debug(f"DEBUG_CURRENT_PRICE_VAL: Current Price: {current_price_val:.2f}")

    # Validasi akhir untuk nilai EMA
    if np.isnan(ema_short_val) or np.isnan(ema_medium_val) or np.isnan(ema_long_val):
        reason_msg = f"Beberapa nilai EMA adalah NaN setelah ekstraksi scalar. Tidak dapat menentukan tren."
        logger.warning(f"DEBUG_TREND_CONFIG: {reason_msg}")
        result_context.update({"overall_trend": "Undefined", "reason": reason_msg})
        return result_context

    # Logika Crossover Signals (contoh sederhana Short vs Medium EMA) - Ini harus dievaluasi DULU
    # Karena crossover memberikan sinyal yang lebih spesifik
    if len(ema_short.dropna()) >= 2 and len(ema_medium.dropna()) >= 2:
        prev_ema_short = ema_short.dropna().iloc[-2]
        prev_ema_medium = ema_medium.dropna().iloc[-2]

        if not np.isnan(prev_ema_short) and not np.isnan(prev_ema_medium):
            if prev_ema_short < prev_ema_medium and ema_short_val > ema_medium_val:
                result_context['ema_crossover_signal'] = "Bullish Crossover"
                result_context['overall_trend'] = "Bullish Crossover"  # Prioritaskan overall_trend
                result_context['reason'] = f"EMA short ({ema_short_val:.2f}) crossed above EMA medium ({ema_medium_val:.2f})."
            elif prev_ema_short > prev_ema_medium and ema_short_val < ema_medium_val:
                result_context['ema_crossover_signal'] = "Bearish Crossover"
                result_context['overall_trend'] = "Bearish Crossover"  # Prioritaskan overall_trend
                result_context['reason'] = f"EMA short ({ema_short_val:.2f}) crossed below EMA medium ({ema_medium_val:.2f})."
    
    # Logika penentuan tren utama berdasarkan hubungan EMA (JIKA TIDAK ADA CROSSOVER YANG TERDETEKSI)
    if result_context['ema_crossover_signal'] == "No Signal": # Hanya jalankan logika ini jika tidak ada crossover
        logger.debug(f"DEBUG_TREND_COMPARE_START: short > medium: {ema_short_val > ema_medium_val}, medium > long: {ema_medium_val > ema_long_val}")
        logger.debug(f"DEBUG_TREND_COMPARE_START: short < medium: {ema_short_val < ema_medium_val}, medium < long: {ema_medium_val < ema_long_val}")

        if ema_short_val > ema_medium_val and ema_medium_val > ema_long_val:
            result_context['overall_trend'] = "Bullish"
            result_context['reason'] = f"EMA short ({ema_short_val:.2f}) > EMA medium ({ema_medium_val:.2f}) > EMA long ({ema_long_val:.2f})."
            bullish_score += 1 # Variabel ini didefinisikan di awal

            # Cek Pullback Bullish
            if current_price_val < ema_short_val:
                result_context['overall_trend'] = "Pullback"
                result_context['reason'] += f" Price pulling back within an underlying Bullish trend (Price: {current_price_val:.2f} < EMA short)."

        elif ema_short_val < ema_medium_val and ema_medium_val < ema_long_val:
            result_context['overall_trend'] = "Bearish"
            result_context['reason'] = f"EMA short ({ema_short_val:.2f}) < EMA medium ({ema_medium_val:.2f}) < EMA long ({ema_long_val:.2f})."
            bearish_score += 1 # Variabel ini didefinisikan di awal

            # Cek Pullback Bearish
            if current_price_val > ema_short_val:
                result_context['overall_trend'] = "Pullback"
                result_context['reason'] += f" Price pulling back within an underlying Bearish trend (Price: {current_price_val:.2f} > EMA short)."
        else:
            result_context['overall_trend'] = "Ranging" 
            result_context['reason'] = "EMAs are close or crossing, indicating ranging or unclear trend."
            neutral_score += 1 # Variabel ini didefinisikan di awal
    
    logger.debug(f"DEBUG_TREND_SCORES: Total Bullish Score: {bullish_score}, Bearish Score: {bearish_score}, Neutral Score: {neutral_score}")
    logger.info(f"MODIFIKASI: Debugging Tambahan dan Robustness -- Konteks Tren Multi-Timeframe untuk {symbol_param}: {result_context['overall_trend']} (B: {bullish_score}, Be: {bearish_score}, N: {neutral_score}).")

    return result_context

# --- MAIN PROCESSING FUNCTIONS ---

def update_daily_open_prices_logic(symbol_param):
    """
    Mengambil harga pembukaan harian (Daily Open Price) dari candle D1
    dan menyimpannya ke database.
    """
    logger.info(f"Mengupdate harga pembukaan harian untuk {symbol_param}...")
    try:
        daily_candles = mt5_connector.get_historical_candles(symbol_param, "D1", num_candles=1)
        if not daily_candles:
            logger.warning(f"Tidak ada candle D1 yang ditemukan untuk {symbol_param} untuk menghitung daily open price. Tidak dapat mengupdate daily open.")
            return False
        latest_daily_candle = daily_candles[0]
        daily_open_price = latest_daily_candle.get('open_price')
        open_time_utc = latest_daily_candle.get('open_time_utc')
        if daily_open_price is None or open_time_utc is None:
            logger.warning(f"Data candle D1 yang tidak lengkap untuk {symbol_param}. 'open_price' atau 'open_time_utc' hilang. Daily open price tidak dapat disimpan.")
            return False
        if isinstance(open_time_utc, str):
            try:
                open_time_utc = datetime.fromisoformat(open_time_utc)
            except ValueError as ve:
                logger.error(f"Gagal mengonversi open_time_utc string '{open_time_utc}' ke datetime: {ve}", exc_info=True)
                return False
        date_referenced = open_time_utc.date().isoformat()

        # --- PERUBAHAN: Sekarang langsung panggil database_manager, retry ditangani oleh DB Writer Worker ---
        try:
            database_manager.save_daily_open_price(
                date_referenced=date_referenced,
                reference_price=daily_open_price,
                source_candle_time_utc=open_time_utc,
                symbol=symbol_param
            )
        except Exception as e: # Tangkap error umum jika pengiriman ke antrean gagal
            logger.error(f"Gagal mengirim Daily Open ke antrean DB untuk {symbol_param} {date_referenced}: {e}", exc_info=True)
            return False
        # --- AKHIR PERUBAHAN ---

        logger.info(f"Harga pembukaan harian untuk {symbol_param} pada {date_referenced} berhasil diatur ke {daily_open_price}.")
        return True
    except Exception as e:
        logger.error(f"Error saat menghitung atau menyimpan daily open prices untuk {symbol_param}: {e}", exc_info=True)
        return False



def _detect_new_sr_levels_historically(symbol_param: str, timeframe_str: str, candles_df: pd.DataFrame, current_atr_value: Decimal): # <--- MODIFIKASI INI
    """
    Mendeteksi level Support & Resistance (S&R) baru secara historis.
    Level S&R adalah area di mana harga menunjukkan reaksi signifikan di masa lalu.
    Menyimpan level S&R baru ke database secara batch dengan deduplikasi.
    """
    logger.info(f"Mendeteksi level Support/Resistance BARU historis untuk {symbol_param} {timeframe_str}...")

    if candles_df.empty or len(candles_df) < 50: # Minimal lilin untuk deteksi S&R
        logger.debug(f"Tidak cukup lilin ({len(candles_df)}) untuk deteksi level S&R historis di {timeframe_str}.")
        return

    # Pastikan _SYMBOL_POINT_VALUE diinisialisasi
    if _SYMBOL_POINT_VALUE is None:
        _initialize_symbol_point_value()
    if _SYMBOL_POINT_VALUE is None:
        logger.error(f"Nilai POINT untuk {symbol_param} tidak tersedia. Melewatkan deteksi level S&R.")
        return

        # Buat salinan DataFrame dan konversi kolom harga ke float untuk operasi internal
    df_processed = candles_df.copy()
    df_processed = df_processed.rename(columns={
        'open_price': 'open', 'high_price': 'high', 'low_price': 'low',
        'close_price': 'close', 'tick_volume': 'volume', 'real_volume': 'real_volume', 'spread': 'spread'
    })
    # KOREKSI: HANYA konversi kolom harga dan volume
    for col in ['open', 'high', 'low', 'close', 'volume', 'real_volume', 'spread']:
        if col in df_processed.columns:
            df_processed[col] = df_processed[col].apply(to_float_or_none)

    # Ambil swing highs/lows untuk menentukan titik acuan S&R
    swing_results_df = _calculate_swing_highs_lows_internal(df_processed, config.AIAnalysts.SWING_EXT_BARS)
    
    if swing_results_df.empty or swing_results_df['HighLow'].isnull().all():
        logger.debug(f"Tidak ada swing points yang valid untuk {symbol_param} {timeframe_str}. Melewatkan deteksi level S&R.")
        return

    # --- Deduplikasi: Ambil level S&R yang sudah ada di DB ---
    existing_sr_keys = set()
    try:
        existing_srs_db = database_manager.get_support_resistance_levels(
            symbol=symbol_param,
            timeframe=timeframe_str,
            start_time_utc=candles_df.index.min().to_pydatetime()
        )
        for sr_item in existing_srs_db:
            formation_time_dt = utils.to_utc_datetime_or_none(sr_item['formation_time_utc'])
            if formation_time_dt:
                existing_sr_keys.add((
                    sr_item['symbol'], sr_item['timeframe'], sr_item['level_type'],
                    utils.to_float_or_none(sr_item['price_level']), # Gunakan float untuk perbandingan set key
                    formation_time_dt.replace(microsecond=0).isoformat()
                ))
        logger.debug(f"Ditemukan {len(existing_sr_keys)} level S&R yang sudah ada di DB untuk {timeframe_str}.")
    except Exception as e:
        logger.error(f"Gagal mengambil existing S&R Levels dari DB untuk pre-filtering: {e}", exc_info=True)
        pass

    detected_sr_levels_to_save_batch = []
    
    sr_tolerance_points = config.RuleBasedStrategy.RULE_SR_TOLERANCE_POINTS
    sr_tolerance_value = sr_tolerance_points * _SYMBOL_POINT_VALUE # Ini adalah Decimal

    # Toleransi untuk mengelompokkan level S&R (dalam poin)
    if current_atr_value is not None and current_atr_value > Decimal('0.0'):
        sr_tolerance_value = current_atr_value * config.MarketData.ATR_MULTIPLIER_FOR_TOLERANCE
        logger.debug(f"S&R Detektor: Menggunakan toleransi dinamis ATR: {float(sr_tolerance_value):.5f} (dari ATR {float(current_atr_value):.5f})")
    else:
        sr_tolerance_points = config.RuleBasedStrategy.RULE_SR_TOLERANCE_POINTS
        sr_tolerance_value = sr_tolerance_points * _SYMBOL_POINT_VALUE
        logger.warning(f"S&R Detektor: ATR tidak valid. Menggunakan toleransi S&R statis: {float(sr_tolerance_value):.5f} (dari {sr_tolerance_points} poin)")
    
    retest_window_candles = config.RuleBasedStrategy.SR_STRENGTH_RETEST_WINDOW_CANDLES
    break_tolerance_multiplier = config.RuleBasedStrategy.SR_STRENGTH_BREAK_TOLERANCE_MULTIPLIER


    # Iterasi melalui setiap swing point untuk mencari level S&R
    for i in range(len(df_processed)):
        if pd.isna(swing_results_df['HighLow'].iloc[i]):
            continue # Lewati jika bukan swing point

        current_candle_time = df_processed.index[i].to_pydatetime()
        current_swing_type = swing_results_df['HighLow'].iloc[i] # 1 for high, -1 for low
        current_swing_level = utils.to_decimal_or_none(swing_results_df['Level'].iloc[i])

        if current_swing_level is None:
            continue

        level_type = "Resistance" if current_swing_type == 1 else "Support"
        
        # Untuk S&R, kita bisa menggunakan swing high/low sebagai level potensial
        price_level_dec = current_swing_level

        # Buat zona S&R sedikit di sekitar level
        zone_start_price_dec = price_level_dec - (sr_tolerance_value / Decimal('2'))
        zone_end_price_dec = price_level_dec + (sr_tolerance_value / Decimal('2'))

        # --- MULAI KODE MODIFIKASI UNTUK PERHITUNGAN strength_score ---
        calculated_strength_score = 0
        # Tentukan jendela pencarian untuk retest (menggunakan parameter dari config)
        search_window_start_idx = i + 1 
        search_window_end_idx = min(i + config.RuleBasedStrategy.SR_STRENGTH_RETEST_WINDOW_CANDLES, len(df_processed)) 

        if search_window_start_idx < search_window_end_idx:
            # Ambil lilin dalam jendela pencarian
            relevant_candles_for_strength = df_processed.iloc[search_window_start_idx:search_window_end_idx]

            # Hitung ambang batas break signifikan berdasarkan toleransi SR dan multiplier dari config
            significant_break_tolerance = sr_tolerance_value * config.RuleBasedStrategy.SR_STRENGTH_BREAK_TOLERANCE_MULTIPLIER

            for _, candle_row in relevant_candles_for_strength.iterrows():
                # Pastikan ini mengakses kolom 'high' dan 'low' yang sudah di-rename
                candle_high = candle_row['high']
                candle_low = candle_row['low']
                candle_close = candle_row['close'] # Perlu juga close price untuk konfirmasi break

                # Kriteria "touch" atau "retest": lilin menyentuh level dalam toleransi SR umum
                if (price_level_dec <= utils.to_decimal_or_none(candle_high) + sr_tolerance_value and
                    price_level_dec >= utils.to_decimal_or_none(candle_low) - sr_tolerance_value):

                    # Pastikan sentuhan tidak menembus level secara signifikan (close di luar toleransi break)
                    is_broken_significantly = False
                    if level_type == "Resistance" and utils.to_decimal_or_none(candle_close) > price_level_dec + significant_break_tolerance:
                        is_broken_significantly = True
                    elif level_type == "Support" and utils.to_decimal_or_none(candle_close) < price_level_dec - significant_break_tolerance:
                        is_broken_significantly = True

                    if not is_broken_significantly:
                        calculated_strength_score += 1 # Tambahkan skor jika ada retest yang valid
                        
        # --- AKHIR KODE MODIFIKASI UNTUK PERHITUNGAN strength_score --
        
        # Tambahkan ke batch jika belum ada duplikat
        new_key = (symbol_param, timeframe_str, level_type, 
                   float(price_level_dec), # Gunakan float untuk perbandingan set key
                   current_candle_time.replace(microsecond=0).isoformat())

        if new_key not in existing_sr_keys:
            detected_sr_levels_to_save_batch.append({
                "symbol": symbol_param, "timeframe": timeframe_str,
                "level_type": level_type,
                "price_level": price_level_dec,
                "zone_start_price": zone_start_price_dec,
                "zone_end_price": zone_end_price_dec,
                "strength_score": calculated_strength_score, # Gunakan skor yang dihitung
                "is_active": True, # Awalnya aktif
                "formation_time_utc": current_candle_time,
                "last_test_time_utc": None, # Akan diupdate oleh fungsi update_existing_sr_levels_status
                "is_key_level": False, # Akan diupdate oleh fungsi identify_key_levels_across_timeframes
                "confluence_score": 0 # Akan diupdate oleh fungsi lain
            })
            existing_sr_keys.add(new_key)
            logger.debug(f"Level S&R {level_type} terdeteksi di {timeframe_str} pada {current_candle_time}.")


    if detected_sr_levels_to_save_batch:
        try:
            database_manager.save_support_resistance_levels_batch(detected_sr_levels_to_save_batch, [])
            logger.info(f"BACKFILL WORKER: Berhasil menyimpan {len(detected_sr_levels_to_save_batch)} level S&R baru unik untuk {symbol_param} {timeframe_str}.")
        except Exception as e:
            logger.error(f"BACKFILL WORKER: Gagal menyimpan level S&R batch: {e}", exc_info=True)

    logger.info(f"Selesai mendeteksi level Support/Resistance BARU historis untuk {symbol_param} {timeframe_str}.")






def _update_sr_level_activity(symbol_param, timeframe_str, recent_candles_data: list, tolerance_points: int = 10):
    """
    Menggunakan parameter toleransi yang baru dari config.RuleBasedStrategy.
    """
    logger.info(f"Memperbarui aktivitas level S&R untuk {symbol_param} {timeframe_str}...")

    # Pastikan _SYMBOL_POINT_VALUE sudah diinisialisasi
    if _SYMBOL_POINT_VALUE is None:
        _initialize_symbol_point_value()
    if _SYMBOL_POINT_VALUE is None:
        logger.error(f"Nilai POINT untuk {symbol_param} tidak tersedia. Tidak dapat memperbarui aktivitas S&R.")
        return

    active_sr_levels = database_manager.get_support_resistance_levels(
        symbol=symbol_param, timeframe=timeframe_str, is_active=True
    )

    if not active_sr_levels:
        logger.debug(f"Tidak ada level S&R aktif untuk {symbol_param} {timeframe_str} untuk diperbarui.")
        return
    if not recent_candles_data:
        logger.warning(f"Tidak ada data lilin terbaru untuk {symbol_param} {timeframe_str} untuk memperbarui aktivitas S&R.")
        return

    # MENGGUNAKAN PARAMETER DARI CONFIG.RULEBASEDSTRATEGY.RULE_SR_TOLERANCE_POINTS
    tolerance_value = Decimal(str(config.RuleBasedStrategy.RULE_SR_TOLERANCE_POINTS)) * _SYMBOL_POINT_VALUE
    logger.debug(f"Toleransi S&R untuk {timeframe_str}: {float(tolerance_value):.5f} Dolar (dari {config.RuleBasedStrategy.RULE_SR_TOLERANCE_POINTS} MT5 points).")

    df_recent = pd.DataFrame(recent_candles_data)
    df_recent['high_price'] = df_recent['high_price'].apply(lambda x: Decimal(str(x)))
    df_recent['low_price'] = df_recent['low_price'].apply(lambda x: Decimal(str(x)))
    df_recent['open_time_utc'] = pd.to_datetime(df_recent['open_time_utc'])

    updated_count = 0
    deactivated_count = 0

    for sr_level in active_sr_levels:
        level_price = Decimal(str(sr_level['price_level']))
        is_currently_interacting = False
        last_test_time_dt = None

        if sr_level['last_test_time_utc']:
            try:
                last_test_time_dt = datetime.fromisoformat(sr_level['last_test_time_utc'])
            except (ValueError, TypeError) as e:
                logger.warning(f"Gagal mengonversi last_test_time_utc '{sr_level['last_test_time_utc']}' (tipe: {type(sr_level['last_test_time_utc'])}) ke datetime: {e}. Menggunakan None.")
                last_test_time_dt = None

        for _, candle_row in df_recent.iterrows():
            candle_time_dt = candle_row['open_time_utc'].to_pydatetime()

            if (level_price <= candle_row['high_price'] + tolerance_value and
                    level_price >= candle_row['low_price'] - tolerance_value):
                is_currently_interacting = True
                if last_test_time_dt is None or candle_time_dt > last_test_time_dt:
                    last_test_time_dt = candle_time_dt
                break

        if not is_currently_interacting:
            sr_level['is_active'] = False
            deactivated_count += 1
            logger.debug(f"Deaktivasi S&R {sr_level['level_type']} @ {float(level_price):.5f} di {timeframe_str} karena tidak ada interaksi di {len(recent_candles_data)} lilin terakhir.")

        database_manager.save_support_resistance_level(
            symbol=symbol_param,
            timeframe=timeframe_str,
            level_type=sr_level['level_type'],
            price_level=level_price,
            is_active=sr_level['is_active'],
            formation_time=pd.to_datetime(sr_level['formation_time_utc']).to_pydatetime(),
            strength_score=sr_level.get('strength_score'),
            zone_start_price=sr_level.get('zone_start_price'),
            zone_end_price=sr_level.get('zone_end_price'),
            last_test_time_utc=last_test_time_dt if last_test_time_dt else None,
            is_key_level=sr_level.get('is_key_level', False),
            confluence_score=sr_level.get('confluence_score', 0)
        )
        updated_count += 1
    logger.info(f"Selesai memperbarui aktivitas S&R untuk {symbol_param} {timeframe_str}. Deaktivasi: {deactivated_count}.")


def update_all_sr_sd_data(symbol_param):
    """
    Mendeteksi dan memperbarui level Support & Resistance serta Supply & Demand Zones.
    Menggunakan parameter toleransi yang baru dari config.RuleBasedStrategy.
    """
    logger.info(f"Mendeteksi dan memperbarui Support/Resistance dan Supply & Demand untuk {symbol_param}...")

    _initialize_symbol_point_value()

    if _SYMBOL_POINT_VALUE is None:
        logger.error(f"Nilai POINT untuk {symbol_param} tidak tersedia. Melewatkan deteksi S&R/S&D.")
        return

    timeframes = ["M15", "M30", "H1", "H4", "D1"]

    min_base_candles = 2
    max_base_candles = 5

    if current_atr_value is not None and current_atr_value > Decimal('0.0'):
        min_impulse_move_value_dec = config.RuleBasedStrategy.SD_MIN_IMPULSIVE_MOVE_ATR_MULTIPLIER * current_atr_value
        if current_atr_value <= Decimal('0.0'): # Fallback jika ATR tidak valid
            min_impulse_move_value_dec = Decimal('50') * globals().get('_SYMBOL_POINT_VALUE')
            logger.warning(f"S&D Detektor: ATR tidak valid ({current_atr_value}). Menggunakan min_impulse_move_value statis: {float(min_impulse_move_value_dec):.5f}")
        else:
            logger.debug(f"S&D Detektor: Menggunakan min_impulse_move_value dinamis ATR: {float(min_impulse_move_value_dec):.5f} (dari ATR {float(current_atr_value):.5f})")

    base_candle_body_ratio = Decimal('0.3')
    sr_tolerance_points_cfg = config.RuleBasedStrategy.RULE_SR_TOLERANCE_POINTS

    for tf in timeframes:
        if tf == "M15":
            candles_limit = 1000
        elif tf == "M30":
            candles_limit = 750
        elif tf == "H1":
            candles_limit = 500
        elif tf == "H4":
            candles_limit = 300
        elif tf == "D1":
            candles_limit = 200
        else:
            candles_limit = 200

        logger.info(f"Mengambil {candles_limit} candle untuk deteksi S&R/S&D di {tf}...")
        candles = database_manager.get_historical_candles_from_db(symbol_param, tf, limit=candles_limit)

        if not candles or len(candles) < max(12, min_base_candles + 2):
            logger.warning(f"Tidak ada candle cukup ({len(candles)}) untuk {symbol_param} {tf} untuk deteksi S&R/S&D. Diperlukan minimal {max(12, min_base_candles + 2)} lilin.")
            continue

        try:
            df = pd.DataFrame(candles).sort_values(by='open_time_utc', ascending=True)
            df['open_time_utc'] = pd.to_datetime(df['open_time_utc'])
            df.set_index('open_time_utc', inplace=True)
            for col in ['open_price', 'high_price', 'low_price', 'close_price']:
                df[col] = df[col].apply(lambda x: Decimal(str(x)))

            # S&R Levels (Deteksi baru)
            sr_levels_found = _detect_sr_levels_from_candles(
                symbol_param, tf, candles,
                tolerance_points=sr_tolerance_points_cfg,
                min_bar_distance=10, num_extreme_candles=10
            )
            # --- PERUBAHAN: Sekarang langsung panggil database_manager.save_support_resistance_levels_batch ---
            if sr_levels_found:
                try:
                    # save_support_resistance_levels_batch menerima new_items_data dan updated_items_data
                    database_manager.save_support_resistance_levels_batch(sr_levels_found, [])
                except Exception as e:
                    logger.error(f"Gagal mengirim S&R Levels ke antrean DB: {e}", exc_info=True)
            # --- AKHIR PERUBAHAN ---
            logger.info(f"Berhasil mendeteksi {len(sr_levels_found)} level S&R untuk {symbol_param} {tf} dan dikirim ke antrean.")

            # --- PERUBAHAN: Sekarang langsung panggil _update_sr_level_activity ---
            try:
                # updated_srs_list sebagai argumen untuk dikosongkan setelahnya
                _update_existing_sr_levels_status(symbol_param, tf, df, [])
            except Exception as e:
                logger.error(f"Gagal mengirim update S&R Level Activity ke antrean DB: {e}", exc_info=True)
            # --- AKHIR PERUBAHAN ---

            detected_sd_count = 0

            # --- PERUBAHAN: Mengumpulkan S&D baru ke list lalu panggil batch save ---
            new_sds_to_save_in_batch = []
            # --- AKHIR PERUBAHAN ---

            min_required_idx_for_sd = max_base_candles + 1

            for i in range(len(df) - 1, min_required_idx_for_sd, -1):
                for base_len in range(min_base_candles, max_base_candles + 1):
                    base_start_idx = i - base_len
                    if base_start_idx < 0:
                        continue

                    if base_start_idx == 0:
                        continue

                    base_candles_segment = df.iloc[base_start_idx: i]
                    is_valid_base = True
                    for _, b_candle in base_candles_segment.iterrows():
                        candle_range = b_candle['high_price'] - b_candle['low_price']
                        candle_body = abs(b_candle['close_price'] - b_candle['open_price'])
                        if candle_range == 0:
                            if candle_body > (Decimal('0.1') * _SYMBOL_POINT_VALUE):
                                is_valid_base = False
                                break
                        elif (candle_body / candle_range) > base_candle_body_ratio:
                            is_valid_base = False
                            break
                    if not is_valid_base:
                        continue

                    prev_impulse_candle = df.iloc[base_start_idx - 1]
                    current_impulse_candle = df.iloc[i]

                    is_rally_before_base = (prev_impulse_candle['close_price'] - prev_impulse_candle['open_price']) > min_impulse_move_value # <--- MODIFIKASI INI
                    is_drop_before_base = (prev_impulse_candle['open_price'] - prev_impulse_candle['close_price']) > min_impulse_move_value # <--- MODIFIKASI INI
                    is_rally_after_base = (current_impulse_candle['close_price'] - current_impulse_candle['open_price']) > min_impulse_move_value # <--- MODIFIKASI INI
                    is_drop_after_base = (current_impulse_candle['open_price'] - current_impulse_candle['close_price']) > min_impulse_move_value # <--- MODIFIKASI INI

                    if is_valid_base and is_rally_after_base:
                        zone_type_sd = "Demand"
                        zone_top_price_sd = base_candles_segment['high_price'].max()
                        zone_bottom_price_sd = base_candles_segment['low_price'].min()
                        formation_time_sd = base_candles_segment.index[0].to_pydatetime()

                        if is_drop_before_base:
                            base_type_sd = "DropBaseRally"
                            # --- PERUBAHAN: Tambahkan ke list batch ---
                            new_sds_to_save_in_batch.append({
                                "symbol": symbol_param, "timeframe": tf, "zone_type": zone_type_sd, "base_type": base_type_sd,
                                "zone_top_price": zone_top_price_sd, "zone_bottom_price": zone_bottom_price_sd,
                                "formation_time_utc": formation_time_sd, "is_mitigated": False
                            })
                            # --- AKHIR PERUBAHAN ---
                            detected_sd_count += 1
                            logger.info(f"Demand Zone (DBR) terdeteksi: {float(zone_bottom_price_sd)}-{float(zone_top_price_sd)} di {tf} pada {formation_time_sd}.")
                        elif is_rally_before_base:
                            base_type_sd = "RallyBaseRally"
                            # --- PERUBAHAN: Tambahkan ke list batch ---
                            new_sds_to_save_in_batch.append({
                                "symbol": symbol_param, "timeframe": tf, "zone_type": zone_type_sd, "base_type": base_type_sd,
                                "zone_top_price": zone_top_price_sd, "zone_bottom_price": zone_bottom_price_sd,
                                "formation_time_utc": formation_time_sd, "is_mitigated": False
                            })
                            # --- AKHIR PERUBAHAN ---
                            detected_sd_count += 1
                            logger.info(f"Demand Zone (RBR) terdeteksi: {float(zone_bottom_price_sd)}-{float(zone_top_price_sd)} di {tf} pada {formation_time_sd}.")

                    elif is_valid_base and is_drop_after_base:
                        zone_type_sd = "Supply"
                        zone_top_price_sd = base_candles_segment['high_price'].max()
                        zone_bottom_price_sd = base_candles_segment['low_price'].min()
                        formation_time_sd = base_candles_segment.index[0].to_pydatetime()

                        if is_rally_before_base:
                            base_type_sd = "RallyBaseDrop"
                            # --- PERUBAHAN: Tambahkan ke list batch ---
                            new_sds_to_save_in_batch.append({
                                "symbol": symbol_param, "timeframe": tf, "zone_type": zone_type_sd, "base_type": base_type_sd,
                                "zone_top_price": zone_top_price_sd, "zone_bottom_price": zone_bottom_price_sd,
                                "formation_time_utc": formation_time_sd, "is_mitigated": False
                            })
                            # --- AKHIR PERUBAHAN ---
                            detected_sd_count += 1
                            logger.info(f"Supply Zone (RBD) terdeteksi: {float(zone_bottom_price_sd)}-{float(zone_top_price_sd)} di {tf} pada {formation_time_sd}.")
                        elif is_drop_before_base:
                            base_type_sd = "DropBaseDrop"
                            # --- PERUBAHAN: Tambahkan ke list batch ---
                            new_sds_to_save_in_batch.append({
                                "symbol": symbol_param, "timeframe": tf, "zone_type": zone_type_sd, "base_type": base_type_sd,
                                "zone_top_price": zone_top_price_sd, "zone_bottom_price": zone_bottom_price_sd,
                                "formation_time_utc": formation_time_sd, "is_mitigated": False
                            })
                            # --- AKHIR PERUBAHAN ---
                            detected_sd_count += 1
                            logger.info(f"Supply Zone (DBD) terdeteksi: {float(zone_bottom_price_sd)}-{float(zone_top_price_sd)} di {tf} pada {formation_time_sd}.")

            logger.info(f"Total {detected_sd_count} Supply & Demand Zones terdeteksi dan dikirim ke antrean untuk {symbol_param} {tf}.")

            # --- PERUBAHAN: Panggil batch save untuk S&D baru ---
            if new_sds_to_save_in_batch:
                try:
                    database_manager.save_supply_demand_zones_batch(new_sds_to_save_in_batch, [])
                except Exception as e:
                    logger.error(f"Gagal mengirim new S&D Zones batch ke antrean DB: {e}", exc_info=True)
            # --- AKHIR PERUBAHAN ---

            logger.info(f"Memeriksa invalidasi Supply & Demand Zones untuk {symbol_param} {tf}...")
            # --- PERUBAHAN: Panggil _update_existing_supply_demand_zones_status ---
            try:
                _update_existing_supply_demand_zones_status(symbol_param, tf, df, [])
            except Exception as e:
                logger.error(f"Gagal mengirim update S&D Zone Status ke antrean DB: {e}", exc_info=True)
            # --- AKHIR PERUBAHAN ---

        except Exception as e:
            logger.error(f"Gagal mendeteksi S&R/S&D untuk {tf}: {e}", exc_info=True)

    logger.info(f"Selesai memperbarui Support/Resistance dan Supply & Demand untuk {symbol_param}.")


def update_all_historical_candles():
    """
    Mengambil dan menyimpan candle historis untuk semua timeframe yang dikonfigurasi
    dan diaktifkan. Akan mengambil data mulai dari HISTORICAL_DATA_START_DATE_FULL.
    """
    logger.info("Memulai update semua historical candles (pengambilan data mentah dari MT5)...")
    
    enabled_timeframes = [
        tf for tf, enabled in config.MarketData.ENABLED_TIMEFRAMES.items()
        if enabled and tf in config.MarketData.COLLECT_TIMEFRAMES
    ]
    
    # Ambil tanggal awal historis yang diizinkan untuk penyimpanan (batas bawah keras)
    full_historical_start_date_str = config.MarketData.HISTORICAL_DATA_START_DATE_FULL
    try:
        full_historical_start_datetime_for_storage = datetime.strptime(full_historical_start_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    except ValueError as ve:
        logger.critical(f"Format HISTORICAL_DATA_START_DATE_FULL di config.py tidak valid: {full_historical_start_date_str}. Error: {ve}", exc_info=True)
        return # Hentikan jika konfigurasi awal tidak valid

    for tf_name in enabled_timeframes:
        try:
            # Query DB untuk candle terlama yang sudah ada, TAPI hanya yang >= full_historical_start_datetime_for_storage
            # Ini memastikan kita hanya mempertimbangkan data dalam rentang yang diinginkan.
            earliest_candle_in_db = database_manager.get_historical_candles_from_db(
                config.TRADING_SYMBOL, tf_name, limit=1, order_asc=True,
                min_open_time_utc=full_historical_start_datetime_for_storage # MODIFIKASI INI
            )
            
            from_date_for_mt5_query = None
            if earliest_candle_in_db:
                # Jika ada data di DB (dan sudah difilter >= tanggal awal), mulai query MT5 dari 1 detik setelahnya.
                from_date_for_mt5_query = earliest_candle_in_db[0]['open_time_utc'] + timedelta(seconds=1)
                logger.info(f"Untuk TF {tf_name}, DB sudah punya data sampai {from_date_for_mt5_query - timedelta(seconds=1)} (>= tanggal awal). Akan ambil data baru dari MT5 mulai dari {from_date_for_mt5_query.isoformat()}.")
            else:
                # Jika database kosong untuk TF ini (dalam rentang tanggal awal),
                # atau semua data yang ada lebih tua dari tanggal awal,
                # mulailah query MT5 dari full_historical_start_datetime_for_storage.
                from_date_for_mt5_query = full_historical_start_datetime_for_storage
                logger.info(f"Untuk TF {tf_name}, DB kosong dalam rentang tanggal awal. Akan ambil data dari MT5 mulai dari HISTORICAL_DATA_START_DATE_FULL: {from_date_for_mt5_query.isoformat()}.")

            to_date_for_mt5_query = datetime.now(timezone.utc)

            logger.info(f"Mengambil candle historis dari MT5 untuk {config.TRADING_SYMBOL} TF {tf_name} dari {from_date_for_mt5_query.date()} hingga {to_date_for_mt5_query.date()}.")
            # Panggil mt5_connector dengan from_date_for_mt5_query (yang sudah menghormati batas bawah)
            candles_data = mt5_connector.get_historical_candles_in_range(config.TRADING_SYMBOL, tf_name, from_date_for_mt5_query, to_date_for_mt5_query)
            
            if candles_data:
                # mt5_connector.get_historical_candles_in_range sudah memfilter data yang diambil
                # agar hanya berada dalam rentang [from_date_for_mt5_query, to_date_for_mt5_query].
                # Jadi, data yang disimpan ke DB sudah sesuai batas bawah.
                try:
                    database_manager.save_historical_candles(candles_data)
                except Exception as e:
                    logger.error(f"Gagal mengirim historical candles ke antrean DB untuk {tf_name}: {e}", exc_info=True)
                logger.info(f"Berhasil mengirim {len(candles_data)} candle untuk {tf_name} ke antrean DB.")
            else:
                logger.warning(f"Tidak ada candle baru yang diterima dari MT5 untuk {config.TRADING_SYMBOL} TF {tf_name} dalam rentang {from_date_for_mt5_query} - {to_date_for_mt5_query}.")
        except Exception as e:
            logger.error(f"Gagal memperbarui candle historis untuk {tf_name}: {e}", exc_info=True)
    logger.info(f"Selesai update semua historical candles (pengambilan data mentah).")

def _detect_yearly_extremes(symbol_param: str, min_start_date: datetime) -> int:
    """
    Mendeteksi harga tertinggi dan terendah untuk setiap tahun dari tanggal mulai hingga saat ini.
    Menyimpan ini sebagai MarketStructureEvent.
    Args:
        symbol_param (str): Simbol trading.
        min_start_date (datetime): Tanggal mulai minimum untuk mendeteksi ekstrem tahunan.
                                   Ini harus sesuai dengan HISTORICAL_DATA_START_DATE_FULL.
    Returns:
        int: Jumlah ekstrem tahunan baru yang berhasil dideteksi dan dikirim ke antrean DB.
    """
    logger.info(f"Mendeteksi ekstrem tahunan untuk {symbol_param} dari {min_start_date.isoformat()}...")
    processed_count = 0 
    
    # Inisialisasi timeframe_str karena fungsi ini selalu untuk D1
    timeframe_str = "D1" # D1 untuk yearly extremes

    # Ambil semua candle D1 dari database (yang sudah difilter oleh backfill process)
    all_d1_candles = database_manager.get_historical_candles_from_db(
        symbol_param,
        "D1",
        start_time_utc=min_start_date,
        end_time_utc=datetime.now(timezone.utc), # Ini akan menjadi titik ketidakkonsistenan jika datetime.now tidak dimock
        order_asc=True
    )

    if not all_d1_candles:
        logger.warning(f"Tidak ada data candle D1 yang cukup untuk {symbol_param} dari {min_start_date}. Melewatkan deteksi ekstrem tahunan.")
        return 0 

    df_d1 = pd.DataFrame(all_d1_candles)
    df_d1['open_time_utc'] = pd.to_datetime(df_d1['open_time_utc'])
    df_d1.set_index('open_time_utc', inplace=True)
    for col in ['open_price', 'high_price', 'low_price', 'close_price']:
        if col in df_d1.columns: 
            df_d1[col] = df_d1[col].apply(lambda x: Decimal(str(x)) if pd.notna(x) else None)
        else:
            df_d1[col] = None 
    
    initial_len = len(df_d1)
    df_d1.dropna(subset=['high_price', 'low_price'], inplace=True)
    if len(df_d1) < initial_len:
        logger.warning(f"Dihapus {initial_len - len(df_d1)} baris dengan NaN di high_price/low_price untuk _detect_yearly_extremes.")

    if df_d1.empty:
        logger.warning(f"DataFrame D1 kosong setelah pembersihan NaN untuk {symbol_param}. Melewatkan deteksi ekstrem tahunan.")
        return 0

    # Kelompokkan berdasarkan tahun
    df_d1['year'] = df_d1.index.year

    extremes_to_save_batch = []

    existing_extreme_keys = set()
    try:
        existing_extremes_db = database_manager.get_market_structure_events(
            symbol=symbol_param,
            timeframe=timeframe_str, # Menggunakan variabel yang sudah didefinisikan
            event_type=['Yearly High', 'Yearly Low'],
            start_time_utc=min_start_date 
        )
        for ext in existing_extremes_db:
            ext_event_time_dt = ext['event_time_utc']
            price_level_for_key = float(ext['price_level'])
            existing_extreme_keys.add((ext['symbol'], ext['timeframe'], ext_event_time_dt.replace(microsecond=0).isoformat(), ext['event_type'], price_level_for_key))
        # Perbaikan log: gunakan timeframe_str
        logger.debug(f"Ditemukan {len(existing_extreme_keys)} ekstrem tahunan yang sudah ada di DB untuk {timeframe_str}.") 
    except Exception as e:
        logger.error(f"Gagal mengambil existing yearly extremes dari DB untuk pre-filtering: {e}", exc_info=True)
        pass

    for year, year_data in df_d1.groupby('year'):
        if year_data.empty:
            logger.warning(f"Tidak ada data untuk tahun {year} setelah grouping/pembersihan. Melewatkan.")
            continue
        
        # PERBAIKAN: Validasi jumlah lilin minimum per tahun untuk menghitung ekstrem yang valid
        # Misalnya, minimal 2 lilin agar high dan low tidak selalu sama persis.
        # Atau jika ingin lebih ketat, bisa diset ke nilai yang lebih tinggi (misal 30 hari).
        MIN_CANDLES_PER_YEAR_FOR_EXTREME = 2 
        if len(year_data) < MIN_CANDLES_PER_YEAR_FOR_EXTREME:
            logger.warning(f"Tidak cukup data ({len(year_data)} bar) untuk tahun {year} untuk menghitung ekstrem tahunan yang berarti. Diperlukan minimal {MIN_CANDLES_PER_YEAR_FOR_EXTREME} lilin. Melewatkan.")
            continue

        yearly_high_price = year_data['high_price'].max()
        yearly_high_time_index = year_data['high_price'].idxmax()
        yearly_high_time_utc = yearly_high_time_index.to_pydatetime()

        yearly_low_price = year_data['low_price'].min()
        yearly_low_time_index = year_data['low_price'].idxmin()
        yearly_low_time_utc = yearly_low_time_index.to_pydatetime()

        # Simpan Yearly High
        new_high_data = {
            "symbol": symbol_param, "timeframe": timeframe_str, "event_type": "Yearly High", "direction": "Bullish",
            "price_level": yearly_high_price, "event_time_utc": yearly_high_time_utc,
            "swing_high_ref_time": None, "swing_low_ref_time": None
        }
        new_high_key = (new_high_data['symbol'], new_high_data['timeframe'], new_high_data['event_time_utc'].replace(microsecond=0).isoformat(), new_high_data['event_type'], float(new_high_data['price_level']))

        if new_high_key not in existing_extreme_keys:
            extremes_to_save_batch.append(new_high_data)
            logger.debug(f"Yearly High {year} terdeteksi: {float(yearly_high_price):.2f} pada {yearly_high_time_utc}.")
        else:
            logger.debug(f"Yearly High {year} sudah ada di DB, dilewati: {float(yearly_high_price):.2f} pada {yearly_high_time_utc}.")


        # Simpan Yearly Low
        new_low_data = {
            "symbol": symbol_param, "timeframe": timeframe_str, "event_type": "Yearly Low", "direction": "Bearish",
            "price_level": yearly_low_price, "event_time_utc": yearly_low_time_utc,
            "swing_high_ref_time": None, "swing_low_ref_time": None
        }
        new_low_key = (new_low_data['symbol'], new_low_data['timeframe'], new_low_data['event_time_utc'].replace(microsecond=0).isoformat(), new_low_data['event_type'], float(new_low_data['price_level']))

        if new_low_key not in existing_extreme_keys:
            extremes_to_save_batch.append(new_low_data)
            logger.debug(f"Yearly Low {year} terdeteksi: {float(yearly_low_price):.2f} pada {yearly_low_time_utc}.")
        else:
            logger.debug(f"Yearly Low {year} sudah ada di DB, dilewati: {float(yearly_low_price):.2f} pada {yearly_low_time_utc}.")

        # Simpan batch setiap 50 ekstrem (bisa disesuaikan)
        if len(extremes_to_save_batch) >= 50:
            try:
                database_manager.save_market_structure_events_batch(extremes_to_save_batch)
                processed_count += len(extremes_to_save_batch) 
                logger.info(f"BACKFILL WORKER: Auto-saved {len(extremes_to_save_batch)} yearly extremes.")
                extremes_to_save_batch = [] 
            except Exception as e:
                logger.error(f"BACKFILL WORKER: Gagal auto-save yearly extremes batch: {e}", exc_info=True)
                extremes_to_save_batch = [] 

    # Simpan sisa data setelah loop selesai
    if extremes_to_save_batch:
        try:
            database_manager.save_market_structure_events_batch(extremes_to_save_batch)
            processed_count += len(extremes_to_save_batch) 
            logger.info(f"BACKFILL WORKER: Final auto-saved {len(extremes_to_save_batch)} remaining yearly extremes.")
        except Exception as e:
            logger.error(f"BACKFILL WORKER: Gagal final auto-save remaining yearly extremes batch: {e}", exc_info=True)

    logger.info(f"Selesai mendeteksi {processed_count} ekstrem tahunan untuk {symbol_param}.")
    return processed_count 

def _update_existing_supply_demand_zones_status(symbol_param: str, timeframe_str: str, all_candles_df: pd.DataFrame, current_atr_value: Decimal) -> int:
    """
    Memperbarui status 'is_mitigated' dan 'last_retest_time_utc' untuk Supply & Demand Zones yang sudah ada di database.
    Ini berjalan pada seluruh riwayat candle untuk memastikan akurasi.
    Args:
        symbol_param (str): Simbol trading.
        timeframe_str (str): Timeframe.
        all_candles_df (pd.DataFrame): DataFrame candle historis (sudah dalam format float atau Decimal).
        current_atr_value (Decimal): Nilai ATR saat ini untuk timeframe ini.
    Returns:
        int: Jumlah S&D Zones yang statusnya berhasil diperbarui dan dikirim ke antrean DB.
    """
    logger.info(f"Memperbarui status Supply & Demand Zones untuk {symbol_param} {timeframe_str}...")
    processed_count = 0 

    if all_candles_df.empty:
        logger.debug(f"DataFrame candle kosong untuk update status S&D di {timeframe_str}.")
        return 0 

    # MODIFIKASI INI: Tambahkan mekanisme fallback renaming di awal fungsi
    # Untuk memastikan 'high' dan 'low' tersedia, meskipun DataFrame datang dengan 'high_price'
    if 'high_price' in all_candles_df.columns and 'high' not in all_candles_df.columns:
        all_candles_df = all_candles_df.rename(columns={'high_price': 'high', 'low_price': 'low', 'open_price': 'open', 'close_price': 'close', 'tick_volume': 'volume', 'real_volume': 'real_volume', 'spread': 'spread'})
        # Pastikan kolom-kolom ini juga dikonversi ke float jika belum
        for col in ['open', 'high', 'low', 'close', 'volume', 'real_volume', 'spread']:
            if col in all_candles_df.columns:
                all_candles_df[col] = all_candles_df[col].apply(utils.to_float_or_none)

    # Pastikan _SYMBOL_POINT_VALUE sudah diinisialisasi
    if globals().get('_SYMBOL_POINT_VALUE') is None:
        globals().get('_initialize_symbol_point_value')()
    if globals().get('_SYMBOL_POINT_VALUE') is None:
        logger.error(f"Nilai POINT untuk {symbol_param} tidak tersedia. Melewatkan pembaruan status S&D.")
        return 0 

    # Ambil batas harga dinamis global
    with globals().get('_dynamic_price_lock', threading.Lock()): 
        min_price_for_update_filter = globals().get('_dynamic_price_min_for_analysis')
        max_price_for_update_filter = globals().get('_dynamic_price_max_for_analysis')

    # Ambil zona S&D yang aktif dari DB dalam rentang harga yang relevan
    active_sd_zones = database_manager.get_supply_demand_zones(
        symbol=symbol_param, timeframe=timeframe_str, is_mitigated=False,
        min_price_level=min_price_for_update_filter,
        max_price_level=max_price_for_update_filter
    )

    if not active_sd_zones: 
        logger.debug(f"Tidak ada S&D Zones aktif untuk diperbarui statusnya di {symbol_param} {timeframe_str}.")
        return 0 

    # MODIFIKASI INI: Hitung toleransi secara dinamis menggunakan ATR
    if current_atr_value is not None and current_atr_value > Decimal('0.0'):
        # Gunakan ATR_MULTIPLIER_FOR_TOLERANCE dari MarketData untuk toleransi S&D
        tolerance_value_dec = current_atr_value * config.MarketData.ATR_MULTIPLIER_FOR_TOLERANCE
        logger.debug(f"S&D Status Updater: Menggunakan toleransi dinamis ATR: {float(tolerance_value_dec):.5f} (dari ATR {float(current_atr_value):.5f})")
    else:
        # Fallback ke nilai statis jika ATR tidak tersedia atau nol
        tolerance_value_dec = config.RuleBasedStrategy.RULE_SR_TOLERANCE_POINTS * globals().get('_SYMBOL_POINT_VALUE')
        logger.warning(f"S&D Status Updater: ATR tidak valid. Menggunakan toleransi statis: {float(tolerance_value_dec):.5f}")
    
    updated_sd_zones_batch = []
    
    # Menggunakan all_candles_df yang sudah dihandle kolomnya
    ohlc_df_for_status_check = all_candles_df 

    for sd_zone in active_sd_zones:
        sd_id = sd_zone['id']
        zone_top_price = sd_zone['zone_top_price'] 
        zone_bottom_price = sd_zone['zone_bottom_price'] 
        zone_type = sd_zone['zone_type'] 
        current_retest_count = sd_zone.get('retest_count', 0) 
        last_retest_time = sd_zone.get('last_retest_time_utc', None) 

        is_still_active = True
        
        current_last_retest_time_initial = sd_zone.get('last_retest_time_utc')
        if current_last_retest_time_initial is None:
            current_last_retest_time_initial = sd_zone['formation_time_utc']
            if current_last_retest_time_initial is None:
                current_last_retest_time_initial = datetime(1970, 1, 1, tzinfo=timezone.utc) 
                logger.warning(f"S&D Zone {sd_id} tidak memiliki formation_time_utc atau last_retest_time_utc. Menggunakan default 1970-01-01.")

        relevant_candles_for_test = ohlc_df_for_status_check[
            ohlc_df_for_status_check.index > current_last_retest_time_initial
        ].copy()

        if relevant_candles_for_test.empty:
            logger.debug(f"S&D Zone {zone_type} @ {utils.to_float_or_none(zone_bottom_price):.5f}-{utils.to_float_or_none(zone_top_price):.5f} dilewati karena tidak ada lilin baru setelah last_retest_time_utc.")
            continue


        for _, candle_row in relevant_candles_for_test.iterrows():
            if not is_still_active: 
                break

            candle_high = candle_row['high'] 
            candle_low = candle_row['low'] 
            candle_close = candle_row['close']
            candle_time_dt = candle_row.name.to_pydatetime()


            is_mitigated_now = False
            if zone_type == "Demand": 
                if candle_low < utils.to_float_or_none(zone_bottom_price) - utils.to_float_or_none(tolerance_value_dec) and \
                   candle_close < utils.to_float_or_none(zone_bottom_price) - utils.to_float_or_none(tolerance_value_dec): # Memerlukan close confirmation
                    is_mitigated_now = True
                    logger.debug(f"Demand Zone (ID: {sd_id}, Bottom: {zone_bottom_price}) menjadi termitigasi (Close: {float(candle_close):.5f}).")
                elif candle_low <= utils.to_float_or_none(zone_top_price) and candle_high >= utils.to_float_or_none(zone_bottom_price): # Cek jika range lilin menyentuh OB
                    if (last_retest_time is None) or ((candle_time_dt - last_retest_time).total_seconds() > config.System.RETRY_DELAY_SECONDS * 5):
                        current_retest_count += 1
                        last_retest_time = candle_time_dt 
                        logger.debug(f"S&D Zone Demand (ID: {sd_id}) retest terjadi (High: {float(candle_high):.5f}, Low: {float(candle_low):.5f}). Retest count: {current_retest_count}.")

            elif zone_type == "Supply": 
                if candle_high > utils.to_float_or_none(zone_top_price) + utils.to_float_or_none(tolerance_value_dec) and \
                   candle_close > utils.to_float_or_none(zone_top_price) + utils.to_float_or_none(tolerance_value_dec): # Memerlukan close confirmation
                    is_mitigated_now = True
                    logger.debug(f"Supply Zone (ID: {sd_id}, Top: {zone_top_price}) menjadi termitigasi (Close: {float(candle_close):.5f}).")
                elif candle_high >= utils.to_float_or_none(zone_bottom_price) and candle_low <= utils.to_float_or_none(zone_top_price): # Cek jika range lilin menyentuh OB
                    if (last_retest_time is None) or ((candle_time_dt - last_retest_time).total_seconds() > config.System.RETRY_DELAY_SECONDS * 5):
                        current_retest_count += 1
                        last_retest_time = candle_time_dt 
                        logger.debug(f"S&D Zone Supply (ID: {sd_id}) retest terjadi (High: {float(candle_high):.5f}, Low: {float(candle_low):.5f}). Retest count: {current_retest_count}.")
            
            if is_mitigated_now:
                is_still_active = False 
                last_retest_time = candle_time_dt 
                break 
        
        if (sd_zone['is_mitigated'] != (not is_still_active)) or \
           (sd_zone.get('retest_count', 0) != current_retest_count) or \
           (sd_zone.get('last_retest_time_utc', None) != last_retest_time): 
            
            updated_sd_zones_batch.append({
                'id': sd_id,
                'is_mitigated': not is_still_active, 
                'last_retest_time_utc': last_retest_time, 
                'retest_count': current_retest_count,
                'symbol': sd_zone['symbol'],
                'timeframe': sd_zone['timeframe'],
                'zone_type': sd_zone['zone_type'],
                'base_type': sd_zone['base_type'],
                'zone_top_price': zone_top_price,
                'zone_bottom_price': zone_bottom_price,
                'formation_time_utc': sd_zone['formation_time_utc'],
                'strength_score': sd_zone.get('strength_score'),
                'is_key_level': sd_zone.get('is_key_level', False),
                'confluence_score': sd_zone.get('confluence_score', 0)
            })
            processed_count += 1
    
    if updated_sd_zones_batch:
        try:
            database_manager.save_supply_demand_zones_batch([], updated_sd_zones_batch)
            logger.info(f"BACKFILL WORKER: Berhasil mem-batch update {len(updated_sd_zones_batch)} status S&D Zones untuk {symbol_param} {timeframe_str}.")
        except Exception as e:
            logger.error(f"BACKFILL WORKER: Gagal mem-batch update status S&D Zones: {e}", exc_info=True)

    logger.info(f"Selesai memperbarui status Supply & Demand Zones untuk {symbol_param} {timeframe_str}. {processed_count} diperbarui.")
    return processed_count



def update_moving_averages_for_single_tf(symbol_param: str, tf: str, candles_df: pd.DataFrame) -> int:
    """
    Menghitung dan menyimpan Moving Averages untuk satu timeframe tertentu menggunakan DataFrame candle yang diberikan.
    Args:
        symbol_param (str): Simbol trading.
        tf (str): Timeframe (misal "H1").
        candles_df (pd.DataFrame): DataFrame candle historis.
    Returns:
        int: Jumlah Moving Averages yang berhasil dihitung dan dikirim ke antrean DB.
    """
    # KOREKSI: Menggunakan nilai dari config.py
    ma_periods = config.MarketData.MA_PERIODS_TO_CALCULATE
    ma_types = config.MarketData.MA_TYPES_TO_CALCULATE

    logger.info(f"Menghitung Moving Averages untuk {symbol_param} {tf} (single TF update).")
    processed_count = 0 

    if candles_df.empty:
        logger.warning(f"Tidak ada candle untuk {symbol_param} {tf} untuk menghitung MA (single TF update).")
        return 0 

    # MODIFIKASI KRITIS DI SINI: Mengubah 'close_price' menjadi 'close'
    # Ini adalah penyebab utama KeyError sebelumnya karena DataFrame yang masuk
    # ke fungsi ini sudah di-rename kolomnya di pemanggil.
    if 'close_price' in candles_df.columns:
        # Jika kolom 'close_price' masih ada (misal dari testing atau jalur berbeda), gunakan itu
        # Namun, log menunjukkan ia sudah di-rename menjadi 'close' oleh pemanggil
        close_prices_decimal = candles_df['close_price']
        logger.debug(f"MA_CALC_DEBUG: Menggunakan 'close_price' dari input DataFrame.")
    elif 'close' in candles_df.columns:
        # Ini adalah jalur yang seharusnya aktif setelah renaming oleh pemanggil
        close_prices_decimal = candles_df['close']
        logger.debug(f"MA_CALC_DEBUG: Menggunakan 'close' dari input DataFrame (setelah renaming).")
    else:
        logger.error(f"MA_CALC_DEBUG: Kolom 'close' atau 'close_price' tidak ditemukan di DataFrame. Tidak dapat menghitung MA.")
        return 0

    timestamps_pydatetime = candles_df.index.to_pydatetime() 

    for ma_type in ma_types:
        for period_dec in ma_periods:
            period = int(period_dec)

            logger.debug(f"MA_CALC_DEBUG: Memproses {ma_type}-{period} untuk TF {tf}.") 

            if len(close_prices_decimal) < period:
                logger.debug(f"MA_CALC_DEBUG: Tidak cukup data ({len(close_prices_decimal)}) untuk menghitung {ma_type}-{period} pada {tf}. Diperlukan {period} candle. Melewatkan.")
                continue

            ma_value_last = None
            ma_timestamp_last = timestamps_pydatetime[-1]

            try:
                if ma_type == "SMA":
                    ma_value_last = close_prices_decimal.iloc[-period:].sum() / Decimal(str(period))
                elif ma_type == "EMA":
                    # Pastikan prices_for_ema_float dikirim sebagai Series dari Decimal ke float
                    prices_for_ema_float = close_prices_decimal.apply(utils.to_float_or_none)
                    ema_float_series = globals().get('_calculate_ema_internal')(prices_for_ema_float, period)

                    if ema_float_series is not None and not ema_float_series.empty and pd.notna(ema_float_series.iloc[-1]):
                        ma_value_last = Decimal(str(ema_float_series.iloc[-1]))
                    else:
                        logger.warning(f"MA_CALC_DEBUG: EMA-{period} untuk {tf} menghasilkan NaN atau Series kosong. Melewatkan penyimpanan.")
                        continue

                if ma_value_last is None:
                    logger.warning(f"MA_CALC_DEBUG: Perhitungan {ma_type}-{period} untuk {tf} menghasilkan None. Melewatkan penyimpanan.")
                    continue

                logger.debug(f"MA_CALC_DEBUG: Dihitung {ma_type}-{period} untuk {tf} pada {ma_timestamp_last.strftime('%Y-%m-%d %H:%M UTC')} = {float(ma_value_last):.5f} (dari harga {float(close_prices_decimal.iloc[-1]):.5f})")

                logger.debug(f"MA_CALC_DEBUG: Mengirim {ma_type}-{period} (Value: {float(ma_value_last):.5f}, Time: {ma_timestamp_last.isoformat()}) ke save_moving_average.")

                try:
                    database_manager.save_moving_average(
                        symbol=symbol_param, timeframe=tf, ma_type=ma_type, period=period,
                        timestamp_utc=ma_timestamp_last, value=ma_value_last
                    )
                    processed_count += 1
                    logger.debug(f"MA_CALC_DEBUG: {ma_type}-{period} untuk {tf} berhasil dikirim ke antrean DB.")
                except Exception as e:
                    logger.error(f"MA_CALC_DEBUG: Gagal mengirim Moving Average ke antrean DB untuk {symbol_param} {tf} {ma_type}-{period}: {e}", exc_info=True)

            except Exception as e:
                logger.error(f"MA_CALC_DEBUG: Gagal menghitung {ma_type}-{period} untuk {tf}: {e}", exc_info=True)
    logger.info(f"Selesai menghitung Moving Averages untuk {symbol_param} {tf} (single TF update).")
    return processed_count


def _detect_new_and_update_retracements_historically(symbol_param: str, timeframe_str: str, candles_df: pd.DataFrame):
    logger.info(f"Mendeteksi dan memperbarui retracements untuk {symbol_param} {timeframe_str}")

    swing_length = config.AIAnalysts.SWING_EXT_BARS # Mengambil swing_length dari konfigurasi

    if candles_df.empty or len(candles_df) < (swing_length * 2 + 2):
        logger.debug(f"Tidak cukup lilin ({len(candles_df)}) untuk deteksi retracement di {timeframe_str}.")
        return

    # Persiapan DataFrame untuk perhitungan internal
    # Konversi ke float untuk fungsi _calculate_swing_highs_lows_internal (yang menggunakan NumPy)
    # dan _calculate_retracements_internal (yang juga menggunakan NumPy/float internal)
    ohlc_df_for_smc_calc = candles_df.copy()
    ohlc_df_for_smc_calc = ohlc_df_for_smc_calc.rename(columns={
        'open_price': 'open', 'high_price': 'high', 'low_price': 'low',
        'close_price': 'close', 'tick_volume': 'volume' # asumsi tick_volume digunakan untuk volume
    })
    for col in ['open', 'high', 'low', 'close', 'volume']:
        if col in ohlc_df_for_smc_calc.columns:
            # Pastikan ini adalah konversi ke float menggunakan .astype(float)
            ohlc_df_for_smc_calc[col] = ohlc_df_for_smc_calc[col].astype(float)

    # Hitung Swing Highs/Lows yang dibutuhkan sebagai input untuk retracements
    swing_results_df = _calculate_swing_highs_lows_internal(ohlc_df_for_smc_calc, swing_length=swing_length)
    
    if swing_results_df.empty or swing_results_df['HighLow'].isnull().all():
        logger.debug(f"Tidak ada swing points yang valid untuk {symbol_param} {timeframe_str}. Melewatkan deteksi retracement.")
        return

    # --- MODIFIKASI KRUSIAL DI SINI ---
    retracement_results_df = _calculate_retracements_internal(ohlc_df_for_smc_calc, swing_results_df)

    if retracement_results_df.empty:
        logger.debug(f"_calculate_retracements_internal mengembalikan DataFrame kosong untuk {symbol_param} {timeframe_str}. Melewatkan pembaruan retracement.")
        return
    
    # Ambil level Fibonacci aktif dari database untuk diperbarui status retracement-nya
    all_fib_levels_db = database_manager.get_fibonacci_levels(
        symbol=symbol_param,
        timeframe=timeframe_str,
        is_active=True,
        limit=None
    )

    if not all_fib_levels_db:
        logger.debug(f"Tidak ada level Fibonacci aktif di DB untuk {symbol_param} {timeframe_str}. Tidak ada yang bisa diperbarui dengan data retracement.")
        return

    updated_fib_levels_batch = []
    
    # Buat maping Fibonacci levels berdasarkan referensi swing High/Low untuk pencarian cepat
    fib_levels_by_swing_ref = defaultdict(list) 
    for fib_level_data in all_fib_levels_db:
        # Kunci: tuple dari (high_price_ref, low_price_ref) yang dikonversi ke Decimal
        key = (utils.to_decimal_or_none(fib_level_data['high_price_ref']), utils.to_decimal_or_none(fib_level_data['low_price_ref']))
        fib_levels_by_swing_ref[key].append(fib_level_data)

    tolerance_for_fib_match = Decimal('0.0001') # Toleransi untuk mencocokkan level harga Fibonacci
    if _SYMBOL_POINT_VALUE is not None:
         tolerance_for_fib_match = Decimal(str(config.RuleBasedStrategy.RULE_SR_TOLERANCE_POINTS)) * _SYMBOL_POINT_VALUE * Decimal('0.1') # Toleransi yang lebih kecil dari SR

    # Iterasi hasil retracement_results_df
    for i in range(len(retracement_results_df)):
        current_retracement_data = retracement_results_df.iloc[i]
        candle_time_utc = retracement_results_df.index[i].to_pydatetime()

        current_direction = current_retracement_data.get('Direction') 
        current_retracement_perc = current_retracement_data.get('CurrentRetracement%')
        deepest_retracement_perc = current_retracement_data.get('DeepestRetracement%')

        if pd.isna(current_retracement_perc) or pd.isna(deepest_retracement_perc) or pd.isna(current_direction):
            continue

        # Dapatkan 2 swing point terakhir yang relevan hingga waktu lilin saat ini
        last_relevant_swing_query = database_manager.get_market_structure_events(
            symbol=symbol_param,
            timeframe=timeframe_str,
            event_type=['Swing High', 'Swing Low'],
            end_time_utc=candle_time_utc,
            limit=2 
        )
        
        if len(last_relevant_swing_query) < 2:
            continue

        # Urutkan berdasarkan waktu event secara menurun untuk mendapatkan 2 terakhir
        last_relevant_swing_query.sort(key=lambda x: x['event_time_utc'], reverse=True)
        
        swing_new = last_relevant_swing_query[0] # Swing paling baru
        swing_old = last_relevant_swing_query[1] # Swing sebelumnya

        high_ref_for_fib = None
        low_ref_for_fib = None
        
        # Tentukan referensi high/low untuk Fibonacci berdasarkan dua swing point ini
        if swing_old['event_time_utc'] < swing_new['event_time_utc']:
            if swing_old['event_type'] == 'Swing High' and swing_new['event_type'] == 'Swing Low':
                high_ref_for_fib = utils.to_decimal_or_none(swing_old['price_level'])
                low_ref_for_fib = utils.to_decimal_or_none(swing_new['price_level'])
            elif swing_old['event_type'] == 'Swing Low' and swing_new['event_type'] == 'Swing High':
                high_ref_for_fib = utils.to_decimal_or_none(swing_new['price_level'])
                low_ref_for_fib = utils.to_decimal_or_none(swing_old['price_level'])
        
        if high_ref_for_fib is None or low_ref_for_fib is None:
            continue

        found_fib_levels_for_this_swing = []
        # Cari Fibonacci levels yang sudah ada di DB yang cocok dengan swing reference ini
        for fib_key, fib_levels in fib_levels_by_swing_ref.items():
            fib_high_ref_key = fib_key[0]
            fib_low_ref_key = fib_key[1]
            
            if (abs(fib_high_ref_key - high_ref_for_fib) < tolerance_for_fib_match and
                abs(fib_low_ref_key - low_ref_for_fib) < tolerance_for_fib_match):
                
                found_fib_levels_for_this_swing.extend(fib_levels)

        if found_fib_levels_for_this_swing:
            for fib_level_to_update in found_fib_levels_for_this_swing:
                current_retracement_perc_dec = utils.to_decimal_or_none(current_retracement_perc)
                deepest_retracement_perc_dec = utils.to_decimal_or_none(deepest_retracement_perc)
                
                # Perbarui hanya jika ada perubahan pada persentase retracement atau arah
                if (utils.to_decimal_or_none(fib_level_to_update.get('current_retracement_percent')) != current_retracement_perc_dec or
                    utils.to_decimal_or_none(fib_level_to_update.get('deepest_retracement_percent')) != deepest_retracement_perc_dec or
                    utils.to_decimal_or_none(fib_level_to_update.get('retracement_direction')) != utils.to_decimal_or_none(current_direction)): 
                    
                    updated_fib_levels_batch.append({
                        'id': fib_level_to_update['id'], 
                        'current_retracement_percent': utils.to_float_or_none(current_retracement_perc_dec),
                        'deepest_retracement_percent': utils.to_float_or_none(deepest_retracement_perc_dec),
                        'retracement_direction': float(current_direction) 
                    })
    
    if updated_fib_levels_batch:
        try:
            database_manager.save_fibonacci_levels_batch([], updated_fib_levels_batch)
            logger.info(f"Berhasil mem-batch update {len(updated_fib_levels_batch)} Fib Levels dengan data retracement untuk {symbol_param} {timeframe_str}.")
        except Exception as e:
            logger.error(f"Gagal mem-batch update Fib Levels dengan data retracement: {e}", exc_info=True)

    logger.info(f"Selesai mendeteksi dan memperbarui retracements untuk {symbol_param} {timeframe_str}.")


def update_all_fibonacci_levels(symbol_param):
    """
    Mendeteksi dan memperbarui level Fibonacci.
    Ini mengimplementasikan deteksi level Fibonacci nyata berdasarkan swing points.
    """
    logger.info(f"Mendeteksi dan memperbarui level Fibonacci untuk {symbol_param}...")

    _initialize_symbol_point_value()
    if _SYMBOL_POINT_VALUE is None:
        logger.error(f"Nilai POINT untuk {symbol_param} tidak tersedia. Melewatkan deteksi Fibonacci.")
        return

    timeframes = ["M15", "M30", "H1", "H4", "D1"]

    fib_ratios = [Decimal('0.236'), Decimal('0.382'), Decimal('0.5'), Decimal('0.618'), Decimal('0.786')]
    fib_type = "Retracement"

    for tf in timeframes:
        # Menyesuaikan candles_limit berdasarkan timeframe
        if tf == "M15":
            candles_limit = 1000
            swing_filter_candles = 200
            price_level_quantize_decimal = Decimal('0.00001')
        elif tf == "M30":
            candles_limit = 750
            swing_filter_candles = 150
            price_level_quantize_decimal = Decimal('0.00001')
        elif tf == "H1":
            candles_limit = 500
            swing_filter_candles = 100
            price_level_quantize_decimal = Decimal('0.001')
        elif tf == "H4":
            candles_limit = 300
            swing_filter_candles = 75
            price_level_quantize_decimal = Decimal('0.001')
        elif tf == "D1":
            candles_limit = 200
            swing_filter_candles = 50
            price_level_quantize_decimal = Decimal('0.001')
        else:  # Default fallback
            candles_limit = 200
            swing_filter_candles = 50
            price_level_quantize_decimal = Decimal('0.001')

        logger.info(f"Mengambil {candles_limit} candle untuk deteksi Fibonacci di {tf}...")
        candles = database_manager.get_historical_candles_from_db(symbol_param, tf, limit=candles_limit)

        if not candles or len(candles) < 20:  # Pastikan ada minimal candle untuk perhitungan
            logger.warning(f"Tidak ada candle cukup ({len(candles)}) untuk {symbol_param} {tf} untuk deteksi Fibonacci. Diperlukan minimal 20 lilin.")
            continue

        try:
            df_candles = pd.DataFrame(candles)
            df_candles['open_time_utc'] = pd.to_datetime(df_candles['open_time_utc'])
            df_candles = df_candles.set_index('open_time_utc').sort_index(ascending=True)

            # Ambil swing points dari database dalam jendela waktu yang relevan
            # Filter swing points berdasarkan jumlah candle yang telah ditentukan
            start_idx = max(0, len(df_candles) - swing_filter_candles)
            if len(df_candles) > 0:
                start_time_for_swing_filter = df_candles.index[start_idx].to_pydatetime()
            else:
                logger.warning(f"df_candles kosong untuk {symbol_param} {tf}. Melewatkan deteksi Fibonacci.")
                continue

            swing_points_from_db = database_manager.get_market_structure_events(
                symbol=symbol_param,
                timeframe=tf,
                event_type=['Swing High', 'Swing Low'],
                start_time_utc=start_time_for_swing_filter
            )

            detected_fib_count = 0

            # Urutkan swing points berdasarkan waktu
            sorted_swing_points = sorted(swing_points_from_db, key=lambda x: datetime.fromisoformat(x['event_time_utc']))

            # Iterasi pasangan swing points untuk mendeteksi Fibonacci
            new_fibs_to_save_in_batch = [] # Untuk mengumpulkan yang baru terdeteksi
            for i in range(len(sorted_swing_points) - 1):
                swing1 = sorted_swing_points[i]
                swing2 = sorted_swing_points[i + 1]

                swing_price_1 = Decimal(str(swing1['price_level']))
                swing_time_1 = swing1['event_time_utc']
                swing_price_2 = Decimal(str(swing2['price_level']))
                swing_time_2 = swing2['event_time_utc']

                high_price_ref = Decimal('0')
                low_price_ref = Decimal('0')
                start_time_ref_utc = None
                end_time_ref_utc = None

                # Kasus 1: Swing High diikuti Swing Low (potensi tren turun untuk retracement)
                if swing1['event_type'] == "Swing High" and swing2['event_type'] == "Swing Low":
                    high_price_ref = swing_price_1
                    low_price_ref = swing_price_2
                    start_time_ref_utc = swing_time_1
                    end_time_ref_utc = swing_time_2
                    logger.debug(f"Potensi Fib Swing H-L: {float(high_price_ref)}-{float(low_price_ref)} di {tf}")

                # Kasus 2: Swing Low diikuti Swing High (potensi tren naik untuk retracement)
                elif swing1['event_type'] == "Swing Low" and swing2['event_type'] == "Swing High":
                    high_price_ref = swing_price_2
                    low_price_ref = swing_price_1
                    start_time_ref_utc = swing_time_1
                    end_time_ref_utc = swing_time_2
                    logger.debug(f"Potensi Fib Swing L-H: {float(low_price_ref)}-{float(high_price_ref)} di {tf}")

                if high_price_ref != low_price_ref:  # Pastikan ada rentang harga
                    for ratio in fib_ratios:
                        # Perhitungan umum untuk retracement
                        if swing1['event_type'] == "Swing High":  # H-L: retracement ke atas dari low
                            level_price = high_price_ref - (high_price_ref - low_price_ref) * ratio
                        else:  # L-H: retracement ke bawah dari high
                            level_price = low_price_ref + (high_price_ref - low_price_ref) * ratio

                        # --- PERUBAHAN: Tambahkan ke list batch ---
                        new_fibs_to_save_in_batch.append({
                            "symbol": symbol_param,
                            "timeframe": tf,
                            "type": fib_type,
                            "high_price_ref": high_price_ref,
                            "low_price_ref": low_price_ref,
                            "start_time_ref_utc": start_time_ref_utc,
                            "end_time_ref_utc": end_time_ref_utc,
                            "ratio": ratio,
                            "price_level": level_price.quantize(price_level_quantize_decimal),
                            "is_active": True
                        })
                        # --- AKHIR PERUBAHAN ---
                        detected_fib_count += 1
                        logger.debug(f"Fib {float(ratio)} level: {float(level_price)} terdeteksi di {tf}.")

            logger.info(f"Total {detected_fib_count} Fibonacci Levels terdeteksi dan dikirim ke antrean untuk {symbol_param} {tf}.")

            # --- PERUBAHAN: Panggil batch save untuk Fibonacci Levels baru ---
            if new_fibs_to_save_in_batch:
                try:
                    database_manager.save_fibonacci_levels_batch(new_fibs_to_save_in_batch, [])
                except Exception as e:
                    logger.error(f"Gagal mengirim new Fib Levels batch ke antrean DB: {e}", exc_info=True)
            # --- AKHIR PERUBAHAN ---

            # --- Manajemen Aktivitas Fibonacci Levels ---
            logger.info(f"Memeriksa aktivitas Fibonacci Levels untuk {symbol_param} {tf}...")
            # --- PERUBAHAN: Panggil _update_existing_fibonacci_levels_status ---
            try:
                _update_existing_fibonacci_levels_status(symbol_param, tf, df_candles, [])
            except Exception as e:
                logger.error(f"Gagal mengirim update Fib Level Status ke antrean DB: {e}", exc_info=True)
            # --- AKHIR PERUBAHAN ---

        except Exception as e:
            logger.error(f"Gagal mendeteksi level Fibonacci untuk {symbol_param} {tf}: {e}", exc_info=True)
    logger.info(f"Selesai memperbarui level Fibonacci untuk {symbol_param}.")


def _detect_new_volume_profiles_historically(symbol_param: str, timeframe_str: str, candles_df: pd.DataFrame, new_vps_list: list):
    """
    Mendeteksi dan menyimpan Volume Profile dari seluruh dataset historis per periode D1/H4.
    Menambahkan Volume Profiles baru ke new_vps_list SETELAH MEMERIKSA DUPLIKAT.
    """
    logger.info(f"Mendeteksi Volume Profiles BARU historis untuk {symbol_param} {timeframe_str}...")

    price_level_granularity_points = 10
    value_area_percentage = Decimal('0.70')
    
    # Pastikan _SYMBOL_POINT_VALUE diinisialisasi
    if _SYMBOL_POINT_VALUE is None:
        _initialize_symbol_point_value()
    if _SYMBOL_POINT_VALUE is None:
        logger.error(f"Nilai POINT untuk {symbol_param} tidak tersedia. Melewatkan perhitungan Volume Profile.")
        return

    price_granularity_value = price_level_granularity_points * _SYMBOL_POINT_VALUE
    if price_granularity_value == Decimal('0'):
        logger.warning(f"Price granularity value is 0 for {symbol_param}. Skipping Volume Profile calculation.")
        return

    if candles_df.empty or len(candles_df) < 20: # Minimal 20 lilin untuk VP yang valid
        logger.debug(f"Tidak cukup lilin ({len(candles_df)}) untuk deteksi Volume Profile historis di {timeframe_str}.")
        return

    # --- PENAMBAHAN KODE UNTUK MEMERIKSA DUPLIKAT YANG SUDAH ADA DI DB ---
    existing_vp_keys = set()
    try:
        existing_vps_db = database_manager.get_volume_profiles(
            symbol=symbol_param,
            timeframe=timeframe_str,
            start_time_utc=None, # Mengambil semua yang ada
            end_time_utc=candles_df.index.max().to_pydatetime() + timedelta(days=1) # Batasi akhir rentang
        )
        for vp in existing_vps_db:
            # period_start_utc sudah datetime object dari DB
            vp_period_start_dt = vp['period_start_utc']
            existing_vp_keys.add((vp['symbol'], vp['timeframe'], vp_period_start_dt.replace(microsecond=0).isoformat()))
        logger.debug(f"Ditemukan {len(existing_vp_keys)} Volume Profiles yang sudah ada di DB untuk {timeframe_str}.")
    except Exception as e:
        logger.error(f"Gagal mengambil existing Volume Profiles dari DB untuk {timeframe_str} (untuk pre-filtering): {e}", exc_info=True)
        pass 

    # --- PERBAIKAN: Implementasi Batching Penyimpanan Internal ---
    BATCH_SAVE_THRESHOLD_VP = 100 # Simpan setiap 100 VP baru (sesuaikan jika perlu)
    detected_vps_to_save_in_batch = []
    
    vp_keys_in_current_chunk = set() # Untuk melacak duplikat dalam chunk yang sedang diproses

    if timeframe_str == "D1":
        grouped_periods = candles_df.groupby(pd.Grouper(freq='d'))
    elif timeframe_str == "H4":
        grouped_periods = candles_df.groupby(pd.Grouper(freq='4h'))
    else:
        logger.warning(f"Volume Profile historis belum didukung untuk timeframe {timeframe_str}.")
        return

    try:
        for period_start_timestamp, period_df in grouped_periods:
            if period_df.empty or len(period_df) < 1:
                continue

            period_start_utc = period_start_timestamp.to_pydatetime()
            period_end_utc = period_df.index[-1].to_pydatetime() if not period_df.empty else period_start_utc

            # Buat kunci unik untuk VP ini
            new_vp_key = (symbol_param, timeframe_str, period_start_utc.replace(microsecond=0).isoformat())

            if new_vp_key in existing_vp_keys or new_vp_key in vp_keys_in_current_chunk:
                logger.debug(f"Melewatkan Volume Profile duplikat yang sudah ada atau sudah terdeteksi di chunk ini: {new_vp_key}")
                continue

            # Pastikan 'tick_volume' juga dikonversi ke Decimal
            total_volume = period_df['tick_volume'].apply(to_decimal_or_none).sum()
            if total_volume is None or total_volume == Decimal('0'):
                continue

            volume_at_price = {}
            for _, candle_row in period_df.iterrows():
                # Pastikan harga candle dikonversi ke Decimal
                high_price_dec = to_decimal_or_none(candle_row['high_price'])
                low_price_dec = to_decimal_or_none(candle_row['low_price'])
                tick_volume_dec = to_decimal_or_none(candle_row['tick_volume'])

                if high_price_dec is None or low_price_dec is None or tick_volume_dec is None:
                    continue

                mid_price = (high_price_dec + low_price_dec) / Decimal('2')
                bin_price = (mid_price // price_granularity_value) * price_granularity_value

                volume_at_price[bin_price] = volume_at_price.get(bin_price, Decimal('0')) + tick_volume_dec
            
            sorted_volumes = sorted(volume_at_price.items(), key=lambda item: item[1], reverse=True)

            poc_price = Decimal('0')
            if sorted_volumes:
                poc_price = sorted_volumes[0][0]

            vah_price = poc_price
            val_price = poc_price
            current_accumulated_volume = Decimal('0')
            sorted_by_price = sorted(volume_at_price.items())

            poc_idx = -1
            for idx, (price, _) in enumerate(sorted_by_price):
                if price == poc_price:
                    poc_idx = idx
                    break
            
            if poc_idx != -1:
                left_idx = poc_idx
                right_idx = poc_idx
                vah_price = sorted_by_price[right_idx][0]
                val_price = sorted_by_price[left_idx][0]
                current_accumulated_volume += volume_at_price.get(poc_price, Decimal('0'))
                
                while current_accumulated_volume < total_volume * value_area_percentage and (left_idx > 0 or right_idx < len(sorted_by_price) - 1):
                    can_go_left = left_idx > 0
                    can_go_right = right_idx < len(sorted_by_price) - 1

                    vol_left = sorted_by_price[left_idx - 1][1] if can_go_left else Decimal('-1')
                    vol_right = sorted_by_price[right_idx + 1][1] if can_go_right else Decimal('-1')
                    
                    if can_go_left and (not can_go_right or vol_left >= vol_right):
                        left_idx -= 1
                        current_accumulated_volume += sorted_by_price[left_idx][1]
                        val_price = sorted_by_price[left_idx][0]
                    elif can_go_right:
                        right_idx += 1
                        current_accumulated_volume += sorted_by_price[right_idx][1]
                        vah_price = sorted_by_price[right_idx][0]
                    else:
                        break

            profile_data_json = json.dumps([{"price": float(p), "volume": float(v)} for p, v in sorted_by_price])

            new_vp_data = {
                "symbol": symbol_param, "timeframe": timeframe_str,
                "period_start_utc": period_start_utc, "period_end_utc": period_end_utc,
                "poc_price": poc_price, "vah_price": vah_price, "val_price": val_price,
                "total_volume": total_volume, "profile_data_json": profile_data_json,
                "row_height_value": price_granularity_value # Pastikan ini ada
            }

            detected_vps_to_save_in_batch.append(new_vp_data) # <--- Tambahkan ke daftar internal
            vp_keys_in_current_chunk.add(new_vp_key) # <--- Tambahkan ke set kunci chunk

            # === PENTING: Lakukan penyimpanan batch di sini jika ambang batas tercapai ===
            if len(detected_vps_to_save_in_batch) >= BATCH_SAVE_THRESHOLD_VP:
                try:
                    database_manager.save_volume_profiles_batch(detected_vps_to_save_in_batch)
                    logger.info(f"BACKFILL WORKER: Auto-saved {len(detected_vps_to_save_in_batch)} new VPs for {timeframe_str}.")
                    detected_vps_to_save_in_batch = [] # Kosongkan setelah disimpan
                except Exception as e:
                    logger.error(f"BACKFILL WORKER: Gagal auto-save VPs batch: {e}", exc_info=True)
                    detected_vps_to_save_in_batch = [] # Kosongkan agar tidak mengulang error yang sama

    except Exception as e: # <--- TANGKAP EXCEPTION DARI LOGIKA DETEKSI
        logger.error(f"Error saat mendeteksi Volume Profiles BARU historis untuk {symbol_param} {timeframe_str}: {e}", exc_info=True)
    finally: # Pastikan ini selalu dieksekusi
        # === PENTING: Simpan sisa data setelah loop selesai ===
        if detected_vps_to_save_in_batch:
            try:
                database_manager.save_volume_profiles_batch(detected_vps_to_save_in_batch)
                logger.info(f"BACKFILL WORKER: Final auto-saved {len(detected_vps_to_save_in_batch)} remaining new VPs for {timeframe_str}.")
            except Exception as e:
                logger.error(f"BACKFILL WORKER: Gagal final auto-save VPs batch: {e}", exc_info=True)

    # new_vps_list yang diteruskan tidak lagi digunakan untuk mengumpulkan data, jadi kosongkan.
    if new_vps_list:
        new_vps_list.clear()
    
    logger.info(f"Selesai mendeteksi Volume Profiles BARU unik historis untuk {symbol_param} {timeframe_str}.")




def _update_existing_divergences_status(symbol_param: str, timeframe_str: str, candles_df: pd.DataFrame, current_atr_value: Decimal) -> int:
    """
    Memperbarui status divergensi yang sudah ada (is_active) berdasarkan pergerakan harga terbaru.
    Divergensi menjadi tidak aktif jika harga telah menembus titik tertinggi/terendah dari divergensi.
    Args:
        symbol_param (str): Simbol trading.
        timeframe_str (str): Timeframe.
        candles_df (pd.DataFrame): DataFrame candle historis (diasumsikan sudah dalam format
                                   kolom pendek 'open', 'high', 'low', 'close' sebagai float).
        current_atr_value (Decimal): Nilai ATR saat ini untuk timeframe ini.
    Returns:
        int: Jumlah divergensi yang statusnya berhasil diperbarui dan dikirim ke antrean DB.
    """
    logger.info(f"Memperbarui status divergensi yang sudah ada untuk {symbol_param} {timeframe_str}...")
    
    processed_count = 0 # Inisialisasi penghitung divergensi yang diperbarui

    # 1. Validasi Input Awal
    if candles_df.empty:
        logger.debug(f"DataFrame candle kosong untuk update status divergensi di {timeframe_str}.")
        return 0 # Mengembalikan 0 jika tidak ada candle untuk diproses

    # 2. Inisialisasi _SYMBOL_POINT_VALUE (jika belum)
    # Fungsi ini akan memastikan _SYMBOL_POINT_VALUE tersedia untuk perhitungan toleransi.
    if globals().get('_SYMBOL_POINT_VALUE') is None:
        globals().get('_initialize_symbol_point_value')()
    if globals().get('_SYMBOL_POINT_VALUE') is None:
        logger.error(f"Nilai POINT untuk {symbol_param} tidak tersedia. Melewatkan update status divergensi.")
        return 0

    # 3. Mendapatkan Divergensi Aktif dari Database
    # Filter divergensi yang aktif dan relevan berdasarkan rentang harga dinamis
    # (min_price_limit & max_price_limit disetel secara global di backfill_historical_features)
    active_divergences = database_manager.get_divergences(
        symbol=symbol_param,
        timeframe=timeframe_str,
        is_active=True,
        limit=None, # Ambil semua divergensi aktif
        min_price_level=globals().get('_dynamic_price_min_for_analysis'),
        max_price_level=globals().get('_dynamic_price_max_for_analysis')
    )

    if not active_divergences:
        logger.debug(f"Tidak ada divergensi aktif untuk diperbarui statusnya di {symbol_param} {timeframe_str}.")
        return 0

    # 4. Menentukan Toleransi Penembusan (Break Tolerance)
    # Toleransi dihitung secara dinamis berdasarkan ATR atau fallback ke nilai statis.
    tolerance_value_dec = Decimal('0.0') # Default aman
    if current_atr_value is not None and current_atr_value > Decimal('0.0'):
        tolerance_value_dec = current_atr_value * config.RuleBasedStrategy.DIVERGENCE_PRICE_TOLERANCE_ATR_MULTIPLIER
        logger.debug(f"Divergence Status Updater: Menggunakan toleransi dinamis ATR: {float(tolerance_value_dec):.5f} (dari ATR {float(current_atr_value):.5f})")
    else:
        tolerance_value_dec = config.RuleBasedStrategy.RULE_SR_TOLERANCE_POINTS * globals().get('_SYMBOL_POINT_VALUE')
        logger.warning(f"Divergence Status Updater: ATR tidak valid. Menggunakan toleransi statis: {float(tolerance_value_dec):.5f}")
    
    updated_divergences_batch = [] # List untuk mengumpulkan divergensi yang perlu diupdate

    # 5. Persiapan DataFrame Lilin untuk Iterasi
    # Asumsi: `candles_df` yang diterima di sini sudah dalam format yang benar 
    # ('open', 'high', 'low', 'close' sebagai float) dari pemanggil.
    ohlc_df_for_status_check = candles_df.copy() 

    # KOREKSI DI SINI: Akses kolom 'high' dan 'low' langsung dari candles_df,
    # karena sekarang kita ASUMSIKAN sudah di-rename dan dikonversi dengan benar.
    latest_high = ohlc_df_for_status_check['high'].apply(utils.to_decimal_or_none).max() 
    latest_low = ohlc_df_for_status_check['low'].apply(utils.to_decimal_or_none).min()   
    
    if latest_high is None or latest_low is None:
        logger.warning(f"Harga high/low terbaru tidak valid untuk update status divergensi di {timeframe_str}.")
        return 0 # KOREKSI: Pastikan mengembalikan 0 di sini juga

    # Filter lilin yang relevan: hanya yang terjadi setelah `formation_time_utc` divergensi paling terakhir di `active_divergences`.
    # Ini untuk mengoptimalkan kinerja.
    latest_active_div_time = datetime(1970, 1, 1, tzinfo=timezone.utc)
    if active_divergences:
        latest_active_div_time = max([div['price_point_time_utc'] for div in active_divergences if div['price_point_time_utc'] is not None])
        if latest_active_div_time is None:
            latest_active_div_time = datetime(1970, 1, 1, tzinfo=timezone.utc)

    relevant_candles_for_test = ohlc_df_for_status_check[
        ohlc_df_for_status_check.index > latest_active_div_time
    ].copy()

    if relevant_candles_for_test.empty and active_divergences:
        logger.debug(f"Tidak ada lilin baru setelah divergensi aktif terbaru untuk {symbol_param} {timeframe_str}. Melewatkan update status divergensi.")
        return 0

    # 6. Iterasi Melalui Divergensi Aktif dan Lilin yang Relevan
    for div in active_divergences:
        div_id = div['id']
        div_price_level_1 = div['price_level_1']
        div_price_level_2 = div['price_level_2']
        div_type = div['divergence_type'] 

        is_still_active = True # Asumsi divergensi masih aktif, akan diubah jika ada penembusan

        # Tentukan titik acuan tertinggi/terendah dari divergensi
        # Tergantung pada apakah divergensi Bullish (SL-SL) atau Bearish (SH-SH)
        divergence_high_level = max(div_price_level_1, div_price_level_2) # Decimal
        divergence_low_level = min(div_price_level_1, div_price_level_2)   # Decimal
        
        # Jika `last_processed_time_utc` pada divergensi adalah `None`, gunakan `price_point_time_utc`
        # sebagai titik awal pencarian interaksi.
        current_last_processed_time_for_div = div.get('last_processed_time_utc')
        if current_last_processed_time_for_div is None:
            current_last_processed_time_for_div = div['price_point_time_utc'] # Ini adalah `event_time_utc` untuk titik harga pertama divergensi

        # Filter lilin yang relevan untuk *divergensi spesifik ini*
        div_specific_relevant_candles = relevant_candles_for_test[
            relevant_candles_for_test.index > current_last_processed_time_for_div
        ].copy()

        if div_specific_relevant_candles.empty:
            logger.debug(f"Divergensi {div_id} dilewati, tidak ada lilin baru setelah terakhir diproses.")
            continue


        for _, candle_row in div_specific_relevant_candles.iterrows():
            if not is_still_active: # Jika status sudah berubah menjadi tidak aktif, hentikan iterasi
                break

            # KOREKSI: Akses kolom 'high' dan 'low' langsung dari candle_row
            candle_high = candle_row['high'] 
            candle_low = candle_row['low'] 

            # Logika untuk menentukan apakah divergensi menjadi tidak aktif (tertembus/invalid)
            # Divergensi menjadi tidak aktif jika harga menembus di luar rentang divergensi,
            # biasanya dengan konfirmasi melewati ambang toleransi.

            if div_type == "Regular Bullish" or div_type == "Hidden Bullish":
                # Bullish Divergence: Menjadi tidak aktif jika harga menembus di bawah titik terendah divergensi.
                # `candle_low` menembus `divergence_low_level` ke bawah.
                # Pastikan operan to_float_or_none adalah Decimal untuk konsistensi
                if candle_low < (float(divergence_low_level) - float(tolerance_value_dec)):
                    is_still_active = False
                    logger.debug(f"Divergensi Bullish (ID: {div_id}) menjadi tidak aktif (harga menembus low).")
            elif div_type == "Regular Bearish" or div_type == "Hidden Bearish":
                # Bearish Divergence: Menjadi tidak aktif jika harga menembus di atas titik tertinggi divergensi.
                # `candle_high` menembus `divergence_high_level` ke atas.
                # Pastikan operan to_float_or_none adalah Decimal untuk konsistensi
                if candle_high > (float(divergence_high_level) + float(tolerance_value_dec)):
                    is_still_active = False
                    logger.debug(f"Divergensi Bearish (ID: {div_id}) menjadi tidak aktif (harga menembus high).")
        
        # 7. Memperbarui Status Divergensi dan Menambah ke Batch
        # Hanya tambahkan ke batch jika status `is_active` berubah
        if div['is_active'] != is_still_active: # Bandingkan dengan status di DB
            updated_divergences_batch.append({
                'id': div_id,
                'is_active': is_still_active,
                'last_processed_time_utc': candle_row.name.to_pydatetime(), # Catat waktu lilin yang memicu perubahan status
                # Sertakan juga kolom-kolom lain yang tidak berubah untuk memastikan batch update berfungsi
                'symbol': div['symbol'],
                'timeframe': div['timeframe'],
                'indicator_type': div['indicator_type'],
                'divergence_type': div['divergence_type'],
                'price_point_time_utc': div['price_point_time_utc'],
                'indicator_point_time_utc': div['indicator_point_time_utc'],
                'price_level_1': div['price_level_1'],
                'price_level_2': div['price_level_2'],
                'indicator_value_1': div['indicator_value_1'],
                'indicator_value_2': div['indicator_value_2']
            })
            processed_count += 1
    
    # 8. Menyimpan Batch Update ke Database
    if updated_divergences_batch:
        try:
            database_manager.save_divergences_batch([], updated_divergences_batch) # new_items_data kosong, updated_items_data diisi
            logger.info(f"BACKFILL WORKER: Berhasil mem-batch update {len(updated_divergences_batch)} status divergensi untuk {symbol_param} {timeframe_str}.")
        except Exception as e:
            logger.error(f"BACKFILL WORKER: Gagal mem-batch update status divergensi: {e}", exc_info=True)

    logger.info(f"Selesai memperbarui status divergensi untuk {symbol_param} {timeframe_str}. {processed_count} diperbarui.")
    return processed_count


def _detect_new_divergences_historically(symbol_param: str, timeframe_str: str, candles_df: pd.DataFrame, current_atr_value: Decimal):
    """
    Mendeteksi Divergensi BARU historis (canggih) untuk {symbol_param} {timeframe_str}
    menggunakan swing points internal dan indikator yang dihitung.
    Args:
        symbol_param (str): Simbol trading.
        timeframe_str (str): Timeframe.
        candles_df (pd.DataFrame): DataFrame candle historis.
        new_divergences_list (list): Parameter ini tidak lagi digunakan untuk mengumpulkan data divergensi baru.
                                      Data divergensi baru akan disimpan secara internal via batching ke DB.
    Returns:
        int: Jumlah divergensi baru yang berhasil dideteksi dan dikirim ke antrean DB.
    """
    logger.info(f"Mendeteksi Divergensi BARU historis (canggih) untuk {symbol_param} {timeframe_str} menggunakan swing points internal...")
    # DIV_DEBUG: breakpoint 1
    logger.debug(f"DIV_DEBUG: Memulai _detect_new_divergences_historically untuk {symbol_param} {timeframe_str}. Jumlah lilin input (candles_df): {len(candles_df)}.")
    processed_count = 0 

    candles_df_copy = candles_df.copy()

    rsi_period = config.AIAnalysts.RSI_PERIOD
    macd_fast = config.AIAnalysts.MACD_FAST_PERIOD
    macd_slow = config.AIAnalysts.MACD_SLOW_PERIOD
    macd_signal = config.AIAnalysts.MACD_SIGNAL_PERIOD
    swing_ext_bars = config.AIAnalysts.SWING_EXT_BARS # Menggunakan parameter swing dari config

    rsi_overbought_threshold = config.AIAnalysts.RSI_OVERBOUGHT
    rsi_oversold_threshold = config.AIAnalysts.RSI_OVERSOLD
    price_tolerance = config.RuleBasedStrategy.RULE_SR_TOLERANCE_POINTS * _SYMBOL_POINT_VALUE

    if current_atr_value is not None and current_atr_value > Decimal('0.0'):
        price_tolerance = current_atr_value * config.RuleBasedStrategy.DIVERGENCE_PRICE_TOLERANCE_ATR_MULTIPLIER
        logger.debug(f"Divergence Detektor: Menggunakan price_tolerance dinamis ATR: {float(price_tolerance):.5f} (dari ATR {float(current_atr_value):.5f})")
    else:
        # Fallback ke nilai statis jika ATR tidak tersedia atau nol
        price_tolerance = config.RuleBasedStrategy.RULE_SR_TOLERANCE_POINTS * _SYMBOL_POINT_VALUE
        logger.warning(f"Divergence Detektor: ATR tidak valid. Menggunakan price_tolerance statis: {float(price_tolerance):.5f}")


    indicator_tolerance_rsi = Decimal('0.5') # Toleransi kecil untuk RSI
    indicator_tolerance_macd = Decimal('0.000001') # Toleransi sangat kecil untuk MACD

    min_candles_required_for_indicators = max(
        config.AIAnalysts.RSI_PERIOD,
        config.AIAnalysts.MACD_SLOW_PERIOD + config.AIAnalysts.MACD_SIGNAL_PERIOD
    ) + 5 # Buffer kecil untuk swing points dan dropna

    if candles_df.empty or len(candles_df) < min_candles_required_for_indicators:
        logger.warning(f"Tidak cukup candle ({len(candles_df)}) untuk deteksi divergensi di {timeframe_str}. "
                       f"Membutuhkan setidaknya {min_candles_required_for_indicators} candle.")
        return 0 
    
    # Memastikan ada cukup lilin untuk periode indikator terpanjang DAN deteksi swing
    min_required_candles = max(rsi_period, macd_slow, macd_signal) + (2 * swing_ext_bars) + 3
    if candles_df_copy.empty or len(candles_df_copy) < min_required_candles:
        logger.debug(f"Tidak cukup lilin ({len(candles_df_copy)}) untuk deteksi divergensi historis di {timeframe_str}. Diperlukan minimal {min_required_candles}.")
        return 0 

    # --- Persiapan DataFrame untuk perhitungan ---
    # Konversi Decimal ke float untuk operasi NumPy/TA-Lib
    ohlc_df_for_calc = candles_df_copy.rename(columns={
        'open_price': 'open', 'high_price': 'high', 'low_price': 'low',
        'close_price': 'close', 'tick_volume': 'volume', 'real_volume': 'real_volume', 'spread': 'spread'
    })
    # DIV_DEBUG: breakpoint 1 (setelah rename)
    logger.debug(f"DIV_DEBUG: ohlc_df_for_calc head (setelah rename):\n{ohlc_df_for_calc.head()}")
    logger.debug(f"DIV_DEBUG: ohlc_df_for_calc dtypes (setelah rename):\n{ohlc_df_for_calc.dtypes}")

    for col in ['open', 'high', 'low', 'close', 'volume', 'real_volume', 'spread']:
        if col in ohlc_df_for_calc.columns:
            ohlc_df_for_calc[col] = ohlc_df_for_calc[col].apply(utils.to_float_or_none)
    # DIV_DEBUG: breakpoint 1 (setelah konversi float)
    logger.debug(f"DIV_DEBUG: ohlc_df_for_calc head (setelah konversi float):\n{ohlc_df_for_calc.head()}")
    logger.debug(f"DIV_DEBUG: ohlc_df_for_calc dtypes (setelah konversi float):\n{ohlc_df_for_calc.dtypes}")


    # --- Hitung Indikator untuk seluruh DataFrame ---
    # DIV_DEBUG: breakpoint 2
    logger.debug(f"DIV_DEBUG: Memanggil _calculate_rsi dengan periode {rsi_period}.")
    rsi_series_result = _calculate_rsi(ohlc_df_for_calc, rsi_period)
    # DIV_DEBUG: breakpoint 2 (hasil RSI)
    logger.debug(f"DIV_DEBUG: rsi_series_result head:\n{rsi_series_result.head()}")
    logger.debug(f"DIV_DEBUG: rsi_series_result tail:\n{rsi_series_result.tail()}")
    logger.debug(f"DIV_DEBUG: rsi_series_result isnull count: {rsi_series_result.isnull().sum()}")

    ohlc_df_for_calc['rsi'] = pd.Series(rsi_series_result, index=ohlc_df_for_calc.index)
    # DIV_DEBUG: breakpoint 2 (setelah assign RSI)
    logger.debug(f"DIV_DEBUG: ohlc_df_for_calc['rsi'] head (after assignment):\n{ohlc_df_for_calc['rsi'].head()}")
    logger.debug(f"DIV_DEBUG: ohlc_df_for_calc['rsi'] isnull count (after assignment): {ohlc_df_for_calc['rsi'].isnull().sum()}")


    # DIV_DEBUG: breakpoint 3
    logger.debug(f"DIV_DEBUG: Memanggil _calculate_macd dengan fast={macd_fast}, slow={macd_slow}, signal={macd_signal}.")
    macd_results = _calculate_macd(ohlc_df_for_calc, macd_fast, macd_slow, macd_signal)
    # DIV_DEBUG: breakpoint 3 (hasil MACD)
    logger.debug(f"DIV_DEBUG: macd_results macd_line head:\n{macd_results.get('macd_line').head()}")
    logger.debug(f"DIV_DEBUG: macd_results macd_line isnull count: {macd_results.get('macd_line').isnull().sum()}")
    logger.debug(f"DIV_DEBUG: macd_results histogram head:\n{macd_results.get('histogram').head()}")
    logger.debug(f"DIV_DEBUG: macd_results macd_pcent head:\n{macd_results.get('macd_pcent').head()}")
    
    ohlc_df_for_calc['macd_line'] = pd.Series(macd_results.get('macd_line'), index=ohlc_df_for_calc.index) \
                                     if isinstance(macd_results, dict) else pd.Series([np.nan]*len(ohlc_df_for_calc), index=ohlc_df_for_calc.index)
    ohlc_df_for_calc['macd_signal'] = pd.Series(macd_results.get('signal_line'), index=ohlc_df_for_calc.index) \
                                     if isinstance(macd_results, dict) else pd.Series([np.nan]*len(ohlc_df_for_calc), index=ohlc_df_for_calc.index)
    ohlc_df_for_calc['macd_hist'] = pd.Series(macd_results.get('histogram'), index=ohlc_df_for_calc.index) \
                                   if isinstance(macd_results, dict) else pd.Series([np.nan]*len(ohlc_df_for_calc), index=ohlc_df_for_calc.index)
    ohlc_df_for_calc['macd_pcent'] = pd.Series(macd_results.get('macd_pcent'), index=ohlc_df_for_calc.index) \
                                    if isinstance(macd_results, dict) else pd.Series([np.nan]*len(ohlc_df_for_calc), index=ohlc_df_for_calc.index)
    # DIV_DEBUG: breakpoint 3 (setelah assign MACD)
    logger.debug(f"DIV_DEBUG: ohlc_df_for_calc['macd_line'] head (after assignment):\n{ohlc_df_for_calc['macd_line'].head()}")
    logger.debug(f"DIV_DEBUG: ohlc_df_for_calc['macd_line'] isnull count (after assignment): {ohlc_df_for_calc['macd_line'].isnull().sum()}")


    # Filter hanya baris yang memiliki nilai indikator yang valid untuk perbandingan.
    # DIV_DEBUG: breakpoint 4
    initial_len_before_dropna = len(ohlc_df_for_calc)
    ohlc_df_processed = ohlc_df_for_calc.dropna(subset=['rsi', 'macd_line', 'macd_pcent']).copy()
    # DIV_DEBUG: breakpoint 4 (setelah dropna)
    logger.debug(f"DIV_DEBUG: ohlc_df_processed head (after dropna):\n{ohlc_df_processed.head()}")
    logger.debug(f"DIV_DEBUG: ohlc_df_processed tail (after dropna):\n{ohlc_df_processed.tail()}")
    logger.debug(f"DIV_DEBUG: ohlc_df_processed dtypes (after dropna):\n{ohlc_df_processed.dtypes}")
    logger.debug(f"DIV_DEBUG: Jumlah lilin sebelum dropna: {initial_len_before_dropna}, setelah dropna: {len(ohlc_df_processed)}. Dihapus: {initial_len_before_dropna - len(ohlc_df_processed)}.")


    if len(ohlc_df_processed) < (2 * swing_ext_bars + 3):
        logger.debug(f"Tidak cukup data setelah perhitungan indikator dan dropna untuk divergensi historis di {timeframe_str}. Hanya {len(ohlc_df_processed)} valid.")
        return 0 

    # --- Hitung Swing Highs/Lows pada data yang sudah diproses ---
    # DIV_DEBUG: breakpoint 5
    logger.debug(f"DIV_DEBUG: Memanggil _calculate_swing_highs_lows_internal dengan swing_ext_bars={swing_ext_bars}.")
    swing_results_df = globals().get('_calculate_swing_highs_lows_internal')(ohlc_df_processed, swing_length=swing_ext_bars)
    # DIV_DEBUG: breakpoint 5 (hasil swing)
    logger.debug(f"DIV_DEBUG: swing_results_df head:\n{swing_results_df.head()}")
    logger.debug(f"DIV_DEBUG: swing_results_df tail:\n{swing_results_df.tail()}")
    logger.debug(f"DIV_DEBUG: Total non-NaN swing points in swing_results_df: {len(swing_results_df.dropna())}")

    ohlc_df_processed.loc[:, 'swing_hl'] = swing_results_df['HighLow']
    ohlc_df_processed.loc[:, 'swing_level'] = swing_results_df['Level']

    valid_swing_points_data = []
    # DIV_DEBUG: breakpoint 6 (awal loop valid_swing_points_data)
    for idx, row in swing_results_df.iterrows():
        if pd.notna(row['HighLow']):
            # Menggunakan .get_indexer untuk mendapatkan posisi integer
            pos_in_processed_df = ohlc_df_processed.index.get_indexer([idx])
            
            # DIV_DEBUG: breakpoint 6 (dalam loop, periksa sebelum append)
            if pos_in_processed_df[0] == -1:
                logger.debug(f"DIV_DEBUG: Swing point {idx.isoformat()} tidak ditemukan di ohlc_df_processed.index. Melewatkan.")
                continue

            current_rsi = utils.to_decimal_or_none(ohlc_df_processed.loc[idx, 'rsi'])
            current_macd = utils.to_decimal_or_none(ohlc_df_processed.loc[idx, 'macd_line'])
            
            # Cek apakah indikator valid pada swing point ini
            if current_rsi is None or current_macd is None:
                logger.debug(f"DIV_DEBUG: Indikator (RSI/MACD) adalah None/NaN pada swing point {idx.isoformat()}. Melewatkan.")
                continue

            valid_swing_points_data.append({
                'index': pos_in_processed_df[0],
                'time': idx.to_pydatetime(),
                'type': "SH" if int(row['HighLow']) == 1 else "SL",
                'price': utils.to_decimal_or_none(row['Level']),
                'rsi': current_rsi,
                'macd': current_macd
            })
    # DIV_DEBUG: breakpoint 6 (setelah loop valid_swing_points_data)
    logger.debug(f"DIV_DEBUG: Final valid_swing_points_data: {valid_swing_points_data}")


    logger.debug(f"Ditemukan {len(valid_swing_points_data)} swing points untuk deteksi divergensi di {timeframe_str}.")
    if len(valid_swing_points_data) < 2:
        logger.debug(f"Tidak cukup swing points untuk deteksi divergensi di {timeframe_str}.")
        return 0 

    # --- PENGECEKAN DUPLIKAT DARI DATABASE YANG SUDAH ADA ---
    existing_div_keys = set()
    try:
        existing_divs_db = database_manager.get_divergences(
            symbol=symbol_param,
            timeframe=timeframe_str,
            is_active=True
        )
        for div_db_item in existing_divs_db:
            div_time_dt_for_key = div_db_item['price_point_time_utc']
            indicator_point_time_dt_for_key = div_db_item['indicator_point_time_utc']
            existing_div_keys.add((div_db_item['symbol'], div_db_item['timeframe'], div_db_item['indicator_type'], div_db_item['divergence_type'],
                                   div_time_dt_for_key.isoformat(), indicator_point_time_dt_for_key.isoformat()))
        logger.debug(f"Ditemukan {len(existing_div_keys)} Divergensi yang aktif yang sudah ada di DB untuk {timeframe_str}.")
    except Exception as e:
        logger.error(f"Gagal mengambil existing Divergences dari DB untuk {timeframe_str} (untuk pre-filtering): {e}", exc_info=True)
        pass

    BATCH_SAVE_THRESHOLD_DIV = config.System.DATABASE_BATCH_SIZE // 2 if config.System.DATABASE_BATCH_SIZE > 1 else 100
    detected_divergences_to_save_in_batch = []
    div_keys_in_current_chunk = set()

    # --- Iterasi Pasangan Swing Points untuk Deteksi Divergensi ---
    # DIV_DEBUG: breakpoint 7 (awal loop deteksi divergensi)
    logger.debug(f"DIV_DEBUG: Memulai loop deteksi divergensi. Total valid_swing_points_data: {len(valid_swing_points_data)}")
    for i in range(len(valid_swing_points_data) - 1, 0, -1):
        current_swing = valid_swing_points_data[i]
        search_start_idx = max(0, i - 50) 
        
        # Log detail swing yang sedang dibandingkan
        logger.debug(f"DIV_DEBUG: Current Swing (P2) at index {current_swing['index']} ({current_swing['time'].isoformat()}): Type={current_swing['type']}, Price={current_swing['price']:.5f}, RSI={current_swing['rsi']:.2f}, MACD={current_swing['macd']:.5f}")

        for j in range(i - 1, search_start_idx -1, -1):
            prev_swing = valid_swing_points_data[j]
            
            # Log detail swing sebelumnya yang sedang dibandingkan
            logger.debug(f"DIV_DEBUG: Comparing with Previous Swing (P1) at index {prev_swing['index']} ({prev_swing['time'].isoformat()}): Type={prev_swing['type']}, Price={prev_swing['price']:.5f}, RSI={prev_swing['rsi']:.2f}, MACD={prev_swing['macd']:.5f}")

            min_candle_distance = 5
            if abs(current_swing['index'] - prev_swing['index']) < min_candle_distance:
                logger.debug(f"DIV_DEBUG: Melewatkan pasangan swing (jarak terlalu dekat: {abs(current_swing['index'] - prev_swing['index'])} < {min_candle_distance} lilin).")
                continue

            if current_swing['rsi'] is None or prev_swing['rsi'] is None or \
               current_swing['macd'] is None or prev_swing['macd'] is None:
                logger.debug("DIV_DEBUG: Melewatkan pasangan swing (nilai indikator None/NaN).")
                continue

            # --- Deteksi Divergensi RSI ---
            # Bullish Regular Divergence (Harga LL, RSI HL)
            logger.debug(f"DIV_DEBUG: --- Mengecek Bullish Regular RSI ---")
            logger.debug(f"DIV_DEBUG: P1 Type == 'SL' ({prev_swing['type'] == 'SL'})")
            logger.debug(f"DIV_DEBUG: P2 Type == 'SL' ({current_swing['type'] == 'SL'})")
            logger.debug(f"DIV_DEBUG: Price P2 < P1 ({current_swing['price']:.5f} < {prev_swing['price']:.5f}): {current_swing['price'] < prev_swing['price']}")
            logger.debug(f"DIV_DEBUG: RSI P2 > P1 ({current_swing['rsi']:.2f} > {prev_swing['rsi']:.2f}): {current_swing['rsi'] > prev_swing['rsi']}")
            logger.debug(f"DIV_DEBUG: RSI P2 < Oversold Threshold ({current_swing['rsi']:.2f} < {rsi_oversold_threshold:.2f}): {current_swing['rsi'] < rsi_oversold_threshold}")
            
            if prev_swing['type'] == 'SL' and current_swing['type'] == 'SL' and \
               current_swing['price'] < prev_swing['price'] and \
               current_swing['rsi'] > prev_swing['rsi'] and \
               current_swing['rsi'] < rsi_oversold_threshold:
                
                new_div_data = {
                    "symbol": symbol_param, "timeframe": timeframe_str, "indicator_type": "RSI",
                    "divergence_type": "Bullish Regular", 
                    "price_point_time_utc": prev_swing['time'],
                    "indicator_point_time_utc": current_swing['time'],
                    "price_level_1": prev_swing['price'],
                    "price_level_2": current_swing['price'],
                    "indicator_value_1": prev_swing['rsi'],
                    "indicator_value_2": current_swing['rsi'],
                    "is_active": True
                }
                div_key = (new_div_data['symbol'], new_div_data['timeframe'], new_div_data['indicator_type'], new_div_data['divergence_type'], 
                           new_div_data['price_point_time_utc'].isoformat(), 
                           new_div_data['indicator_point_time_utc'].isoformat())
                
                if div_key not in existing_div_keys and div_key not in div_keys_in_current_chunk:
                    detected_divergences_to_save_in_batch.append(new_div_data)
                    div_keys_in_current_chunk.add(div_key)
                    logger.debug(f"DIV_DEBUG: Divergensi Bullish Regular (RSI) TERDETEKSI di {timeframe_str} pada {new_div_data['price_point_time_utc']}.")
                # No 'pass' needed here because the 'if' block has content.
            
            elif prev_swing['type'] == 'SH' and current_swing['type'] == 'SH' and \
                   current_swing['price'] > prev_swing['price'] and \
                   current_swing['rsi'] < prev_swing['rsi'] and \
                   current_swing['rsi'] > rsi_overbought_threshold:
                
                logger.debug(f"DIV_DEBUG: --- Mengecek Bearish Regular RSI ---")
                logger.debug(f"DIV_DEBUG: P1 Type == 'SH' ({prev_swing['type'] == 'SH'})")
                logger.debug(f"DIV_DEBUG: P2 Type == 'SH' ({current_swing['type'] == 'SH'})")
                logger.debug(f"DIV_DEBUG: Price P2 > P1 ({current_swing['price']:.5f} > {prev_swing['price']:.5f}): {current_swing['price'] > prev_swing['price']}")
                logger.debug(f"DIV_DEBUG: RSI P2 < P1 ({current_swing['rsi']:.2f} < {prev_swing['rsi']:.2f}): {current_swing['rsi'] < prev_swing['rsi']}")
                logger.debug(f"DIV_DEBUG: RSI P2 > Overbought Threshold ({current_swing['rsi']:.2f} > {rsi_overbought_threshold:.2f}): {current_swing['rsi'] > rsi_overbought_threshold}")
                
                new_div_data = {
                    "symbol": symbol_param, "timeframe": timeframe_str, "indicator_type": "RSI",
                    "divergence_type": "Bearish Regular", 
                    "price_point_time_utc": prev_swing['time'],
                    "indicator_point_time_utc": current_swing['time'],
                    "price_level_1": prev_swing['price'],
                    "price_level_2": current_swing['price'],
                    "indicator_value_1": prev_swing['rsi'],
                    "indicator_value_2": current_swing['rsi'],
                    "is_active": True
                }
                div_key = (new_div_data['symbol'], new_div_data['timeframe'], new_div_data['indicator_type'], new_div_data['divergence_type'], 
                           new_div_data['price_point_time_utc'].isoformat(), 
                           new_div_data['indicator_point_time_utc'].isoformat())
                if div_key not in existing_div_keys and div_key not in div_keys_in_current_chunk:
                    detected_divergences_to_save_in_batch.append(new_div_data)
                    div_keys_in_current_chunk.add(div_key)
                    logger.debug(f"DIV_DEBUG: Divergensi Bearish Regular (RSI) TERDETEKSI di {timeframe_str} pada {new_div_data['price_point_time_utc']}.")
                # No 'pass' needed here.

            elif prev_swing['type'] == 'SL' and current_swing['type'] == 'SL' and \
                 current_swing['price'] > prev_swing['price'] and \
                 current_swing['rsi'] < prev_swing['rsi']:
                
                logger.debug(f"DIV_DEBUG: --- Mengecek Hidden Bullish RSI ---")
                logger.debug(f"DIV_DEBUG: P1 Type == 'SL' ({prev_swing['type'] == 'SL'})")
                logger.debug(f"DIV_DEBUG: P2 Type == 'SL' ({current_swing['type'] == 'SL'})")
                logger.debug(f"DIV_DEBUG: Price P2 > P1 ({current_swing['price']:.5f} > {prev_swing['price']:.5f}): {current_swing['price'] > prev_swing['price']}")
                logger.debug(f"DIV_DEBUG: RSI P2 < P1 ({current_swing['rsi']:.2f} < {prev_swing['rsi']:.2f}): {current_swing['rsi'] < prev_swing['rsi']}")
                
                new_div_data = {
                    "symbol": symbol_param, "timeframe": timeframe_str, "indicator_type": "RSI",
                    "divergence_type": "Hidden Bullish", 
                    "price_point_time_utc": prev_swing['time'],
                    "indicator_point_time_utc": current_swing['time'],
                    "price_level_1": prev_swing['price'],
                    "price_level_2": current_swing['price'],
                    "indicator_value_1": prev_swing['rsi'],
                    "indicator_value_2": current_swing['rsi'],
                    "is_active": True
                }
                div_key = (new_div_data['symbol'], new_div_data['timeframe'], new_div_data['indicator_type'], new_div_data['divergence_type'], 
                           new_div_data['price_point_time_utc'].isoformat(), 
                           new_div_data['indicator_point_time_utc'].isoformat())
                if div_key not in existing_div_keys and div_key not in div_keys_in_current_chunk:
                    detected_divergences_to_save_in_batch.append(new_div_data)
                    div_keys_in_current_chunk.add(div_key)
                    logger.debug(f"DIV_DEBUG: Divergensi Hidden Bullish (RSI) TERDETEKSI di {timeframe_str} pada {new_div_data['price_point_time_utc']}.")
                # No 'pass' needed here.

            elif prev_swing['type'] == 'SH' and current_swing['type'] == 'SH' and \
                 current_swing['price'] < prev_swing['price'] and \
                 current_swing['rsi'] > prev_swing['rsi']:
                
                logger.debug(f"DIV_DEBUG: --- Mengecek Hidden Bearish RSI ---")
                logger.debug(f"DIV_DEBUG: P1 Type == 'SH' ({prev_swing['type'] == 'SH'})")
                logger.debug(f"DIV_DEBUG: P2 Type == 'SH' ({current_swing['type'] == 'SH'})")
                logger.debug(f"DIV_DEBUG: Price P2 < P1 ({current_swing['price']:.5f} < {prev_swing['price']:.5f}): {current_swing['price'] < prev_swing['price']}")
                logger.debug(f"DIV_DEBUG: RSI P2 > P1 ({current_swing['rsi']:.2f} > {prev_swing['rsi']:.2f}): {current_swing['rsi'] > prev_swing['rsi']}")
                
                new_div_data = {
                    "symbol": symbol_param, "timeframe": timeframe_str, "indicator_type": "RSI",
                    "divergence_type": "Hidden Bearish", 
                    "price_point_time_utc": prev_swing['time'],
                    "indicator_point_time_utc": current_swing['time'],
                    "price_level_1": prev_swing['price'],
                    "price_level_2": current_swing['price'],
                    "indicator_value_1": prev_swing['rsi'],
                    "indicator_value_2": current_swing['rsi'],
                    "is_active": True
                }
                div_key = (new_div_data['symbol'], new_div_data['timeframe'], new_div_data['indicator_type'], new_div_data['divergence_type'], 
                           new_div_data['price_point_time_utc'].isoformat(), 
                           new_div_data['indicator_point_time_utc'].isoformat())
                if div_key not in existing_div_keys and div_key not in div_keys_in_current_chunk:
                    detected_divergences_to_save_in_batch.append(new_div_data)
                    div_keys_in_current_chunk.add(div_key)
                    logger.debug(f"DIV_DEBUG: Divergensi Hidden Bearish (RSI) TERDETEKSI di {timeframe_str} pada {new_div_data['price_point_time_utc']}.")
                # No 'pass' needed here.

            # --- Deteksi Divergensi MACD (menggunakan macd_line) ---
            elif prev_swing['type'] == 'SL' and current_swing['type'] == 'SL' and \
               current_swing['price'] < prev_swing['price'] and \
               current_swing['macd'] > prev_swing['macd']:
                
                logger.debug(f"DIV_DEBUG: --- Mengecek Bullish Regular MACD ---")
                logger.debug(f"DIV_DEBUG: P1 Type == 'SL' ({prev_swing['type'] == 'SL'})")
                logger.debug(f"DIV_DEBUG: P2 Type == 'SL' ({current_swing['type'] == 'SL'})")
                logger.debug(f"DIV_DEBUG: Price P2 < P1 ({current_swing['price']:.5f} < {prev_swing['price']:.5f}): {current_swing['price'] < prev_swing['price']}")
                logger.debug(f"DIV_DEBUG: MACD P2 > P1 ({current_swing['macd']:.5f} > {prev_swing['macd']:.5f}): {current_swing['macd'] > prev_swing['macd']}")
                
                new_div_data = {
                    "symbol": symbol_param, "timeframe": timeframe_str, "indicator_type": "MACD",
                    "divergence_type": "Bullish Regular", 
                    "price_point_time_utc": prev_swing['time'],
                    "indicator_point_time_utc": current_swing['time'],
                    "price_level_1": prev_swing['price'],
                    "price_level_2": current_swing['price'],
                    "indicator_value_1": prev_swing['macd'],
                    "indicator_value_2": current_swing['macd'],
                    "is_active": True
                }
                div_key = (new_div_data['symbol'], new_div_data['timeframe'], new_div_data['indicator_type'], new_div_data['divergence_type'], 
                           new_div_data['price_point_time_utc'].isoformat(), 
                           new_div_data['indicator_point_time_utc'].isoformat())
                if div_key not in existing_div_keys and div_key not in div_keys_in_current_chunk:
                    detected_divergences_to_save_in_batch.append(new_div_data)
                    div_keys_in_current_chunk.add(div_key)
                    logger.debug(f"DIV_DEBUG: Divergensi Bullish Regular (MACD) TERDETEKSI di {timeframe_str} pada {new_div_data['price_point_time_utc']}.")
                # No 'pass' needed here.
            
            elif prev_swing['type'] == 'SH' and current_swing['type'] == 'SH' and \
                 current_swing['price'] > prev_swing['price'] and \
                 current_swing['macd'] < prev_swing['macd']:
                
                logger.debug(f"DIV_DEBUG: --- Mengecek Bearish Regular MACD ---")
                logger.debug(f"DIV_DEBUG: P1 Type == 'SH' ({prev_swing['type'] == 'SH'})")
                logger.debug(f"DIV_DEBUG: P2 Type == 'SH' ({current_swing['type'] == 'SH'})")
                logger.debug(f"DIV_DEBUG: Price P2 > P1 ({current_swing['price']:.5f} > {prev_swing['price']:.5f}): {current_swing['price'] > prev_swing['price']}")
                logger.debug(f"DIV_DEBUG: MACD P2 < P1 ({current_swing['macd']:.5f} < {prev_swing['macd']:.5f}): {current_swing['macd'] < prev_swing['macd']}")
                
                new_div_data = {
                    "symbol": symbol_param, "timeframe": timeframe_str, "indicator_type": "MACD",
                    "divergence_type": "Bearish Regular", 
                    "price_point_time_utc": prev_swing['time'],
                    "indicator_point_time_utc": current_swing['time'],
                    "price_level_1": prev_swing['price'],
                    "price_level_2": current_swing['price'],
                    "indicator_value_1": prev_swing['macd'],
                    "indicator_value_2": current_swing['macd'],
                    "is_active": True
                }
                div_key = (new_div_data['symbol'], new_div_data['timeframe'], new_div_data['indicator_type'], new_div_data['divergence_type'], 
                           new_div_data['price_point_time_utc'].isoformat(), 
                           new_div_data['indicator_point_time_utc'].isoformat())
                if div_key not in existing_div_keys and div_key not in div_keys_in_current_chunk:
                    detected_divergences_to_save_in_batch.append(new_div_data)
                    div_keys_in_current_chunk.add(div_key)
                    logger.debug(f"DIV_DEBUG: Divergensi Bearish Regular (MACD) TERDETEKSI di {timeframe_str} pada {new_div_data['price_point_time_utc']}.")
                # No 'pass' needed here.

            elif prev_swing['type'] == 'SL' and current_swing['type'] == 'SL' and \
                 current_swing['price'] > prev_swing['price'] and \
                 current_swing['macd'] < prev_swing['macd']:
                
                logger.debug(f"DIV_DEBUG: --- Mengecek Hidden Bullish MACD ---")
                logger.debug(f"DIV_DEBUG: P1 Type == 'SL' ({prev_swing['type'] == 'SL'})")
                logger.debug(f"DIV_DEBUG: P2 Type == 'SL' ({current_swing['type'] == 'SL'})")
                logger.debug(f"DIV_DEBUG: Price P2 > P1 ({current_swing['price']:.5f} > {prev_swing['price']:.5f}): {current_swing['price'] > prev_swing['price']}")
                logger.debug(f"DIV_DEBUG: MACD P2 < P1 ({current_swing['macd']:.5f} < {prev_swing['macd']:.5f}): {current_swing['macd'] < prev_swing['macd']}")
                
                new_div_data = {
                    "symbol": symbol_param, "timeframe": timeframe_str, "indicator_type": "MACD",
                    "divergence_type": "Hidden Bullish", 
                    "price_point_time_utc": prev_swing['time'],
                    "indicator_point_time_utc": current_swing['time'],
                    "price_level_1": prev_swing['price'],
                    "price_level_2": current_swing['price'],
                    "indicator_value_1": prev_swing['macd'],
                    "indicator_value_2": current_swing['macd'],
                    "is_active": True
                }
                div_key = (new_div_data['symbol'], new_div_data['timeframe'], new_div_data['indicator_type'], new_div_data['divergence_type'], 
                           new_div_data['price_point_time_utc'].isoformat(), 
                           new_div_data['indicator_point_time_utc'].isoformat())
                if div_key not in existing_div_keys and div_key not in div_keys_in_current_chunk:
                    detected_divergences_to_save_in_batch.append(new_div_data)
                    div_keys_in_current_chunk.add(div_key)
                    logger.debug(f"DIV_DEBUG: Divergensi Hidden Bullish (MACD) TERDETEKSI di {timeframe_str} pada {new_div_data['price_point_time_utc']}.")
                # No 'pass' needed here.

            elif prev_swing['type'] == 'SH' and current_swing['type'] == 'SH' and \
                 current_swing['price'] < prev_swing['price'] and \
                 current_swing['macd'] > prev_swing['macd']:
                
                logger.debug(f"DIV_DEBUG: --- Mengecek Hidden Bearish MACD ---")
                logger.debug(f"DIV_DEBUG: P1 Type == 'SH' ({prev_swing['type'] == 'SH'})")
                logger.debug(f"DIV_DEBUG: P2 Type == 'SH' ({current_swing['type'] == 'SH'})")
                logger.debug(f"DIV_DEBUG: Price P2 < P1 ({current_swing['price']:.5f} < {prev_swing['price']:.5f}): {current_swing['price'] < prev_swing['price']}")
                logger.debug(f"DIV_DEBUG: MACD P2 > P1 ({current_swing['macd']:.5f} > {prev_swing['macd']:.5f}): {current_swing['macd'] > prev_swing['macd']}")
                
                new_div_data = {
                    "symbol": symbol_param, "timeframe": timeframe_str, "indicator_type": "MACD",
                    "divergence_type": "Hidden Bearish", 
                    "price_point_time_utc": prev_swing['time'],
                    "indicator_point_time_utc": current_swing['time'],
                    "price_level_1": prev_swing['price'],
                    "price_level_2": current_swing['price'],
                    "indicator_value_1": prev_swing['macd'],
                    "indicator_value_2": current_swing['macd'],
                    "is_active": True
                }
                div_key = (new_div_data['symbol'], new_div_data['timeframe'], new_div_data['indicator_type'], new_div_data['divergence_type'], 
                           new_div_data['price_point_time_utc'].isoformat(), 
                           new_div_data['indicator_point_time_utc'].isoformat())
                if div_key not in existing_div_keys and div_key not in div_keys_in_current_chunk:
                    detected_divergences_to_save_in_batch.append(new_div_data)
                    div_keys_in_current_chunk.add(div_key)
                    logger.debug(f"DIV_DEBUG: Divergensi Hidden Bearish (MACD) TERDETEKSI di {timeframe_str} pada {new_div_data['price_point_time_utc']}.")
                # No 'pass' needed here.
            # No 'else' block here, the loop simply continues if no condition is met.
        
        # Auto-save batch jika ambang batas tercapai
        if len(detected_divergences_to_save_in_batch) >= BATCH_SAVE_THRESHOLD_DIV:
            try:
                database_manager.save_divergences_batch(detected_divergences_to_save_in_batch, [])
                processed_count += len(detected_divergences_to_save_in_batch) 
                logger.info(f"BACKFILL WORKER: Auto-saved {len(detected_divergences_to_save_in_batch)} new Divergences for {timeframe_str}.")
                detected_divergences_to_save_in_batch = []
                div_keys_in_current_chunk.clear()
            except Exception as e:
                logger.error(f"BACKFILL WORKER: Gagal auto-save Divergences batch: {e}", exc_info=True)
                detected_divergences_to_save_in_batch = []
                div_keys_in_current_chunk.clear()

    # Simpan sisa data setelah loop selesai
    if detected_divergences_to_save_in_batch:
        try:
            database_manager.save_divergences_batch(detected_divergences_to_save_in_batch, [])
            processed_count += len(detected_divergences_to_save_in_batch) 
            logger.info(f"BACKFILL WORKER: Final auto-saved {len(detected_divergences_to_save_in_batch)} remaining new Divergences for {timeframe_str}.")
        except Exception as e:
            logger.error(f"BACKFILL WORKER: Gagal final auto-save Divergences batch: {e}", exc_info=True)
    
    
    logger.info(f"Selesai mendeteksi Divergensi BARU unik historis untuk {symbol_param} {timeframe_str}. Deteksi: {processed_count}.")
    return processed_count

def _detect_and_save_rsi_historically(symbol_param: str, timeframe_str: str, candles_df: pd.DataFrame) -> int:
    """
    Menghitung RSI secara historis dan menyimpannya ke database.
    Args:
        symbol_param (str): Simbol trading.
        timeframe_str (str): Timeframe.
        candles_df (pd.DataFrame): DataFrame candle historis (sudah dalam format Decimal dari df_candles_raw).
                                   Akan diubah ke float dan diganti nama kolom di sini.
    Returns:
        int: Jumlah nilai RSI yang berhasil dihitung dan dikirim ke antrean DB.
    """
    logger.info(f"Mendeteksi dan menyimpan RSI historis untuk {symbol_param} {timeframe_str}...")
    processed_count = 0

    rsi_period = config.AIAnalysts.RSI_PERIOD

    # --- Persiapan DataFrame untuk perhitungan RSI ---
      # Kita perlu membuat salinan dan mengonversinya ke float dan mengganti nama kolom untuk _calculate_rsi.
    ohlc_df_for_rsi_calc_float = candles_df.copy()
    ohlc_df_for_rsi_calc_float = ohlc_df_for_rsi_calc_float.rename(columns={
        'open_price': 'open', 'high_price': 'high', 'low_price': 'low',
        'close_price': 'close', 'tick_volume': 'volume', 'real_volume': 'real_volume', 'spread': 'spread' # <--- PASTIKAN real_volume dan spread juga ada di sini jika relevan
    })
    # KOREKSI: HANYA konversi kolom harga dan volume
    for col in ['open', 'high', 'low', 'close', 'volume', 'real_volume', 'spread']: # <--- PASTIKAN LIST KOLOM INI
        if col in ohlc_df_for_rsi_calc_float.columns:
            ohlc_df_for_rsi_calc_float[col] = ohlc_df_for_rsi_calc_float[col].apply(to_float_or_none)
    # --- Akhir persiapan DataFrame ---


    if ohlc_df_for_rsi_calc_float.empty:
        logger.warning(f"RSI results kosong atau tidak valid untuk {timeframe_str}. Input DataFrame kosong.")
        return 0

    if len(ohlc_df_for_rsi_calc_float) < rsi_period:
        logger.warning(f"Tidak cukup candle ({len(ohlc_df_for_rsi_calc_float)}) untuk menghitung RSI di {timeframe_str}. Diperlukan minimal {rsi_period}.")
        # --- TAMBAHKAN LOG DEBUG INI ---
        logger.debug(f"DEBUG RSI INPUT: Input DF head (len={len(ohlc_df_for_rsi_calc_float)}):\n{ohlc_df_for_rsi_calc_float.head()}")
        logger.debug(f"DEBUG RSI INPUT: Input DF tail (len={len(ohlc_df_for_rsi_calc_float)}):\n{ohlc_df_for_rsi_calc_float.tail()}")
        # -----------------------------
        return 0

    rsi_series = _calculate_rsi(ohlc_df_for_rsi_calc_float, rsi_period)

    if rsi_series is not None and not rsi_series.empty:
        for idx in range(len(rsi_series)):
            timestamp = ohlc_df_for_rsi_calc_float.index[idx].to_pydatetime()
            rsi_val = rsi_series.iloc[idx]

            if pd.notna(rsi_val): # Hanya simpan jika nilai RSI tidak NaN
                try:
                    database_manager.save_rsi_value(
                        symbol=symbol_param,
                        timeframe=timeframe_str,
                        timestamp_utc=timestamp,
                        value=to_decimal_or_none(rsi_val)
                    )
                    processed_count += 1
                except Exception as e:
                    logger.error(f"Gagal menyimpan nilai RSI untuk {timeframe_str} pada {timestamp}: {e}", exc_info=True)
        logger.info(f"Selesai menghitung dan menyimpan RSI untuk {timeframe_str}. Disimpan: {processed_count} titik data.")
    else:
        logger.warning(f"RSI results kosong atau tidak valid untuk {timeframe_str}.") # Log ini muncul jika _calculate_rsi mengembalikan Series kosong
    
    return processed_count

def _detect_and_save_macd_historically(symbol_param: str, timeframe_str: str, candles_df: pd.DataFrame) -> int:
    """
    Menghitung MACD secara historis dan menyimpannya ke database.
    Args:
        symbol_param (str): Simbol trading.
        timeframe_str (str): Timeframe.
        candles_df (pd.DataFrame): DataFrame candle historis.
    Returns:
        int: Jumlah nilai MACD yang berhasil dihitung dan dikirim ke antrean DB.
    """
    logger.info(f"Mendeteksi dan menyimpan MACD historis untuk {symbol_param} {timeframe_str}...")
    processed_count = 0

    macd_fast = config.AIAnalysts.MACD_FAST_PERIOD
    macd_slow = config.AIAnalysts.MACD_SLOW_PERIOD
    macd_signal = config.AIAnalysts.MACD_SIGNAL_PERIOD

    # Pastikan candles_df memiliki kolom yang sesuai dan tipe data yang benar untuk _calculate_macd
    # Pastikan candles_df memiliki kolom yang sesuai dan tipe data yang benar untuk _calculate_macd
    # _calculate_macd mengharapkan DataFrame dengan kolom 'close' dalam float.
    # Namun, candles_df yang masuk ke sini adalah DataFrame Decimal.
    # Kita perlu membuat salinan dan mengonversinya ke float untuk _calculate_macd.
    ohlc_df_for_macd_calc_float = candles_df.copy()
    ohlc_df_for_macd_calc_float = ohlc_df_for_macd_calc_float.rename(columns={
        'open_price': 'open', 'high_price': 'high', 'low_price': 'low',
        'close_price': 'close', 'tick_volume': 'volume', 'real_volume': 'real_volume', 'spread': 'spread' # <--- PASTIKAN real_volume dan spread juga ada di sini jika relevan
    })
    # KOREKSI: HANYA konversi kolom harga dan volume
    for col in ['open', 'high', 'low', 'close', 'volume', 'real_volume', 'spread']: # <--- PASTIKAN LIST KOLOM INI
        if col in ohlc_df_for_macd_calc_float.columns:
            ohlc_df_for_macd_calc_float[col] = ohlc_df_for_macd_calc_float[col].apply(to_float_or_none)




    if ohlc_df_for_macd_calc_float.empty:
        logger.warning(f"Tidak ada data candle untuk {timeframe_str}. Tidak dapat menghitung dan menyimpan MACD.")
        return 0

    macd_results = _calculate_macd(ohlc_df_for_macd_calc_float, macd_fast, macd_slow, macd_signal)

    if macd_results and not macd_results['macd_line'].empty:
        for idx in range(len(macd_results['macd_line'])):
            timestamp = ohlc_df_for_macd_calc_float.index[idx].to_pydatetime()
            macd_val = macd_results['macd_line'].iloc[idx]
            signal_val = macd_results['signal_line'].iloc[idx]
            hist_val = macd_results['histogram'].iloc[idx]
            pcent_val = macd_results['macd_pcent'].iloc[idx] # Ambil nilai macd_pcent

            if pd.notna(macd_val) and pd.notna(signal_val) and pd.notna(hist_val):
                try:
                    database_manager.save_macd_value(
                        symbol=symbol_param,
                        timeframe=timeframe_str,
                        timestamp_utc=timestamp,
                        macd_line=to_decimal_or_none(macd_val),
                        signal_line=to_decimal_or_none(signal_val),
                        histogram=to_decimal_or_none(hist_val),
                        macd_pcent=to_decimal_or_none(pcent_val) # Simpan macd_pcent
                    )
                    processed_count += 1
                except Exception as e:
                    logger.error(f"Gagal menyimpan nilai MACD untuk {timeframe_str} pada {timestamp}: {e}", exc_info=True)
        logger.info(f"Selesai menghitung dan menyimpan MACD untuk {timeframe_str}. Disimpan: {processed_count} titik data.")
    else:
        logger.warning(f"MACD results kosong atau tidak valid untuk {timeframe_str}.")

    return processed_count

def _detect_overbought_oversold_conditions_historically(symbol_param: str, timeframe_str: str, candles_df: pd.DataFrame):
    """
    Mendeteksi kondisi overbought dan oversold berdasarkan RSI historis.
    Menyimpan sinyal baru ke database secara batch dengan deduplikasi.
    """
    logger.info(f"Mendeteksi kondisi Overbought/Oversold historis untuk {symbol_param} {timeframe_str}...")

    rsi_period = config.AIAnalysts.RSI_PERIOD
    rsi_overbought_level = config.AIAnalysts.RSI_OVERBOUGHT
    rsi_oversold_level = config.AIAnalysts.RSI_OVERSOLD

    if candles_df.empty or len(candles_df) < rsi_period:
        logger.debug(f"Tidak cukup lilin ({len(candles_df)}) untuk deteksi Overbought/Oversold di {timeframe_str}.")
        return

    # Buat salinan DataFrame dan konversi kolom harga ke float untuk perhitungan RSI
    df_for_rsi_calc = candles_df.copy()
    df_for_rsi_calc = df_for_rsi_calc.rename(columns={'close_price': 'close'})
    df_for_rsi_calc['close'] = df_for_rsi_calc['close'].apply(to_float_or_none)

    rsi_values = _calculate_rsi(df_for_rsi_calc, rsi_period)

    if rsi_values.empty or rsi_values.isnull().all():
        logger.warning(f"RSI tidak dapat dihitung untuk {timeframe_str}. Melewatkan deteksi Overbought/Oversold.")
        return

    # --- Deduplikasi: Ambil sinyal Overbought/Oversold yang sudah ada di DB ---
    existing_ob_os_keys = set()
    try:
        existing_signals_db = database_manager.get_market_structure_events(
            symbol=symbol_param,
            timeframe=timeframe_str,
            event_type=['Overbought', 'Oversold'],
            start_time_utc=candles_df.index.min().to_pydatetime() # Hanya cek dalam rentang data yang sedang diproses
        )
        for sig in existing_signals_db:
            sig_time_dt = utils.to_utc_datetime_or_none(sig['event_time_utc'])
            if sig_time_dt:
                existing_ob_os_keys.add((sig['symbol'], sig['timeframe'], sig_time_dt.replace(microsecond=0).isoformat(), sig['event_type']))
        logger.debug(f"Ditemukan {len(existing_ob_os_keys)} sinyal Overbought/Oversold yang sudah ada di DB untuk {timeframe_str}.")
    except Exception as e:
        logger.error(f"Gagal mengambil existing Overbought/Oversold signals dari DB untuk pre-filtering: {e}", exc_info=True)
        pass

    detected_ob_os_signals_batch = []

    # Logika deteksi Overbought/Oversold
    for i in range(1, len(rsi_values)):
        current_index = rsi_values.index[i]
        prev_index = rsi_values.index[i-1]
        event_time = current_index.to_pydatetime()
        current_close_price_dec = utils.to_decimal_or_none(candles_df.loc[current_index, 'close'])

        if pd.isna(rsi_values.loc[current_index]) or pd.isna(rsi_values.loc[prev_index]) or current_close_price_dec is None:
            continue

        current_rsi_val = rsi_values.loc[current_index]
        prev_rsi_val = rsi_values.loc[prev_index]

        # Overbought (RSI melintasi batas overbought dari atas ke bawah)
        if current_rsi_val < rsi_overbought_level and prev_rsi_val >= rsi_overbought_level:
            event_type = "Overbought"
            direction = "Bearish" # Potensi pembalikan turun
            
            new_key = (symbol_param, timeframe_str, event_time.replace(microsecond=0).isoformat(), event_type)
            if new_key not in existing_ob_os_keys:
                detected_ob_os_signals_batch.append({
                    "symbol": symbol_param, "timeframe": timeframe_str, "event_type": event_type,
                    "direction": direction, "price_level": current_close_price_dec, "event_time_utc": event_time,
                    "indicator_value_at_event": utils.to_decimal_or_none(current_rsi_val)
                })
                existing_ob_os_keys.add(new_key)
                logger.debug(f"Overbought terdeteksi di {timeframe_str} pada {event_time} (RSI: {current_rsi_val:.2f}).")

        # Oversold (RSI melintasi batas oversold dari bawah ke atas)
        elif current_rsi_val > rsi_oversold_level and prev_rsi_val <= rsi_oversold_level:
            event_type = "Oversold"
            direction = "Bullish" # Potensi pembalikan naik
            
            new_key = (symbol_param, timeframe_str, event_time.replace(microsecond=0).isoformat(), event_type)
            if new_key not in existing_ob_os_keys:
                detected_ob_os_signals_batch.append({
                    "symbol": symbol_param, "timeframe": timeframe_str, "event_type": event_type,
                    "direction": direction, "price_level": current_close_price_dec, "event_time_utc": event_time,
                    "indicator_value_at_event": utils.to_decimal_or_none(current_rsi_val)
                })
                existing_ob_os_keys.add(new_key)
                logger.debug(f"Oversold terdeteksi di {timeframe_str} pada {event_time} (RSI: {current_rsi_val:.2f}).")

    if detected_ob_os_signals_batch:
        try:
            database_manager.save_market_structure_events_batch(detected_ob_os_signals_batch, [])
            logger.info(f"BACKFILL WORKER: Berhasil menyimpan {len(detected_ob_os_signals_batch)} sinyal Overbought/Oversold unik untuk {symbol_param} {timeframe_str}.")
        except Exception as e:
            logger.error(f"BACKFILL WORKER: Gagal menyimpan sinyal Overbought/Oversold batch: {e}", exc_info=True)

    logger.info(f"Selesai mendeteksi kondisi Overbought/Oversold historis untuk {symbol_param} {timeframe_str}.")

def _update_existing_fibonacci_levels_status(symbol_param: str, timeframe_str: str, all_candles_df: pd.DataFrame, current_atr_value: Decimal) -> int:
    """
    Memperbarui status 'is_active' dan 'last_test_time_utc' untuk level Fibonacci yang sudah ada di database.
    Args:
        symbol_param (str): Simbol trading.
        timeframe_str (str): Timeframe.
        all_candles_df (pd.DataFrame): DataFrame candle historis (Diharapkan memiliki kolom '_price' awalnya).
        current_atr_value (Decimal): Nilai ATR saat ini untuk timeframe ini.
    Returns:
        int: Jumlah Fib levels yang statusnya berhasil diperbarui dan dikirim ke antrean DB.
    """
    logger.info(f"Memeriksa aktivitas Fibonacci Levels untuk {symbol_param} {timeframe_str}...")
    processed_count = 0

    if all_candles_df.empty:
        logger.debug(f"DataFrame candle kosong untuk update status Fib di {timeframe_str}.")
        return 0

    df_processed_main = all_candles_df.copy()

    df_processed_main = df_processed_main.rename(columns={
        'open_price': 'open',
        'high_price': 'high',
        'low_price': 'low',
        'close_price': 'close',
        'tick_volume': 'volume',
        'real_volume': 'real_volume',
        'spread': 'spread'
    })

    numeric_cols_to_convert = ['open', 'high', 'low', 'close', 'volume', 'real_volume', 'spread']
    for col in numeric_cols_to_convert:
        if col in df_processed_main.columns:
            df_processed_main[col] = df_processed_main[col].apply(utils.to_float_or_none)
    
    initial_len_main = len(df_processed_main)
    df_processed_main.dropna(subset=['open', 'high', 'low', 'close'], inplace=True)
    if len(df_processed_main) < initial_len_main:
        logger.warning(f"FIB_STATUS_DEBUG: Dihapus {initial_len_main - len(df_processed_main)} baris dengan NaN di kolom OHLC setelah renaming dan konversi float.")

    if df_processed_main.empty:
        logger.warning(f"FIB_STATUS_DEBUG: DataFrame kosong setelah pembersihan NaN. Melewatkan update status Fib Levels.")
        return 0

    if globals().get('_SYMBOL_POINT_VALUE') is None:
        globals().get('_initialize_symbol_point_value')()
    if globals().get('_SYMBOL_POINT_VALUE') is None:
        logger.error(f"Nilai POINT untuk {symbol_param} tidak tersedia. Melewatkan pembaruan status Fib Levels.")
        return 0

    with globals().get('_dynamic_price_lock', threading.Lock()):
        global_min_price_filter = globals().get('_dynamic_price_min_for_analysis')
        global_max_price_filter = globals().get('_dynamic_price_max_for_analysis')

    min_price_filter_for_levels = Decimal('0.0')
    max_price_filter_for_levels = Decimal('99999.0')

    if not df_processed_main.empty:
        latest_close_price = df_processed_main['close'].iloc[-1]
        highest_price_in_chunk = df_processed_main['high'].max()
        lowest_price_in_chunk = df_processed_main['low'].min()
        
        price_range_buffer_percentage = getattr(config.MarketData, 'PRICE_RANGE_BUFFER_PERCENTAGE', Decimal('0.10'))
        
        if latest_close_price is not None and not pd.isna(latest_close_price):
            buffer_value = (Decimal(str(highest_price_in_chunk)) - Decimal(str(lowest_price_in_chunk))) * price_range_buffer_percentage
            
            min_price_filter_for_levels = Decimal(str(lowest_price_in_chunk)) - buffer_value
            max_price_filter_for_levels = Decimal(str(highest_price_in_chunk)) + buffer_value
            
            logger.info(f"FIB_STATUS_DEBUG: Menghitung ulang filter harga lokal: Current={latest_close_price:.5f}, Min={min_price_filter_for_levels:.5f}, Max={max_price_filter_for_levels:.5f}")
        else:
            logger.warning("FIB_STATUS_DEBUG: Harga candle tidak valid untuk menghitung filter harga lokal, menggunakan fallback luas.")
    else:
        logger.warning("FIB_STATUS_DEBUG: df_processed_main kosong, menggunakan filter harga lokal fallback yang sangat luas.")

    active_fibs = database_manager.get_fibonacci_levels(
        symbol=symbol_param, timeframe=timeframe_str, is_active=True,
        limit=None,
        min_price_level=min_price_filter_for_levels,
        max_price_level=max_price_filter_for_levels
    )

    if not active_fibs:
        logger.debug(f"Tidak ada Fibonacci Level aktif atau data candle kosong untuk {symbol_param} {timeframe_str} untuk diperbarui statusnya.")
        return 0

    if current_atr_value is not None and current_atr_value > Decimal('0.0'):
        tolerance_value_dec = current_atr_value * config.MarketData.ATR_MULTIPLIER_FOR_TOLERANCE
        logger.debug(f"Fib Status Updater: Menggunakan toleransi pencocokan dinamis ATR: {float(tolerance_value_dec):.5f} (dari ATR {float(current_atr_value):.5f})")
    else:
        tolerance_value_dec = config.RuleBasedStrategy.RULE_SR_TOLERANCE_POINTS * globals().get('_SYMBOL_POINT_VALUE')
        logger.warning(f"Fib Status Updater: ATR tidak valid. Menggunakan toleransi statis: {float(tolerance_value_dec):.5f}")

    updated_fib_levels_batch = []

    # --- MULAI PERBAIKAN DI SINI ---
    # Logika untuk mencari 'latest_active_fib_time' harus lebih robust
    # Ini adalah tempat peringatan 'Timestamp resolve ke None' sering muncul
    list_of_all_fib_timestamps = []
    for fib_item in active_fibs:
        # Prioritaskan last_test_time_utc, lalu formation_time_utc
        test_time = utils.to_utc_datetime_or_none(fib_item.get('last_test_time_utc'))
        form_time = utils.to_utc_datetime_or_none(fib_item.get('formation_time_utc'))

        # Jika last_test_time_utc valid, gunakan itu
        if test_time:
            list_of_all_fib_timestamps.append(test_time)
        # Jika last_test_time_utc tidak valid TAPI formation_time_utc valid, gunakan formation_time_utc
        elif form_time:
            list_of_all_fib_timestamps.append(form_time)
        # Jika keduanya tidak valid, itu akan memicu WARNING, dan kita bisa melewati item ini
        # Namun, agar max() tidak gagal, kita bisa tambahkan fallback 1970 untuk list
        # Kita juga bisa logging jika keduanya None/invalid
        else:
            logger.warning(f"Fib Status Updater: Fib ID {fib_item.get('id', 'N/A')} tidak memiliki last_test_time_utc atau formation_time_utc yang valid. Mengabaikan untuk perhitungan 'latest_active_fib_time'.")
            
    if list_of_all_fib_timestamps:
        latest_active_fib_time = max(list_of_all_fib_timestamps)
    else:
        # Fallback jika tidak ada timestamp valid sama sekali (daftar kosong)
        latest_active_fib_time = datetime(1970, 1, 1, tzinfo=timezone.utc)
        logger.warning(f"Fib Status Updater: Tidak ada timestamp valid dari semua Fib Level aktif. Menggunakan default '1970-01-01' untuk 'latest_active_fib_time'.")
    # --- AKHIR PERBAIKAN DI SINI ---


    relevant_candles_for_test = df_processed_main[
        df_processed_main.index > latest_active_fib_time
    ].copy()

    df_processed_main = df_processed_main.rename(columns={
        'open_price': 'open',
        'high_price': 'high',
        'low_price': 'low',
        'close_price': 'close',
        'tick_volume': 'volume',
        'real_volume': 'real_volume',
        'spread': 'spread'
    })

    numeric_cols_to_convert = ['open', 'high', 'low', 'close', 'volume', 'real_volume', 'spread']
    for col in numeric_cols_to_convert:
        if col in df_processed_main.columns:
            df_processed_main[col] = df_processed_main[col].apply(utils.to_float_or_none)
    
    initial_len_main = len(df_processed_main)
    df_processed_main.dropna(subset=['open', 'high', 'low', 'close'], inplace=True)
    if len(df_processed_main) < initial_len_main:
        logger.warning(f"FIB_STATUS_DEBUG: Dihapus {initial_len_main - len(df_processed_main)} baris dengan NaN di kolom OHLC setelah renaming dan konversi float.")

    if df_processed_main.empty:
        logger.warning(f"FIB_STATUS_DEBUG: DataFrame kosong setelah pembersihan NaN. Melewatkan update status Fib Levels.")
        return 0

    if relevant_candles_for_test.empty and active_fibs:
        logger.debug(f"Tidak ada lilin baru setelah Fib aktif terbaru untuk {symbol_param} {timeframe_str}. Melewatkan update status Fib Levels.")
        return 0

    swing_results_df = globals().get('_calculate_swing_highs_lows_internal')(relevant_candles_for_test, config.AIAnalysts.SWING_EXT_BARS)
    
    if swing_results_df.empty or swing_results_df['HighLow'].isnull().all():
        logger.debug(f"Tidak ada swing points yang valid untuk {symbol_param} {timeframe_str}. Melewatkan deteksi retracement.")
        retracement_results_for_this_fib = pd.DataFrame()
    else:
        temp_df_decimal_for_retracement = relevant_candles_for_test.copy()
        for col in ['open', 'high', 'low', 'close']:
            if col in temp_df_decimal_for_retracement.columns:
                temp_df_decimal_for_retracement[col] = temp_df_decimal_for_retracement[col].apply(utils.to_decimal_or_none)

        retracement_results_for_this_fib = globals().get('_calculate_retracements_internal')(
            temp_df_decimal_for_retracement, swing_results_df
        )

    if retracement_results_for_this_fib.empty or retracement_results_for_this_fib['Direction'].isnull().all():
        logger.debug(f"Tidak ada hasil retracement yang valid untuk Fib Levels di {symbol_param} {timeframe_str}. Melewatkan update.")
        pass

    for fib in active_fibs:
        fib_id = fib['id']
        fib_top_price_ref = fib['high_price_ref']
        fib_low_price_ref = fib['low_price_ref']
        fib_level_price = fib['price_level']
        fib_type = fib['type']
        
        is_still_active = True
        
        current_retracement_percent_db = fib.get('current_retracement_percent')
        deepest_retracement_percent_db = fib.get('deepest_retracement_percent')
        retracement_direction_db = fib.get('retracement_direction')
        
        updated_current_retracement_percent = current_retracement_percent_db
        updated_deepest_retracement_percent = deepest_retracement_percent_db
        updated_retracement_direction = retracement_direction_db

        if not retracement_results_for_this_fib.empty and not relevant_candles_for_test.empty:
            last_candle_idx_in_relevant = relevant_candles_for_test.index[-1]
            if last_candle_idx_in_relevant in retracement_results_for_this_fib.index:
                last_retracement_data = retracement_results_for_this_fib.loc[last_candle_idx_in_relevant]

                updated_current_retracement_percent = utils.to_decimal_or_none(last_retracement_data.get('CurrentRetracement%'))
                updated_deepest_retracement_percent = utils.to_decimal_or_none(last_retracement_data.get('DeepestRetracement%'))
                updated_retracement_direction = utils.to_int_or_none(last_retracement_data.get('Direction'))
            else:
                logger.debug(f"FIB_STATUS_DEBUG: Fib {fib_id}: Candle terakhir {last_candle_idx_in_relevant} tidak ditemukan di retracement_results_for_this_fib. Update retracement dilewati.")


        # --- PERBAIKAN DI SINI: Pastikan last_test_time_for_update selalu datetime object yang valid ---
        last_test_time_for_update = utils.to_utc_datetime_or_none(fib.get('last_test_time_utc'))
        if last_test_time_for_update is None:
            last_test_time_for_update = utils.to_utc_datetime_or_none(fib.get('formation_time_utc'))
            if last_test_time_for_update is None:
                last_test_time_for_update = datetime(1970, 1, 1, tzinfo=timezone.utc)
                logger.warning(f"FIB_STATUS_DEBUG: Fib {fib_id} tidak memiliki formation_time_utc atau last_test_time_utc yang valid. Menggunakan default 1970-01-01 untuk filtering.")
        # --- AKHIR PERBAIKAN DI SINI ---

        fib_relevant_candles = relevant_candles_for_test[
            relevant_candles_for_test.index > last_test_time_for_update
        ].copy()

        if fib_relevant_candles.empty:
            logger.debug(f"FIB_STATUS_DEBUG: Fib {fib_type} (ID: {fib_id}) dilewati karena tidak ada lilin baru setelah last_test_time_utc ({last_test_time_for_update}).")
            continue


        for _, candle_row in fib_relevant_candles.iterrows():
            if not is_still_active:
                break

            candle_high = candle_row['high']
            candle_low = candle_row['low']
            candle_close = candle_row['close']

            if candle_high is None or candle_low is None or candle_close is None:
                logger.debug(f"FIB_STATUS_DEBUG: Lilin dengan NaN high/low/close setelah konversi untuk Fib {fib_id} di {candle_row.name}. Melewatkan pengecekan.")
                continue

            candle_time_dt = candle_row.name.to_pydatetime()

            is_broken = False
            fib_high_ref_dec = fib_top_price_ref
            fib_low_price_ref_dec = fib_low_price_ref
            fib_level_price_dec = fib_level_price
            tolerance_dec = tolerance_value_dec

            if fib_high_ref_dec > fib_low_price_ref_dec: # Bullish leg
                if candle_low < float(fib_low_price_ref_dec - tolerance_dec):
                    is_broken = True
                    logger.debug(f"FIB_STATUS_DEBUG: Fib {fib_id} (Bullish leg) DINONAKTIFKAN (menembus low_ref: {fib_low_price_ref_dec}) oleh {candle_time_dt}.")
            elif fib_high_ref_dec < fib_low_price_ref_dec: # Bearish leg
                if candle_high > float(fib_high_ref_dec + tolerance_dec):
                    is_broken = True
                    logger.debug(f"FIB_STATUS_DEBUG: Fib {fib_id} (Bearish leg) DINONAKTIFKAN (menembus high_ref: {fib_high_ref_dec}) oleh {candle_time_dt}.")
            
            if is_broken:
                is_still_active = False
                last_test_time_for_update = candle_time_dt
                break

            if (is_still_active and
                candle_high >= float(fib_level_price_dec - tolerance_dec) and
                candle_low <= float(fib_level_price_dec + tolerance_dec)):
                
                if (last_test_time_for_update is None) or \
                   ((candle_time_dt - last_test_time_for_update).total_seconds() > config.System.RETRY_DELAY_SECONDS * 5):
                    last_test_time_for_update = candle_time_dt
                    logger.debug(f"FIB_STATUS_DEBUG: Fib {fib_type} (ID: {fib_id}) retest terjadi. Waktu terakhir diuji: {last_test_time_for_update.isoformat()}.")
        
        logger.debug(f"FIB_STATUS_DEBUG: Fib {fib_id} final status: is_active={is_still_active}, last_test_time={last_test_time_for_update.isoformat() if last_test_time_for_update else 'None'}")
        
        if (fib['is_active'] != is_still_active) or \
           (utils.to_utc_datetime_or_none(fib.get('last_test_time_utc')) != last_test_time_for_update) or \
           (utils.to_decimal_or_none(fib.get('current_retracement_percent')) != updated_current_retracement_percent) or \
           (utils.to_decimal_or_none(fib.get('deepest_retracement_percent')) != updated_deepest_retracement_percent) or \
           (fib.get('retracement_direction') != updated_retracement_direction):
            
            updated_fib_levels_batch.append({
                'id': fib_id,
                'is_active': is_still_active,
                'last_test_time_utc': last_test_time_for_update,
                'symbol': fib['symbol'],
                'timeframe': fib['timeframe'],
                'type': fib['type'],
                'high_price_ref': fib_top_price_ref,
                'low_price_ref': fib_low_price_ref,
                'start_time_ref_utc': fib['start_time_ref_utc'],
                'end_time_ref_utc': fib['end_time_ref_utc'],
                'ratio': fib['ratio'],
                'price_level': fib_level_price,
                'confluence_score': fib.get('confluence_score', 0),
                'current_retracement_percent': updated_current_retracement_percent,
                'deepest_retracement_percent': updated_deepest_retracement_percent,
                'retracement_direction': updated_retracement_direction
            })
            processed_count += 1
            logger.debug(f"FIB_STATUS_DEBUG: Fib {fib_id} ditambahkan ke batch update. Processed count: {processed_count}")

    if updated_fib_levels_batch:
        processed_updated_fib_batch = []
        for fib_data_dict in updated_fib_levels_batch:
            processed_item_dict = fib_data_dict.copy()
            for key, value in processed_item_dict.items():
                if isinstance(value, Decimal):
                    processed_item_dict[key] = utils.to_float_or_none(value)
                elif isinstance(value, datetime):
                    processed_item_dict[key] = utils.to_iso_format_or_none(value)
            processed_updated_fib_batch.append(processed_item_dict)

        try:
            database_manager.save_fibonacci_levels_batch([], processed_updated_fib_batch)
            logger.info(f"BACKFILL WORKER: Berhasil mem-batch update {len(processed_updated_fib_batch)} status Fib Levels untuk {symbol_param} {timeframe_str}.")
        except Exception as e:
            logger.error(f"BACKFILL WORKER: Gagal mem-batch update status Fib Levels: {e}", exc_info=True)

    logger.info(f"Selesai memeriksa aktivitas Fibonacci Levels untuk {symbol_param} {timeframe_str}. {processed_count} diperbarui.")
    return processed_count

def _detect_new_fibonacci_levels_historically(symbol_param: str, timeframe_str: str, candles_df: pd.DataFrame, current_atr_value: Decimal) -> int:
    """
    Mendeteksi Fibonacci Levels BARU historis untuk simbol dan timeframe tertentu.
    Menggunakan swing points internal dan data retracement untuk menghitung level.
    Menyimpan level baru ke database secara batch dengan deduplikasi yang robust.

    Args:
        symbol_param (str): Simbol trading.
        timeframe_str (str): Timeframe.
        candles_df (pd.DataFrame): DataFrame candle historis (diharapkan memiliki kolom '_price' awalnya).
                                   Akan dikonversi ke float dan di-rename secara internal.
        current_atr_value (Decimal): Nilai ATR saat ini untuk timeframe ini, digunakan untuk toleransi dinamis.

    Returns:
        int: Jumlah Fibonacci Levels baru yang berhasil dideteksi dan dikirim ke antrean DB.
    """
    logger.info(f"Mendeteksi Fibonacci Levels BARU historis untuk {symbol_param} {timeframe_str}...")
    processed_count = 0

    # Pastikan _SYMBOL_POINT_VALUE sudah diinisialisasi
    if globals().get('_SYMBOL_POINT_VALUE') is None:
        globals().get('_initialize_symbol_point_value')()
    if globals().get('_SYMBOL_POINT_VALUE') is None:
        logger.error(f"Nilai POINT untuk {symbol_param} tidak tersedia. Melewatkan deteksi Fibonacci Levels.")
        return 0

    # Parameter konfigurasi untuk Swing Length (dari config.py)
    swing_length = config.AIAnalysts.SWING_EXT_BARS

    # Validasi awal data candle: Cukup lilin untuk deteksi swing
    if candles_df.empty or len(candles_df) < (2 * swing_length + 1):
        logger.debug(f"Tidak cukup lilin ({len(candles_df)}) untuk deteksi Fibonacci Levels historis di {timeframe_str}. Diperlukan minimal {2 * swing_length + 1} lilin.")
        return 0

    # --- Persiapan DataFrame untuk perhitungan internal ---
    # Buat salinan DataFrame dan lakukan renaming kolom dari format '_price' ke nama pendek.
    df_processed = candles_df.copy()

    df_processed = df_processed.rename(columns={
        'open_price': 'open',
        'high_price': 'high',
        'low_price': 'low',
        'close_price': 'close',
        'tick_volume': 'volume',
        'real_volume': 'real_volume',
        'spread': 'spread'
    })

    # Buat versi float untuk swing calculation
    df_float_for_swing = df_processed.copy()
    for col in ['open', 'high', 'low', 'close', 'volume', 'real_volume', 'spread']:
        if col in df_float_for_swing.columns:
            df_float_for_swing[col] = df_float_for_swing[col].apply(utils.to_float_or_none)
    initial_len_float = len(df_float_for_swing)
    df_float_for_swing.dropna(subset=['open', 'high', 'low', 'close'], inplace=True)
    if len(df_float_for_swing) < initial_len_float:
        logger.warning(f"Fibonacci Detektor: Dihapus {initial_len_float - len(df_float_for_swing)} baris dengan NaN di kolom OHLC untuk {symbol_param} {timeframe_str} (setelah konversi float).")

    # Buat versi Decimal untuk retracement calculation
    df_decimal_for_retracement = df_processed.copy()
    for col in ['open', 'high', 'low', 'close', 'volume', 'real_volume', 'spread']:
        if col in df_decimal_for_retracement.columns:
            df_decimal_for_retracement[col] = df_decimal_for_retracement[col].apply(utils.to_decimal_or_none)
    initial_len_decimal = len(df_decimal_for_retracement)
    df_decimal_for_retracement.dropna(subset=['open', 'high', 'low', 'close'], inplace=True)
    if len(df_decimal_for_retracement) < initial_len_decimal:
        logger.warning(f"Fibonacci Detektor: Dihapus {initial_len_decimal - len(df_decimal_for_retracement)} baris dengan NaN di kolom OHLC untuk {symbol_param} {timeframe_str} (setelah konversi Decimal).")


    # Validasi jika DataFrame kosong setelah pembersihan NaN
    if df_float_for_swing.empty or len(df_float_for_swing) < (2 * swing_length + 1):
        logger.warning(f"Fibonacci Detektor: DataFrame float kosong atau tidak cukup lilin ({len(df_float_for_swing)}) setelah pembersihan NaN. Melewatkan deteksi Fibonacci untuk {timeframe_str}.")
        return 0
    if df_decimal_for_retracement.empty or len(df_decimal_for_retracement) < (2 * swing_length + 1):
        logger.warning(f"Fibonacci Detektor: DataFrame Decimal kosong atau tidak cukup lilin ({len(df_decimal_for_retracement)}) setelah pembersihan NaN. Melewatkan deteksi Fibonacci untuk {timeframe_str}.")
        return 0

    # --- Hitung Swing Highs/Lows ---
    swing_results_df = globals().get('_calculate_swing_highs_lows_internal')(df_float_for_swing, swing_length=swing_length)

    if swing_results_df.empty or swing_results_df['HighLow'].isnull().all():
        logger.debug(f"Tidak ada Swing High/Low terdeteksi untuk {symbol_param} {timeframe_str}. Melewatkan deteksi Fibonacci.")
        return 0

    # --- Hitung retracement Fibonacci ---
    retracement_results_df = globals().get('_calculate_retracements_internal')(df_decimal_for_retracement, swing_results_df)

    if retracement_results_df.empty or retracement_results_df['Direction'].isnull().all():
        logger.debug(f"Tidak ada retracement Fibonacci terdeteksi untuk {symbol_param} {timeframe_str}. Melewatkan deteksi Fibonacci Levels.")
        return 0

    # --- Deduplikasi: Ambil level Fibonacci yang sudah ada di DB ---
    existing_fib_keys = set()
    try:
        start_time_for_dedup = df_processed.index.min().to_pydatetime() - timedelta(days=365) # Cek data 1 tahun ke belakang untuk deduplikasi
        existing_fibs_db = database_manager.get_fibonacci_levels(
            symbol=symbol_param,
            timeframe=timeframe_str,
            start_time_utc=start_time_for_dedup # Batasi query DB untuk efisiensi
        )
        for fib_item in existing_fibs_db:
            start_time_ref_dt_existing = utils.to_utc_datetime_or_none(fib_item.get('start_time_ref_utc'))
            end_time_ref_dt_existing = utils.to_utc_datetime_or_none(fib_item.get('end_time_ref_utc'))

            start_time_str_for_key = (start_time_ref_dt_existing.replace(microsecond=0).isoformat() if start_time_ref_dt_existing else "NaT_Date")
            end_time_str_for_key = (end_time_ref_dt_existing.replace(microsecond=0).isoformat() if end_time_ref_dt_existing else "NaT_Date")
            
            ratio_float_for_key = utils.to_float_or_none(fib_item['ratio'])

            existing_fib_keys.add((
                fib_item['symbol'], fib_item['timeframe'], fib_item['type'],
                start_time_str_for_key,
                end_time_str_for_key,
                ratio_float_for_key
            ))
        logger.debug(f"Ditemukan {len(existing_fib_keys)} Fibonacci Levels yang sudah ada di DB untuk {timeframe_str} (dari rentang deduplikasi).")
    except Exception as e:
        logger.error(f"Gagal mengambil existing Fibonacci Levels dari DB untuk pre-filtering: {e}", exc_info=True)
        pass

    detected_fib_levels_to_save_batch = []

    # Iterasi melalui hasil retracement untuk mendeteksi level Fibonacci
    fib_ratios = config.MarketData.FIBO_RETRACTION_LEVELS

    for i in range(len(retracement_results_df)):
        fib_type = "Retracement" # Saat ini hanya mendeteksi retracement

        # Ambil nilai referensi dari hasil retracement
        high_price_ref_float = retracement_results_df['HighRef'].iloc[i]
        low_price_ref_float = retracement_results_df['LowRef'].iloc[i]
        raw_start_time_ref = retracement_results_df['StartRefTime'].iloc[i]
        raw_end_time_ref = retracement_results_df['EndRefTime'].iloc[i]

        current_retracement_percent = utils.to_decimal_or_none(retracement_results_df['CurrentRetracement%'].iloc[i])
        deepest_retracement_percent = utils.to_decimal_or_none(retracement_results_df['DeepestRetracement%'].iloc[i])
        retracement_direction_int = utils.to_int_or_none(retracement_results_df['Direction'].iloc[i]) # 1 for bullish, -1 for bearish

        # Konversi ke datetime objects UTC-aware
        start_time_ref_dt = utils.to_utc_datetime_or_none(raw_start_time_ref)
        end_time_ref_dt = utils.to_utc_datetime_or_none(raw_end_time_ref)
        
        # --- PENAMBAHAN KRITIS: PASTIKAN TIDAK ADA NONE UNTUK KOLOM NOT NULL SAAT INSERT ---
        if start_time_ref_dt is None:
            start_time_ref_dt = datetime(1970, 1, 1, tzinfo=timezone.utc)
            logger.error(f"FIB_DETECT_ERROR: start_time_ref_dt untuk Fib level menjadi None. Menggunakan fallback 1970. Periksa input raw_start_time_ref: {raw_start_time_ref}")
        if end_time_ref_dt is None:
            end_time_ref_dt = datetime(1970, 1, 1, tzinfo=timezone.utc)
            logger.error(f"FIB_DETECT_ERROR: end_time_ref_dt untuk Fib level menjadi None. Menggunakan fallback 1970. Periksa input raw_end_time_ref: {raw_end_time_ref}")
        # --- AKHIR PENAMBAHAN KRITIS ---

        # Konversi harga referensi ke Decimal
        high_price_ref_dec = utils.to_decimal_or_none(high_price_ref_float)
        low_price_ref_dec = utils.to_decimal_or_none(low_price_ref_float)

        # Validasi minimal: pastikan data referensi tidak None
        if high_price_ref_dec is None or low_price_ref_dec is None:
            # Timestamp sudah di-handle di atas. Jika harga masih None, lewati.
            continue

        # Format timestamp ke ISO string tanpa mikrosekon untuk konsistensi kunci
        start_time_str_for_key = start_time_ref_dt.replace(microsecond=0).isoformat()
        end_time_str_for_key = end_time_ref_dt.replace(microsecond=0).isoformat()

        # Iterasi melalui setiap rasio Fibonacci yang dikonfigurasi
        for ratio_dec in fib_ratios:
            price_level_dec = None
            # Perhitungan level harga Fibonacci
            if fib_type == "Retracement":
                # Jika high_price_ref > low_price_ref (uptrend leg, bullish retracement)
                if high_price_ref_dec > low_price_ref_dec: 
                    price_level_dec = low_price_ref_dec + (high_price_ref_dec - low_price_ref_dec) * ratio_dec
                # Jika high_price_ref < low_price_ref (downtrend leg, bearish retracement)
                else: 
                    price_level_dec = high_price_ref_dec + (low_price_ref_dec - high_price_ref_dec) * ratio_dec

            if price_level_dec is None:
                continue

            # Bentuk kunci unik untuk deduplikasi
            new_key = (
                symbol_param, timeframe_str, fib_type,
                start_time_str_for_key,
                end_time_str_for_key,
                utils.to_float_or_none(ratio_dec) # Konversi ratio ke float untuk kunci tuple set
            )
            
            # Periksa apakah level Fibonacci ini sudah ada
            if new_key not in existing_fib_keys:
                detected_fib_levels_to_save_batch.append({
                    "symbol": symbol_param,
                    "timeframe": timeframe_str,
                    "type": fib_type,
                    "high_price_ref": high_price_ref_dec,
                    "low_price_ref": low_price_ref_dec,
                    "start_time_ref_utc": start_time_ref_dt, # Simpan objek datetime asli ke DB
                    "end_time_ref_utc": end_time_ref_dt,     # Simpan objek datetime asli ke DB
                    "ratio": ratio_dec,                       # Simpan objek Decimal asli ke DB
                    "price_level": price_level_dec,           # Simpan objek Decimal asli ke DB
                    "is_active": True, # Level baru dianggap aktif
                    "formation_time_utc": end_time_ref_dt, # Waktu pembentukan adalah waktu akhir leg
                    "last_test_time_utc": None, # Akan diupdate oleh fungsi update_existing_fibonacci_levels_status
                    "confluence_score": 0, # Akan diupdate oleh identify_key_levels_across_timeframes
                    "current_retracement_percent": current_retracement_percent,
                    "deepest_retracement_percent": deepest_retracement_percent,
                    "retracement_direction": retracement_direction_int # Simpan int asli
                })
                existing_fib_keys.add(new_key) # Tambahkan ke set untuk deduplikasi di iterasi selanjutnya
                logger.debug(f"Fibonacci Level {fib_type} {float(ratio_dec):.3f} terdeteksi di {timeframe_str} pada {end_time_ref_dt} (Level: {float(price_level_dec):.5f}).")

    if detected_fib_levels_to_save_batch:
        try:
            database_manager.save_fibonacci_levels_batch(detected_fib_levels_to_save_batch, []) 
            processed_count += len(detected_fib_levels_to_save_batch)
            logger.info(f"BACKFILL WORKER: Berhasil menyimpan {len(detected_fib_levels_to_save_batch)} Fibonacci Levels baru unik untuk {symbol_param} {timeframe_str}.")
        except Exception as e:
            logger.error(f"BACKFILL WORKER: Gagal menyimpan Fibonacci Levels batch: {e}", exc_info=True)
    logger.info(f"Selesai mendeteksi Fibonacci Levels BARU historis untuk {symbol_param} {timeframe_str}. Deteksi: {processed_count}.")
    return processed_count

def _detect_new_market_structure_events_historically(symbol_param: str, timeframe_str: str, candles_df: pd.DataFrame, current_atr_value: Decimal) -> int:
    """
    Mendeteksi Market Structure Events (Swing High/Low, Break of Structure, Change of Character)
    yang baru secara historis dan menyimpannya.
    Menggunakan ATR adaptif untuk toleransi break dan memerlukan konfirmasi candle close.

    Args:
        symbol_param (str): Simbol trading.
        timeframe_str (str): Timeframe dari candles_df.
        candles_df (pd.DataFrame): DataFrame candle historis (diharapkan sudah Decimal dan memiliki kolom _price).
                                   Akan dikonversi ke float dan di-rename secara internal.
        current_atr_value (Decimal): Nilai ATR saat ini untuk timeframe ini.
    Returns:
        int: Jumlah event Market Structure baru yang berhasil dideteksi dan dikirim ke antrean DB.
    """
    logger.info(f"Mendeteksi Market Structure Events BARU historis untuk {symbol_param} {timeframe_str}...")
    processed_count = 0

    # Ketergantungan: config.AIAnalysts.SWING_EXT_BARS
    swing_length_for_ms = config.AIAnalysts.SWING_EXT_BARS 

    if candles_df.empty or len(candles_df) < swing_length_for_ms * 2 + 1:
        logger.debug(f"Tidak cukup lilin ({len(candles_df)}) untuk deteksi Market Structure Events historis di {timeframe_str}.")
        return 0

    # Pastikan _SYMBOL_POINT_VALUE sudah diinisialisasi
    if globals().get('_SYMBOL_POINT_VALUE') is None:
        globals().get('_initialize_symbol_point_value')()
    if globals().get('_SYMBOL_POINT_VALUE') is None:
        logger.error(f"Nilai POINT untuk {symbol_param} tidak tersedia. Melewatkan deteksi Market Structure Events.")
        return 0

    # Buat salinan DataFrame dan konversi kolom harga ke float untuk perhitungan internal
    # `candles_df` datang dalam Decimal dari `df_candles_raw` di backfill.
    df_processed = candles_df.copy()
    
    ohlc_df_for_calc = df_processed.rename(columns={
        'open_price': 'open', 'high_price': 'high', 'low_price': 'low',
        'close_price': 'close', 'tick_volume': 'volume'
    })
    for col in ['open', 'high', 'low', 'close', 'volume']:
        if col in ohlc_df_for_calc.columns:
            ohlc_df_for_calc[col] = ohlc_df_for_calc[col].apply(utils.to_float_or_none)

    # Penting: Lakukan dropna di sini, dan ini akan menjadi DataFrame yang digunakan untuk *semua* indexing berikutnya.
    initial_len_ohlc_for_calc = len(ohlc_df_for_calc)
    ohlc_df_for_calc.dropna(subset=['open', 'high', 'low', 'close'], inplace=True)
    if len(ohlc_df_for_calc) < initial_len_ohlc_for_calc:
        logger.warning(f"MS Events Detektor: Dihapus {initial_len_ohlc_for_calc - len(ohlc_df_for_calc)} baris dengan NaN di kolom OHLC untuk {symbol_param} {timeframe_str} (setelah konversi float).")


    # Validasi jika DataFrame kosong setelah pembersihan NaN
    if ohlc_df_for_calc.empty or len(ohlc_df_for_calc) < swing_length_for_ms * 2 + 1:
        logger.warning(f"MS Events Detektor: DataFrame ohlc_df_for_calc kosong atau tidak cukup lilin ({len(ohlc_df_for_calc)}) setelah pembersihan NaN. Melewatkan deteksi MS Events untuk {timeframe_str}.")
        return 0


    # Hitung Swing Highs/Lows
    # Ketergantungan: _calculate_swing_highs_lows_internal
    swing_results_df = globals().get('_calculate_swing_highs_lows_internal')(ohlc_df_for_calc, swing_length=swing_length_for_ms)
    logger.debug(f"Internal swing_highs_lows mengembalikan total {len(swing_results_df)} baris data, dengan {len(swing_results_df.dropna())} swing points terdeteksi untuk {timeframe_str}.")

    # Pastikan swing_results_df memiliki indeks yang sama dengan ohlc_df_for_calc setelah dropna.
    # Jika tidak, ini dapat menyebabkan IndexError. Ini seharusnya sudah diatasi oleh _calculate_swing_highs_lows_internal
    # yang mereindeks outputnya ke indeks input.

    if swing_results_df.empty: # or swing_results_df['HighLow'].isnull().all():
        logger.debug(f"Tidak ada Swing High/Low terdeteksi untuk {symbol_param} {timeframe_str}. Melewatkan deteksi BoS/ChoCh.")
        return 0

    # Hitung toleransi penembusan dinamis menggunakan ATR
    # Ketergantungan: config.RuleBasedStrategy.MS_BREAK_ATR_MULTIPLIER, _SYMBOL_POINT_VALUE
    break_tolerance_value = Decimal('0.0') # Default
    if current_atr_value is not None and current_atr_value > Decimal('0.0'):
        break_tolerance_value = current_atr_value * config.RuleBasedStrategy.MS_BREAK_ATR_MULTIPLIER
        logger.debug(f"MS Events Detektor: Menggunakan break_tolerance dinamis ATR: {float(break_tolerance_value):.5f} (dari ATR {float(current_atr_value):.5f})")
    else:
        break_tolerance_value = config.RuleBasedStrategy.RULE_SR_TOLERANCE_POINTS * globals().get('_SYMBOL_POINT_VALUE')
        logger.warning(f"MS Events Detektor: ATR tidak valid. Menggunakan break_tolerance statis: {float(break_tolerance_value):.5f}")

    # Deduplikasi: Ambil event struktur pasar yang sudah ada di DB
    # Ketergantungan: database_manager.get_market_structure_events, utils.to_utc_datetime_or_none
    existing_ms_event_keys = set()
    try:
        # Cek event yang ada dalam rentang yang lebih luas untuk memastikan tidak ada duplikasi
        # Misalnya, cek 30 hari terakhir.
        start_time_for_dedup = ohlc_df_for_calc.index.min().to_pydatetime() - timedelta(days=30)
        existing_events_db = database_manager.get_market_structure_events(
            symbol=symbol_param,
            timeframe=timeframe_str,
            event_type=['Break of Structure', 'Change of Character'], # Hanya event yang relevan untuk deduplikasi BoS/ChoCh
            start_time_utc=start_time_for_dedup # Perluas rentang untuk deduplikasi
        )
        for event_item in existing_events_db:
            event_time_dt = utils.to_utc_datetime_or_none(event_item['event_time_utc'])
            if event_time_dt:
                existing_ms_event_keys.add((
                    event_item['symbol'], event_item['timeframe'],
                    event_time_dt.replace(microsecond=0).isoformat(), event_item['event_type'], event_item['direction']
                ))
        logger.debug(f"Ditemukan {len(existing_ms_event_keys)} event Market Structure yang sudah ada di DB untuk {timeframe_str}.")
    except Exception as e:
        logger.error(f"Gagal mengambil existing Market Structure events dari DB untuk pre-filtering: {e}", exc_info=True)
        pass

    detected_ms_events_to_save_batch = []

    # Logika deteksi BoS/ChoCh
    ohlc_len = len(ohlc_df_for_calc)
    _open = ohlc_df_for_calc['open'].values
    _high = ohlc_df_for_calc['high'].values
    _low = ohlc_df_for_calc['low'].values
    _close = ohlc_df_for_calc['close'].values

    # Inisialisasi status swing high/low terakhir
    last_swing_high_idx = None
    last_swing_low_idx = None

    # Tambahkan pelacakan untuk swing points yang sudah ditembus (untuk mencegah deteksi BoS berulang)
    # Kunci: index dari swing point yang ditembus
    # Nilai: True (sudah ditembus)
    broken_swing_highs = {}
    broken_swing_lows = {}

    # Membangun kembali broken_swing_highs/lows dari event yang ada di DB untuk menghindari duplikasi BoS yang sudah pernah dicatat
    for event_key in existing_ms_event_keys:
        event_type_str = event_key[3]
        direction_str = event_key[4]
        # Jika event adalah BoS, kita bisa mencoba menandai swing referensinya sebagai 'broken'
        # Namun, karena event_key tidak langsung menyimpan indeks swing,
        # pendekatan yang lebih aman adalah hanya mencegah deteksi duplikat berdasarkan kunci event itu sendiri.
        # Atau, kita perlu mengambil swing_high_ref_time/swing_low_ref_time dari DB.
        # Untuk tujuan ini, kita akan mengandalkan `new_key not in existing_ms_event_keys`.

    for i in range(ohlc_len):
        current_candle_idx = i
        current_time = ohlc_df_for_calc.index[current_candle_idx].to_pydatetime()

        # Update swing high/low terakhir yang VALID (dari swing_results_df)
        # Pastikan kita hanya mempertimbangkan swing yang belum ditembus
        if pd.notna(swing_results_df['HighLow'].iloc[current_candle_idx]):
            swing_type = int(swing_results_df['HighLow'].iloc[current_candle_idx])
            swing_level = float(swing_results_df['Level'].iloc[current_candle_idx])

            if swing_type == 1: # Swing High
                # Cek apakah SH ini sudah ditembus oleh lilin sebelumnya dalam rentang pencarian yang sama
                if not broken_swing_highs.get(current_candle_idx, False):
                    last_swing_high_idx = current_candle_idx
            elif swing_type == -1: # Swing Low
                # Cek apakah SL ini sudah ditembus oleh lilin sebelumnya dalam rentang pencarian yang sama
                if not broken_swing_lows.get(current_candle_idx, False):
                    last_swing_low_idx = current_candle_idx

        # Deteksi BoS Bullish (harga menembus swing high terakhir)
        if last_swing_high_idx is not None and current_candle_idx > last_swing_high_idx:
            # Periksa jika SH ini belum ditandai sebagai ditembus
            if not broken_swing_highs.get(last_swing_high_idx, False):
                # Perketat kriteria BoS dengan konfirmasi candle_close
                if _close[current_candle_idx] > _high[last_swing_high_idx] + float(break_tolerance_value):
                    event_type = "Break of Structure"
                    direction = "Bullish"
                    price_level_val = utils.to_decimal_or_none(_high[last_swing_high_idx])
                    
                    # Bangun kunci unik untuk deduplikasi
                    new_key = (symbol_param, timeframe_str, current_time.replace(microsecond=0).isoformat(), event_type, direction)
                    
                    if new_key not in existing_ms_event_keys:
                        detected_ms_events_to_save_batch.append({
                            "symbol": symbol_param, "timeframe": timeframe_str, "event_type": event_type,
                            "direction": direction, "price_level": price_level_val, "event_time_utc": current_time,
                            "swing_high_ref_time": ohlc_df_for_calc.index[last_swing_high_idx].to_pydatetime(),
                            "swing_low_ref_time": None
                        })
                        existing_ms_event_keys.add(new_key) # Tambahkan ke set untuk deduplikasi
                        broken_swing_highs[last_swing_high_idx] = True # Tandai SH ini sudah ditembus
                        logger.debug(f"BoS Bullish terdeteksi di {timeframe_str} pada {current_time} (Harga: {float(price_level_val):.5f}).")

            # Setelah BoS terdeteksi dan swing ditandai, reset last_swing_high_idx untuk mencegah deteksi berulang
            # terhadap swing yang sama jika ada lilin lain yang juga memenuhi kriteria break.
            if last_swing_high_idx in broken_swing_highs and broken_swing_highs[last_swing_high_idx]:
                 last_swing_high_idx = None # Reset untuk mencari SH berikutnya


        # Deteksi BoS Bearish (harga menembus swing low terakhir)
        if last_swing_low_idx is not None and current_candle_idx > last_swing_low_idx:
            if not broken_swing_lows.get(last_swing_low_idx, False):
                # Perketat kriteria BoS dengan konfirmasi candle_close
                if _close[current_candle_idx] < _low[last_swing_low_idx] - float(break_tolerance_value):
                    event_type = "Break of Structure"
                    direction = "Bearish"
                    price_level_val = utils.to_decimal_or_none(_low[last_swing_low_idx])
                    
                    new_key = (symbol_param, timeframe_str, current_time.replace(microsecond=0).isoformat(), event_type, direction)
                    
                    if new_key not in existing_ms_event_keys:
                        detected_ms_events_to_save_batch.append({
                            "symbol": symbol_param, "timeframe": timeframe_str, "event_type": event_type,
                            "direction": direction, "price_level": price_level_val, "event_time_utc": current_time,
                            "swing_high_ref_time": None,
                            "swing_low_ref_time": ohlc_df_for_calc.index[last_swing_low_idx].to_pydatetime()
                        })
                        existing_ms_event_keys.add(new_key)
                        broken_swing_lows[last_swing_low_idx] = True # Tandai SL ini sudah ditembus
                        logger.debug(f"BoS Bearish terdeteksi di {timeframe_str} pada {current_time} (Harga: {float(price_level_val):.5f}).")
            
            if last_swing_low_idx in broken_swing_lows and broken_swing_lows[last_swing_low_idx]:
                last_swing_low_idx = None # Reset untuk mencari SL berikutnya


        # Deteksi Change of Character (ChoCh) - pembalikan tren setelah penembusan swing high/low
        # Logika ini biasanya lebih kompleks, melibatkan bias tren yang lebih tinggi.
        # Untuk saat ini, fungsi ini berfokus pada BoS.

    if detected_ms_events_to_save_batch:
        try:
            database_manager.save_market_structure_events_batch(detected_ms_events_to_save_batch, [])
            processed_count += len(detected_ms_events_to_save_batch)
            logger.info(f"BACKFILL WORKER: Berhasil menyimpan {len(detected_ms_events_to_save_batch)} event Market Structure baru unik untuk {symbol_param} {timeframe_str}.")
        except Exception as e:
            logger.error(f"BACKFILL WORKER: Gagal menyimpan event Market Structure batch: {e}", exc_info=True)

    logger.info(f"Selesai mendeteksi Market Structure Events BARU historis untuk {symbol_param} {timeframe_str}. Deteksi: {processed_count}.")
    return processed_count




def _update_existing_fair_value_gaps_status(symbol_param: str, timeframe_str: str, candles_df: pd.DataFrame, current_atr_value: Decimal) -> int:
    """
    Memperbarui status Fair Value Gaps (FVG) yang sudah ada (is_filled, retest_count, last_retest_time_utc)
    berdasarkan pergerakan harga terbaru.
    FVG menjadi terisi jika harga telah melewati levelnya.
    Args:
        symbol_param (str): Simbol trading.
        timeframe_str (str): Timeframe.
        candles_df (pd.DataFrame): DataFrame candle historis (Diharapkan memiliki kolom '_price' awalnya).
        current_atr_value (Decimal): Nilai ATR saat ini untuk timeframe ini.
    Returns:
        int: Jumlah FVG yang statusnya berhasil diperbarui dan dikirim ke antrean DB.
    """
    logger.info(f"Memperbarui status Fair Value Gaps yang sudah ada untuk {symbol_param} {timeframe_str}...")
    
    processed_count = 0 

    # 1. Validasi Input Awal
    if candles_df.empty:
        logger.debug(f"DataFrame candle kosong untuk update status FVG di {timeframe_str}.")
        return 0 

    # --- MULAI PENANGANAN DATAFRAME UTAMA DI SINI ---
    # Buat salinan DataFrame untuk menghindari modifikasi input asli
    df_processed = candles_df.copy()

    # Rename kolom harga dan volume dari '_price' ke nama pendek
    df_processed = df_processed.rename(columns={
        'open_price': 'open',
        'high_price': 'high',
        'low_price': 'low',
        'close_price': 'close',
        'tick_volume': 'volume',
        'real_volume': 'real_volume',
        'spread': 'spread'
    })

    # Konversi kolom-kolom numerik ke float secara aman
    # Ini penting karena data bisa datang sebagai Decimal atau string dari database.
    numeric_cols_to_convert = ['open', 'high', 'low', 'close', 'volume', 'real_volume', 'spread']
    for col in numeric_cols_to_convert:
        if col in df_processed.columns:
            df_processed[col] = df_processed[col].apply(utils.to_float_or_none)
    
    # Pembersihan NaN pada kolom OHLC kritis setelah konversi
    initial_len_processed = len(df_processed)
    df_processed.dropna(subset=['open', 'high', 'low', 'close'], inplace=True)
    if len(df_processed) < initial_len_processed:
        logger.warning(f"FVG_STATUS_DEBUG: Dihapus {initial_len_processed - len(df_processed)} baris dengan NaN di kolom OHLC setelah renaming dan konversi float.")

    if df_processed.empty:
        logger.warning(f"FVG_STATUS_DEBUG: DataFrame kosong setelah pembersihan NaN. Melewatkan update status FVG.")
        return 0
    # --- AKHIR PENANGANAN DATAFRAME UTAMA DI SINI ---


    # 2. Inisialisasi _SYMBOL_POINT_VALUE (jika belum)
    if globals().get('_SYMBOL_POINT_VALUE') is None:
        globals().get('_initialize_symbol_point_value')()
    if globals().get('_SYMBOL_POINT_VALUE') is None:
        logger.error(f"Nilai POINT untuk {symbol_param} tidak tersedia. Melewatkan update status FVG.")
        return 0 

    # 3. Ambil Batas Harga Dinamis Global (tidak perlu modifikasi di sini)
    with globals().get('_dynamic_price_lock', threading.Lock()):
        min_price_for_update_filter = globals().get('_dynamic_price_min_for_analysis')
        max_price_for_update_filter = globals().get('_dynamic_price_max_for_analysis')

    # 4. Mendapatkan FVG Aktif dari Database
    active_fvgs = database_manager.get_fair_value_gaps(
        symbol=symbol_param,
        timeframe=timeframe_str,
        is_filled=False, # Hanya ambil yang belum terisi
        limit=None, # Ambil semua FVG aktif
        min_price_level=min_price_for_update_filter, # Filter berdasarkan rentang harga
        max_price_level=max_price_for_update_filter
    )

    if not active_fvgs:
        logger.debug(f"Tidak ada FVG aktif untuk diperbarui statusnya di {symbol_param} {timeframe_str}.")
        return 0 

    # 5. Menentukan Toleransi Pengisian/Retest
    if current_atr_value is not None and current_atr_value > Decimal('0.0'):
        tolerance_value_dec = current_atr_value * config.MarketData.ATR_MULTIPLIER_FOR_TOLERANCE
        logger.debug(f"FVG Status Updater: Menggunakan toleransi dinamis ATR: {float(tolerance_value_dec):.5f} (dari ATR {float(current_atr_value):.5f})")
    else:
        tolerance_value_dec = config.RuleBasedStrategy.RULE_SR_TOLERANCE_POINTS * globals().get('_SYMBOL_POINT_VALUE')
        logger.warning(f"FVG Status Updater: ATR tidak valid. Menggunakan toleransi statis: {float(tolerance_value_dec):.5f}")
    
    updated_fvgs_batch = []

    # 6. Menentukan Waktu Mulai Pencarian Lilin yang Relevan (Optimasi)
    # --- MULAI MODIFIKASI UNTUK PENANGANAN NoneType PADA latest_active_fvg_time ---
    list_for_max_fvg = []
    for fvg_item in active_fvgs:
        # Prioritaskan last_retest_time_utc, lalu formation_time_utc
        effective_timestamp = fvg_item.get('last_retest_time_utc')
        if effective_timestamp is None:
            effective_timestamp = fvg_item.get('formation_time_utc')
        
        # Pastikan effective_timestamp adalah objek datetime yang valid.
        processed_timestamp = utils.to_utc_datetime_or_none(effective_timestamp)
        
        if processed_timestamp is not None:
            list_for_max_fvg.append(processed_timestamp)
        else:
            logger.warning(f"FVG Status Updater: Timestamp untuk FVG ID {fvg_item.get('id', 'N/A')} resolve ke None setelah konversi. Dilewati untuk perhitungan max.")

    if list_for_max_fvg:
        latest_active_fvg_time = max(list_for_max_fvg)
    else:
        # Jika daftar kosong (semua timestamp tidak valid atau active_fvgs kosong), gunakan waktu fallback yang aman
        latest_active_fvg_time = datetime(1970, 1, 1, tzinfo=timezone.utc)
        logger.warning(f"FVG Status Updater: Tidak ada timestamp valid untuk menghitung max. Menggunakan default {latest_active_fvg_time.isoformat()}.")
    # --- AKHIR MODIFIKASI UNTUK PENANGANAN NoneType PADA latest_active_fvg_time ---

    # relevant_candles_for_test sekarang diambil dari df_processed yang sudah siap
    relevant_candles_for_test = df_processed[
        df_processed.index > latest_active_fvg_time
    ].copy()

    if relevant_candles_for_test.empty and active_fvgs: 
        logger.debug(f"Tidak ada lilin baru setelah FVG aktif terbaru untuk {symbol_param} {timeframe_str}. Melewatkan update status FVG.")
        return 0 

    # 7. Iterasi Melalui FVG Aktif dan Lilin yang Relevan
    for fvg in active_fvgs:
        fvg_id = fvg['id']
        fvg_top_price = fvg['fvg_top_price'] # Decimal
        fvg_bottom_price = fvg['fvg_bottom_price'] # Decimal
        fvg_type = fvg['type'] 
        
        is_still_active = True # Asumsi FVG masih aktif
        current_retest_count = fvg['retest_count'] # Ambil retest_count dari DB
        
        # Inisialisasi last_retest_time_for_update untuk FVG saat ini
        last_retest_time_for_update = fvg.get('last_retest_time_utc')
        if last_retest_time_for_update is None:
            last_retest_time_for_update = fvg.get('formation_time_utc', datetime(1970, 1, 1, tzinfo=timezone.utc))
            if last_retest_time_for_update is None:
                last_retest_time_for_update = datetime(1970, 1, 1, tzinfo=timezone.utc)
                logger.warning(f"FVG_STATUS_DEBUG: FVG {fvg_id} tidak memiliki formation_time_utc atau last_retest_time_utc. Menggunakan default 1970-01-01.")

        fvg_relevant_candles = relevant_candles_for_test[
            relevant_candles_for_test.index > last_retest_time_for_update 
        ].copy()

        if fvg_relevant_candles.empty:
            logger.debug(f"FVG_STATUS_DEBUG: FVG {fvg_type} (ID: {fvg_id}) dilewati karena tidak ada lilin baru setelah last_retest_time_utc ({last_retest_time_for_update}).")
            continue


        for _, candle_row in fvg_relevant_candles.iterrows():
            if not is_still_active: 
                break

            # --- MULAI PENAMBAHAN KODE UNTUK NAMA KOLOM high/low/close DALAM LOOP ---
            # Karena df_processed (dan turunannya relevant_candles_for_test & fvg_relevant_candles)
            # sudah di-rename di atas, kita bisa langsung mengaksesnya dengan nama pendek.
            candle_high = candle_row['high'] 
            candle_low = candle_row['low'] 
            candle_close = candle_row['close'] 
            
            # Ini adalah validasi akhir untuk memastikan tidak ada None/NaN yang lolos
            if candle_high is None or candle_low is None or candle_close is None:
                logger.debug(f"FVG_STATUS_DEBUG: Lilin dengan NaN high/low/close setelah konversi untuk FVG {fvg_id} di {candle_row.name}. Melewatkan pengecekan.")
                continue # Lewati lilin ini jika ada NaN setelah konversi
            # --- AKHIR PENAMBAHAN KODE UNTUK NAMA KOLOM high/low/close DALAM LOOP ---

            candle_time_dt = candle_row.name.to_pydatetime()

            is_mitigated_now = False
            
            fvg_top_float = float(fvg_top_price)
            fvg_bottom_float = float(fvg_bottom_price)
            tolerance_float = float(tolerance_value_dec)

            # Logika pengisian (fill): Harga menembus FVG sepenuhnya
            if fvg_type == "Bullish Imbalance": # Bullish FVG: low menembus di bawah fvg_bottom
                if candle_low < fvg_bottom_float - tolerance_float:
                    is_mitigated_now = True
                    logger.debug(f"FVG_STATUS_DEBUG: FVG Bullish (ID: {fvg_id}, Bottom: {fvg_bottom_price}) menjadi terisi.")
            elif fvg_type == "Bearish Imbalance": # Bearish FVG: high menembus di atas fvg_top
                if candle_high > fvg_top_float + tolerance_float:
                    is_mitigated_now = True
                    logger.debug(f"FVG_STATUS_DEBUG: FVG Bearish (ID: {fvg_id}, Top: {fvg_top_price}) menjadi terisi.")
            
            if is_mitigated_now:
                is_still_active = False
                last_fill_time_for_update = candle_time_dt # Update waktu fill
                logger.debug(f"FVG_STATUS_DEBUG: FVG {fvg_id} filled. last_fill_time updated to {last_fill_time_for_update.isoformat()}.")
                break # FVG terisi, tidak perlu cek candle selanjutnya untuk FVG ini

            # Logika retest: Harga menyentuh FVG tetapi tidak menembus
            if (is_still_active and # Hanya retest jika FVG masih aktif
                candle_high >= fvg_bottom_float - tolerance_float and # High lilin di atas batas bawah FVG (dengan toleransi)
                candle_low <= fvg_top_float + tolerance_float): # Low lilin di bawah batas atas FVG (dengan toleransi)
                
                # Tambahkan kondisi cooldown untuk retest_count
                # Hanya bertambah jika ada interval waktu yang cukup sejak retest terakhir
                if (last_retest_time_for_update is None) or \
                   ((candle_time_dt - last_retest_time_for_update).total_seconds() > config.System.RETRY_DELAY_SECONDS * 5): # Misalnya, 5 detik cooldown
                    current_retest_count += 1
                    last_retest_time_for_update = candle_time_dt # Update waktu retest terakhir
                    logger.debug(f"FVG_STATUS_DEBUG: FVG {fvg_type} (ID: {fvg_id}) retest terjadi. Retest count: {current_retest_count}.")
        
        logger.debug(f"FVG_STATUS_DEBUG: FVG {fvg_id} final status: is_filled={not is_still_active}, last_fill_time={last_fill_time_for_update.isoformat() if 'last_fill_time_for_update' in locals() and last_fill_time_for_update else 'None'}, retest_count={current_retest_count}, last_retest_time={last_retest_time_for_update.isoformat() if last_retest_time_for_update else 'None'}")
        
        if (fvg['is_filled'] != (not is_still_active)) or \
           (utils.to_utc_datetime_or_none(fvg.get('last_fill_time_utc')) != (last_fill_time_for_update if 'last_fill_time_for_update' in locals() else None)) or \
           (fvg['retest_count'] != current_retest_count) or \
           (utils.to_utc_datetime_or_none(fvg.get('last_retest_time_utc')) != last_retest_time_for_update):
            
            updated_fvgs_batch.append({
                'id': fvg_id,
                'is_filled': not is_still_active, 
                'last_fill_time_utc': (last_fill_time_for_update if 'last_fill_time_for_update' in locals() else None), # Set None jika tidak pernah diisi
                'retest_count': current_retest_count,
                'last_retest_time_utc': last_retest_time_for_update,
                'symbol': fvg['symbol'],
                'timeframe': fvg['timeframe'],
                'type': fvg['type'],
                'fvg_top_price': fvg_top_price,
                'fvg_bottom_price': fvg_bottom_price,
                'formation_time_utc': fvg['formation_time_utc']
            })
            processed_count += 1
            logger.debug(f"FVG_STATUS_DEBUG: FVG {fvg_id} ditambahkan ke batch update. Processed count: {processed_count}")

    # 8. Menyimpan Batch Update ke Database
    if updated_fvgs_batch:
        # Konversi tipe data Decimal dan datetime ke float dan string ISO untuk batch update
        processed_updated_fvgs_batch = []
        for fvg_data_dict in updated_fvgs_batch:
            processed_item_dict = fvg_data_dict.copy()
            for key, value in processed_item_dict.items():
                if isinstance(value, Decimal):
                    processed_item_dict[key] = utils.to_float_or_none(value)
                elif isinstance(value, datetime):
                    processed_item_dict[key] = utils.to_iso_format_or_none(value)
            processed_updated_fvgs_batch.append(processed_item_dict)

        try:
            database_manager.save_fair_value_gaps_batch([], processed_updated_fvgs_batch)
            logger.info(f"BACKFILL WORKER: Berhasil mem-batch update {len(processed_updated_fvgs_batch)} status FVG untuk {symbol_param} {timeframe_str}.")
        except Exception as e:
            logger.error(f"BACKFILL WORKER: Gagal mem-batch update status FVG: {e}", exc_info=True)

    logger.info(f"Selesai memperbarui status Fair Value Gaps untuk {symbol_param} {timeframe_str}. {processed_count} diperbarui.")
    return processed_count


def _detect_new_liquidity_zones_historically(symbol_param: str, timeframe_str: str, candles_df: pd.DataFrame, current_atr_value: Decimal):
    """
    Mendeteksi Liquidity Zones (Equal Highs/Lows) baru dari seluruh dataset historis.
    Liquidity Zones adalah area harga di mana volume perdagangan tinggi atau ada akumulasi order,
    seringkali diidentifikasi di dekat harga tertinggi atau terendah yang berulang.
    Menyimpan Liquidity Zones baru ke database secara batch dengan deduplikasi.
    Args:
        symbol_param (str): Simbol trading (misal: "XAUUSD").
        timeframe_str (str): Timeframe (misal: "M5", "H1", "D1").
        candles_df (pd.DataFrame): DataFrame candle historis. Diharapkan memiliki kolom '_price' (e.g., 'high_price').
                                   Data ini diasumsikan berasal dari DB dan mungkin masih dalam Decimal.
        current_atr_value (Decimal): Nilai ATR saat ini untuk timeframe ini, digunakan untuk toleransi dinamis.
    Returns:
        int: Jumlah Liquidity Zones baru yang berhasil dideteksi dan dikirim ke antrean DB.
    """
    logger.info(f"Mendeteksi Liquidity Zones BARU historis untuk {symbol_param} {timeframe_str}...")
    processed_count = 0 

    # Ambil toleransi level yang sama dari konfigurasi
    equal_level_tolerance_points_cfg = config.RuleBasedStrategy.RULE_EQUAL_LEVEL_TOLERANCE_POINTS
    # Sesuaikan toleransi berdasarkan nilai point simbol dan ATR (jika valid)
    tolerance_liq_value = Decimal(str(equal_level_tolerance_points_cfg)) * globals().get('_SYMBOL_POINT_VALUE')
    if current_atr_value is not None and current_atr_value > Decimal('0.0'):
        tolerance_liq_value = current_atr_value * config.MarketData.ATR_MULTIPLIER_FOR_TOLERANCE
        logger.debug(f"Liquidity Detektor: Menggunakan tolerance_liq_value dinamis ATR: {float(tolerance_liq_value):.5f} (dari ATR {float(current_atr_value):.5f})")
    else:
        logger.warning(f"Liquidity Detektor: ATR tidak valid. Menggunakan tolerance_liq_value statis: {float(tolerance_liq_value):.5f}.")
    
    liq_check_window_size = 50 # Ukuran jendela candle untuk mencari Equal Highs/Lows

    # Pastikan ada cukup lilin untuk deteksi Liquidity Zones
    if candles_df.empty or len(candles_df) < liq_check_window_size + 1:
        logger.debug(f"Tidak cukup lilin ({len(candles_df)}) untuk deteksi Liquidity Zones historis di {timeframe_str}.")
        return 0

    # Inisialisasi set untuk deduplikasi FVG yang sudah ada di database
    existing_liq_keys = set()
    try:
        # Ambil Liquidity Zones yang sudah ada dari DB yang belum "tapped"
        existing_liqs_db = database_manager.get_liquidity_zones(
            symbol=symbol_param,
            timeframe=timeframe_str,
            is_tapped=False, # Hanya yang belum "tapped" yang dianggap aktif dan potensial duplikat
            start_time_utc=candles_df.index.min().to_pydatetime(), # Batasi pencarian pada rentang data yang diproses
            end_time_utc=candles_df.index.max().to_pydatetime() + timedelta(days=1)
        )
        for liq in existing_liqs_db:
            liq_formation_time_dt = utils.to_utc_datetime_or_none(liq.get('formation_time_utc'))
            if liq_formation_time_dt:
                # Kunci deduplikasi mencakup harga yang dibulatkan untuk menangani sedikit perbedaan float
                rounded_price_for_key = round(float(utils.to_decimal_or_none(liq['price_level'])), 5)
                existing_liq_keys.add((liq['symbol'], liq['timeframe'], liq['zone_type'], rounded_price_for_key, liq_formation_time_dt.isoformat()))
        logger.debug(f"Ditemukan {len(existing_liq_keys)} Liquidity Zones aktif yang sudah ada di DB untuk {timeframe_str} (untuk pre-filtering).")
    except Exception as e:
        logger.error(f"Gagal mengambil existing Liquidity Zones dari DB untuk {timeframe_str} (untuk pre-filtering): {e}", exc_info=True)
        pass # Lanjutkan tanpa pre-filter yang sempurna jika ini gagal

    # List untuk mengumpulkan Liquidity Zones baru yang akan disimpan secara batch
    detected_liqs_to_save_in_batch = []
    # Ambang batas untuk penyimpanan batch ke database
    BATCH_SAVE_LIQ_THRESHOLD = config.System.DATABASE_BATCH_SIZE # Menggunakan ukuran batch dari konfigurasi

    try:
        # PERBAIKAN PENTING DI SINI:
        # Lakukan renaming kolom-kolom harga dari '_price' ke nama pendek pada DataFrame lokal.
        # Ini memastikan bahwa semua akses kolom di dalam fungsi ini menggunakan nama yang konsisten.
        candles_df_processed = candles_df.copy()
        candles_df_processed = candles_df_processed.rename(columns={
            'open_price': 'open',
            'high_price': 'high',
            'low_price': 'low',
            'close_price': 'close',
            'tick_volume': 'volume', # Rename tick_volume to volume
            'real_volume': 'real_volume',
            'spread': 'spread'
        })
        
        # Konversi kolom-kolom yang sudah di-rename ke Decimal dan bersihkan NaN
        # Pastikan ini sesuai dengan tipe data yang diharapkan fungsi ini.
        # Data Liquiditas seringkali menggunakan Decimal untuk presisi harga.
        for col in ['open', 'high', 'low', 'close', 'volume', 'real_volume', 'spread']:
            if col in candles_df_processed.columns:
                candles_df_processed[col] = candles_df_processed[col].apply(utils.to_decimal_or_none)
                # Hapus baris dengan NaN di kolom harga kunci setelah konversi
                if col in ['open', 'high', 'low', 'close']:
                    initial_len_processed = len(candles_df_processed)
                    candles_df_processed.dropna(subset=[col], inplace=True)
                    if len(candles_df_processed) < initial_len_processed:
                        logger.warning(f"Liquidity Detektor: Dihapus {initial_len_processed - len(candles_df_processed)} baris dengan {col} NaN di {timeframe_str} (setelah konversi Decimal).")
        
        # Validasi jika DataFrame kosong setelah pembersihan NaN
        if candles_df_processed.empty or len(candles_df_processed) < liq_check_window_size + 1:
            logger.warning(f"Liquidity Detektor: DataFrame kosong atau tidak cukup lilin ({len(candles_df_processed)}) setelah pembersihan NaN untuk {symbol_param} {timeframe_str}. Melewatkan deteksi Liquidity Zones.")
            return 0


        # Iterasi melalui DataFrame yang sudah diproses dalam jendela geser untuk mendeteksi Liquidity Zones
        for i in range(liq_check_window_size, len(candles_df_processed)):
            current_window_df = candles_df_processed.iloc[i - liq_check_window_size : i] # Ambil jendela data yang sudah di-rename

            # Deteksi Equal Highs (likuiditas di atas harga)
            # Carikan harga 'high' yang berulang dalam jendela tertentu
            unique_highs_in_window = sorted(list(current_window_df['high'].drop_duplicates()))
            for j in range(len(unique_highs_in_window)):
                level1 = unique_highs_in_window[j]
                count_near_level = 0
                highs_near_level = []

                # Hitung berapa banyak 'high' yang berada dalam toleransi di sekitar level1
                for high_val in current_window_df['high']:
                    if abs(high_val - level1) <= tolerance_liq_value:
                        count_near_level += 1
                        highs_near_level.append(high_val)

                # Jika ada 2 atau lebih 'high' yang berdekatan, anggap itu Liquidity Zone
                if count_near_level >= 2: 
                    avg_liq_price = sum(highs_near_level) / Decimal(str(len(highs_near_level))) # Rata-rata harga level likuiditas
                    formation_time_liq = current_window_df.index[-1].to_pydatetime() # Waktu lilin terakhir di jendela

                    # Buat kunci unik untuk FVG ini untuk deduplikasi
                    new_liq_data = {
                        "symbol": symbol_param, "timeframe": timeframe_str, "zone_type": "Equal Highs",
                        "price_level": avg_liq_price, "formation_time_utc": formation_time_liq,
                        "is_tapped": False, "tap_time_utc": None, # Awalnya belum disapu
                        "strength_score": count_near_level, # Kekuatan berdasarkan jumlah sentuhan
                        "confluence_score": 0 # Akan diupdate oleh fungsi lain
                    }
                    # Kunci deduplikasi mencakup harga yang dibulatkan untuk presisi perbandingan
                    new_liq_key = (new_liq_data['symbol'], new_liq_data['timeframe'], new_liq_data['zone_type'], round(float(new_liq_data['price_level']), 5), new_liq_data['formation_time_utc'].isoformat())

                    # Cek apakah Liquidity Zone ini sudah ada di database atau sudah terdeteksi di batch saat ini
                    if new_liq_key not in existing_liq_keys:
                        detected_liqs_to_save_in_batch.append(new_liq_data)
                        existing_liq_keys.add(new_liq_key) # Tambahkan ke set untuk deduplikasi di iterasi selanjutnya
                        logger.debug(f"Liquidity Zone (Equal Highs) terdeteksi di {timeframe_str} pada {formation_time_liq} (Level: {float(avg_liq_price):.5f}, Sentuhan: {count_near_level}).")
            
            # Deteksi Equal Lows (likuiditas di bawah harga)
            # Carikan harga 'low' yang berulang dalam jendela tertentu
            unique_lows_in_window = sorted(list(current_window_df['low'].drop_duplicates()))
            for j in range(len(unique_lows_in_window)):
                level1 = unique_lows_in_window[j]
                count_near_level = 0
                lows_near_level = []

                # Hitung berapa banyak 'low' yang berada dalam toleransi di sekitar level1
                for low_val in current_window_df['low']:
                    if abs(low_val - level1) <= tolerance_liq_value:
                        count_near_level += 1
                        lows_near_level.append(low_val)

                # Jika ada 2 atau lebih 'low' yang berdekatan, anggap itu Liquidity Zone
                if count_near_level >= 2: 
                    avg_liq_price = sum(lows_near_level) / Decimal(str(len(lows_near_level))) # Rata-rata harga level likuiditas
                    formation_time_liq = current_window_df.index[-1].to_pydatetime() # Waktu lilin terakhir di jendela

                    # Buat kunci unik untuk Liquidity Zone ini untuk deduplikasi
                    new_liq_data = {
                        "symbol": symbol_param, "timeframe": timeframe_str, "zone_type": "Equal Lows",
                        "price_level": avg_liq_price, "formation_time_utc": formation_time_liq,
                        "is_tapped": False, "tap_time_utc": None, # Awalnya belum disapu
                        "strength_score": count_near_level, # Kekuatan berdasarkan jumlah sentuhan
                        "confluence_score": 0 # Akan diupdate oleh fungsi lain
                    }
                    # Kunci deduplikasi mencakup harga yang dibulatkan untuk presisi perbandingan
                    new_liq_key = (new_liq_data['symbol'], new_liq_data['timeframe'], new_liq_data['zone_type'], round(float(new_liq_data['price_level']), 5), new_liq_data['formation_time_utc'].isoformat())

                    # Cek apakah Liquidity Zone ini sudah ada di database atau sudah terdeteksi di batch saat ini
                    if new_liq_key not in existing_liq_keys:
                        detected_liqs_to_save_in_batch.append(new_liq_data)
                        existing_liq_keys.add(new_liq_key) # Tambahkan ke set untuk deduplikasi di iterasi selanjutnya
                        logger.debug(f"Liquidity Zone (Equal Lows) terdeteksi di {timeframe_str} pada {formation_time_liq} (Level: {float(avg_liq_price):.5f}, Sentuhan: {count_near_level}).")
            
            # === Lakukan penyimpanan batch jika ambang batas tercapai ===
            if len(detected_liqs_to_save_in_batch) >= BATCH_SAVE_LIQ_THRESHOLD:
                try:
                    database_manager.save_liquidity_zones_batch(detected_liqs_to_save_in_batch, [])
                    processed_count += len(detected_liqs_to_save_in_batch)
                    logger.info(f"BACKFILL WORKER: Auto-saved {len(detected_liqs_to_save_in_batch)} new Liq Zones for {timeframe_str}.")
                    detected_liqs_to_save_in_batch = [] 
                except Exception as e:
                    logger.error(f"BACKFILL WORKER: Gagal auto-save Liq Zones batch: {e}", exc_info=True)
                    detected_liqs_to_save_in_batch = [] 
        
    except Exception as e: 
        logger.error(f"Error saat mendeteksi Liquidity Zones BARU historis untuk {symbol_param} {timeframe_str}: {e}", exc_info=True)
        return processed_count 
    finally:
        # === Simpan sisa data setelah loop selesai ===
        if detected_liqs_to_save_in_batch:
            try:
                database_manager.save_liquidity_zones_batch(detected_liqs_to_save_in_batch, [])
                processed_count += len(detected_liqs_to_save_in_batch)
                logger.info(f"BACKFILL WORKER: Final auto-saved {len(detected_liqs_to_save_in_batch)} remaining new Liq Zones for {timeframe_str}.")
            except Exception as e:
                logger.error(f"BACKFILL WORKER: Gagal final auto-save Liq Zones batch: {e}", exc_info=True)
    
    logger.info(f"Selesai mendeteksi Liquidity Zones BARU unik historis untuk {symbol_param} {timeframe_str}. Deteksi: {processed_count}.")
    return processed_count

def _detect_new_supply_demand_zones_historically(symbol_param: str, timeframe_str: str, candles_df: pd.DataFrame, current_atr_value: Decimal) -> int:
    logger.info(f"Mendeteksi zona Supply/Demand BARU historis untuk {symbol_param} {timeframe_str}...")
    processed_count = 0

    if globals().get('_SYMBOL_POINT_VALUE') is None:
        globals().get('_initialize_symbol_point_value')()
    if globals().get('_SYMBOL_POINT_VALUE') is None:
        logger.error(f"Nilai POINT untuk {symbol_param} tidak tersedia. Melewatkan deteksi zona Supply/Demand.")
        return 0

    min_base_candles = getattr(config.RuleBasedStrategy, 'min_base_candles', 2)
    max_base_candles = getattr(config.RuleBasedStrategy, 'max_base_candles', 5)
    base_candle_body_ratio = getattr(config.RuleBasedStrategy, 'CANDLE_BODY_MIN_RATIO', Decimal('0.3'))
    
    min_impulse_move_value = Decimal('0.0')
    if current_atr_value is not None and current_atr_value > Decimal('0.0'):
        min_impulse_move_value = current_atr_value * config.RuleBasedStrategy.SD_MIN_IMPULSIVE_MOVE_ATR_MULTIPLIER
        logger.debug(f"S&D Detektor: Menggunakan min_impulse_move_value dinamis ATR: {float(min_impulse_move_value):.5f} (dari ATR {float(current_atr_value):.5f})")
    else:
        min_impulse_move_value = Decimal('50') * globals().get('_SYMBOL_POINT_VALUE')
        logger.warning(f"S&D Detektor: ATR tidak valid. Menggunakan min_impulse_move_value statis: {float(min_impulse_move_value):.5f}")

    df_processed = candles_df.copy()
    
    df_processed = df_processed.rename(columns={
        'open_price': 'open', 'high_price': 'high', 'low_price': 'low',
        'close_price': 'close', 'tick_volume': 'volume', 'real_volume': 'real_volume', 'spread': 'spread'
    })

    for col in ['open', 'high', 'low', 'close', 'volume', 'real_volume', 'spread']:
        if col in df_processed.columns:
            df_processed[col] = df_processed[col].apply(utils.to_float_or_none)
            if col in ['open', 'high', 'low', 'close']:
                initial_len_df = len(df_processed)
                df_processed.dropna(subset=[col], inplace=True)
                if len(df_processed) < initial_len_df:
                    logger.warning(f"S&D Detektor: Dihapus {initial_len_df - len(df_processed)} baris dengan {col} NaN di {timeframe_str} (setelah konversi float).")

    MIN_CANDLES_FOR_SD_PATTERN = max_base_candles + 2

    if df_processed.empty or len(df_processed) < MIN_CANDLES_FOR_SD_PATTERN:
        logger.debug(f"Tidak cukup lilin ({len(df_processed)}) untuk deteksi zona Supply/Demand historis di {timeframe_str}. Diperlukan minimal {MIN_CANDLES_FOR_SD_PATTERN} lilin.")
        return 0

    swing_results_df = globals().get('_calculate_swing_highs_lows_internal')(df_processed, config.AIAnalysts.SWING_EXT_BARS)
    
    if swing_results_df.empty or swing_results_df['HighLow'].isnull().all():
        logger.debug(f"Tidak ada swing points yang valid untuk {symbol_param} {timeframe_str}. Melewatkan deteksi zona S&D.")
        return 0

    existing_sd_keys = set()
    try:
        start_time_for_dedup = df_processed.index.min().to_pydatetime() - timedelta(days=30) 
        existing_sds_db = database_manager.get_supply_demand_zones(
            symbol=symbol_param,
            timeframe=timeframe_str,
            start_time_utc=start_time_for_dedup
        )
        for sd in existing_sds_db:
            formation_time_dt = utils.to_utc_datetime_or_none(sd['formation_time_utc'])
            if formation_time_dt:
                existing_sd_keys.add((
                    sd['symbol'], sd['timeframe'], sd['zone_type'],
                    formation_time_dt.replace(microsecond=0).isoformat()
                ))
        logger.debug(f"Ditemukan {len(existing_sd_keys)} zona S&D yang sudah ada di DB untuk {timeframe_str}.")
    except Exception as e:
        logger.error(f"Gagal mengambil existing S&D Zones dari DB untuk pre-filtering: {e}", exc_info=True)
        pass 

    detected_sd_zones_to_save_batch = []
    
    for i in range(len(df_processed) - 1, max_base_candles, -1):
        for base_len in range(min_base_candles, max_base_candles + 1):
            base_start_idx = i - base_len
            
            if base_start_idx <= 0:
                continue

            base_candles_segment = df_processed.iloc[base_start_idx:i]
            
            is_valid_base = True
            for _, b_candle in base_candles_segment.iterrows():
                candle_range = b_candle['high'] - b_candle['low']
                candle_body = abs(b_candle['close'] - b_candle['open'])
                
                if candle_range == 0:
                    if candle_body > float(Decimal('0.00001')):
                        is_valid_base = False
                        break
                elif (candle_body / candle_range) > float(base_candle_body_ratio):
                    is_valid_base = False
                    break
            
            if not is_valid_base:
                continue

            prev_impulse_candle = df_processed.iloc[base_start_idx - 1]
            current_impulse_candle = df_processed.iloc[i]               

            is_rally_before_base = (prev_impulse_candle['close'] - prev_impulse_candle['open']) > float(min_impulse_move_value)
            is_drop_before_base = (prev_impulse_candle['open'] - prev_impulse_candle['close']) > float(min_impulse_move_value)
            is_rally_after_base = (current_impulse_candle['close'] - current_impulse_candle['open']) > float(min_impulse_move_value)
            is_drop_after_base = (current_impulse_candle['open'] - current_impulse_candle['close']) > float(min_impulse_move_value)


            if is_valid_base and is_rally_after_base:
                zone_type_sd = "Demand"
                zone_top_price_sd_float = base_candles_segment['high'].max()
                zone_bottom_price_sd_float = base_candles_segment['low'].min()
                formation_time_sd_dt = base_candles_segment.index[0].to_pydatetime()

                zone_top_price_sd_dec = utils.to_decimal_or_none(zone_top_price_sd_float)
                zone_bottom_price_sd_dec = utils.to_decimal_or_none(zone_bottom_price_sd_float)
                
                if zone_top_price_sd_dec is None or zone_bottom_price_sd_dec is None:
                    continue

                if is_drop_before_base:
                    base_type_sd = "DropBaseRally"
                    new_key = (symbol_param, timeframe_str, zone_type_sd, formation_time_sd_dt.replace(microsecond=0).isoformat())
                    if new_key not in existing_sd_keys:
                        detected_sd_zones_to_save_batch.append({
                            "symbol": symbol_param, "timeframe": timeframe_str, "zone_type": zone_type_sd, "base_type": base_type_sd,
                            "zone_top_price": zone_top_price_sd_dec, "zone_bottom_price": zone_bottom_price_sd_dec,
                            "formation_time_utc": formation_time_sd_dt, "is_mitigated": False
                        })
                        existing_sd_keys.add(new_key)
                        processed_count += 1
                        logger.info(f"Demand Zone (DBR) terdeteksi: {float(zone_bottom_price_sd_dec):.5f}-{float(zone_top_price_sd_dec):.5f} di {timeframe_str} pada {formation_time_sd_dt}.")
                elif is_rally_before_base:
                    base_type_sd = "RallyBaseRally"
                    new_key = (symbol_param, timeframe_str, zone_type_sd, formation_time_sd_dt.replace(microsecond=0).isoformat())
                    if new_key not in existing_sd_keys:
                        detected_sd_zones_to_save_batch.append({
                            "symbol": symbol_param, "timeframe": timeframe_str, "zone_type": zone_type_sd, "base_type": base_type_sd,
                            "zone_top_price": zone_top_price_sd_dec, "zone_bottom_price": zone_bottom_price_sd_dec,
                            "formation_time_utc": formation_time_sd_dt, "is_mitigated": False
                        })
                        existing_sd_keys.add(new_key)
                        processed_count += 1
                        logger.info(f"Demand Zone (RBR) terdeteksi: {float(zone_bottom_price_sd_dec):.5f}-{float(zone_top_price_sd_dec):.5f} di {timeframe_str} pada {formation_time_sd_dt}.")

            elif is_valid_base and is_drop_after_base:
                zone_type_sd = "Supply"
                zone_top_price_sd_float = base_candles_segment['high'].max()
                zone_bottom_price_sd_float = base_candles_segment['low'].min()
                formation_time_sd_dt = base_candles_segment.index[0].to_pydatetime()

                zone_top_price_sd_dec = utils.to_decimal_or_none(zone_top_price_sd_float)
                zone_bottom_price_sd_dec = utils.to_decimal_or_none(zone_bottom_price_sd_float)

                if zone_top_price_sd_dec is None or zone_bottom_price_sd_dec is None:
                    continue

                if is_rally_before_base:
                    base_type_sd = "RallyBaseDrop"
                    new_key = (symbol_param, timeframe_str, zone_type_sd, formation_time_sd_dt.replace(microsecond=0).isoformat())
                    if new_key not in existing_sd_keys:
                        detected_sd_zones_to_save_batch.append({
                            "symbol": symbol_param, "timeframe": timeframe_str, "zone_type": zone_type_sd, "base_type": base_type_sd,
                            "zone_top_price": zone_top_price_sd_dec, "zone_bottom_price": zone_bottom_price_sd_dec,
                            "formation_time_utc": formation_time_sd_dt, "is_mitigated": False
                        })
                        existing_sd_keys.add(new_key)
                        processed_count += 1
                        logger.info(f"Supply Zone (RBD) terdeteksi: {float(zone_bottom_price_sd_dec):.5f}-{float(zone_top_price_sd_dec):.5f} di {timeframe_str} pada {formation_time_sd_dt}.")
                elif is_drop_before_base:
                    base_type_sd = "DropBaseDrop"
                    new_key = (symbol_param, timeframe_str, zone_type_sd, formation_time_sd_dt.replace(microsecond=0).isoformat())
                    if new_key not in existing_sd_keys:
                        detected_sd_zones_to_save_batch.append({
                            "symbol": symbol_param, "timeframe": timeframe_str, "zone_type": zone_type_sd, "base_type": base_type_sd,
                            "zone_top_price": zone_top_price_sd_dec, "zone_bottom_price": zone_bottom_price_sd_dec,
                            "formation_time_utc": formation_time_sd_dt, "is_mitigated": False
                        })
                        existing_sd_keys.add(new_key)
                        processed_count += 1
                        logger.info(f"Supply Zone (DBD) terdeteksi: {float(zone_bottom_price_sd_dec):.5f}-{float(zone_top_price_sd_dec):.5f} di {timeframe_str} pada {formation_time_sd_dt}.")

    if detected_sd_zones_to_save_batch:
        try:
            database_manager.save_supply_demand_zones_batch(detected_sd_zones_to_save_batch, [])
            logger.info(f"BACKFILL WORKER: Berhasil menyimpan {len(detected_sd_zones_to_save_batch)} zona Supply/Demand baru unik untuk {symbol_param} {timeframe_str}.")
        except Exception as e:
            logger.error(f"BACKFILL WORKER: Gagal menyimpan zona Supply/Demand batch: {e}", exc_info=True)

    logger.info(f"Selesai mendeteksi zona Supply/Demand BARU historis untuk {symbol_param} {timeframe_str}. Deteksi: {processed_count}.")
    return processed_count


def _detect_new_fair_value_gaps(symbol_param: str, timeframe_str: str, candles_df: pd.DataFrame, new_fvgs_list: list):
    """
    Mendeteksi Fair Value Gaps (FVG) yang baru terbentuk berdasarkan pola 3-lilin.
    Menambahkan FVG baru ke new_fvgs_list SETELAH MEMERIKSA DUPLIKAT, DENGAN BATCHING INTERNAL.
    """
    logger.info(f"Mendeteksi FVG BARU untuk {symbol_param} {timeframe_str}...")
    
    
    min_fvg_value = config.AIAnalysts.FVG_MIN_DOLLARS

    if current_atr_value is not None and current_atr_value > Decimal('0.0'):
        # Gunakan ATR_MULTIPLIER_FOR_TOLERANCE dari MarketData untuk FVG
        # Ini akan membuat FVG_MIN_DOLLARS menjadi multiple dari ATR
        min_fvg_value = current_atr_value * config.MarketData.ATR_MULTIPLIER_FOR_TOLERANCE 
        logger.debug(f"FVG Detektor: Menggunakan min_fvg_value dinamis ATR: {float(min_fvg_value):.5f} (dari ATR {float(current_atr_value):.5f})")
    else:
        # Fallback ke nilai statis jika ATR tidak tersedia atau nol
        min_fvg_value = config.AIAnalysts.FVG_MIN_DOLLARS
        logger.warning(f"FVG Detektor: ATR tidak valid. Menggunakan min_fvg_value statis: {float(min_fvg_value):.5f}")

    if candles_df.empty or len(candles_df) < 3:
        logger.debug(f"Tidak cukup lilin ({len(candles_df)}) untuk deteksi FVG baru di {timeframe_str}. Diperlukan minimal 3.")
        return

    # --- PENAMBAHAN KODE UNTUK MEMERIKSA DUPLIKAT YANG SUDAH ADA DI DB ---
    existing_fvg_keys = set()
    try: # <--- TAMBAHKAN try-except UNTUK QUERY DATABASE
        existing_fvgs_db = database_manager.get_fair_value_gaps(
            symbol=symbol_param,
            timeframe=timeframe_str,
            start_time_utc=None, # <--- UBAH INI MENJADI NONE UNTUK MENGAMBIL SEMUA YANG ADA
            end_time_utc=candles_df.index.max().to_pydatetime() + timedelta(days=1), # Batasi akhir rentang untuk efisiensi query
            is_filled=False # Hanya FVG yang belum terisi yang relevan sebagai duplikat
        )
        for fvg in existing_fvgs_db:
            # fvg['formation_time_utc'] sekarang sudah datetime object dari database_manager.
            # Jadi, tidak perlu lagi memeriksa dan mengonversi dari string ke datetime di sini.
            # Cukup pastikan itu objek datetime dan konversi ke isoformat() untuk kunci set.
            fvg_formation_time_dt = fvg['formation_time_utc'] # Gunakan objek datetime
            existing_fvg_keys.add((fvg['symbol'], fvg['timeframe'], fvg_formation_time_dt.isoformat(), fvg['type']))
        logger.debug(f"Ditemukan {len(existing_fvg_keys)} FVG yang sudah ada di DB untuk {timeframe_str}.")
    except Exception as e:
        logger.error(f"Gagal mengambil existing FVG dari DB untuk {timeframe_str} (untuk pre-filtering): {e}", exc_info=True)
        pass # Lanjutkan tanpa pre-filter yang sempurna jika ini gagal

    fvg_keys_in_current_chunk = set() # Baris ini sepertinya salah tempat, seharusnya di Liq. Zones, bukan FVG. Biarkan untuk saat ini.

    # --- PERBAIKAN: Implementasi Batching Penyimpanan Internal ---
    BATCH_SAVE_THRESHOLD_FVG = 500 # Simpan setiap 500 FVG baru
    detected_fvgs_to_save_in_batch = []
    
    try: # <--- TAMBAHKAN try-except UNTUK LOGIKA DETEKSI UTAMA
        for i in range(2, len(candles_df)):
            candle_0 = candles_df.iloc[i]
            candle_1 = candles_df.iloc[i - 1]
            candle_2 = candles_df.iloc[i - 2]

            fvg_top_price = None
            fvg_bottom_price = None
            fvg_type = None
            is_fvg_valid = False

            if candle_0['low_price'] > candle_2['high_price']:
                gap_size = candle_0['low_price'] - candle_2['high_price']
                if gap_size > min_fvg_value:
                    fvg_type = "Bullish Imbalance"
                    fvg_top_price = candle_0['low_price']
                    fvg_bottom_price = candle_2['high_price']
                    is_fvg_valid = True
                    # logger.debug(f"FVG Bullish deteksi: {fvg_bottom_price}-{fvg_top_price} di {timeframe_str} pada {candle_1.name.to_pydatetime()}")

            elif candle_0['high_price'] < candle_2['low_price']:
                gap_size = candle_2['low_price'] - candle_0['high_price']
                if gap_size > min_fvg_value:
                    fvg_type = "Bearish Imbalance"
                    fvg_top_price = candle_2['low_price']
                    fvg_bottom_price = candle_0['high_price']
                    is_fvg_valid = True
                    # logger.debug(f"FVG Bearish deteksi: {fvg_bottom_price}-{fvg_top_price} di {timeframe_str} pada {candle_1.name.to_pydatetime()}")

            if is_fvg_valid:
                formation_time = candle_1.name.to_pydatetime()

                # --- MULAI KODE MODIFIKASI UNTUK PERHITUNGAN strength_score FVG ---
                calculated_fvg_strength = 0
                impulsive_candle_for_strength = None
                if fvg_type == "Bullish Imbalance": # Lilin impulsif naik adalah candle_0
                    impulsive_candle_for_strength = candle_0
                elif fvg_type == "Bearish Imbalance": # Lilin impulsif turun adalah candle_0
                    impulsive_candle_for_strength = candle_0

                if impulsive_candle_for_strength is not None:
                    impulsive_body_size = abs(impulsive_candle_for_strength['close'] - impulsive_candle_for_strength['open'])
                    impulsive_range_size = impulsive_candle_for_strength['high'] - impulsive_candle_for_strength['low']

                    if impulsive_range_size > 0:
                        body_to_range_ratio = impulsive_body_size / impulsive_range_size
                        if body_to_range_ratio >= fvg_min_candle_body_percent_for_strength:
                            calculated_fvg_strength += 1 # Tambahkan skor jika body lilin impulsif cukup besar

   
                if impulsive_candle_for_strength is not None and impulsive_candle_for_strength['volume'] is not None:
                   calculated_fvg_strength += int(utils.to_decimal_or_none(impulsive_candle_for_strength['volume']) / Decimal('1000') * fvg_volume_factor_for_strength) # Contoh: +1 skor per 1000 volume


                new_fvg_key = (symbol_param, timeframe_str, formation_time.isoformat(), fvg_type)

                if new_fvg_key not in existing_fvg_keys or new_fvg_key in fvg_keys_in_current_chunk:    
                    logger.debug(f"Melewatkan FVG duplikat yang sudah ada atau sudah terdeteksi di chunk ini: {new_fvg_key}")
                    continue

                detected_fvgs_to_save_batch.append({ 
                    "symbol": symbol_param,
                    "timeframe": timeframe_str,
                    "type": fvg_type,
                    "fvg_top_price": fvg_top_price,
                    "fvg_bottom_price": fvg_bottom_price,
                    "formation_time_utc": formation_time,
                    "is_filled": pd.notna(fvg_results_df['MitigatedIndex'].iloc[i]), # True jika ada MitigatedIndex
                    "last_fill_time_utc": fvg_results_df.index[int(fvg_results_df['MitigatedIndex'].iloc[i])].to_pydatetime() if pd.notna(fvg_results_df['MitigatedIndex'].iloc[i]) else None,
                    "retest_count": 0, 
                    "last_retest_time_utc": None,
                    "strength_score": calculated_fvg_strength # <--- GUNAKAN SKOR YANG DIHITUNG DI SINI
                })
                fvg_keys_in_current_chunk.add(new_fvg_key)



                # === PENTING: Lakukan penyimpanan batch di sini jika ambang batas tercapai ===
                if len(detected_fvgs_to_save_in_batch) >= BATCH_SAVE_THRESHOLD_FVG:
                    try:
                        database_manager.save_fair_value_gaps_batch(detected_fvgs_to_save_in_batch, [])
                        logger.info(f"BACKFILL WORKER: Auto-saved {len(detected_fvgs_to_save_in_batch)} new FVG for {timeframe_str}.")
                        detected_fvgs_to_save_in_batch = [] # Kosongkan setelah disimpan
                        fvg_keys_in_current_chunk.clear() # <<< TAMBAHKAN BARIS INI
                    except Exception as e:
                        logger.error(f"BACKFILL WORKER: Gagal auto-save FVG batch: {e}", exc_info=True)
                        detected_fvgs_to_save_in_batch = [] # Kosongkan agar tidak mengulang error yang sama
                        fvg_keys_in_current_chunk.clear() # <<< TAMBAHKAN BARIS INI

    except Exception as e: 
        logger.error(f"Error saat mendeteksi FVG BARU historis untuk {symbol_param} {timeframe_str}: {e}", exc_info=True)
    finally:
        # === PENTING: Simpan sisa data setelah loop selesai ===
        if detected_fvgs_to_save_in_batch:
            try:
                database_manager.save_fair_value_gaps_batch(detected_fvgs_to_save_in_batch, [])
                logger.info(f"BACKFILL WORKER: Final auto-saved {len(detected_fvgs_to_save_in_batch)} remaining new FVG for {timeframe_str}.")
            except Exception as e:
                logger.error(f"BACKFILL WORKER: Gagal final auto-save FVG batch: {e}", exc_info=True)
        
        # Pastikan fvg_keys_in_current_chunk juga di-clear di sini di setiap akhir eksekusi fungsi
        fvg_keys_in_current_chunk.clear() # <<< Pastikan baris ini ada.

    # new_fvgs_list yang diteruskan tidak lagi digunakan untuk mengumpulkan data, jadi kosongkan.
    if new_fvgs_list: 
        new_fvgs_list.clear() 
    
    logger.info(f"Selesai mendeteksi FVG BARU unik historis untuk {symbol_param} {timeframe_str}.")


def _backfill_worker_thread(task_queue, symbol_param):
    """
    Worker thread untuk memproses tugas backfill timeframe dari antrean.
    """
    while True:
        task = None # Inisialisasi task sebagai None di awal setiap iterasi
        try:
            task = task_queue.get(timeout=5) # Ambil tugas. Timeout lebih besar jika antrean sering kosong.
                                            # Jika timeout habis dan antrean kosong, ini akan raise queue.Empty

        except queue.Empty:
            # Jika antrean kosong, ini berarti semua tugas sudah selesai atau belum ada.
            # Worker thread bisa keluar atau menunggu lebih lama.
            # Dalam konteks backfill, ini biasanya berarti tugas sudah selesai.
            logger.debug(f"BackfillWorker Thread - {threading.current_thread().name}: Antrean tugas kosong, keluar.")
            break # Keluar dari loop while True, mengakhiri thread.

        except Exception as e_get: # Tangani error saat mengambil dari antrean (selain Empty)
            logger.critical(f"BackfillWorker Thread - {threading.current_thread().name}: ERROR KRITIS saat mengambil tugas dari antrean: {e_get}", exc_info=True)
            time.sleep(10) # Beri waktu sebelum mencoba lagi
            continue # Lanjut ke iterasi berikutnya untuk mencoba mengambil tugas lagi
            
        # Jika sampai di sini, artinya task berhasil diambil (task is not None)
        try:
            tf = task['timeframe']
            start_date_for_single_tf = task['start_date']
            end_date_for_single_tf = task['end_date']

            logger.debug(f"BackfillWorker Thread - {threading.current_thread().name}: Memulai memproses task untuk TF {tf}.")
            
            _backfill_single_timeframe_features(
                symbol_param, 
                tf, 
                backfill_start_date=start_date_for_single_tf, 
                backfill_end_date=end_date_for_single_tf
            )
            logger.debug(f"BackfillWorker Thread - {threading.current_thread().name}: Selesai memproses task untuk TF {tf}.")

        except Exception as e_process: # Tangani error saat memproses tugas (di dalam _backfill_single_timeframe_features)
            logger.critical(f"BackfillWorker Thread - {threading.current_thread().name}: ERROR KRITIS saat memproses tugas untuk TF {tf}: {e_process}", exc_info=True)
        finally:
            # task_done() HANYA dipanggil di sini, setelah tugas diambil (apakah berhasil diproses atau tidak)
            task_queue.task_done()


def _detect_new_order_blocks_historically(symbol_param: str, timeframe_str: str, candles_df: pd.DataFrame, current_atr_value: Decimal):
    """
    Mendeteksi Order Blocks (OB) baru secara historis.
    ...
    """
    logger.info(f"Mendeteksi Order Blocks BARU historis untuk {symbol_param} {timeframe_str}...")
    processed_count = 0 

    if candles_df.empty or len(candles_df) < config.RuleBasedStrategy.OB_SHOULDER_LENGTH + 2:
        logger.debug(f"Tidak cukup lilin ({len(candles_df)}) untuk deteksi Order Blocks historis di {timeframe_str}.")
        return 0 

    # Pastikan _SYMBOL_POINT_VALUE sudah diinisialisasi
    if globals().get('_SYMBOL_POINT_VALUE') is None:
        globals().get('_initialize_symbol_point_value')()
    if globals().get('_SYMBOL_POINT_VALUE') is None:
        logger.error(f"Nilai POINT untuk {symbol_param} tidak tersedia. Melewatkan deteksi Order Blocks.")
        return 0 

    # Ambil parameter konfigurasi baru
    ob_min_impulsive_candle_body_percent = config.RuleBasedStrategy.OB_MIN_IMPULSIVE_CANDLE_BODY_PERCENT
    ob_min_impulsive_move_multiplier = config.RuleBasedStrategy.OB_MIN_IMPULSIVE_MOVE_MULTIPLIER
    ob_volume_factor_multiplier = config.RuleBasedStrategy.OB_VOLUME_FACTOR_MULTIPLIER

    # Buat salinan DataFrame dan konversi kolom harga ke float untuk perhitungan internal
    # Catatan: candles_df yang masuk ke sini sudah di-rename kolomnya dan float dari _backfill_single_timeframe_features
    df_processed = candles_df.copy() # <<< Ini adalah DF Decimal dari database_manager.get_historical_candles_from_db

    # Rename kolom dan konversi ke float untuk df_processed
    df_processed = df_processed.rename(columns={
        'open_price': 'open', 'high_price': 'high', 'low_price': 'low',
        'close_price': 'close', 'tick_volume': 'volume', 'real_volume': 'real_volume', 'spread': 'spread'
    })
    # KOREKSI PENTING DI SINI: Pastikan konversi ke float juga menangani NaN dari Decimal
    for col in ['open', 'high', 'low', 'close', 'volume', 'real_volume', 'spread']:
        if col in df_processed.columns:
            # Gunakan utils.to_float_or_none yang sudah robust
            df_processed[col] = df_processed[col].apply(utils.to_float_or_none)
            
            # <<< PENAMBAHAN PENTING INI >>>
            # Hapus baris di mana kolom harga utama adalah NaN setelah konversi
            # Ini akan membersihkan data sebelum perhitungan lebih lanjut
            if col in ['open', 'high', 'low', 'close']: # Hanya hapus jika harga utama NaN
                original_len = len(df_processed)
                df_processed.dropna(subset=[col], inplace=True)
                if len(df_processed) < original_len:
                    logger.warning(f"OB Detektor: Dihapus {original_len - len(df_processed)} baris dengan {col} NaN di {timeframe_str}.")
            # <<< AKHIR PENAMBAHAN PENTING INI >>>

    # Tambahkan validasi jika DataFrame kosong setelah renaming/konversi
    if df_processed.empty:
        logger.warning(f"OB Detektor: DataFrame df_processed kosong setelah renaming/konversi/dropna. Melewatkan deteksi OB untuk {timeframe_str}.")
        return 0
    
    # Deduplikasi: Ambil Order Blocks yang sudah ada di DB
    existing_ob_keys = set()
    try:
        existing_obs_db = database_manager.get_order_blocks(
            symbol=symbol_param,
            timeframe=timeframe_str,
            start_time_utc=df_processed.index.min().to_pydatetime()
        )
        for ob_item in existing_obs_db:
            formation_time_dt = utils.to_utc_datetime_or_none(ob_item['formation_time_utc'])
            if formation_time_dt:
                ob_type_str = ob_item['type']
                existing_ob_keys.add((
                    ob_item['symbol'], ob_item['timeframe'],
                    ob_type_str, formation_time_dt.replace(microsecond=0).isoformat()
                ))
        logger.debug(f"Ditemukan {len(existing_ob_keys)} Order Blocks yang sudah ada di DB untuk {timeframe_str}.")
    except Exception as e:
        logger.error(f"Gagal mengambil existing Order Blocks dari DB untuk pre-filtering: {e}", exc_info=True)
        pass

    detected_obs_to_save_batch = []

    for i in range(len(ob_results_df)):
        ob_type_val = ob_results_df['OB'].iloc[i] 
        ob_top_float = ob_results_df['Top'].iloc[i]
        ob_bottom_float = ob_results_df['Bottom'].iloc[i]
        formation_time_dt = ob_results_df.index[i].to_pydatetime()
        ob_volume_float = ob_results_df['OBVolume'].iloc[i]
        ob_percentage_float = ob_results_df['Percentage'].iloc[i]

        ob_type_str = "Bullish" if ob_type_val == 1 else "Bearish"

        ob_top_dec = utils.to_decimal_or_none(ob_top_float)
        ob_bottom_dec = utils.to_decimal_or_none(ob_bottom_float)

        # MODIFIKASI INI: Periksa dan tangani Decimal('NaN') untuk harga top/bottom OB
        if ob_top_dec is not None and ob_top_dec.is_nan():
            ob_top_dec = None # Set ke None jika NaN
            logger.warning(f"OB Top Price is NaN for OB at {formation_time_dt}. Setting to None.")
        if ob_bottom_dec is not None and ob_bottom_dec.is_nan():
            ob_bottom_dec = None # Set ke None jika NaN
            logger.warning(f"OB Bottom Price is NaN for OB at {formation_time_dt}. Setting to None.")

        
        # MODIFIKASI INI: Pastikan ob_volume_float bukan NaN sebelum konversi ke Decimal
        ob_volume_dec = None # Default ke None di awal
        if ob_volume_float is not None and pd.notna(ob_volume_float):
            try:
                temp_ob_volume_dec = utils.to_decimal_or_none(ob_volume_float)
                if temp_ob_volume_dec is not None and not temp_ob_volume_dec.is_nan(): # Pastikan bukan None dan bukan Decimal('NaN')
                    ob_volume_dec = temp_ob_volume_dec
                else:
                    logger.warning(f"OB Volume float '{ob_volume_float}' (tipe: {type(ob_volume_float)}) dikonversi menjadi None atau Decimal('NaN'). Disetel ke None.")
            except Exception as e:
                logger.warning(f"OB Volume float '{ob_volume_float}' (tipe: {type(ob_volume_float)}) menyebabkan error saat konversi ke Decimal: {e}. Disetel ke None.")
        else:
            logger.debug(f"OB Volume float adalah NaN/None untuk OB di {formation_time_dt}. Disetel ke None.")
        # AKHIR MODIFIKASI

        ob_percentage_dec = utils.to_decimal_or_none(ob_percentage_float)

        if ob_top_dec is None or ob_bottom_dec is None: # Lanjutkan jika harga top/bottom tidak valid
            continue

        # --- MULAI KODE MODIFIKASI UNTUK PERHITUNGAN strength_score OB ---
        calculated_ob_strength = 0

        # Dapatkan lilin OB itu sendiri dari df_processed (yang sudah di-rename dan float)
        ob_candle_data = df_processed[df_processed.index == formation_time_dt]
        if not ob_candle_data.empty:
            ob_candle_open = ob_candle_data['open'].iloc[0]
            ob_candle_close = ob_candle_data['close'].iloc[0]
            ob_candle_high = ob_candle_data['high'].iloc[0]
            ob_candle_low = ob_candle_data['low'].iloc[0]
            ob_candle_volume = ob_candle_data['volume'].iloc[0]

            # 1. Ukuran Body Lilin OB (semakin besar body, semakin kuat)
            ob_body_size = abs(ob_candle_close - ob_candle_open)
            ob_range_size = ob_candle_high - ob_candle_low
            if ob_range_size > 0:
                body_to_range_ratio = ob_body_size / ob_range_size
                if body_to_range_ratio >= ob_min_impulsive_candle_body_percent:
                    calculated_ob_strength += 1 

            # 2. Volume Lilin OB (semakin tinggi volume, semakin kuat)
            if ob_candle_volume is not None and ob_volume_dec is not None and ob_volume_dec > 0:
                calculated_ob_strength += int(ob_volume_dec / Decimal('1000') * ob_volume_factor_multiplier) 

            # 3. Impulsive Move Setelah OB (semakin besar move, semakin kuat OB)
            impulsive_move_candles = df_processed[df_processed.index > formation_time_dt].iloc[:2] 
            if not impulsive_move_candles.empty:
                first_impulsive_candle = impulsive_move_candles.iloc[0]
                
                impulsive_move_abs = abs(utils.to_decimal_or_none(first_impulsive_candle['close']) - utils.to_decimal_or_none(ob_candle_close))
                
                if current_atr_value is not None and current_atr_value > Decimal('0.0'):
                    if impulsive_move_abs > (current_atr_value * ob_min_impulsive_move_multiplier):
                        calculated_ob_strength += 2 
                        logger.debug(f"OB Strength: Impulsive move ({float(impulsive_move_abs):.5f}) > ATR x Multiplier ({float(current_atr_value * ob_min_impulsive_move_multiplier):.5f}). +2 Strength.")
                else:
                    ob_range_dec = ob_top_dec - ob_bottom_dec
                    if ob_range_dec > 0 and impulsive_move_abs > (ob_range_dec * ob_min_impulsive_move_multiplier):
                        calculated_ob_strength += 2
                        logger.debug(f"OB Strength: Impulsive move ({float(impulsive_move_abs):.5f}) > OB Range x Multiplier ({float(ob_range_dec * ob_min_impulsive_move_multiplier):.5f}). +2 Strength (ATR Fallback).")

        # --- AKHIR KODE MODIFIKASI UNTUK PERHITUNGAN strength_score OB ---

        ob_type_str = "Bullish" if ob_type_val == 1 else "Bearish" # Pastikan ob_type_str didefinisikan

        new_key = (symbol_param, timeframe_str, ob_type_str, formation_time_dt.replace(microsecond=0).isoformat())
        if new_key not in existing_ob_keys:
            detected_obs_to_save_batch.append({
                "symbol": symbol_param, "timeframe": timeframe_str,
                "type": ob_type_str,
                "ob_top_price": ob_top_dec, "ob_bottom_price": ob_bottom_dec,
                "formation_time_utc": formation_time_dt,
                "is_mitigated": pd.notna(ob_results_df['MitigatedIndex'].iloc[i]),
                "last_mitigation_time_utc": ob_results_df.index[int(ob_results_df['MitigatedIndex'].iloc[i])].to_pydatetime() if pd.notna(ob_results_df['MitigatedIndex'].iloc[i]) else None,
                "strength_score": calculated_ob_strength, 
                "confluence_score": 0 
            })
            existing_ob_keys.add(new_key)
            logger.debug(f"Order Block {ob_type_str} terdeteksi di {timeframe_str} pada {formation_time_dt} (Strength: {calculated_ob_strength}).")

    if detected_obs_to_save_batch:
        try:
            database_manager.save_order_blocks_batch(detected_obs_to_save_batch, []) # <--- MODIFIKASI INI
            processed_count += len(detected_obs_to_save_batch)
            logger.info(f"BACKFILL WORKER: Berhasil menyimpan {len(detected_obs_to_save_batch)} Order Blocks baru unik untuk {symbol_param} {timeframe_str}.")
        except Exception as e:
            logger.error(f"BACKFILL WORKER: Gagal menyimpan Order Blocks batch: {e}", exc_info=True)

    logger.info(f"Selesai mendeteksi Order Blocks BARU historis untuk {symbol_param} {timeframe_str}. Deteksi: {processed_count}.")
    return processed_count

def _detect_new_order_blocks_historically(symbol_param: str, timeframe_str: str, candles_df: pd.DataFrame, current_atr_value: Decimal):
    """
    Mendeteksi Order Blocks (OB) baru secara historis.
    Menyimpan OB baru ke database secara batch dengan deduplikasi.
    Menggunakan ATR adaptif dan faktor-faktor lain untuk menghitung strength_score.
    Args:
        symbol_param (str): Simbol trading.
        timeframe_str (str): Timeframe.
        candles_df (pd.DataFrame): DataFrame candle historis (sudah dalam format float).
        current_atr_value (Decimal): Nilai ATR saat ini untuk timeframe ini.
    """
    logger.info(f"Mendeteksi Order Blocks BARU historis untuk {symbol_param} {timeframe_str}...")
    processed_count = 0 # <--- PASTIKAN BARIS INI ADA DI SINI

    if candles_df.empty or len(candles_df) < config.RuleBasedStrategy.OB_SHOULDER_LENGTH + 2:
        logger.debug(f"Tidak cukup lilin ({len(candles_df)}) untuk deteksi Order Blocks historis di {timeframe_str}.")
        return 0 # Perbaikan: return 0 jika tidak ada cukup lilin

    # Pastikan _SYMBOL_POINT_VALUE sudah diinisialisasi
    if globals().get('_SYMBOL_POINT_VALUE') is None:
        globals().get('_initialize_symbol_point_value')()
    if globals().get('_SYMBOL_POINT_VALUE') is None:
        logger.error(f"Nilai POINT untuk {symbol_param} tidak tersedia. Melewatkan deteksi Order Blocks.")
        return 0 # Perbaikan: return 0 jika SYMBOL_POINT_VALUE tidak tersedia

    # Ambil parameter konfigurasi baru
    ob_min_impulsive_candle_body_percent = config.RuleBasedStrategy.OB_MIN_IMPULSIVE_CANDLE_BODY_PERCENT
    ob_min_impulsive_move_multiplier = config.RuleBasedStrategy.OB_MIN_IMPULSIVE_MOVE_MULTIPLIER
    ob_volume_factor_multiplier = config.RuleBasedStrategy.OB_VOLUME_FACTOR_MULTIPLIER

    # Buat salinan DataFrame dan konversi kolom harga ke float untuk perhitungan internal
    # Catatan: candles_df yang masuk ke sini sudah di-rename kolomnya dan float dari _backfill_single_timeframe_features
    df_processed = candles_df.copy()
    # Rename kolom dan konversi ke float untuk df_processed
    df_processed = df_processed.rename(columns={
        'open_price': 'open', 'high_price': 'high', 'low_price': 'low',
        'close_price': 'close', 'tick_volume': 'volume', 'real_volume': 'real_volume', 'spread': 'spread' # <--- PASTIKAN real_volume dan spread juga ada di sini jika relevan
    })
    # KOREKSI: HANYA konversi kolom harga dan volume
    for col in ['open', 'high', 'low', 'close', 'volume', 'real_volume', 'spread']: # <--- PASTIKAN LIST KOLOM INI
        if col in df_processed.columns:
            df_processed[col] = df_processed[col].apply(utils.to_float_or_none)

    # Hitung Swing Highs/Lows (diperlukan untuk _calculate_order_blocks_internal)
    swing_results_df = globals().get('_calculate_swing_highs_lows_internal')(df_processed, swing_length=config.AIAnalysts.SWING_EXT_BARS)
    if swing_results_df.empty:
        logger.warning(f"Tidak ada swing highs/lows untuk {symbol_param} {timeframe_str}. Melewatkan deteksi Order Blocks.")
        return 0 # Perbaikan: return 0 jika tidak ada swing

    # Hitung break_tolerance_value (diperlukan untuk _calculate_order_blocks_internal)
    break_tolerance_value = Decimal('0.0')
    if current_atr_value is not None and current_atr_value > Decimal('0.0'):
        break_tolerance_value = current_atr_value * config.MarketData.ATR_MULTIPLIER_FOR_TOLERANCE 
        logger.debug(f"OB Detektor: Menggunakan break_tolerance dinamis ATR: {float(break_tolerance_value):.5f} (dari ATR {float(current_atr_value):.5f})")
    else:
        break_tolerance_value = config.RuleBasedStrategy.RULE_SR_TOLERANCE_POINTS * globals().get('_SYMBOL_POINT_VALUE')
        logger.warning(f"OB Detektor: ATR tidak valid. Menggunakan break_tolerance statis: {float(break_tolerance_value):.5f}")


    # Hitung Order Blocks menggunakan fungsi internal
    ob_results_df = globals().get('_calculate_order_blocks_internal')(
        df_processed, 
        shoulder_length=config.RuleBasedStrategy.OB_SHOULDER_LENGTH,
        swing_highs_lows_df=swing_results_df, # Meneruskan swing_results_df
        consolidation_tolerance_points=config.RuleBasedStrategy.RULE_OB_CONSOLIDATION_TOLERANCE_POINTS * globals().get('_SYMBOL_POINT_VALUE'),
        break_tolerance_value=break_tolerance_value # Meneruskan break_tolerance_value
    )
    if not isinstance(ob_results_df.index, pd.DatetimeIndex):
        try:
            ob_results_df.index = pd.to_datetime(ob_results_df.index)
            ob_results_df = ob_results_df.tz_localize(timezone.utc) # Pastikan timezone-aware UTC
            logger.warning(f"OB Detektor: Indeks ob_results_df dikonversi ke DatetimeIndex untuk {timeframe_str}.")
        except Exception as e:
            logger.error(f"OB Detektor: Gagal mengonversi indeks ob_results_df ke DatetimeIndex: {e}. Melewatkan deteksi OB.", exc_info=True)
            return 0
    
    # Keluar jika indeks tidak bisa dikonversi
    if ob_results_df.empty:
        logger.debug(f"Tidak ada Order Blocks terdeteksi untuk {symbol_param} {timeframe_str}.")
        return 0 # Perbaikan: return 0 jika tidak ada OB terdeteksi

    # Deduplikasi: Ambil Order Blocks yang sudah ada di DB
    existing_ob_keys = set()
    try:
        existing_obs_db = database_manager.get_order_blocks(
            symbol=symbol_param,
            timeframe=timeframe_str,
            start_time_utc=df_processed.index.min().to_pydatetime()
        )
        for ob_item in existing_obs_db:
            formation_time_dt = utils.to_utc_datetime_or_none(ob_item['formation_time_utc'])
            if formation_time_dt:
                ob_type_str = ob_item['type']
                existing_ob_keys.add((
                    ob_item['symbol'], ob_item['timeframe'],
                    ob_type_str, formation_time_dt.replace(microsecond=0).isoformat()
                ))
        logger.debug(f"Ditemukan {len(existing_ob_keys)} Order Blocks yang sudah ada di DB untuk {timeframe_str}.")
    except Exception as e:
        logger.error(f"Gagal mengambil existing Order Blocks dari DB untuk pre-filtering: {e}", exc_info=True)
        pass

    detected_obs_to_save_batch = []

    for i in range(len(ob_results_df)):
        ob_type_val = ob_results_df['OB'].iloc[i] 
        ob_top_float = ob_results_df['Top'].iloc[i]
        ob_bottom_float = ob_results_df['Bottom'].iloc[i]
        formation_time_dt = ob_results_df.index[i].to_pydatetime()
        ob_volume_float = ob_results_df['OBVolume'].iloc[i]
        ob_percentage_float = ob_results_df['Percentage'].iloc[i]

        ob_type_str = "Bullish" if ob_type_val == 1 else "Bearish"

        ob_top_dec = utils.to_decimal_or_none(ob_top_float)
        ob_bottom_dec = utils.to_decimal_or_none(ob_bottom_float)

        # KOREKSI: Pastikan ini disetel ke None jika is_nan(), BUKAN Decimal('0.0')
        if ob_top_dec is not None and ob_top_dec.is_nan():
            ob_top_dec = None # Set ke None jika NaN
            logger.warning(f"OB Top Price is NaN for OB at {formation_time_dt}. Setting to None.")
        if ob_bottom_dec is not None and ob_bottom_dec.is_nan():
            ob_bottom_dec = None # Set ke None jika NaN
            logger.warning(f"OB Bottom Price is NaN for OB at {formation_time_dt}. Setting to None.")

        # KOREKSI: Pastikan ob_volume_float bukan NaN sebelum konversi ke Decimal
        ob_volume_dec = None # Default ke None di awal
        if ob_volume_float is not None and pd.notna(ob_volume_float):
            try:
                temp_ob_volume_dec = utils.to_decimal_or_none(ob_volume_float)
                if temp_ob_volume_dec is not None and not temp_ob_volume_dec.is_nan(): # Pastikan bukan None dan bukan Decimal('NaN')
                    ob_volume_dec = temp_ob_volume_dec
                else:
                    logger.warning(f"OB Volume float '{ob_volume_float}' (tipe: {type(ob_volume_float)}) dikonversi menjadi None atau Decimal('NaN'). Disetel ke None.")
            except Exception as e:
                logger.warning(f"OB Volume float '{ob_volume_float}' (tipe: {type(ob_volume_float)}) menyebabkan error saat konversi ke Decimal: {e}. Disetel ke None.")
        else:
            logger.debug(f"OB Volume float adalah NaN/None untuk OB di {formation_time_dt}. Disetel ke None.")

        ob_percentage_dec = utils.to_decimal_or_none(ob_percentage_float)

        if ob_top_dec is None or ob_bottom_dec is None:
            continue

        # --- MULAI KODE MODIFIKASI UNTUK PERHITUNGAN strength_score OB ---
        calculated_ob_strength = 0

        # Dapatkan lilin OB itu sendiri dari df_processed (yang sudah di-rename dan float)
        ob_candle_data = df_processed[df_processed.index == formation_time_dt]
        if not ob_candle_data.empty:
            ob_candle_open = ob_candle_data['open'].iloc[0]
            ob_candle_close = ob_candle_data['close'].iloc[0]
            ob_candle_high = ob_candle_data['high'].iloc[0]
            ob_candle_low = ob_candle_data['low'].iloc[0]
            ob_candle_volume = ob_candle_data['volume'].iloc[0]

            # 1. Ukuran Body Lilin OB (semakin besar body, semakin kuat)
            ob_body_size = abs(ob_candle_close - ob_candle_open)
            ob_range_size = ob_candle_high - ob_candle_low
            if ob_range_size > 0:
                body_to_range_ratio = ob_body_size / ob_range_size
                if body_to_range_ratio >= ob_min_impulsive_candle_body_percent:
                    calculated_ob_strength += 1 

            # 2. Volume Lilin OB (semakin tinggi volume, semakin kuat)
            if ob_candle_volume is not None and ob_volume_dec is not None and ob_volume_dec > 0:
                calculated_ob_strength += int(ob_volume_dec / Decimal('1000') * ob_volume_factor_multiplier) 

            # 3. Impulsive Move Setelah OB (semakin besar move, semakin kuat OB)
            impulsive_move_candles = df_processed[df_processed.index > formation_time_dt].iloc[:2] 
            if not impulsive_move_candles.empty:
                first_impulsive_candle = impulsive_move_candles.iloc[0]
                
                impulsive_move_abs = abs(utils.to_decimal_or_none(first_impulsive_candle['close']) - utils.to_decimal_or_none(ob_candle_close))
                
                if current_atr_value is not None and current_atr_value > Decimal('0.0'):
                    if impulsive_move_abs > (current_atr_value * ob_min_impulsive_move_multiplier):
                        calculated_ob_strength += 2 
                        logger.debug(f"OB Strength: Impulsive move ({float(impulsive_move_abs):.5f}) > ATR x Multiplier ({float(current_atr_value * ob_min_impulsive_move_multiplier):.5f}). +2 Strength.")
                else:
                    ob_range_dec = ob_top_dec - ob_bottom_dec
                    if ob_range_dec > 0 and impulsive_move_abs > (ob_range_dec * ob_min_impulsive_move_multiplier):
                        calculated_ob_strength += 2
                        logger.debug(f"OB Strength: Impulsive move ({float(impulsive_move_abs):.5f}) > OB Range x Multiplier ({float(ob_range_dec * ob_min_impulsive_move_multiplier):.5f}). +2 Strength (ATR Fallback).")

        # --- AKHIR KODE MODIFIKASI UNTUK PERHITUNGAN strength_score OB ---

        new_key = (symbol_param, timeframe_str, ob_type_str, formation_time_dt.replace(microsecond=0).isoformat())
        if new_key not in existing_ob_keys:
            detected_obs_to_save_batch.append({
                "symbol": symbol_param, "timeframe": timeframe_str,
                "type": ob_type_str,
                "ob_top_price": ob_top_dec, "ob_bottom_price": ob_bottom_dec,
                "formation_time_utc": formation_time_dt,
                "is_mitigated": pd.notna(ob_results_df['MitigatedIndex'].iloc[i]),
                "last_mitigation_time_utc": ob_results_df.index[int(ob_results_df['MitigatedIndex'].iloc[i])].to_pydatetime() if pd.notna(ob_results_df['MitigatedIndex'].iloc[i]) else None,
                "strength_score": calculated_ob_strength, 
                "confluence_score": 0 
            })
            existing_ob_keys.add(new_key)
            logger.debug(f"Order Block {ob_type_str} terdeteksi di {timeframe_str} pada {formation_time_dt} (Strength: {calculated_ob_strength}).")

    if detected_obs_to_save_batch:
        try:
            database_manager.save_order_blocks_batch(detected_obs_to_save_batch, []) # <--- MODIFIKASI INI
            processed_count += len(detected_obs_to_save_batch)
            logger.info(f"BACKFILL WORKER: Berhasil menyimpan {len(detected_obs_to_save_batch)} Order Blocks baru unik untuk {symbol_param} {timeframe_str}.")
        except Exception as e:
            logger.error(f"BACKFILL WORKER: Gagal menyimpan Order Blocks batch: {e}", exc_info=True)

    logger.info(f"Selesai mendeteksi Order Blocks BARU historis untuk {symbol_param} {timeframe_str}. Deteksi: {processed_count}.")
    return processed_count




def _update_existing_order_blocks_status(symbol_param: str, timeframe_str: str, candles_df: pd.DataFrame, current_atr_value: Decimal):
    """
    Memperbarui status Order Blocks (OB) yang sudah ada (is_mitigated, last_mitigation_time_utc)
    berdasarkan pergerakan harga terbaru.
    OB menjadi termitigasi jika harga telah melewati levelnya.
    Args:
        symbol_param (str): Simbol trading.
        timeframe_str (str): Timeframe.
        candles_df (pd.DataFrame): DataFrame candle historis (diasumsikan sudah dalam format
                                   kolom pendek 'open', 'high', 'low', 'close' sebagai float).
        current_atr_value (Decimal): Nilai ATR saat ini untuk timeframe ini.
    Returns:
        int: Jumlah Order Blocks yang statusnya berhasil diperbarui dan dikirim ke antrean DB.
    """
    logger.info(f"Memperbarui status Order Blocks yang sudah ada untuk {symbol_param} {timeframe_str}...")

    processed_count = 0 # Inisialisasi penghitung yang akan dikembalikan

    # 1. Validasi Input Awal
    if candles_df.empty:
        logger.debug(f"DataFrame candle kosong untuk update status OB di {timeframe_str}.")
        return 0 

    # 2. Inisialisasi _SYMBOL_POINT_VALUE (jika belum)
    if globals().get('_SYMBOL_POINT_VALUE') is None:
        globals().get('_initialize_symbol_point_value')()
    if globals().get('_SYMBOL_POINT_VALUE') is None:
        logger.error(f"Nilai POINT untuk {symbol_param} tidak tersedia. Melewatkan update status Order Blocks.")
        return 0 

    # 3. Ambil Batas Harga Dinamis Global (tidak perlu modifikasi di sini)
    with globals().get('_dynamic_price_lock', threading.Lock()):
        min_price_for_update_filter = globals().get('_dynamic_price_min_for_analysis')
        max_price_for_update_filter = globals().get('_dynamic_price_max_for_analysis')

    # 4. Mendapatkan Order Blocks Aktif dari Database
    active_obs = database_manager.get_order_blocks(
        symbol=symbol_param,
        timeframe=timeframe_str,
        is_mitigated=False, # Hanya ambil yang belum termitigasi
        limit=None, # Ambil semua OB aktif
        min_price_level=min_price_for_update_filter, # Filter berdasarkan rentang harga
        max_price_level=max_price_for_update_filter
    )

    if not active_obs:
        logger.debug(f"Tidak ada Order Blocks aktif untuk diperbarui statusnya di {symbol_param} {timeframe_str}.")
        return 0 

    # 5. Menentukan Toleransi Penembusan/Mitigasi
    if current_atr_value is not None and current_atr_value > Decimal('0.0'):
        tolerance_value_dec = current_atr_value * config.MarketData.ATR_MULTIPLIER_FOR_TOLERANCE
        logger.debug(f"OB Status Updater: Menggunakan toleransi dinamis ATR: {float(tolerance_value_dec):.5f} (dari ATR {float(current_atr_value):.5f})")
    else:
        tolerance_value_dec = config.RuleBasedStrategy.RULE_SR_TOLERANCE_POINTS * globals().get('_SYMBOL_POINT_VALUE')
        logger.warning(f"OB Status Updater: ATR tidak valid. Menggunakan toleransi statis: {float(tolerance_value_dec):.5f}")
    
    updated_obs_batch = []

    # 6. Menentukan Waktu Mulai Pencarian Lilin yang Relevan (Optimasi)
    # --- MULAI MODIFIKASI UNTUK PENANGANAN NoneType PADA latest_active_ob_time ---
    list_for_max_ob = []
    for ob_item in active_obs:
        # Prioritaskan last_mitigation_time_utc, lalu formation_time_utc
        effective_timestamp = ob_item.get('last_mitigation_time_utc')
        if effective_timestamp is None:
            effective_timestamp = ob_item.get('formation_time_utc')
        
        # Pastikan effective_timestamp adalah objek datetime yang valid.
        # utils.to_utc_datetime_or_none akan mengonversi string atau memvalidasi datetime,
        # dan mengembalikan None jika tidak valid, sehingga kita bisa memfilter None.
        processed_timestamp = utils.to_utc_datetime_or_none(effective_timestamp)
        
        if processed_timestamp is not None:
            list_for_max_ob.append(processed_timestamp)
        else:
            logger.warning(f"OB Status Updater: Timestamp untuk OB ID {ob_item.get('id', 'N/A')} resolve ke None setelah konversi. Dilewati untuk perhitungan max.")

    if list_for_max_ob:
        latest_active_ob_time = max(list_for_max_ob)
    else:
        # Jika daftar kosong (semua timestamp tidak valid atau active_obs kosong), gunakan waktu fallback yang aman
        latest_active_ob_time = datetime(1970, 1, 1, tzinfo=timezone.utc)
        logger.warning(f"OB Status Updater: Tidak ada timestamp valid untuk menghitung max. Menggunakan default {latest_active_ob_time.isoformat()}.")
    # --- AKHIR MODIFIKASI UNTUK PENANGANAN NoneType PADA latest_active_ob_time ---

    relevant_candles_for_test = candles_df[
        candles_df.index > latest_active_ob_time
    ].copy()

    if relevant_candles_for_test.empty and active_obs:
        logger.debug(f"Tidak ada lilin baru setelah OB aktif terbaru untuk {symbol_param} {timeframe_str}. Melewatkan update status OB.")
        return 0 

    # 7. Iterasi Melalui Order Blocks Aktif dan Lilin yang Relevan
    for ob in active_obs:
        ob_id = ob['id']
        ob_top_price = ob['ob_top_price'] # Ini sudah Decimal dari DB
        ob_bottom_price = ob['ob_bottom_price'] # Ini sudah Decimal dari DB
        ob_type = ob['type'] # Bullish atau Bearish
        
        is_still_active = True 
        
        # Inisialisasi last_mitigation_time_for_update untuk OB saat ini
        last_mitigation_time_for_update = ob.get('last_mitigation_time_utc')
        if last_mitigation_time_for_update is None:
            last_mitigation_time_for_update = ob.get('formation_time_utc', datetime(1970, 1, 1, tzinfo=timezone.utc))
            if last_mitigation_time_for_update is None:
                last_mitigation_time_for_update = datetime(1970, 1, 1, tzinfo=timezone.utc)
                logger.warning(f"OB_STATUS_DEBUG: OB {ob_id} tidak memiliki formation_time_utc atau last_mitigation_time_utc. Menggunakan default 1970-01-01.")

        # Filter lilin yang relevan untuk *OB spesifik ini*
        ob_relevant_candles = relevant_candles_for_test[
            relevant_candles_for_test.index > last_mitigation_time_for_update 
        ].copy()

        if ob_relevant_candles.empty:
            logger.debug(f"OB_STATUS_DEBUG: OB {ob_type} (ID: {ob_id}) dilewati karena tidak ada lilin baru setelah last_mitigation_time_utc ({last_mitigation_time_for_update}).")
            continue


        for _, candle_row in ob_relevant_candles.iterrows(): 
            if not is_still_active: 
                break

            # --- MULAI PENAMBAHAN KODE UNTUK NAMA KOLOM high/low/close ---
            candle_high = None
            candle_low = None
            candle_close = None

            # Cek untuk 'high'
            if 'high' in candle_row:
                candle_high = candle_row['high']
            elif 'high_price' in candle_row: # Fallback jika renaming belum terjadi
                candle_high = candle_row['high_price']
            else:
                logger.error(f"OB_STATUS_DEBUG: Kolom 'high' atau 'high_price' tidak ditemukan di candle_row untuk OB {ob_id}. Melewatkan lilin ini.")
                continue # Lanjutkan ke lilin berikutnya jika data penting tidak ada

            # Cek untuk 'low'
            if 'low' in candle_row:
                candle_low = candle_row['low']
            elif 'low_price' in candle_row: # Fallback jika renaming belum terjadi
                candle_low = candle_row['low_price']
            else:
                logger.error(f"OB_STATUS_DEBUG: Kolom 'low' atau 'low_price' tidak ditemukan di candle_row untuk OB {ob_id}. Melewatkan lilin ini.")
                continue # Lanjutkan ke lilin berikutnya jika data penting tidak ada

            # Cek untuk 'close'
            if 'close' in candle_row:
                candle_close = candle_row['close']
            elif 'close_price' in candle_row: # Fallback jika renaming belum terjadi
                candle_close = candle_row['close_price']
            else:
                logger.error(f"OB_STATUS_DEBUG: Kolom 'close' atau 'close_price' tidak ditemukan di candle_row untuk OB {ob_id}. Melewatkan lilin ini.")
                continue # Lanjutkan ke lilin berikutnya jika data penting tidak ada

            # Konversi nilai ke float menggunakan helper utility
            candle_high = utils.to_float_or_none(candle_high)
            candle_low = utils.to_float_or_none(candle_low)
            candle_close = utils.to_float_or_none(candle_close)

            # Validasi akhir setelah konversi ke float
            if candle_high is None or candle_low is None or candle_close is None:
                logger.debug(f"OB_STATUS_DEBUG: Lilin dengan NaN high/low/close setelah konversi untuk OB {ob_id} di {candle_row.name}. Melewatkan pengecekan.")
                continue # Lewati lilin ini jika ada NaN setelah konversi
            # --- AKHIR PENAMBAHAN KODE UNTUK NAMA KOLOM high/low/close ---

            candle_time_dt = candle_row.name.to_pydatetime()

            is_mitigated_now = False
            
            # KOREKSI PENTING: Lakukan perbandingan menggunakan float dari Decimal
            # Konversi OB prices dan tolerance ke float untuk perbandingan yang konsisten dengan candle_row
            ob_top_float = float(ob_top_price)
            ob_bottom_float = float(ob_bottom_price)
            tolerance_float = float(tolerance_value_dec)

            # Logika mitigasi (tap): Harga menembus OB sepenuhnya dengan konfirmasi close
            if ob_type == "Bullish": # Bullish OB: harga harus menembus ke bawah ob_bottom_price untuk mitigasi
                if candle_low < ob_bottom_float - tolerance_float and \
                   candle_close < ob_bottom_float - tolerance_float: 
                    is_mitigated_now = True
                    logger.debug(f"OB_STATUS_DEBUG: Order Block Bullish (ID: {ob_id}, Bottom: {ob_bottom_price}) menjadi termitigasi (Close: {candle_close:.5f}).")
                elif candle_low <= ob_top_float and candle_high >= ob_bottom_float: # Retest jika harga menyentuh OB tanpa menembus
                    # Jika ada interaksi, update last_mitigation_time_for_update
                    if (last_mitigation_time_for_update is None) or ((candle_time_dt - last_mitigation_time_for_update).total_seconds() > config.System.RETRY_DELAY_SECONDS * 5):
                        last_mitigation_time_for_update = candle_time_dt
                        logger.debug(f"OB_STATUS_DEBUG: Order Block Bullish (ID: {ob_id}) retest terjadi (High: {candle_high:.5f}, Low: {candle_low:.5f}).")

            elif ob_type == "Bearish": # Bearish OB: harga harus menembus ke atas ob_top_price untuk mitigasi
                if candle_high > ob_top_float + tolerance_float and \
                   candle_close > ob_top_float + tolerance_float: 
                    is_mitigated_now = True
                    logger.debug(f"OB_STATUS_DEBUG: Order Block Bearish (ID: {ob_id}, Top: {ob_top_price}) menjadi termitigasi (Close: {candle_close:.5f}).")
                elif candle_high >= ob_bottom_float and candle_low <= ob_top_float: # Retest jika harga menyentuh OB tanpa menembus
                    # Jika ada interaksi, update last_mitigation_time_for_update
                    if (last_mitigation_time_for_update is None) or ((candle_time_dt - last_mitigation_time_for_update).total_seconds() > config.System.RETRY_DELAY_SECONDS * 5):
                        last_mitigation_time_for_update = candle_time_dt
                        logger.debug(f"OB_STATUS_DEBUG: Order Block Bearish (ID: {ob_id}) retest terjadi (High: {candle_high:.5f}, Low: {candle_low:.5f}).")
        
            if is_mitigated_now:
                is_still_active = False
                break 
        
        logger.debug(f"OB_STATUS_DEBUG: OB {ob_id} final status: is_mitigated={not is_still_active}, last_mitigation_time={last_mitigation_time_for_update.isoformat() if last_mitigation_time_for_update else 'None'}")
        
        if (ob['is_mitigated'] != (not is_still_active)) or \
           (utils.to_utc_datetime_or_none(ob.get('last_mitigation_time_utc')) != last_mitigation_time_for_update):
            
            updated_obs_batch.append({
                'id': ob_id,
                'is_mitigated': not is_still_active, 
                'last_mitigation_time_utc': last_mitigation_time_for_update, 
                'symbol': ob['symbol'],
                'timeframe': ob['timeframe'],
                'type': ob['type'],
                'ob_top_price': ob_top_price,
                'ob_bottom_price': ob_bottom_price,
                'formation_time_utc': ob['formation_time_utc'],
                'strength_score': ob.get('strength_score'),
                'confluence_score': ob.get('confluence_score', 0)
            })
            processed_count += 1
            logger.debug(f"OB_STATUS_DEBUG: OB {ob_id} ditambahkan ke batch update. Processed count: {processed_count}")

    # 8. Menyimpan Batch Update ke Database
    if updated_obs_batch:
        # --- MULAI MODIFIKASI UNTUK KONVERSI TIPE DATA SEBELUM SAVE BATCH ---
        processed_updated_obs_batch = []
        for ob_data_dict in updated_obs_batch:
            processed_item_dict = ob_data_dict.copy()
            for key, value in processed_item_dict.items():
                if isinstance(value, Decimal):
                    processed_item_dict[key] = utils.to_float_or_none(value)
                elif isinstance(value, datetime):
                    processed_item_dict[key] = utils.to_iso_format_or_none(value)
            processed_updated_obs_batch.append(processed_item_dict)
        # --- AKHIR MODIFIKASI UNTUK KONVERSI TIPE DATA ---

        try:
            database_manager.save_order_blocks_batch([], processed_updated_obs_batch)
            logger.info(f"BACKFILL WORKER: Berhasil mem-batch update {len(processed_updated_obs_batch)} status Order Blocks untuk {symbol_param} {timeframe_str}.")
        except Exception as e:
            logger.error(f"BACKFILL WORKER: Gagal mem-batch update status Order Blocks: {e}", exc_info=True)

    logger.info(f"Selesai memperbarui status Order Blocks untuk {symbol_param} {timeframe_str}. {processed_count} diperbarui.")
    return processed_count


def _detect_new_previous_high_low_historically(symbol_param: str, timeframe_str: str, higher_timeframe_for_phl: str, candles_df: pd.DataFrame, current_atr_value: Decimal) -> int:
    """
    Mendeteksi Previous High dan Low baru secara historis dan menyimpan event ketika level ditembus.
    Menggunakan ATR adaptif untuk toleransi break dan memerlukan konfirmasi candle close.
    Args:
        symbol_param (str): Simbol trading.
        timeframe_str (str): Timeframe dari candles_df.
        higher_timeframe_for_phl (str): Timeframe yang akan digunakan untuk menghitung Previous High/Low (misalnya "D1", "H4").
        candles_df (pd.DataFrame): DataFrame candle historis (sudah dalam format float).
        current_atr_value (Decimal): Nilai ATR saat ini untuk timeframe ini.
    Returns:
        int: Jumlah event Previous High/Low baru yang berhasil dideteksi dan dikirim ke antrean DB.
    """
    logger.info(f"Mendeteksi Previous High/Low BARU historis untuk {symbol_param} {timeframe_str}...")
    processed_count = 0

    if candles_df.empty or len(candles_df) < config.AIAnalysts.SWING_EXT_BARS * 2 + 1:
        logger.debug(f"Tidak cukup lilin ({len(candles_df)}) untuk deteksi Previous High/Low historis di {timeframe_str}.")
        return 0

    # Pastikan _SYMBOL_POINT_VALUE sudah diinisialisasi
    if globals().get('_SYMBOL_POINT_VALUE') is None:
        globals().get('_initialize_symbol_point_value')()
    if globals().get('_SYMBOL_POINT_VALUE') is None:
        logger.error(f"Nilai POINT untuk {symbol_param} tidak tersedia. Melewatkan deteksi Previous High/Low.")
        return 0

    # --- MODIFIKASI INI: Pindahkan inisialisasi dan renaming df_processed ke sini ---
    # Buat salinan DataFrame dan konversi kolom harga ke float untuk perhitungan internal
    df_processed = candles_df.copy()

    # Rename kolom dan konversi ke float untuk df_processed
    df_processed = df_processed.rename(columns={
        'open_price': 'open', 'high_price': 'high', 'low_price': 'low',
        'close_price': 'close', 'tick_volume': 'volume', 'real_volume': 'real_volume', 'spread': 'spread'
    })
    # KOREKSI: HANYA konversi kolom harga dan volume
    for col in ['open', 'high', 'low', 'close', 'volume', 'real_volume', 'spread']:
        if col in df_processed.columns:
            df_processed[col] = df_processed[col].apply(utils.to_float_or_none)

    # Tambahkan validasi jika DataFrame kosong setelah renaming/konversi
    if df_processed.empty:
        logger.warning(f"PHL Detektor: DataFrame df_processed kosong setelah renaming/konversi. Melewatkan deteksi PHL untuk {timeframe_str}.")
        return 0

    # Hitung Previous High/Low menggunakan fungsi internal
    phl_results_df = globals().get('_calculate_previous_high_low_internal')(df_processed, higher_timeframe_for_phl)

    if phl_results_df.empty or (phl_results_df['BrokenHigh'].sum() == 0 and phl_results_df['BrokenLow'].sum() == 0):
        logger.debug(f"Tidak ada Previous High/Low yang ditembus untuk {symbol_param} {timeframe_str}.")
        return 0

    # --- MODIFIKASI INI: Hitung toleransi penembusan dinamis menggunakan ATR ---
    break_tolerance_value = Decimal('0.0') # Default
    if current_atr_value is not None and current_atr_value > Decimal('0.0'):
        # Gunakan ATR_MULTIPLIER_FOR_TOLERANCE dari MarketData untuk toleransi break PHL
        break_tolerance_value = current_atr_value * config.MarketData.ATR_MULTIPLIER_FOR_TOLERANCE
        logger.debug(f"PHL Detektor: Menggunakan break_tolerance dinamis ATR: {float(break_tolerance_value):.5f} (dari ATR {float(current_atr_value):.5f})")
    else:
        # Fallback ke nilai statis jika ATR tidak tersedia atau nol
        break_tolerance_value = config.RuleBasedStrategy.RULE_SR_TOLERANCE_POINTS * globals().get('_SYMBOL_POINT_VALUE')
        logger.warning(f"PHL Detektor: ATR tidak valid. Menggunakan break_tolerance statis: {float(break_tolerance_value):.5f}")
    # --- AKHIR MODIFIKASI ---

    # --- Deduplikasi: Ambil event struktur pasar yang sudah ada di DB ---
    existing_phl_event_keys = set()
    try:
        existing_events_db = database_manager.get_market_structure_events(
            symbol=symbol_param,
            timeframe=timeframe_str,
            event_type=['Previous High Broken', 'Previous Low Broken'],
            start_time_utc=df_processed.index.min().to_pydatetime()
        )
        for event_item in existing_events_db:
            event_time_dt = utils.to_utc_datetime_or_none(event_item['event_time_utc'])
            if event_time_dt:
                existing_phl_event_keys.add((
                    event_item['symbol'], event_item['timeframe'],
                    event_time_dt.replace(microsecond=0).isoformat(), event_item['event_type']
                ))
        logger.debug(f"Ditemukan {len(existing_phl_event_keys)} event Previous High/Low yang sudah ada di DB untuk {timeframe_str}.")
    except Exception as e:
        logger.error(f"Gagal mengambil existing Previous High/Low events dari DB untuk pre-filtering: {e}", exc_info=True)
        pass

    detected_phl_events_to_save_batch = []

    # Iterasi melalui hasil Previous High/Low
    for i in range(len(phl_results_df)):
        current_index = phl_results_df.index[i]
        event_time_dt = current_index.to_pydatetime()
        
        # Ambil data lilin lengkap untuk konfirmasi close
        # Pastikan df_processed memiliki kolom 'close', 'high', 'low'
        candle_close_dec = utils.to_decimal_or_none(df_processed.loc[current_index, 'close']) 
        candle_high_dec = utils.to_decimal_or_none(df_processed.loc[current_index, 'high']) 
        candle_low_dec = utils.to_decimal_or_none(df_processed.loc[current_index, 'low'])   

        if candle_close_dec is None or candle_high_dec is None or candle_low_dec is None:
            continue

        # Deteksi Previous High Broken
        # MODIFIKASI INI: Perketat kriteria break dengan konfirmasi candle_close
        if phl_results_df['BrokenHigh'].iloc[i] == 1:
            price_level = utils.to_decimal_or_none(phl_results_df['PreviousHigh'].iloc[i])
            if price_level is not None:
                # Konfirmasi break: candle_high melewati level DAN candle_close di atas level + toleransi
                if candle_high_dec > price_level and \
                   candle_close_dec > price_level + break_tolerance_value: # Memerlukan close confirmation
                    event_type = "Previous High Broken"
                    direction = "Bullish"
                    new_key = (symbol_param, timeframe_str, event_time_dt.replace(microsecond=0).isoformat(), event_type)
                    if new_key not in existing_phl_event_keys:
                        detected_phl_events_to_save_batch.append({
                            "symbol": symbol_param, "timeframe": timeframe_str, "event_type": event_type,
                            "direction": direction, "price_level": price_level, "event_time_utc": event_time_dt,
                            "swing_high_ref_time": None, "swing_low_ref_time": None 
                        })
                        existing_phl_event_keys.add(new_key)
                        logger.debug(f"Previous High Broken terdeteksi di {timeframe_str} pada {event_time_dt} (Close: {float(candle_close_dec):.5f}).")

        # Deteksi Previous Low Broken
        # MODIFIKASI INI: Perketat kriteria break dengan konfirmasi candle_close
        if phl_results_df['BrokenLow'].iloc[i] == 1:
            price_level = utils.to_decimal_or_none(phl_results_df['PreviousLow'].iloc[i])
            if price_level is not None:
                # Konfirmasi break: candle_low melewati level DAN candle_close di bawah level - toleransi
                if candle_low_dec < price_level and \
                   candle_close_dec < price_level - break_tolerance_value: # Memerlukan close confirmation
                    event_type = "Previous Low Broken"
                    direction = "Bearish"
                    new_key = (symbol_param, timeframe_str, event_time_dt.replace(microsecond=0).isoformat(), event_type)
                    if new_key not in existing_phl_event_keys:
                        detected_phl_events_to_save_batch.append({
                            "symbol": symbol_param, "timeframe": timeframe_str, "event_type": event_type,
                            "direction": direction, "price_level": price_level, "event_time_utc": event_time_dt,
                            "swing_high_ref_time": None, "swing_low_ref_time": None 
                        })
                        existing_phl_event_keys.add(new_key)
                        logger.debug(f"Previous Low Broken terdeteksi di {timeframe_str} pada {event_time_dt} (Close: {float(candle_close_dec):.5f}).")

    if detected_phl_events_to_save_batch:
        try:
            database_manager.save_market_structure_events_batch(detected_phl_events_to_save_batch, [])
            processed_count += len(detected_phl_events_to_save_batch)
            logger.info(f"BACKFILL WORKER: Berhasil menyimpan {len(detected_phl_events_to_save_batch)} event Previous High/Low baru unik untuk {symbol_param} {timeframe_str}.")
        except Exception as e:
            logger.error(f"BACKFILL WORKER: Gagal menyimpan event Previous High/Low batch: {e}", exc_info=True)

    logger.info(f"Selesai mendeteksi Previous High/Low BARU historis untuk {symbol_param} {timeframe_str}. Deteksi: {processed_count}.")
    return processed_count


def _detect_new_fair_value_gaps_historically(symbol_param: str, timeframe_str: str, candles_df: pd.DataFrame, current_atr_value: Decimal) -> int:
    """
    Mendeteksi Fair Value Gaps (FVG) baru secara historis.
    ...
    """
    logger.info(f"Mendeteksi Fair Value Gaps BARU historis untuk {symbol_param} {timeframe_str}...")
    processed_count = 0

    if candles_df.empty or len(candles_df) < 3:
        logger.debug(f"Tidak cukup lilin ({len(candles_df)}) untuk deteksi FVG historis di {timeframe_str}. Diperlukan minimal 3.")
        return 0

    if globals().get('_SYMBOL_POINT_VALUE') is None:
        globals().get('_initialize_symbol_point_value')()
    if globals().get('_SYMBOL_POINT_VALUE') is None:
        logger.error(f"Nilai POINT untuk {symbol_param} tidak tersedia. Melewatkan deteksi FVG.")
        return 0
    
    min_fvg_value = config.AIAnalysts.FVG_MIN_DOLLARS
    fvg_min_candle_body_percent_for_strength = config.RuleBasedStrategy.FVG_MIN_CANDLE_BODY_PERCENT_FOR_STRENGTH
    fvg_volume_factor_for_strength = config.RuleBasedStrategy.FVG_VOLUME_FACTOR_FOR_STRENGTH
    
    if current_atr_value is not None and current_atr_value > Decimal('0.0'):
        min_fvg_value = current_atr_value * config.MarketData.ATR_MULTIPLIER_FOR_TOLERANCE 
        logger.debug(f"FVG Detektor: Menggunakan min_fvg_value dinamis ATR: {float(min_fvg_value):.5f} (dari ATR {float(current_atr_value):.5f})")
    else:
        min_fvg_value = config.AIAnalysts.FVG_MIN_DOLLARS
        logger.warning(f"FVG Detektor: ATR tidak valid. Menggunakan min_fvg_value statis: {float(min_fvg_value):.5f}")

    df_processed = candles_df.copy()

    df_processed = df_processed.rename(columns={
        'open_price': 'open',
        'high_price': 'high',
        'low_price': 'low',
        'close_price': 'close',
        'tick_volume': 'volume',
        'real_volume': 'real_volume',
        'spread': 'spread'
    })

    for col in ['open', 'high', 'low', 'close', 'volume', 'real_volume', 'spread']:
        if col in df_processed.columns:
            df_processed[col] = df_processed[col].apply(utils.to_float_or_none)
            if col in ['open', 'high', 'low', 'close']:
                initial_len = len(df_processed)
                df_processed.dropna(subset=[col], inplace=True)
                if len(df_processed) < initial_len:
                    logger.warning(f"FVG Detektor: Dihapus {initial_len - len(df_processed)} baris dengan {col} NaN di {timeframe_str} (setelah konversi float).")

    if df_processed.empty or len(df_processed) < 3:
        logger.warning(f"FVG Detektor: DataFrame df_processed kosong atau tidak cukup lilin ({len(df_processed)}) setelah pembersihan NaN. Melewatkan deteksi FVG untuk {timeframe_str}.")
        return 0

    fvg_results_df = _calculate_fvg_internal(df_processed, join_consecutive=False)

    if fvg_results_df.empty or fvg_results_df['FVG'].isnull().all():
        logger.debug(f"Tidak ada FVG yang terdeteksi oleh internal FVG calculator untuk {symbol_param} {timeframe_str}.")
        return 0

    existing_fvg_keys = set()
    try:
        existing_fvgs_db = database_manager.get_fair_value_gaps(
            symbol=symbol_param,
            timeframe=timeframe_str,
            is_filled=False,
            start_time_utc=df_processed.index.min().to_pydatetime(),
            end_time_utc=df_processed.index.max().to_pydatetime() + timedelta(days=1)
        )
        for fvg_item in existing_fvgs_db:
            formation_time_dt = utils.to_utc_datetime_or_none(fvg_item.get('formation_time_utc'))
            if formation_time_dt:
                existing_fvg_keys.add((
                    fvg_item['symbol'],
                    fvg_item['timeframe'],
                    fvg_item['type'],
                    formation_time_dt.replace(microsecond=0).isoformat()
                ))
        logger.debug(f"Ditemukan {len(existing_fvg_keys)} FVG aktif yang sudah ada di DB untuk {timeframe_str} (untuk pre-filtering).")
    except Exception as e:
        logger.error(f"Gagal mengambil existing FVG dari DB untuk {timeframe_str} (untuk pre-filtering): {e}", exc_info=True)
        pass

    detected_fvgs_to_save_batch = []
    BATCH_SAVE_THRESHOLD_FVG = config.System.DATABASE_BATCH_SIZE

    try:
        for i in range(2, len(df_processed)):
            candle_0 = df_processed.iloc[i]
            candle_1 = df_processed.iloc[i - 1] # Lilin tengah, lokasi FVG
            candle_2 = df_processed.iloc[i - 2]

            fvg_top_price = None
            fvg_bottom_price = None
            fvg_type = None
            is_fvg_valid = False

            if candle_0['low'] > candle_2['high']:
                gap_size = candle_0['low'] - candle_2['high']
                if gap_size > min_fvg_value:
                    fvg_type = "Bullish Imbalance"
                    fvg_top_price = candle_0['low']
                    fvg_bottom_price = candle_2['high']
                    is_fvg_valid = True

            elif candle_0['high'] < candle_2['low']:
                gap_size = candle_2['low'] - candle_0['high']
                if gap_size > min_fvg_value:
                    fvg_type = "Bearish Imbalance"
                    fvg_top_price = candle_2['low']
                    fvg_bottom_price = candle_0['high']
                    is_fvg_valid = True

            if is_fvg_valid:
                formation_time = candle_1.name.to_pydatetime()
                
                # --- Perbaikan: AKSES FV_RESULTS_DF PADA INDEKS LILIN TENGAH (i-1) ---
                # Menggunakan .loc[candle_1.name] untuk akses yang lebih andal berdasarkan indeks waktu
                # Ini akan mengambil baris yang benar dari fvg_results_df (yang memiliki indeks waktu yang sama dengan df_processed)
                fvg_result_for_current_pattern = fvg_results_df.loc[candle_1.name] 

                # Perhitungan strength_score FVG ---
                calculated_fvg_strength = 0
                impulsive_candle_for_strength = df_processed.iloc[i] 

                if impulsive_candle_for_strength is not None:
                    impulsive_body_size = abs(impulsive_candle_for_strength['close'] - impulsive_candle_for_strength['open'])
                    impulsive_range_size = impulsive_candle_for_strength['high'] - impulsive_candle_for_strength['low']

                    if impulsive_range_size > 0:
                        body_to_range_ratio = impulsive_body_size / impulsive_range_size
                        if body_to_range_ratio >= fvg_min_candle_body_percent_for_strength:
                            calculated_fvg_strength += 1 

                    if impulsive_candle_for_strength['volume'] is not None and pd.notna(impulsive_candle_for_strength['volume']):
                        volume_dec = utils.to_decimal_or_none(impulsive_candle_for_strength['volume'])
                        if volume_dec is not None and volume_dec > 0:
                            calculated_fvg_strength += int(volume_dec / Decimal('1000') * fvg_volume_factor_for_strength)

                new_fvg_key = (symbol_param, timeframe_str, fvg_type, formation_time.replace(microsecond=0).isoformat())
                
                if new_fvg_key not in existing_fvg_keys:
                    
                    # --- Perbaikan Logika Mitigasi (last_fill_time_utc) ---
                    # is_filled_status dan last_fill_time harus diambil dari fvg_result_for_current_pattern
                    is_filled_status = pd.notna(fvg_result_for_current_pattern['MitigatedIndex'])
                    last_fill_time = None
                    if is_filled_status:
                        mitigation_idx_float = fvg_result_for_current_pattern['MitigatedIndex']
                        if pd.notna(mitigation_idx_float):
                            mitigation_idx_int = int(mitigation_idx_float)
                            # Gunakan candles_df asli (input ke fungsi ini) untuk mendapatkan timestamp
                            # karena MitigatedIndex dari mock mungkin merujuk ke indeks posisi dari candles_df asli
                            if 0 <= mitigation_idx_int < len(candles_df):
                                last_fill_time = candles_df.index[mitigation_idx_int].to_pydatetime()
                            else:
                                logger.warning(f"FVG Detektor: MitigatedIndex {mitigation_idx_int} dari _calculate_fvg_internal di luar batas candles_df. Tidak dapat mengatur last_fill_time_utc.")
                    # --- Akhir Perbaikan Logika Mitigasi ---

                    detected_fvgs_to_save_batch.append({
                        "symbol": symbol_param,
                        "timeframe": timeframe_str,
                        "type": fvg_type,
                        "fvg_top_price": utils.to_decimal_or_none(fvg_top_price),
                        "fvg_bottom_price": utils.to_decimal_or_none(fvg_bottom_price),
                        "formation_time_utc": formation_time,
                        "is_filled": is_filled_status,
                        "last_fill_time_utc": last_fill_time,
                        "retest_count": 0,
                        "last_retest_time_utc": None,
                        "strength_score": calculated_fvg_strength
                    })
                    existing_fvg_keys.add(new_fvg_key)
                    logger.debug(f"FVG {fvg_type} terdeteksi di {timeframe_str} pada {formation_time} (Strength: {calculated_fvg_strength}).")

                if len(detected_fvgs_to_save_batch) >= BATCH_SAVE_THRESHOLD_FVG:
                    try:
                        database_manager.save_fair_value_gaps_batch(detected_fvgs_to_save_batch, [])
                        processed_count += len(detected_fvgs_to_save_batch)
                        logger.info(f"BACKFILL WORKER: Auto-saved {len(detected_fvgs_to_save_batch)} new FVG for {timeframe_str}.")
                        detected_fvgs_to_save_batch = []
                    except Exception as e:
                        logger.error(f"BACKFILL WORKER: Gagal auto-save FVG batch: {e}", exc_info=True)
                        detected_fvgs_to_save_batch = []

    except Exception as e:
        logger.error(f"Error saat mendeteksi FVG BARU historis untuk {symbol_param} {timeframe_str}: {e}", exc_info=True)
    finally:
        if detected_fvgs_to_save_batch:
            try:
                database_manager.save_fair_value_gaps_batch(detected_fvgs_to_save_batch, [])
                processed_count += len(detected_fvgs_to_save_batch)
                logger.info(f"BACKFILL WORKER: Final auto-saved {len(detected_fvgs_to_save_batch)} remaining new FVG for {timeframe_str}.")
            except Exception as e:
                logger.error(f"BACKFILL WORKER: Gagal final auto-save FVG batch: {e}", exc_info=True)
        
    logger.info(f"Selesai mendeteksi FVG BARU unik historis untuk {symbol_param} {timeframe_str}. Deteksi: {processed_count}.")
    return processed_count

def _update_existing_liquidity_zones_status(symbol_param: str, timeframe_str: str, all_candles_df: pd.DataFrame, current_atr_value: Decimal) -> int:
    """
    Memperbarui status 'is_tapped' dan 'last_tapped_time_utc' untuk Liquidity Zones yang sudah ada di database.
    Args:
        symbol_param (str): Simbol trading.
        timeframe_str (str): Timeframe.
        all_candles_df (pd.DataFrame): DataFrame candle historis. DIHARAPKAN SUDAH DALAM FORMAT
                                       KOLOM PENDEK ('open', 'high', 'low', 'close') SEBAGAI FLOAT.
        current_atr_value (Decimal): Nilai ATR saat ini untuk timeframe ini.
    Returns:
        int: Jumlah Liquidity Zones yang statusnya berhasil diperbarui dan dikirim ke antrean DB.
    """
    logger.info(f"Memeriksa invalidasi Liquidity Zones untuk {symbol_param} {timeframe_str}...")
    processed_count = 0 

    # 1. Validasi Input Awal
    if all_candles_df.empty:
        logger.debug(f"DataFrame candle kosong untuk update status Liquidity Zones di {timeframe_str}.")
        return 0 

    # --- MULAI PENANGANAN DATAFRAME UTAMA DI SINI ---
    # Karena all_candles_df DIHARAPKAN sudah di-rename dan dikonversi ke float di pemanggil (periodic_combined_advanced_detection_loop),
    # kita hanya perlu memvalidasi dan membersihkannya di sini.
    # Jika Anda ingin renaming dan konversi eksplisit di awal setiap fungsi detektor untuk keamanan ekstra,
    # Anda bisa menyalin blok renaming dari FVG atau OB ke sini juga, seperti ini:
    
    df_processed_local = all_candles_df.copy()

    # Pastikan kolom-kolomnya bertipe numerik (float) dan dropna jika ada NaN
    # Ini adalah lapisan validasi defensif, meskipun diharapkan sudah rapi dari pemanggil.
    numeric_cols_to_check = ['open', 'high', 'low', 'close'] # Kolom yang paling sering menyebabkan masalah
    for col in numeric_cols_to_check:
        if col not in df_processed_local.columns:
            logger.error(f"LIQ_STATUS_DEBUG: Kolom '{col}' tidak ditemukan di DataFrame input. Ini tidak sesuai harapan. Melewatkan fungsi.")
            return 0 # Keluar jika kolom esensial tidak ada

        df_processed_local[col] = df_processed_local[col].apply(utils.to_float_or_none)

    initial_len_local = len(df_processed_local)
    df_processed_local.dropna(subset=numeric_cols_to_check, inplace=True)
    if len(df_processed_local) < initial_len_local:
        logger.warning(f"LIQ_STATUS_DEBUG: Dihapus {initial_len_local - len(df_processed_local)} baris dengan NaN di kolom OHLC (setelah konversi float).")

    if df_processed_local.empty:
        logger.warning(f"LIQ_STATUS_DEBUG: DataFrame kosong setelah pembersihan NaN. Melewatkan update status Liquidity Zones.")
        return 0
    # --- AKHIR PENANGANAN DATAFRAME UTAMA DI SINI ---


    # 2. Inisialisasi _SYMBOL_POINT_VALUE (jika belum)
    if globals().get('_SYMBOL_POINT_VALUE') is None:
        globals().get('_initialize_symbol_point_value')()
    if globals().get('_SYMBOL_POINT_VALUE') is None:
        logger.error(f"Nilai POINT untuk {symbol_param} tidak tersedia. Melewatkan pembaruan status Liquidity Zones.")
        return 0

    # 3. Ambil Batas Harga Dinamis Global
    with globals().get('_dynamic_price_lock', threading.Lock()):
        min_price_for_update_filter = globals().get('_dynamic_price_min_for_analysis')
        max_price_for_update_filter = globals().get('_dynamic_price_max_for_analysis')

    # 4. Mendapatkan Zona Likuiditas Aktif dari Database
    untapped_liqs = database_manager.get_liquidity_zones(
        symbol=symbol_param, timeframe=timeframe_str, is_tapped=False,
        min_price_level=min_price_for_update_filter,
        max_price_level=max_price_for_update_filter
    )

    if not untapped_liqs:
        logger.debug(f"Tidak ada Liquidity Zones aktif untuk diperbarui statusnya di {symbol_param} {timeframe_str}.")
        return 0

    # 5. Menentukan Toleransi Penembusan/Sentuhan (Tap Tolerance)
    if current_atr_value is not None and current_atr_value > Decimal('0.0'):
        tolerance_value_dec = current_atr_value * config.MarketData.ATR_MULTIPLIER_FOR_TOLERANCE
        logger.debug(f"Liquidity Status Updater: Menggunakan toleransi dinamis ATR: {float(tolerance_value_dec):.5f} (dari ATR {float(current_atr_value):.5f})")
    else:
        tolerance_value_dec = Decimal(str(config.RuleBasedStrategy.RULE_EQUAL_LEVEL_TOLERANCE_POINTS)) * globals().get('_SYMBOL_POINT_VALUE')
        logger.warning(f"Liquidity Status Updater: ATR tidak valid. Menggunakan toleransi statis: {float(tolerance_value_dec):.5f}")

    updated_liqs_batch = []

    # 6. Menentukan Waktu Mulai Pencarian Lilin yang Relevan (Optimasi)
    # --- MULAI MODIFIKASI UNTUK PENANGANAN NoneType PADA latest_active_liq_time ---
    list_for_max_liq = []
    for liq_item in untapped_liqs:
        # Prioritaskan last_tapped_time_utc, lalu formation_time_utc
        effective_timestamp = liq_item.get('last_tapped_time_utc')
        if effective_timestamp is None:
            effective_timestamp = liq_item.get('formation_time_utc')
        
        # Pastikan effective_timestamp adalah objek datetime yang valid.
        processed_timestamp = utils.to_utc_datetime_or_none(effective_timestamp)
        
        if processed_timestamp is not None:
            list_for_max_liq.append(processed_timestamp)
        else:
            logger.warning(f"LIQ Status Updater: Timestamp untuk LIQ ID {liq_item.get('id', 'N/A')} resolve ke None setelah konversi. Dilewati untuk perhitungan max.")

    if list_for_max_liq:
        latest_active_liq_time = max(list_for_max_liq)
    else:
        # Jika daftar kosong (semua timestamp tidak valid atau untapped_liqs kosong), gunakan waktu fallback yang aman
        latest_active_liq_time = datetime(1970, 1, 1, tzinfo=timezone.utc)
        logger.warning(f"LIQ Status Updater: Tidak ada timestamp valid untuk menghitung max. Menggunakan default {latest_active_liq_time.isoformat()}.")
    # --- AKHIR MODIFIKASI UNTUK PENANGANAN NoneType PADA latest_active_liq_time ---

    relevant_candles_for_test = df_processed_local[ 
        df_processed_local.index > latest_active_liq_time
    ].copy() 

    if relevant_candles_for_test.empty and untapped_liqs:
        logger.debug(f"Tidak ada lilin baru setelah Liquidity Zone aktif terbaru untuk {symbol_param} {timeframe_str}. Melewatkan update status Liquidity Zones.")
        return 0

    # 7. Iterasi Melalui Zona Likuiditas Aktif dan Lilin yang Relevan
    for liq_zone in untapped_liqs:
        liq_id = liq_zone['id']
        liq_price_level = liq_zone['price_level']
        liq_zone_type = liq_zone['zone_type']
        
        is_still_active = True
        
        # Inisialisasi last_tapped_time untuk zona saat ini (dari DB atau formation_time)
        current_last_tapped_time_initial = liq_zone.get('last_tapped_time_utc')
        if current_last_tapped_time_initial is None:
            current_last_tapped_time_initial = liq_zone.get('formation_time_utc', datetime(1970, 1, 1, tzinfo=timezone.utc))
            if current_last_tapped_time_initial is None:
                current_last_tapped_time_initial = datetime(1970, 1, 1, tzinfo=timezone.utc)
                logger.warning(f"LIQ_STATUS_DEBUG: Zone {liq_id} tidak memiliki formation_time_utc atau last_tapped_time_utc. Menggunakan default 1970-01-01.")
        
        last_tapped_time_for_update = current_last_tapped_time_initial # Variabel yang akan diupdate

        # Filter lilin yang relevan untuk *zona spesifik ini*
        liq_relevant_candles = relevant_candles_for_test[
            relevant_candles_for_test.index > current_last_tapped_time_initial
        ].copy()

        if liq_relevant_candles.empty:
            logger.debug(f"LIQ_STATUS_DEBUG: Zone {liq_zone_type} (ID: {liq_id}) dilewati karena tidak ada lilin baru setelah last_tapped_time_utc ({current_last_tapped_time_initial}).")
            continue


        for _, candle_row in liq_relevant_candles.iterrows():
            if not is_still_active: 
                break

            # --- MULAI PENAMBAHAN KODE UNTUK NAMA KOLOM high/low/close DALAM LOOP ---
            # Karena df_processed_local (dan turunannya relevant_candles_for_test & liq_relevant_candles)
            # sudah di-rename di atas, kita bisa langsung mengaksesnya dengan nama pendek.
            candle_high = candle_row['high']
            candle_low = candle_row['low']
            candle_close = candle_row['close']
            
            # Ini adalah validasi akhir untuk memastikan tidak ada None/NaN yang lolos
            if candle_high is None or candle_low is None or candle_close is None:
                logger.debug(f"LIQ_STATUS_DEBUG: Lilin dengan NaN high/low/close setelah konversi untuk LIQ {liq_id} di {candle_row.name}. Melewatkan pengecekan.")
                continue # Lewati lilin ini jika ada NaN setelah konversi
            # --- AKHIR PENAMBAHAN KODE UNTUK NAMA KOLOM high/low/close DALAM LOOP ---

            candle_time_dt = candle_row.name.to_pydatetime()

            is_tapped_now = False
            
            # Logika mitigasi (tap): Harga menembus zona likuiditas dan reverse
            if liq_zone_type == "Equal Highs":
                # Dianggap disapu jika high lilin menembus level DAN close lilin di bawah level (pembalikan)
                if candle_high > utils.to_float_or_none(liq_price_level) + utils.to_float_or_none(tolerance_value_dec) and \
                   candle_close < utils.to_float_or_none(liq_price_level) - utils.to_float_or_none(tolerance_value_dec): # Konfirmasi reversal
                    is_tapped_now = True
                    logger.debug(f"LIQ_STATUS_DEBUG: Zone Equal Highs (ID: {liq_id}, Level: {float(liq_price_level):.5f}) disapu (Close: {float(candle_close):.5f}).")
            elif liq_zone_type == "Equal Lows":
                # Dianggap disapu jika low lilin menembus level DAN close lilin di atas level (pembalikan)
                if candle_low < utils.to_float_or_none(liq_price_level) - utils.to_float_or_none(tolerance_value_dec) and \
                   candle_close > utils.to_float_or_none(liq_price_level) + utils.to_float_or_none(tolerance_value_dec): # Konfirmasi reversal
                    is_tapped_now = True
                    logger.debug(f"LIQ_STATUS_DEBUG: Zone Equal Lows (ID: {liq_id}, Level: {float(liq_price_level):.5f}) disapu (Close: {float(candle_close):.5f}).")

            if is_tapped_now:
                is_still_active = False
                last_tapped_time_for_update = candle_time_dt # Update waktu tap
                logger.debug(f"LIQ_STATUS_DEBUG: Zone {liq_id} tapped. last_tapped_time updated to {last_tapped_time_for_update.isoformat()}.")
                break # Zone tapped, tidak perlu cek candle selanjutnya untuk zone ini
            
        logger.debug(f"LIQ_STATUS_DEBUG: Zone {liq_id} final status: is_tapped={not is_still_active}, last_tapped_time={last_tapped_time_for_update.isoformat() if last_tapped_time_for_update else 'None'}")
        
        if (liq_zone['is_tapped'] != (not is_still_active)) or \
           (utils.to_utc_datetime_or_none(liq_zone.get('last_tapped_time_utc')) != last_tapped_time_for_update):
            
            updated_liqs_batch.append({
                'id': liq_id,
                'is_tapped': not is_still_active, # True jika tidak aktif
                'tap_time_utc': last_tapped_time_for_update, # Gunakan nama kolom ini
                'symbol': liq_zone['symbol'],
                'timeframe': liq_zone['timeframe'],
                'zone_type': liq_zone['zone_type'],
                'price_level': liq_price_level,
                'formation_time_utc': liq_zone['formation_time_utc'],
                'strength_score': liq_zone.get('strength_score'),
                'confluence_score': liq_zone.get('confluence_score', 0),
                'last_tapped_time_utc': last_tapped_time_for_update # Tambahkan ini
            })
            processed_count += 1
            logger.debug(f"LIQ_STATUS_DEBUG: Zone {liq_id} ditambahkan ke batch update. Processed count: {processed_count}")

    # 8. Menyimpan Batch Update ke Database
    if updated_liqs_batch:
        # Konversi tipe data Decimal dan datetime ke float dan string ISO untuk batch update
        processed_updated_liqs_batch = []
        for liq_data_dict in updated_liqs_batch:
            processed_item_dict = liq_data_dict.copy()
            for key, value in processed_item_dict.items():
                if isinstance(value, Decimal):
                    processed_item_dict[key] = utils.to_float_or_none(value)
                elif isinstance(value, datetime):
                    processed_item_dict[key] = utils.to_iso_format_or_none(value)
            processed_updated_liqs_batch.append(processed_item_dict)

        try:
            database_manager.save_liquidity_zones_batch([], processed_updated_liqs_batch)
            logger.info(f"BACKFILL WORKER: Berhasil mem-batch update {len(processed_updated_liqs_batch)} status Liquidity Zones untuk {symbol_param} {timeframe_str}.")
        except Exception as e:
            logger.error(f"BACKFILL WORKER: Gagal mem-batch update status Liquidity Zones: {e}", exc_info=True)

    logger.info(f"Selesai memeriksa invalidasi Liquidity Zones untuk {symbol_param} {timeframe_str}. {processed_count} diperbarui.")
    return processed_count


def _find_historical_price_extremes_in_range(symbol_param: str, start_date: datetime, end_date: datetime) -> tuple[Decimal, Decimal]:
    """
    Mencari harga 'low' terendah dan 'high' tertinggi dalam rentang tanggal yang diberikan dari candle D1.
    Args:
        symbol_param (str): Simbol trading.
        start_date (datetime): Tanggal mulai rentang pencarian.
        end_date (datetime): Tanggal akhir rentang pencarian.
    Returns:
        tuple[Decimal, Decimal]: (Harga low terendah, Harga high tertinggi) yang ditemukan,
                                  atau (None, None) jika tidak ada data.
    """
    logger.info(f"Mencari harga low terendah dan high tertinggi untuk {symbol_param} dari {start_date.isoformat()} hingga {end_date.isoformat()}...")

    d1_candles = database_manager.get_historical_candles_from_db(
        symbol_param, # Ini adalah argumen posisi pertama: symbol_param
        "D1",         # MODIFIKASI INI: HAPUS 'timeframe='
        start_time_utc=start_date,
        end_time_utc=end_date,
        order_asc=True,
        min_open_time_utc=start_date
    )

    if not d1_candles:
        logger.warning(f"Tidak ada data candle D1 yang ditemukan untuk {symbol_param} dalam rentang {start_date} - {end_date}. Tidak dapat menemukan ekstrem historis.")
        return None, None

    df_d1 = pd.DataFrame(d1_candles)
    df_d1['low_price'] = df_d1['low_price'].apply(lambda x: Decimal(str(x)))
    df_d1['high_price'] = df_d1['high_price'].apply(lambda x: Decimal(str(x)))

    historical_low = df_d1['low_price'].min()
    historical_high = df_d1['high_price'].max()

    logger.info(f"Ekstrem harga yang ditemukan untuk {symbol_param} dalam rentang ini: LOW: {float(historical_low):.5f}, HIGH: {float(historical_high):.5f}")
    return historical_low, historical_high

def _find_historical_low_price_in_range(symbol_param: str, start_date: datetime, end_date: datetime) -> Decimal:
    """
    Mencari harga 'low' terendah dalam rentang tanggal yang diberikan dari candle D1.
    Args:
        symbol_param (str): Simbol trading.
        start_date (datetime): Tanggal mulai rentang pencarian.
        end_date (datetime): Tanggal akhir rentang pencarian.
    Returns:
        Decimal: Harga low terendah yang ditemukan, atau None jika tidak ada data.
    """
    logger.info(f"Mencari harga low terendah untuk {symbol_param} dari {start_date.isoformat()} hingga {end_date.isoformat()}...")

    # Ambil semua candle D1 dalam rentang ini
    # Gunakan min_open_time_utc untuk memastikan data dari DB sudah difilter awal
    d1_candles = database_manager.get_historical_candles_from_db(
        symbol_param, #
        "D1",
        start_time_utc=start_date,
        end_time_utc=end_date,
        order_asc=True,
        min_open_time_utc=start_date # Pastikan ini menghormati batas bawah
    )

    if not d1_candles:
        logger.warning(f"Tidak ada data candle D1 yang ditemukan untuk {symbol_param} dalam rentang {start_date} - {end_date}. Tidak dapat menemukan low historis.")
        return None

    df_d1 = pd.DataFrame(d1_candles)
    df_d1['low_price'] = df_d1['low_price'].apply(lambda x: Decimal(str(x))) # Pastikan dalam Decimal

    historical_low = df_d1['low_price'].min()
    logger.info(f"Harga low terendah yang ditemukan untuk {symbol_param} dalam rentang ini: {float(historical_low):.5f}")
    return historical_low

# --- Implementasi Fungsi identify_key_levels_across_timeframes ---
def identify_key_levels_across_timeframes(symbol_param):
    """
    Mengidentifikasi level kunci (ICT/SMC seperti Order Blocks, FVG, MS, Likuiditas, Fibonacci)
    di seluruh timeframe. Menggunakan parameter toleransi dari config.RuleBasedStrategy.
    """
    logger.info(f"Mengidentifikasi level kunci (OB, FVG, MS, Likuiditas, Swing, BoS) untuk {symbol_param}...")

    logger.debug(f"IDENTIFY_KEY_LEVELS_DIAG: _SYMBOL_POINT_VALUE at start: {globals().get('_SYMBOL_POINT_VALUE')}")
    print(f"\n--- Starting identify_key_levels_across_timeframes for {symbol_param} ---")
    print(f"Initial _SYMBOL_POINT_VALUE: {globals().get('_SYMBOL_POINT_VALUE')}")

    # AMBIL DAFTAR TIMEFRAME DARI KONFIGURASI, FILTER HANYA YANG RELEVAN UNTUK FUNGSI INI
    timeframes = [
        tf for tf, enabled in config.MarketData.ENABLED_TIMEFRAMES.items()
        if enabled and tf in ["M15", "M30", "H1", "H4", "D1"] # Tambahkan D1 jika ingin memproses D1 juga
    ]

    if not timeframes:
        logger.warning(f"IDENTIFY_KEY_LEVELS_WARNING: Tidak ada timeframe yang diaktifkan di config.MarketData.ENABLED_TIMEFRAMES untuk deteksi level kunci. Melewatkan.")
        print("No enabled timeframes found. Exiting function.")
        return

    logger.debug(f"IDENTIFY_KEY_LEVELS_DIAG: Timeframes to process (from config): {timeframes}")
    print(f"Timeframes to process (from config): {timeframes}")

    # Pastikan _SYMBOL_POINT_VALUE diinisialisasi
    _initialize_symbol_point_value()
    if _SYMBOL_POINT_VALUE is None:
        logger.error(f"IDENTIFY_KEY_LEVELS_ERROR: Nilai POINT untuk {symbol_param} tidak tersedia. Melewatkan deteksi level kunci.")
        return
    
    if not isinstance(_SYMBOL_POINT_VALUE, Decimal):
        logger.error(f"IDENTIFY_KEY_LEVELS_ERROR: _SYMBOL_POINT_VALUE bukan tipe Decimal, melainkan {type(_SYMBOL_POINT_VALUE)}. Konversi paksa atau lewati.")
        try:
            temp_symbol_point_value = Decimal(str(_SYMBOL_POINT_VALUE))
            globals()['_SYMBOL_POINT_VALUE'] = temp_symbol_point_value
            logger.warning(f"IDENTIFY_KEY_LEVELS_WARNING: _SYMBOL_POINT_VALUE dikonversi paksa ke Decimal: {temp_symbol_point_value}")
        except Exception as e:
            logger.error(f"IDENTIFY_KEY_LEVELS_ERROR: Gagal mengonversi _SYMBOL_POINT_VALUE ke Decimal: {e}. Melewatkan.")
            return

    confluence_proximity_tolerance_pips = config.RuleBasedStrategy.CONFLUENCE_PROXIMITY_TOLERANCE_PIPS
    confluence_score_per_level = config.RuleBasedStrategy.CONFLUENCE_SCORE_PER_LEVEL
    confluence_proximity_tolerance = confluence_proximity_tolerance_pips * config.PIP_UNIT_IN_DOLLAR
    
    logger.debug(f"IDENTIFY_KEY_LEVELS_DIAG: Calculated confluence_proximity_tolerance: {float(confluence_proximity_tolerance):.5f} (from {confluence_proximity_tolerance_pips} pips)")
    print(f"Calculated confluence_proximity_tolerance: {float(confluence_proximity_tolerance):.5f}")

    # --- START: Mengumpulkan SEMUA level dari SEMUA timeframe di awal ---
    all_levels_across_timeframes = []

    # Ambil semua level S&R
    all_levels_across_timeframes.extend([
        {'type_category': 'SR', 'id': l['id'], 'timeframe': l['timeframe'], 'level_type': l['level_type'], 'price': l['price_level'], 'is_active': l['is_active'], 'confluence_score': l.get('confluence_score', 0), 'db_obj_raw': l} # Store raw dict from DB
        for tf in timeframes for l in database_manager.get_support_resistance_levels(symbol=symbol_param, timeframe=tf, limit=None)
        if l.get('price_level') is not None
    ])
    # Ambil semua SD Zones
    all_levels_across_timeframes.extend([
        {'type_category': 'SD', 'id': z['id'], 'timeframe': z['timeframe'], 'zone_type': z['zone_type'], 'price': (z['zone_top_price'] + z['zone_bottom_price']) / Decimal('2'), 'top_price': z['zone_top_price'], 'bottom_price': z['zone_bottom_price'], 'is_mitigated': z['is_mitigated'], 'confluence_score': z.get('confluence_score', 0), 'db_obj_raw': z}
        for tf in timeframes for z in database_manager.get_supply_demand_zones(symbol=symbol_param, timeframe=tf, limit=None)
        if z.get('zone_top_price') is not None and z.get('zone_bottom_price') is not None
    ])
    # Ambil semua Order Blocks
    all_levels_across_timeframes.extend([
        {'type_category': 'OB', 'id': ob['id'], 'timeframe': ob['timeframe'], 'ob_type': ob['type'], 'price': (ob['ob_top_price'] + ob['ob_bottom_price']) / Decimal('2'), 'top_price': ob['ob_top_price'], 'bottom_price': ob['ob_bottom_price'], 'is_mitigated': ob['is_mitigated'], 'confluence_score': ob.get('confluence_score', 0), 'db_obj_raw': ob}
        for tf in timeframes for ob in database_manager.get_order_blocks(symbol=symbol_param, timeframe=tf, limit=None)
        if ob.get('ob_top_price') is not None and ob.get('ob_bottom_price') is not None
    ])
    # Ambil semua Fair Value Gaps
    all_levels_across_timeframes.extend([
        {'type_category': 'FVG', 'id': fvg['id'], 'timeframe': fvg['timeframe'], 'fvg_type': fvg['type'], 'price': (fvg['fvg_top_price'] + fvg['fvg_bottom_price']) / Decimal('2'), 'top_price': fvg['fvg_top_price'], 'bottom_price': fvg['fvg_bottom_price'], 'is_filled': fvg['is_filled'], 'confluence_score': fvg.get('strength_score', 0), 'db_obj_raw': fvg} # FVG uses strength_score from DB
        for tf in timeframes for fvg in database_manager.get_fair_value_gaps(symbol=symbol_param, timeframe=tf, limit=None)
        if fvg.get('fvg_top_price') is not None and fvg.get('fvg_bottom_price') is not None
    ])
    # Ambil semua Liquidity Zones
    all_levels_across_timeframes.extend([
        {'type_category': 'LIQ', 'id': lq['id'], 'timeframe': lq['timeframe'], 'zone_type': lq['zone_type'], 'price': lq['price_level'], 'is_tapped': lq['is_tapped'], 'confluence_score': lq.get('confluence_score', 0), 'db_obj_raw': lq}
        for tf in timeframes for lq in database_manager.get_liquidity_zones(symbol=symbol_param, timeframe=tf, limit=None)
        if lq.get('price_level') is not None
    ])
    # Ambil semua Fibonacci Levels
    all_levels_across_timeframes.extend([
        {'type_category': 'FIB', 'id': fib['id'], 'timeframe': fib['timeframe'], 'fib_type': fib['type'], 'price': fib['price_level'], 'is_active': fib['is_active'], 'confluence_score': fib.get('confluence_score', 0), 'db_obj_raw': fib}
        for tf in timeframes for fib in database_manager.get_fibonacci_levels(symbol=symbol_param, timeframe=tf, limit=None)
        if fib.get('price_level') is not None
    ])
    # Ambil semua Market Structure Events (MS Events tidak memiliki confluence_score di DB, tapi kita bisa hitung)
    all_levels_across_timeframes.extend([
        {'type_category': 'MS_Event', 'id': ms['id'], 'timeframe': ms['timeframe'], 'event_type': ms['event_type'], 'direction': ms['direction'], 'price': ms['price_level'], 'confluence_score': 0, 'db_obj_raw': ms} # MS Events default confluence 0
        for tf in timeframes for ms in database_manager.get_market_structure_events(symbol=symbol_param, timeframe=tf, limit=None)
        if ms.get('price_level') is not None
    ])

    logger.info(f"IDENTIFY_KEY_LEVELS_INFO: Total {len(all_levels_across_timeframes)} level dikumpulkan dari semua timeframe untuk perhitungan konfluensi.")
    print(f"Total levels collected for confluence across all timeframes: {len(all_levels_across_timeframes)}")
    # --- END MODIFIKASI: Mengumpulkan SEMUA level dari SEMUA timeframe di awal ---

    # --- START MODIFIKASI: Logika Perhitungan Confluence Score Lintas Timeframe yang Dioptimalkan ---
    # Gunakan dictionary untuk melacak level yang perlu diupdate, diindeks oleh (id, timeframe, type_category)
    levels_to_update_tracker = {}

    # 1. Urutkan semua level berdasarkan harga
    # Ini adalah langkah kunci untuk optimasi O(N log N)
    all_levels_across_timeframes.sort(key=lambda x: utils.to_float_or_none(x['price']) if x['price'] is not None else float('inf'))

    # 2. Iterasi melalui level yang sudah diurutkan dan gunakan jendela geser
    for i, main_level in enumerate(all_levels_across_timeframes):
        current_confluence_score = 0
        main_price_dec = utils.to_decimal_or_none(main_level['price'])
        
        if main_price_dec is None:
            continue # Lewati level tanpa harga valid

        # Cari tingkat lain dalam jendela yang berdekatan
        # Gunakan dua pointer atau pencarian biner untuk menemukan batas jendela
        # Optimasi: Asumsikan daftar sudah diurutkan. Cari ke kiri dan kanan hanya sejauh toleransi.
        
        # Pointer untuk jendela
        left_ptr = i - 1
        right_ptr = i + 1

        # Cek ke kiri
        while left_ptr >= 0:
            other_level = all_levels_across_timeframes[left_ptr]
            other_price_dec = utils.to_decimal_or_none(other_level['price'])
            if other_price_dec is None: # Should not happen after initial filter, but defensive
                left_ptr -= 1
                continue

            diff = abs(main_price_dec - other_price_dec)
            if diff <= confluence_proximity_tolerance:
                # Jangan hitung diri sendiri (objek yang sama)
                if not (main_level['id'] == other_level['id'] and main_level['timeframe'] == other_level['timeframe'] and main_level['type_category'] == other_level['type_category']):
                    current_confluence_score += confluence_score_per_level
            else:
                # Karena sudah diurutkan, jika sudah di luar toleransi, yang lebih jauh juga akan di luar toleransi
                break
            left_ptr -= 1

        # Cek ke kanan
        while right_ptr < len(all_levels_across_timeframes):
            other_level = all_levels_across_timeframes[right_ptr]
            other_price_dec = utils.to_decimal_or_none(other_level['price'])
            if other_price_dec is None: # Defensive
                right_ptr += 1
                continue

            diff = abs(main_price_dec - other_price_dec)
            if diff <= confluence_proximity_tolerance:
                if not (main_level['id'] == other_level['id'] and main_level['timeframe'] == other_level['timeframe'] and main_level['type_category'] == other_level['type_category']):
                    current_confluence_score += confluence_score_per_level
            else:
                break
            right_ptr += 1

        # Tambahkan skor konfluensi yang sudah ada di level itu sendiri
        # Ini untuk kasus di mana level itu sudah memiliki skor dari detektor aslinya (misal strength_score FVG)
        # Atau jika kita ingin menyertakan nilai confluence_score sebelumnya dari database
        # saat ini, confluence_score awal adalah 0, jadi ini tidak menambah dua kali.
        # Jika Anda ingin mempertimbangkan skor bawaan level (misal kekuatan SR/OB/FVG),
        # Anda harus menambahkannya ke `current_confluence_score` di sini.
        # Untuk tujuan ini, kita akan asumsikan `confluence_score` hanya dari konfluensi lintas level.

        # Hanya simpan skor baru jika berbeda dari yang ada di database (skor awal saat pengambilan data)
        # dan pastikan nilai yang akan disimpan adalah yang tertinggi
        if current_confluence_score != main_level.get('confluence_score', 0):
            main_level['confluence_score'] = current_confluence_score # Update skor di dict lokal
            levels_to_update_tracker[(main_level['id'], main_level['timeframe'], main_level['type_category'])] = {
                'confluence_score': current_confluence_score,
                'db_obj_raw': main_level['db_obj_raw'] # Simpan raw dict aslinya
            }
            logger.debug(f"CONFLUENCE_CHANGE: Level {main_level['type_category']}-{main_level['id']} ({main_level['timeframe']}) score changed from {main_level.get('confluence_score', 0)} to {current_confluence_score}.")

    # --- END MODIFIKASI: Logika Perhitungan Confluence Score Lintas Timeframe yang Dioptimalkan ---

    # --- START MODIFIKASI: Menulis Kembali Confluence Score ke Database (Batch per Model) ---
    updated_levels_grouped_by_model = defaultdict(list)

    for (level_id, timeframe, type_category), update_info in levels_to_update_tracker.items():
        raw_db_obj = update_info['db_obj_raw']
        raw_db_obj['confluence_score'] = update_info['confluence_score'] # Update skor di dict mentah

        updated_levels_grouped_by_model[type_category].append(raw_db_obj)

    for type_category, updated_objs_raw_dicts in updated_levels_grouped_by_model.items():
        if not updated_objs_raw_dicts:
            continue

        model_class = None
        save_func = None

        # Menentukan fungsi save batch yang benar
        if type_category == 'SR':
            model_class = database_manager.SupportResistanceLevel
            save_func = database_manager.save_support_resistance_levels_batch
        elif type_category == 'SD':
            model_class = database_manager.SupplyDemandZone
            save_func = database_manager.save_supply_demand_zones_batch
        elif type_category == 'OB':
            model_class = database_manager.OrderBlock
            save_func = database_manager.save_order_blocks_batch
        elif type_category == 'FVG':
            model_class = database_manager.FairValueGap
            save_func = database_manager.save_fair_value_gaps_batch
        elif type_category == 'LIQ':
            model_class = database_manager.LiquidityZone
            save_func = database_manager.save_liquidity_zones_batch
        elif type_category == 'FIB':
            model_class = database_manager.FibonacciLevel
            save_func = database_manager.save_fibonacci_levels_batch
        elif type_category == 'MS_Event':
            model_class = database_manager.MarketStructureEvent
            save_func = database_manager.save_market_structure_events_batch # Ini adalah fungsi yang kita perbaiki sebelumnya
        else:
            logger.warning(f"IDENTIFY_KEY_LEVELS_WARNING: Tipe kategori tidak dikenal untuk update confluence: {type_category}. Melewatkan.")
            continue
        
        # Konversi dictionary mentah (yang sudah diupdate confluence_score-nya)
        # ke format yang diharapkan oleh bulk_update_mappings (float untuk Decimal, ISO string untuk datetime)
        processed_updated_items_for_batch = []
        for item_dict_raw in updated_objs_raw_dicts:
            processed_item_dict = {}
            for key, value in item_dict_raw.items():
                if isinstance(value, Decimal):
                    processed_item_dict[key] = utils.to_float_or_none(value)
                elif isinstance(value, datetime):
                    processed_item_dict[key] = utils.to_iso_format_or_none(value)
                else:
                    processed_item_dict[key] = value
            processed_updated_items_for_batch.append(processed_item_dict)

        try:
            # Panggil fungsi save_batch yang relevan
            # save_func menerima (new_items_data, updated_items_data)
            # Karena ini hanya update, new_items_data akan kosong
            save_func([], processed_updated_items_for_batch)
            logger.info(f"IDENTIFY_KEY_LEVELS_INFO: Berhasil mem-batch update {len(processed_updated_items_for_batch)} {type_category} levels dengan confluence score baru.")
            print(f"Successfully updated {len(processed_updated_items_for_batch)} {type_category} levels.")
        except Exception as e:
            logger.error(f"IDENTIFY_KEY_LEVELS_ERROR: Gagal mem-batch update {type_category} levels dengan confluence score: {e}", exc_info=True)
            print(f"ERROR updating {type_category} levels: {e}")

    logger.info(f"IDENTIFY_KEY_LEVELS_INFO: Selesai mengidentifikasi level kunci (OB, FVG, MS, Likuiditas, Swing, BoS) untuk {symbol_param}.")
    print(f"--- Finished identify_key_levels_across_timeframes for {symbol_param} ---")







def _select_most_significant_ob_in_cluster(cluster: list):
    """
    Memilih Order Block yang paling signifikan dari sebuah cluster.
    Prioritas:
    1. OB yang belum mitigated.
    2. Jika status mitigasi sama, OB dengan strength_score tertinggi.
    3. Jika kekuatan sama, OB yang terbentuk paling awal (formation_time_utc paling lama - menandakan lebih estabilished).
    """
    if not cluster:
        return None

    # Gabungkan range harga (min bottom, max top) untuk cluster
    consolidated_ob_bottom = min([o['ob_bottom_price'] for o in cluster if o['ob_bottom_price'] is not None])
    consolidated_ob_top = max([o['ob_top_price'] for o in cluster if o['ob_top_price'] is not None])

    # Pilih OB yang paling signifikan berdasarkan kriteria
    best_ob = max(cluster, key=lambda x: (
        not x['is_mitigated'], # PRIORITAS 1: Belum dimitigasi (True > False)
        x.get('strength_score', 0) if x.get('strength_score') is not None else 0, # PRIORITAS 2: Kekuatan tertinggi
        -x['formation_time_utc'].timestamp() # PRIORITAS 3: Waktu formasi PALING LAMA (negarifkan timestamp agar max memilih yang paling awal)
    ))
    
    consolidated_ob = best_ob.copy() # Mulai dari OB terbaik yang dipilih
    # Tetapkan harga top/bottom yang dikonsolidasi
    consolidated_ob['ob_top_price'] = consolidated_ob_top
    consolidated_ob['ob_bottom_price'] = consolidated_ob_bottom
    
    # KOREKSI KRUSIAL DI SINI: Set is_mitigated menjadi TRUE hanya jika SEMUA OB di cluster mitigated
    consolidated_ob['is_mitigated'] = all(o['is_mitigated'] for o in cluster)
    
    # Pilih formation_time_utc terlama sebagai origin
    consolidated_ob['formation_time_utc'] = min([o['formation_time_utc'] for o in cluster])
    
    # Pilih last_mitigation_time_utc terbaru
    last_mitigation_times = [o['last_mitigation_time_utc'] for o in cluster if o.get('last_mitigation_time_utc')]
    consolidated_ob['last_mitigation_time_utc'] = max(last_mitigation_times) if last_mitigation_times else None

    logger.debug(f"Mengonsolidasi cluster {len(cluster)} OB menjadi 1: {utils.to_float_or_none(consolidated_ob_bottom):.4f}-{utils.to_float_or_none(consolidated_ob_top):.4f} ({best_ob['type']})")
    return consolidated_ob


def _consolidate_and_prioritize_order_blocks(symbol_param: str, timeframe_str: str, levels: list, ob_consolidation_tolerance_points: int):
    """
    Mengelompokkan dan mengkonsolidasi Order Blocks yang sangat berdekatan.
    Memilih OB yang paling signifikan dalam kelompok.
    Menggunakan parameter toleransi yang baru dari config.RuleBasedStrategy.
    """
    if not levels:
        return []
    
    if globals().get('_SYMBOL_POINT_VALUE') is None:
        globals().get('_initialize_symbol_point_value')()
    if globals().get('_SYMBOL_POINT_VALUE') is None:
        logger.error(f"Nilai POINT untuk {symbol_param} tidak tersedia. Melewatkan konsolidasi OB.")
        return levels

    tolerance_value = Decimal(str(ob_consolidation_tolerance_points)) * globals().get('_SYMBOL_POINT_VALUE') 

    grouped_by_type = defaultdict(list)
    for level in levels:
        grouped_by_type[level['type']].append(level)

    consolidated_levels = []

    for ob_type, type_levels in grouped_by_type.items():
        # Urutkan berdasarkan bottom price (Bullish) atau top price (Bearish) agar lilin berdekatan
        type_levels.sort(key=lambda x: x['ob_bottom_price'] if ob_type == "Bullish" else x['ob_top_price'], reverse=(ob_type == "Bearish"))
            
        current_cluster = []
        
        for ob in type_levels:
            if not current_cluster:
                current_cluster.append(ob)
            else:
                # KOREKSI PENTING DI SINI:
                # Hitung batas rentang konsolidasi saat ini dari seluruh cluster, BUKAN hanya ob terakhir
                current_cluster_max_top = max([o['ob_top_price'] for o in current_cluster])
                current_cluster_min_bottom = min([o['ob_bottom_price'] for o in current_cluster])

                overlap = False
                if ob_type == "Bullish":
                    # Bullish OB (bottom-top), cluster dari bawah ke atas.
                    # Jika OB baru overlap dengan cluster saat ini (OB.bottom <= cluster.top + tolerance)
                    if ob['ob_bottom_price'] <= current_cluster_max_top + tolerance_value:
                        overlap = True
                else: # Bearish OB (bottom-top), cluster dari atas ke bawah.
                    # Jika OB baru overlap dengan cluster saat ini (OB.top >= cluster.bottom - tolerance)
                    if ob['ob_top_price'] >= current_cluster_min_bottom - tolerance_value:
                        overlap = True

                if overlap:
                    current_cluster.append(ob)
                else:
                    consolidated_levels.append(_select_most_significant_ob_in_cluster(current_cluster))
                    current_cluster = [ob]

        if current_cluster:
            consolidated_levels.append(_select_most_significant_ob_in_cluster(current_cluster))

    # KOREKSI PENTING DI SINI: Urutkan hasil akhir untuk konsistensi di test_mixed_types_no_consolidation
    # Urutkan berdasarkan tipe (misal: 'Bearish' sebelum 'Bullish' secara alfabetis)
    # dan kemudian berdasarkan harga untuk determinisme.
    consolidated_levels.sort(key=lambda x: (x['type'], x['ob_bottom_price']))


    logger.info(f"Konsolidasi OB untuk {timeframe_str}: {len(levels)} menjadi {len(consolidated_levels)} level.")
    return consolidated_levels

def _select_most_significant_sr_level_in_cluster(cluster: list) -> dict:
    """
    Memilih level Support/Resistance paling signifikan dari sebuah cluster.
    Prioritas: 1. is_active (True > False), 2. strength_score (tertinggi), 
               3. last_test_time_utc (terbaru), 4. formation_time_utc (terlama).
    """
    if not cluster:
        return None

    best_level = max(cluster, key=lambda x: (
        x.get('is_active'), # PRIORITAS 1: Aktif lebih baik (True > False)
        x.get('strength_score', 0) if x.get('strength_score') is not None else 0, # PRIORITAS 2: Strength (tertinggi lebih baik)
        # KOREKSI PENTING: Pastikan utils.to_utc_datetime_or_none mengembalikan datetime yang valid
        # dan .timestamp() dipanggil pada objek datetime yang valid.
        (utils.to_utc_datetime_or_none(x.get('last_test_time_utc')) or datetime(1970, 1, 1, tzinfo=timezone.utc)).timestamp(), # PRIORITAS 3: Waktu terakhir diuji (terbaru lebih baik)
        -(utils.to_utc_datetime_or_none(x.get('formation_time_utc')) or datetime(1970, 1, 1, tzinfo=timezone.utc)).timestamp() # PRIORITAS 4: Waktu formasi PALING LAMA (dinegatifkan agar max memilih yang paling awal)
    ))
    return best_level

    # Kumpulkan semua info untuk level konsolidasi
    consolidated_level = best_level.copy()  # Mulai dari level terbaik yang dipilih
    consolidated_level['price_level'] = avg_price
    consolidated_level['strength_score'] = max([l.get('strength_score', 0) for l in cluster])
    consolidated_level['confluence_score'] = sum([l.get('confluence_score', 0) for l in cluster])
    consolidated_level['is_active'] = any([l['is_active'] for l in cluster]) # Aktif jika setidaknya satu aktif

    # Pilih formation_time_utc terlama sebagai origin
    consolidated_level['formation_time_utc'] = min([l['formation_time_utc'] for l in cluster])

    # Pilih last_test_time_utc terbaru
    # Pastikan ini mengambil nilai datetime yang valid
    all_last_test_times = [utils.to_utc_datetime_or_none(l.get('last_test_time_utc')) for l in cluster if l.get('last_test_time_utc') is not None]
    if all_last_test_times:
        consolidated_level['last_test_time_utc'] = max(all_last_test_times)
    else:
        consolidated_level['last_test_time_utc'] = None # Atau set ke consolidated_level['formation_time_utc'] jika tidak ada test sama sekali

    # Rentang zona konsolidasi (from min bottom to max top)
    all_zone_starts = [l.get('zone_start_price') for l in cluster if l.get('zone_start_price') is not None]
    all_zone_ends = [l.get('zone_end_price') for l in cluster if l.get('zone_end_price') is not None]
    if all_zone_starts and all_zone_ends:
        consolidated_level['zone_start_price'] = min(all_zone_starts)
        consolidated_level['zone_end_price'] = max(all_zone_ends)
    else:
        consolidated_level['zone_start_price'] = None
        consolidated_level['zone_end_price'] = None

    consolidated_level['is_key_level'] = any([l.get('is_key_level', False) for l in cluster])

    logger.debug(f"Mengonsolidasi cluster {len(cluster)} level S&R menjadi 1: {utils.to_float_or_none(avg_price):.5f} ({best_level['level_type']})")

    return consolidated_level

def _consolidate_and_prioritize_sr_levels(symbol_param: str, timeframe_str: str, levels: list, consolidation_tolerance_points: Decimal) -> list:
    """
    Mengonsolidasi level Support dan Resistance yang berdekatan dan memilih yang paling signifikan.
    Args:
        symbol_param (str): Simbol trading.
        timeframe_str (str): Timeframe.
        levels (list): Daftar dictionary level S&R mentah (dari database).
        consolidation_tolerance_points (Decimal): Toleransi dalam poin untuk mengonsolidasi level.
    Returns:
        list: Daftar level S&R yang sudah dikonsolidasi dan diprioritaskan.
    """
    if not levels:
        return []

    # Hitung toleransi dalam unit harga
    _initialize_symbol_point_value()
    if _SYMBOL_POINT_VALUE is None:
        logger.error(f"Nilai POINT untuk {symbol_param} tidak tersedia. Melewatkan konsolidasi SR.")
        return levels # Kembalikan asli jika point tidak tersedia

    tolerance_value_dec = consolidation_tolerance_points * _SYMBOL_POINT_VALUE

    # Kelompokkan level berdasarkan tipe (Support/Resistance)
    grouped_by_type = defaultdict(list)
    for level in levels:
        grouped_by_type[level['level_type']].append(level)

    consolidated_levels = []

    # Iterasi melalui setiap tipe level
    for level_type, type_levels in grouped_by_type.items():
        # Urutkan level berdasarkan harga untuk pemrosesan yang lebih mudah
        # Untuk Support (bottom up): harga terendah dulu
        # Untuk Resistance (top down): harga tertinggi dulu
        type_levels.sort(key=lambda x: x['price_level'], reverse=(level_type == "Resistance"))

        processed_ids = set()
        
        # Iterasi melalui level yang sudah diurutkan untuk membentuk cluster
        for i, level in enumerate(type_levels):
            if level['id'] in processed_ids:
                continue

            current_cluster = [level]
            processed_ids.add(level['id'])

            logger.debug(f"SR_CONSOLIDATE_DEBUG: Starting new cluster for type '{level_type}' with ID {level['id']} at price {float(level['price_level']):.5f}.")

            # Cek level yang tersisa untuk potensi konsolidasi ke dalam cluster ini
            for j in range(i + 1, len(type_levels)):
                current_level = type_levels[j]
                if current_level['id'] in processed_ids:
                    continue

                # KOREKSI: Pastikan perbandingan price_level adalah Decimal
                # Logika utama konsolidasi: jika dua level (tipe yang sama) berdekatan dalam toleransi
                if abs(level['price_level'] - current_level['price_level']) <= tolerance_value_dec:
                    current_cluster.append(current_level)
                    processed_ids.add(current_level['id'])
                    logger.debug(f"SR_CONSOLIDATE_DEBUG:   Consolidated {current_level['id']} (Price: {float(current_level['price_level']):.5f}) into cluster of {level['id']}. Diff: {float(abs(level['price_level'] - current_level['price_level'])):.5f}. Tol: {float(tolerance_value_dec):.5f}.")
                else:
                    logger.debug(f"SR_CONSOLIDATE_DEBUG:   Skipping {current_level['id']} (Price: {float(current_level['price_level']):.5f}) for cluster of {level['id']}. Diff: {float(abs(level['price_level'] - current_level['price_level'])):.5f} > Tol: {float(tolerance_value_dec):.5f}.")
                    # Karena sudah diurutkan, level selanjutnya tidak akan berdekatan, bisa break
                    break # Optimalisasi: Jika sudah tidak dalam toleransi, yang berikutnya juga tidak

            # Setelah cluster terbentuk, pilih level paling signifikan
            best_level_in_cluster = _select_most_significant_sr_level_in_cluster(current_cluster)
            
            # Gabungkan range zona dari semua level dalam cluster
            consolidated_zone_start = min([l['zone_start_price'] for l in current_cluster])
            consolidated_zone_end = max([l['zone_end_price'] for l in current_cluster])
            
            # Hitung harga rata-rata dari semua level dalam cluster
            consolidated_price_level = sum([l['price_level'] for l in current_cluster], Decimal('0.0')) / Decimal(str(len(current_cluster)))

            # Tentukan is_active untuk consolidated level (True jika ada yang aktif di cluster)
            consolidated_is_active = any(l['is_active'] for l in current_cluster)

            # Tentukan last_test_time_utc terbaru dari cluster
            consolidated_last_test_time_utc = max([l['last_test_time_utc'] for l in current_cluster if l.get('last_test_time_utc') is not None])
            if not consolidated_last_test_time_utc: # Fallback jika semua None
                consolidated_last_test_time_utc = best_level_in_cluster['formation_time_utc'] # Gunakan formation time winner sebagai fallback

            consolidated_level_data = {
                'id': best_level_in_cluster['id'], # ID dari level paling signifikan
                'symbol': symbol_param,
                'timeframe': timeframe_str,
                'level_type': level_type, # Tipe level untuk cluster
                'price_level': consolidated_price_level, # Harga rata-rata
                'zone_start_price': consolidated_zone_start,
                'zone_end_price': consolidated_zone_end,
                'strength_score': best_level_in_cluster['strength_score'], # Dari level paling signifikan
                'is_active': consolidated_is_active,
                'formation_time_utc': best_level_in_cluster['formation_time_utc'], # Dari level paling signifikan
                'last_test_time_utc': consolidated_last_test_time_utc,
                'is_key_level': best_level_in_cluster.get('is_key_level', False),
                'confluence_score': best_level_in_cluster.get('confluence_score', 0)
            }
            consolidated_levels.append(consolidated_level_data)
            logger.debug(f"Mengonsolidasi cluster {len(current_cluster)} level S&R menjadi 1: {float(consolidated_price_level):.5f} ({level_type})")

    logger.info(f"Konsolidasi S&R untuk {timeframe_str}: {len(levels)} menjadi {len(consolidated_levels)} level.")
    return consolidated_levels

def _detect_new_supply_demand_zones_historically(symbol_param: str, timeframe_str: str, candles_df: pd.DataFrame, current_atr_value: Decimal) -> int:
    """
    Mendeteksi zona Supply dan Demand baru secara historis.
    Zona S&D adalah area di mana harga menunjukkan reaksi kuat setelah pergerakan impulsif.
    Menyimpan zona S&D baru ke database secara batch dengan deduplikasi.

    Args:
        symbol_param (str): Simbol trading.
        timeframe_str (str): Timeframe (misal: "M5", "H1", dll.).
        candles_df (pd.DataFrame): DataFrame candle historis (diharapkan sudah Decimal dan memiliki kolom _price).
                                   Akan dikonversi dan di-rename secara internal.
        current_atr_value (Decimal): Nilai ATR saat ini untuk timeframe ini, digunakan untuk toleransi dinamis.

    Returns:
        int: Jumlah S&D Zones baru yang berhasil dideteksi dan dikirim ke antrean DB.
    """
    logger.info(f"Mendeteksi zona Supply/Demand BARU historis untuk {symbol_param} {timeframe_str}...")
    processed_count = 0 # Inisialisasi penghitung untuk zona baru yang terdeteksi dan akan disimpan

    # Pastikan _SYMBOL_POINT_VALUE diinisialisasi
    if globals().get('_SYMBOL_POINT_VALUE') is None:
        globals().get('_initialize_symbol_point_value')()
    if globals().get('_SYMBOL_POINT_VALUE') is None:
        logger.error(f"Nilai POINT untuk {symbol_param} tidak tersedia. Melewatkan deteksi zona Supply/Demand.")
        return 0 # Mengembalikan 0 jika prasyarat tidak terpenuhi

    # --- Ambil parameter konfigurasi ---
    min_base_candles = getattr(config.RuleBasedStrategy, 'min_base_candles', 2)
    max_base_candles = getattr(config.RuleBasedStrategy, 'max_base_candles', 5)
    base_candle_body_ratio = getattr(config.RuleBasedStrategy, 'CANDLE_BODY_MIN_RATIO', Decimal('0.3'))
    
    min_impulse_move_value = Decimal('0.0') # Default
    if current_atr_value is not None and current_atr_value > Decimal('0.0'):
        min_impulse_move_value = current_atr_value * config.RuleBasedStrategy.SD_MIN_IMPULSIVE_MOVE_ATR_MULTIPLIER
        logger.debug(f"S&D Detektor: Menggunakan min_impulse_move_value dinamis ATR: {float(min_impulse_move_value):.5f} (dari ATR {float(current_atr_value):.5f})")
    else:
        min_impulse_move_value = Decimal('50') * globals().get('_SYMBOL_POINT_VALUE')
        logger.warning(f"S&D Detektor: ATR tidak valid ({current_atr_value}). Menggunakan min_impulse_move_value statis: {float(min_impulse_move_value):.5f}")

    # --- Persiapan DataFrame untuk perhitungan internal ---
    df_processed = candles_df.copy()
    
    df_processed = df_processed.rename(columns={
        'open_price': 'open', 'high_price': 'high', 'low_price': 'low',
        'close_price': 'close', 'tick_volume': 'volume', 'real_volume': 'real_volume', 'spread': 'spread'
    })

    for col in ['open', 'high', 'low', 'close', 'volume', 'real_volume', 'spread']:
        if col in df_processed.columns:
            df_processed[col] = df_processed[col].apply(utils.to_float_or_none)
            if col in ['open', 'high', 'low', 'close']:
                initial_len_df = len(df_processed)
                df_processed.dropna(subset=[col], inplace=True)
                if len(df_processed) < initial_len_df:
                    logger.warning(f"S&D Detektor: Dihapus {initial_len_df - len(df_processed)} baris dengan {col} NaN di {timeframe_str} (setelah konversi float).")

    # --- Validasi Awal Jumlah Lilin Setelah Pembersihan NaN ---
    # Pola S&D (misalnya Drop-Base-Rally) setidaknya membutuhkan:
    # 1 lilin impulsif (drop) + min_base_candles + 1 lilin impulsif (rally)
    MIN_CANDLES_FOR_SD_PATTERN = max_base_candles + 2 # Minimal lilin untuk pola (misal 5+2=7)

    # BARIS VALIDASI HARDCODE 20 LILIN TELAH DIHAPUS DI SINI.
    # Sekarang hanya menggunakan validasi dinamis di bawah ini.

    if df_processed.empty or len(df_processed) < MIN_CANDLES_FOR_SD_PATTERN:
        logger.debug(f"Tidak cukup lilin ({len(df_processed)}) untuk deteksi zona Supply/Demand historis di {timeframe_str}. Diperlukan minimal {MIN_CANDLES_FOR_SD_PATTERN} lilin.")
        return 0 # Mengembalikan 0 jika data tidak cukup


    swing_results_df = globals().get('_calculate_swing_highs_lows_internal')(df_processed, config.AIAnalysts.SWING_EXT_BARS)
    
    if swing_results_df.empty or swing_results_df['HighLow'].isnull().all():
        logger.debug(f"Tidak ada swing points yang valid untuk {symbol_param} {timeframe_str}. Melewatkan deteksi zona S&D.")
        return 0

    # Deduplikasi: Ambil zona S&D yang sudah ada di DB
    existing_sd_keys = set()
    try:
        start_time_for_dedup = df_processed.index.min().to_pydatetime() - timedelta(days=30) 
        existing_sds_db = database_manager.get_supply_demand_zones(
            symbol=symbol_param,
            timeframe=timeframe_str,
            start_time_utc=start_time_for_dedup
        )
        for sd in existing_sds_db:
            formation_time_dt = utils.to_utc_datetime_or_none(sd['formation_time_utc'])
            if formation_time_dt:
                existing_sd_keys.add((
                    sd['symbol'], sd['timeframe'], sd['zone_type'],
                    formation_time_dt.replace(microsecond=0).isoformat()
                ))
        logger.debug(f"Ditemukan {len(existing_sd_keys)} zona S&D yang sudah ada di DB untuk {timeframe_str}.")
    except Exception as e:
        logger.error(f"Gagal mengambil existing S&D Zones dari DB untuk pre-filtering: {e}", exc_info=True)
        pass 

    detected_sd_zones_to_save_batch = []
    
    for i in range(len(df_processed) - 1, max_base_candles, -1): # Iterasi mundur dari akhir DataFrame
        for base_len in range(min_base_candles, max_base_candles + 1):
            base_start_idx = i - base_len # Indeks awal dari basis

            if base_start_idx < 0:
                continue

            prev_impulse_candle = None
            if base_start_idx > 0:
                prev_impulse_candle = df_processed.iloc[base_start_idx - 1]
            
            base_candles_segment = df_processed.iloc[base_start_idx: i]
            
            current_impulse_candle = df_processed.iloc[i] 

            # --- Validasi Basis ---
            is_valid_base = True
            if base_candles_segment.empty:
                is_valid_base = False
                logger.debug(f"SD_CALC_DIAG: Base segment for i={i}, base_len={base_len} is empty. Disqualifying base.")
            else:
                for _, b_candle in base_candles_segment.iterrows():
                    candle_range = b_candle['high'] - b_candle['low']
                    candle_body = abs(b_candle['close'] - b_candle['open'])
                    
                    if candle_range == 0:
                        if candle_body > float(Decimal('0.00001')):
                            is_valid_base = False
                            logger.debug(f"SD_CALC_DIAG: Invalid base candle (Body > 0 but Range is 0) at {b_candle.name}. Disqualifying base.")
                            break
                    else:
                        if (candle_body / candle_range) > float(base_candle_body_ratio):
                            is_valid_base = False
                            logger.debug(f"SD_CALC_DIAG: Invalid base candle (Body/Range ratio {candle_body/candle_range:.2f} > {base_candle_body_ratio}) at {b_candle.name}. Disqualifying base.")
                            break
            
            logger.debug(f"SD_CALC_DIAG: i={i}, base_len={base_len}, base_start_idx={base_start_idx}, is_valid_base={is_valid_base}")
            if not is_valid_base:
                continue

            # --- Periksa Pergerakan Impulsif ---
            is_rally_before_base = False
            is_drop_before_base = False
            if prev_impulse_candle is not None:
                prev_impulse_open = prev_impulse_candle['open']
                prev_impulse_close = prev_impulse_candle['close']
                if prev_impulse_close > prev_impulse_open:
                    is_rally_before_base = (prev_impulse_close - prev_impulse_open) > float(min_impulse_move_value)
                else:
                    is_drop_before_base = (prev_impulse_open - prev_impulse_close) > float(min_impulse_move_value)
            logger.debug(f"SD_CALC_DIAG: Rally_Before_Base={is_rally_before_base}, Drop_Before_Base={is_drop_before_base} (prev_candle_idx={base_start_idx-1})")

            is_rally_after_base = False
            is_drop_after_base = False
            
            current_impulse_open = current_impulse_candle['open']
            current_impulse_close = current_impulse_candle['close']
            if current_impulse_close > current_impulse_open:
                is_rally_after_base = (current_impulse_close - current_impulse_open) > float(min_impulse_move_value)
            else:
                is_drop_after_base = (current_impulse_open - current_impulse_close) > float(min_impulse_move_value)
            logger.debug(f"SD_CALC_DIAG: Rally_After_Base={is_rally_after_base}, Drop_After_Base={is_drop_after_base} (curr_candle_idx={i})")

            # --- Logika Deteksi Zona S&D ---
            zone_top_price_sd = utils.to_decimal_or_none(base_candles_segment['high'].max())
            zone_bottom_price_sd = utils.to_decimal_or_none(base_candles_segment['low'].min())
            formation_time_sd = base_candles_segment.index[0].to_pydatetime()
            
            if zone_top_price_sd is None or zone_bottom_price_sd is None:
                logger.debug(f"SD_CALC_DIAG: Zone top/bottom price is NaN for base_start_idx={base_start_idx}. Skipping.")
                continue


            # Deteksi Demand Zone (Bullish S&D)
            if is_valid_base and is_rally_after_base:
                if is_drop_before_base: # Drop-Base-Rally (DBR)
                    zone_type = "Demand"
                    base_type = "DropBaseRally"
                    new_key = (symbol_param, timeframe_str, zone_type, formation_time_sd.replace(microsecond=0).isoformat())
                    if new_key not in existing_sd_keys:
                        detected_sd_zones_to_save_batch.append({
                            "symbol": symbol_param, "timeframe": timeframe_str, "zone_type": zone_type, "base_type": base_type,
                            "zone_top_price": zone_top_price_sd, "zone_bottom_price": zone_bottom_price_sd,
                            "formation_time_utc": formation_time_sd, "is_mitigated": False
                        })
                        existing_sd_keys.add(new_key)
                        processed_count += 1
                        logger.info(f"Demand Zone (DBR) terdeteksi: {float(zone_bottom_price_sd):.5f}-{float(zone_top_price_sd):.5f} di {timeframe_str} pada {formation_time_sd}.")
                elif is_rally_before_base: # Rally-Base-Rally (RBR)
                    zone_type = "Demand"
                    base_type = "RallyBaseRally"
                    new_key = (symbol_param, timeframe_str, zone_type, formation_time_sd.replace(microsecond=0).isoformat())
                    if new_key not in existing_sd_keys:
                        detected_sd_zones_to_save_batch.append({
                            "symbol": symbol_param, "timeframe": timeframe_str, "zone_type": zone_type, "base_type": base_type,
                            "zone_top_price": zone_top_price_sd, "zone_bottom_price": zone_bottom_price_sd,
                            "formation_time_utc": formation_time_sd, "is_mitigated": False
                        })
                        existing_sd_keys.add(new_key)
                        processed_count += 1
                        logger.info(f"Demand Zone (RBR) terdeteksi: {float(zone_bottom_price_sd):.5f}-{float(zone_top_price_sd):.5f} di {timeframe_str} pada {formation_time_sd}.")

            # Deteksi Supply Zone (Bearish S&D)
            elif is_valid_base and is_drop_after_base: # FOR SUPPLY ZONE
                if is_rally_before_base: # Rally-Base-Drop (RBD)
                    zone_type = "Supply"
                    base_type = "RallyBaseDrop"
                    new_key = (symbol_param, timeframe_str, zone_type, formation_time_sd.replace(microsecond=0).isoformat())
                    if new_key not in existing_sd_keys:
                        detected_sd_zones_to_save_batch.append({
                            "symbol": symbol_param, "timeframe": timeframe_str, "zone_type": zone_type, "base_type": base_type,
                            "zone_top_price": zone_top_price_sd, "zone_bottom_price": zone_bottom_price_sd,
                            "formation_time_utc": formation_time_sd, "is_mitigated": False
                        })
                        existing_sd_keys.add(new_key)
                        processed_count += 1
                        logger.info(f"Supply Zone (RBD) terdeteksi: {float(zone_bottom_price_sd):.5f}-{float(zone_top_price_sd):.5f} di {timeframe_str} pada {formation_time_sd}.")
                elif is_drop_before_base: # Drop-Base-Drop (DBD)
                    zone_type = "Supply"
                    base_type = "DropBaseDrop"
                    new_key = (symbol_param, timeframe_str, zone_type, formation_time_sd.replace(microsecond=0).isoformat())
                    if new_key not in existing_sd_keys:
                        detected_sd_zones_to_save_batch.append({
                            "symbol": symbol_param, "timeframe": timeframe_str, "zone_type": zone_type, "base_type": base_type,
                            "zone_top_price": zone_top_price_sd, "zone_bottom_price": zone_bottom_price_sd,
                            "formation_time_utc": formation_time_sd, "is_mitigated": False
                        })
                        existing_sd_keys.add(new_key)
                        processed_count += 1
                        logger.info(f"Supply Zone (DBD) terdeteksi: {float(zone_bottom_price_sd):.5f}-{float(zone_top_price_sd):.5f} di {timeframe_str} pada {formation_time_sd}.")

    if detected_sd_zones_to_save_batch:
        try:
            database_manager.save_supply_demand_zones_batch(detected_sd_zones_to_save_batch, [])
            logger.info(f"BACKFILL WORKER: Berhasil menyimpan {len(detected_sd_zones_to_save_batch)} zona Supply/Demand baru unik untuk {symbol_param} {timeframe_str}.")
        except Exception as e:
            logger.error(f"BACKFILL WORKER: Gagal menyimpan zona Supply/Demand batch: {e}", exc_info=True)

    logger.info(f"Selesai mendeteksi zona Supply/Demand BARU historis untuk {symbol_param} {timeframe_str}. Deteksi: {processed_count}.")
    return processed_count

def _detect_new_sr_levels_historically(symbol_param: str, timeframe_str: str, candles_df: pd.DataFrame, current_atr_value: Decimal):
    """
    Mendeteksi level Support & Resistance (S&R) baru secara historis.
    Level S&R adalah area di mana harga menunjukkan reaksi signifikan di masa lalu.
    Menyimpan level S&R baru ke database secara batch dengan deduplikasi.
    """
    logger.info(f"Mendeteksi level Support/Resistance BARU historis untuk {symbol_param} {timeframe_str}...")
    
    processed_count = 0 # MODIFIKASI: INISIALISASI DI SINI UNTUK MENGHINDARI NAMEREPROR

    if candles_df.empty or len(candles_df) < 50: # Minimal lilin untuk deteksi S&R
        logger.debug(f"Tidak cukup lilin ({len(candles_df)}) untuk deteksi level S&R historis di {timeframe_str}. Diperlukan minimal 50 lilin.")
        return 0 # Mengembalikan 0 secara eksplisit

    # Pastikan _SYMBOL_POINT_VALUE diinisialisasi
    if globals().get('_SYMBOL_POINT_VALUE') is None:
        globals().get('_initialize_symbol_point_value')()
    if globals().get('_SYMBOL_POINT_VALUE') is None:
        logger.error(f"Nilai POINT untuk {symbol_param} tidak tersedia. Melewatkan deteksi level S&R.")
        return 0

    # Buat salinan DataFrame dan konversi kolom harga ke float untuk operasi internal
    df_processed = candles_df.copy()
    df_processed = df_processed.rename(columns={
        'open_price': 'open', 'high_price': 'high', 'low_price': 'low',
        'close_price': 'close', 'tick_volume': 'volume', 'real_volume': 'real_volume', 'spread': 'spread'
    })
    # KOREKSI: HANYA konversi kolom harga dan volume ke float
    for col in ['open', 'high', 'low', 'close', 'volume', 'real_volume', 'spread']:
        if col in df_processed.columns: # Pastikan kolom ada
            df_processed[col] = df_processed[col].apply(utils.to_float_or_none)

    # Hapus baris dengan NaN di kolom harga kunci setelah konversi
    initial_len_df = len(df_processed)
    df_processed.dropna(subset=['open', 'high', 'low', 'close'], inplace=True)
    if len(df_processed) < initial_len_df:
        logger.warning(f"S&R Detektor: Dihapus {initial_len_df - len(df_processed)} baris dengan NaN di kolom harga kunci. Sisa lilin: {len(df_processed)}.")

    # Validasi jika DataFrame kosong setelah pembersihan NaN
    if df_processed.empty or len(df_processed) < 50: # Minimal lilin setelah dropna
        logger.warning(f"S&R Detektor: DataFrame df_processed kosong atau tidak cukup lilin ({len(df_processed)}) setelah pembersihan NaN. Melewatkan deteksi S&R untuk {timeframe_str}.")
        return 0


    # Ambil swing highs/lows untuk menentukan titik acuan S&R
    # Pastikan _calculate_swing_highs_lows_internal diakses dari globals()
    swing_results_df = globals().get('_calculate_swing_highs_lows_internal')(df_processed, config.AIAnalysts.SWING_EXT_BARS)
    
    # MODIFIKASI: Validasi output swing_results_df
    if swing_results_df.empty or swing_results_df['HighLow'].isnull().all():
        logger.debug(f"Tidak ada swing points yang valid untuk {symbol_param} {timeframe_str}. Melewatkan deteksi level S&R.")
        return 0

    # --- Deduplikasi: Ambil level S&R yang sudah ada di DB ---
    existing_sr_keys = set()
    try:
        existing_srs_db = database_manager.get_support_resistance_levels(
            symbol=symbol_param,
            timeframe=timeframe_str,
            start_time_utc=df_processed.index.min().to_pydatetime()
        )
        for sr_item in existing_srs_db:
            formation_time_dt = utils.to_utc_datetime_or_none(sr_item['formation_time_utc'])
            if formation_time_dt:
                existing_sr_keys.add((
                    sr_item['symbol'], sr_item['timeframe'], sr_item['level_type'],
                    # MODIFIKASI: Gunakan Decimal untuk price_level_dec dalam kunci deduplikasi
                    utils.to_decimal_or_none(sr_item['price_level']), # Gunakan Decimal untuk perbandingan set key
                    formation_time_dt.replace(microsecond=0).isoformat()
                ))
        logger.debug(f"Ditemukan {len(existing_sr_keys)} level S&R yang sudah ada di DB untuk {timeframe_str}.")
    except Exception as e:
        logger.error(f"Gagal mengambil existing S&R Levels dari DB untuk pre-filtering: {e}", exc_info=True)
        pass

    detected_sr_levels_to_save_batch = []
    
    # Toleransi untuk mengelompokkan level S&R (dalam poin)
    sr_tolerance_value = Decimal(str(config.RuleBasedStrategy.RULE_SR_TOLERANCE_POINTS)) * globals().get('_SYMBOL_POINT_VALUE')
    
    # MODIFIKASI: Gunakan ATR adaptif untuk toleransi
    if current_atr_value is not None and current_atr_value > Decimal('0.0'):
        sr_tolerance_value = current_atr_value * config.MarketData.ATR_MULTIPLIER_FOR_TOLERANCE
        logger.debug(f"S&R Detektor: Menggunakan toleransi dinamis ATR: {float(sr_tolerance_value):.5f} (dari ATR {float(current_atr_value):.5f})")
    else:
        logger.warning(f"S&R Detektor: ATR tidak valid. Menggunakan toleransi S&R statis: {float(sr_tolerance_value):.5f} (dari {config.RuleBasedStrategy.RULE_SR_TOLERANCE_POINTS} poin)")
    
    retest_window_candles = config.RuleBasedStrategy.SR_STRENGTH_RETEST_WINDOW_CANDLES
    break_tolerance_multiplier = config.RuleBasedStrategy.SR_STRENGTH_BREAK_TOLERANCE_MULTIPLIER


    # Iterasi melalui setiap swing point untuk mencari level S&R
    # swing_results_df sekarang sudah punya indeks yang sama dengan df_processed
    for i in range(len(df_processed)): # Iterasi melalui semua lilin di df_processed
        current_candle_idx = i # Simpan indeks integer
        current_candle_time = df_processed.index[i].to_pydatetime() # Dapatkan indeks datetime

        # MODIFIKASI: Akses swing_results_df dengan aman menggunakan .loc dan periksa keberadaan
        current_swing_highlow_val = swing_results_df.loc[current_candle_time, 'HighLow'] if current_candle_time in swing_results_df.index else np.nan
        current_swing_level_val = swing_results_df.loc[current_candle_time, 'Level'] if current_candle_time in swing_results_df.index else np.nan

        if pd.notna(current_swing_highlow_val): # Hanya jika ini adalah swing point yang terdeteksi
            current_swing_type = current_swing_highlow_val
            current_swing_level = utils.to_decimal_or_none(current_swing_level_val) # Konversi ke Decimal
            
            if current_swing_level is None: # Lewati jika level swing tidak valid
                continue

            level_type = "Resistance" if current_swing_type == 1 else "Support"
            price_level_dec = current_swing_level # Ini adalah Decimal

            # Buat zona S&R sedikit di sekitar level
            zone_start_price_dec = price_level_dec - (sr_tolerance_value / Decimal('2'))
            zone_end_price_dec = price_level_dec + (sr_tolerance_value / Decimal('2'))

            # --- MULAI KODE MODIFIKASI UNTUK PERHITUNGAN strength_score ---
            calculated_strength_score = 0
            # Tentukan jendela pencarian untuk retest (menggunakan parameter dari config)
            # Pastikan search_window_start_idx dan search_window_end_idx berada dalam batas df_processed
            search_window_start_idx = current_candle_idx + 1 # Mulai dari lilin setelah swing
            search_window_end_idx = min(current_candle_idx + retest_window_candles + 1, len(df_processed)) 
            
            if search_window_start_idx < search_window_end_idx: # Hanya jika ada lilin dalam jendela retest
                relevant_candles_for_strength = df_processed.iloc[search_window_start_idx:search_window_end_idx]

                significant_break_tolerance = sr_tolerance_value * break_tolerance_multiplier

                for _, candle_row in relevant_candles_for_strength.iterrows():
                    # MODIFIKASI: Konversi ke Decimal untuk perbandingan
                    candle_high_dec = utils.to_decimal_or_none(candle_row['high'])
                    candle_low_dec = utils.to_decimal_or_none(candle_row['low'])
                    candle_close_dec = utils.to_decimal_or_none(candle_row['close']) 

                    if candle_high_dec is None or candle_low_dec is None or candle_close_dec is None: continue # Lewati jika harga lilin tidak valid


                    # Kriteria "touch" atau "retest": lilin menyentuh level dalam toleransi S&R umum
                    if (price_level_dec <= candle_high_dec + sr_tolerance_value and
                        price_level_dec >= candle_low_dec - sr_tolerance_value):
                        
                        # Pastikan sentuhan tidak menembus level secara signifikan (close di luar toleransi break)
                        is_broken_significantly = False
                        if level_type == "Resistance":
                            # Dianggap ditembus jika close lilin di atas level + significant_break_tolerance
                            if candle_close_dec > price_level_dec + significant_break_tolerance:
                                is_broken_significantly = True
                        elif level_type == "Support":
                            # Dianggap ditembus jika close lilin di bawah level - significant_break_tolerance
                            if candle_close_dec < price_level_dec - significant_break_tolerance:
                                is_broken_significantly = True

                        if not is_broken_significantly:
                            calculated_strength_score += 1 # Tambahkan skor jika ada retest yang valid
                            
            # --- AKHIR KODE MODIFIKASI UNTUK PERHITUNGAN strength_score --
            
            # Tambahkan ke batch jika belum ada duplikat
            new_key = (symbol_param, timeframe_str, level_type, 
                       float(price_level_dec), # Gunakan float untuk perbandingan set key
                       current_candle_time.replace(microsecond=0).isoformat())

            if new_key not in existing_sr_keys:
                detected_sr_levels_to_save_batch.append({
                    "symbol": symbol_param, "timeframe": timeframe_str,
                    "level_type": level_type,
                    "price_level": price_level_dec, # Tetap Decimal untuk DB
                    "zone_start_price": zone_start_price_dec,
                    "zone_end_price": zone_end_price_dec,
                    "strength_score": calculated_strength_score, # Gunakan skor yang dihitung
                    "is_active": True, # Awalnya aktif
                    "formation_time_utc": current_candle_time,
                    "last_test_time_utc": None, # Akan diupdate oleh fungsi update_existing_sr_levels_status
                    "is_key_level": False, # Akan diupdate oleh fungsi identify_key_levels_across_timeframes
                    "confluence_score": 0 # Akan diupdate oleh fungsi lain
                })
                existing_sr_keys.add(new_key)
                logger.debug(f"Level S&R {level_type} terdeteksi di {timeframe_str} pada {current_candle_time}.")


    if detected_sr_levels_to_save_batch:
        try:
            database_manager.save_support_resistance_levels_batch(detected_sr_levels_to_save_batch, [])
            processed_count += len(detected_sr_levels_to_save_batch)
            logger.info(f"BACKFILL WORKER: Berhasil menyimpan {processed_count} level S&R baru unik untuk {symbol_param} {timeframe_str}.")
        except Exception as e:
            logger.error(f"BACKFILL WORKER: Gagal menyimpan level S&R batch: {e}", exc_info=True)

    logger.info(f"Selesai mendeteksi level Support/Resistance BARU historis untuk {symbol_param} {timeframe_str}. Deteksi: {processed_count}.")
    return processed_count

def _update_existing_sr_levels_status(symbol_param: str, timeframe_str: str, all_candles_df: pd.DataFrame, current_atr_value: Decimal) -> int:
    """
    Memperbarui status 'is_active' dan 'last_test_time_utc' untuk S&R yang sudah ada di database.
    Args:
        symbol_param (str): Simbol trading.
        timeframe_str (str): Timeframe.
        all_candles_df (pd.DataFrame): DataFrame candle historis (diasumsikan sudah dalam format
                                   kolom pendek 'open', 'high', 'low', 'close' sebagai float).
        current_atr_value (Decimal): Nilai ATR saat ini untuk timeframe ini.
    Returns:
        int: Jumlah S&R yang statusnya berhasil diperbarui dan dikirim ke antrean DB.
    """
    logger.info(f"Memperbarui aktivitas level S&R untuk {symbol_param} {timeframe_str}...")
    processed_count = 0 

    # 1. Validasi Input Awal
    if all_candles_df.empty:
        logger.debug(f"DataFrame candle kosong untuk update status S&R di {timeframe_str}.")
        return 0 

    # 2. Inisialisasi _SYMBOL_POINT_VALUE (jika belum)
    if globals().get('_SYMBOL_POINT_VALUE') is None or globals().get('_SYMBOL_POINT_VALUE') == Decimal('0.0'):
        globals().get('_initialize_symbol_point_value')()
    if globals().get('_SYMBOL_POINT_VALUE') is None or globals().get('_SYMBOL_POINT_VALUE') == Decimal('0.0'):
        logger.error(f"Nilai POINT untuk {symbol_param} tidak tersedia atau 0. Melewatkan update status S&R.")
        return 0 

    # 3. Ambil Batas Harga Dinamis Global (tidak perlu modifikasi di sini)
    with globals().get('_dynamic_price_lock', threading.Lock()):
        min_price_for_update_filter = globals().get('_dynamic_price_min_for_analysis')
        max_price_for_update_filter = globals().get('_dynamic_price_max_for_analysis')

    # 4. Mendapatkan Level S&R Aktif dari Database
    active_sr_levels = database_manager.get_support_resistance_levels(
        symbol=symbol_param, timeframe=timeframe_str, is_active=True,
        limit=None,
        min_price_level=min_price_for_update_filter,
        max_price_level=max_price_for_update_filter
    )

    if not active_sr_levels:
        logger.debug(f"Tidak ada level S&R aktif atau data candle kosong untuk {symbol_param} {timeframe_str} untuk diperbarui statusnya.")
        return 0 

    # 5. Menentukan Toleransi Penembusan/Sentuhan
    if current_atr_value is not None and current_atr_value > Decimal('0.0'):
        tolerance_value_dec = current_atr_value * config.MarketData.ATR_MULTIPLIER_FOR_TOLERANCE
        logger.debug(f"S&R Status Updater: Menggunakan toleransi dinamis ATR: {float(tolerance_value_dec):.5f} (dari ATR {float(current_atr_value):.5f})")
    else:
        tolerance_value_dec = config.RuleBasedStrategy.RULE_SR_TOLERANCE_POINTS * globals().get('_SYMBOL_POINT_VALUE')
        logger.warning(f"S&R Status Updater: ATR tidak valid. Menggunakan toleransi statis: {float(tolerance_value_dec):.5f}")
    
    updated_srs_to_save_in_batch = []

    # 6. Menentukan Waktu Mulai Pencarian Lilin yang Relevan (Optimasi)
    # --- MULAI MODIFIKASI UNTUK PENANGANAN NoneType PADA latest_active_sr_time ---
    list_for_max_sr = []
    for sr_item in active_sr_levels:
        # Prioritaskan last_test_time_utc, lalu formation_time_utc
        effective_timestamp = sr_item.get('last_test_time_utc')
        if effective_timestamp is None:
            effective_timestamp = sr_item.get('formation_time_utc')
        
        # Pastikan effective_timestamp adalah objek datetime yang valid.
        processed_timestamp = utils.to_utc_datetime_or_none(effective_timestamp)
        
        if processed_timestamp is not None:
            list_for_max_sr.append(processed_timestamp)
        else:
            logger.warning(f"SR Status Updater: Timestamp untuk SR ID {sr_item.get('id', 'N/A')} resolve ke None setelah konversi. Dilewati untuk perhitungan max.")

    if list_for_max_sr:
        latest_active_sr_time = max(list_for_max_sr)
    else:
        # Jika daftar kosong (semua timestamp tidak valid atau active_sr_levels kosong), gunakan waktu fallback yang aman
        latest_active_sr_time = datetime(1970, 1, 1, tzinfo=timezone.utc)
        logger.warning(f"SR Status Updater: Tidak ada timestamp valid untuk menghitung max. Menggunakan default {latest_active_sr_time.isoformat()}.")
    # --- AKHIR MODIFIKASI UNTUK PENANGANAN NoneType PADA latest_active_sr_time ---

    relevant_candles_for_test = all_candles_df[ 
        all_candles_df.index > latest_active_sr_time
    ].copy()

    # --- MULAI PERBAIKAN UNTUK dropna PADA relevant_candles_for_test ---
    # Lakukan renaming kolom di sini sebelum dropna
    relevant_candles_for_test = relevant_candles_for_test.rename(columns={
        'open_price': 'open',
        'high_price': 'high',
        'low_price': 'low',
        'close_price': 'close',
        'tick_volume': 'volume',
        'real_volume': 'real_volume',
        'spread': 'spread'
    })

    # Pastikan kolom-kolomnya bertipe numerik (float) sebelum dropna
    for col in ['open', 'high', 'low', 'close', 'volume', 'real_volume', 'spread']:
        if col in relevant_candles_for_test.columns:
            relevant_candles_for_test[col] = relevant_candles_for_test[col].apply(utils.to_float_or_none)

    initial_len_relevant_candles = len(relevant_candles_for_test)
    # Gunakan nama kolom yang sudah di-rename untuk subset dropna
    relevant_candles_for_test.dropna(subset=['high', 'low', 'close'], inplace=True) 
    if len(relevant_candles_for_test) < initial_len_relevant_candles:
        logger.warning(f"S&R_STATUS_DEBUG: Dihapus {initial_len_relevant_candles - len(relevant_candles_for_test)} baris dengan NaN high/low/close dalam relevant_candles_for_test setelah renaming.")
    # --- AKHIR PERBAIKAN UNTUK dropna PADA relevant_candles_for_test ---

    if relevant_candles_for_test.empty and active_sr_levels:
        logger.debug(f"Tidak ada lilin baru yang valid setelah SR aktif terbaru untuk {symbol_param} {timeframe_str}. Melewatkan update status SR.")
        return 0

    # 7. Iterasi Melalui SR Levels Aktif dan Lilin yang Relevan
    for sr_level in active_sr_levels:
        sr_id = sr_level['id']
        level_price = sr_level['price_level'] # Decimal
        sr_type = sr_level['level_type'] # Resistance atau Support
        
        is_still_active = True 
        
        last_test_time_for_update = sr_level.get('last_test_time_utc')
        if last_test_time_for_update is None:
            last_test_time_for_update = sr_level.get('formation_time_utc', datetime(1970, 1, 1, tzinfo=timezone.utc))
            if last_test_time_for_update is None:
                last_test_time_for_update = datetime(1970, 1, 1, tzinfo=timezone.utc)
                logger.warning(f"S&R_STATUS_DEBUG: SR {sr_id} tidak memiliki formation_time_utc atau last_test_time_utc. Menggunakan default 1970-01-01.")

        sr_relevant_candles = relevant_candles_for_test[
            relevant_candles_for_test.index > last_test_time_for_update 
        ].copy()

        if sr_relevant_candles.empty:
            logger.debug(f"S&R_STATUS_DEBUG: SR {sr_type} (ID: {sr_id}) dilewati karena tidak ada lilin baru setelah last_test_time_utc ({last_test_time_for_update}).")
            continue


        for _, candle_row in sr_relevant_candles.iterrows(): 
            if not is_still_active: 
                break

            # --- MULAI PENAMBAHAN KODE UNTUK NAMA KOLOM high/low/close DALAM LOOP ---
            # Karena relevant_candles_for_test (dan turunannya sr_relevant_candles) sudah di-rename di atas,
            # kita bisa langsung mengaksesnya dengan nama pendek.
            candle_high = candle_row['high'] 
            candle_low = candle_row['low'] 
            candle_close = candle_row['close'] 
            # Tidak perlu lagi if/elif di sini karena sudah di-rename di atas.
            # Cukup validasi nilai None/NaN yang mungkin terjadi setelah .apply(utils.to_float_or_none)
            # yang sudah dilakukan pada relevant_candles_for_test di atas.
            
            # Ini adalah validasi akhir untuk memastikan tidak ada None/NaN yang lolos
            if candle_high is None or candle_low is None or candle_close is None:
                logger.debug(f"S&R_STATUS_DEBUG: Lilin dengan NaN high/low/close setelah konversi untuk SR {sr_id} di {candle_row.name}. Melewatkan pengecekan.")
                continue # Lewati lilin ini jika ada NaN setelah konversi
            # --- AKHIR PENAMBAHAN KODE UNTUK NAMA KOLOM high/low/close DALAM LOOP ---

            candle_time_dt = candle_row.name.to_pydatetime()

            is_broken = False
            # KOREKSI PENTING: Lakukan perbandingan menggunakan float dari Decimal
            level_price_float = float(level_price)
            tolerance_float = float(tolerance_value_dec)

            # Logika invalidasi (break): Harga menembus SR level sepenuhnya dengan konfirmasi close
            if sr_type == "Resistance":
                # Dianggap ditembus jika close lilin di atas level + toleransi
                if candle_close > level_price_float + tolerance_float:
                    is_broken = True
                    logger.debug(f"S&R_STATUS_DEBUG: S&R Resistance @ {level_price_float:.5f} di {timeframe_str} DINONAKTIFKAN (ditembus) oleh candle {candle_time_dt}.")
            elif sr_type == "Support":
                # Dianggap ditembus jika close lilin di bawah level - toleransi
                if candle_close < level_price_float - tolerance_float:
                    is_broken = True
                    logger.debug(f"S&R_STATUS_DEBUG: S&R Support @ {level_price_float:.5f} di {timeframe_str} DINONAKTIFKAN (ditembus) oleh candle {candle_time_dt}.")

            if is_broken:
                is_still_active = False
                last_test_time_for_update = candle_time_dt
                break 

            # Logika retest (sentuhan): Harga menyentuh SR level tetapi tidak menembus
            if (is_still_active and 
                candle_high >= level_price_float - tolerance_float and 
                candle_low <= level_price_float + tolerance_float):
                
                # Hanya update last_test_time_utc jika ada interval waktu yang cukup sejak terakhir
                if (last_test_time_for_update is None) or \
                   ((candle_time_dt - last_test_time_for_update).total_seconds() > config.System.RETRY_DELAY_SECONDS * 5):
                    last_test_time_for_update = candle_time_dt
                    logger.debug(f"S&R_STATUS_DEBUG: S&R {sr_type} (ID: {sr_id}) retest terjadi. Waktu terakhir diuji: {last_test_time_for_update.isoformat()}.")
        
        logger.debug(f"S&R_STATUS_DEBUG: SR {sr_id} final status: is_active={is_still_active}, last_test_time={last_test_time_for_update.isoformat() if last_test_time_for_update else 'None'}")
        
        if (sr_level['is_active'] != is_still_active) or \
           (utils.to_utc_datetime_or_none(sr_level.get('last_test_time_utc')) != last_test_time_for_update):
            
            updated_srs_to_save_in_batch.append({
                'id': sr_id,
                'is_active': is_still_active, 
                'last_test_time_utc': last_test_time_for_update,
                'symbol': sr_level['symbol'],
                'timeframe': sr_level['timeframe'],
                'level_type': sr_level['level_type'],
                'price_level': level_price, # Decimal
                'zone_start_price': sr_level.get('zone_start_price'), # Decimal
                'zone_end_price': sr_level.get('zone_end_price'), # Decimal
                'strength_score': sr_level.get('strength_score'),
                'is_key_level': sr_level.get('is_key_level', False),
                'confluence_score': sr_level.get('confluence_score', 0)
            })
            processed_count += 1
            logger.debug(f"S&R_STATUS_DEBUG: SR {sr_id} ditambahkan ke batch update. Processed count: {processed_count}")

    # 8. Menyimpan Batch Update ke Database
    if updated_srs_to_save_in_batch:
        processed_updated_srs_batch = []
        for sr_data_dict in updated_srs_to_save_in_batch:
            processed_item_dict = sr_data_dict.copy()
            for key, value in processed_item_dict.items():
                if isinstance(value, Decimal):
                    processed_item_dict[key] = utils.to_float_or_none(value)
                elif isinstance(value, datetime):
                    processed_item_dict[key] = utils.to_iso_format_or_none(value)
            processed_updated_srs_batch.append(processed_item_dict)

        try:
            database_manager.save_support_resistance_levels_batch([], processed_updated_srs_batch)
            logger.info(f"BACKFILL WORKER: Berhasil mem-batch update {len(processed_updated_srs_batch)} status S&R Levels untuk {symbol_param} {timeframe_str}.")
        except Exception as e:
            logger.error(f"BACKFILL WORKER: Gagal mem-batch update status S&R Levels: {e}", exc_info=True)

    logger.info(f"Selesai memeriksa aktivitas S&R Levels untuk {symbol_param} {timeframe_str}. {processed_count} diperbarui.")
    return processed_count


def _backfill_single_timeframe_features(symbol_param: str, tf: str, backfill_start_date: datetime = None, backfill_end_date: datetime = None):
    """
    Melakukan backfill deteksi dan update status untuk semua fitur teknis
    untuk satu timeframe dan rentang tanggal tertentu.
    Ini adalah worker untuk thread pool.
    Args:
        symbol_param (str): Simbol trading.
        tf (str): Timeframe yang akan diproses.
        backfill_start_date (datetime): Tanggal mulai backfill kustom (opsional).
        backfill_end_date (datetime): Tanggal akhir backfill kustom (opsional).
    """
    logger.info(f"BACKFILL WORKER [{tf}]: Memulai backfill fitur historis untuk TF {tf}.")

    trend_context = {"overall_trend": "Not Processed", "reason": "Tren belum dihitung atau disimpan.", "detailed_votes": {}}

    last_processed_time_in_chunk_for_status = None 
    start_time_for_db_query = None

    try:
        if globals().get('_SYMBOL_POINT_VALUE') is None or globals().get('_SYMBOL_POINT_VALUE') == Decimal('0.0'):
            globals().get('_initialize_symbol_point_value')()
        if globals().get('_SYMBOL_POINT_VALUE') is None or globals().get('_SYMBOL_POINT_VALUE') == Decimal('0.0'):
            logger.error(f"Nilai POINT untuk {symbol_param} tidak tersedia atau 0. Melewatkan backfill fitur historis untuk {tf}.")
            return 

        if tf == "M1": CHUNK_SIZE = 1000
        elif tf == "M5": CHUNK_SIZE = 5000
        elif tf == "M15": CHUNK_SIZE = 7500
        elif tf == "M30": CHUNK_SIZE = 10000
        elif tf == "H1": CHUNK_SIZE = 15000 
        elif tf == "H4": CHUNK_SIZE = 20000
        elif tf == "D1": CHUNK_SIZE = 25000
        else: CHUNK_SIZE = 5000

        full_historical_start_date_str = config.MarketData.HISTORICAL_DATA_START_DATE_FULL
        try:
            config_historical_start_datetime = datetime.strptime(full_historical_start_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        except ValueError as ve:
            logger.critical(f"BACKFILL WORKER [{tf}]: Format HISTORICAL_DATA_START_DATE_FULL di config.py tidak valid: {full_historical_start_date_str}. Error: {ve}", exc_info=True)
            return

        start_time_for_db_query = config_historical_start_datetime # Inisialisasi di sini

        last_processed_time_for_features = database_manager.get_last_feature_backfill_time(symbol_param, tf)
        if last_processed_time_for_features:
            logger.debug(f"BACKFILL WORKER [{tf}]: last_processed_time_for_features dari DB (sebelum konversi): Type={type(last_processed_time_for_features)}, Value={last_processed_time_for_features}") # DEBUG
            last_processed_time_for_features = utils.to_utc_datetime_or_none(last_processed_time_for_features)
            logger.debug(f"BACKFILL WORKER [{tf}]: last_processed_time_for_features (setelah konversi): Type={type(last_processed_time_for_features)}, Value={last_processed_time_for_features}") # DEBUG

            if last_processed_time_for_features < config_historical_start_datetime:
                start_time_for_db_query = config_historical_start_datetime
            else:
                start_time_for_db_query = last_processed_time_for_features + timedelta(seconds=1)
        else:
            start_time_for_db_query = config_historical_start_datetime
        
        logger.debug(f"BACKFILL WORKER [{tf}]: start_time_for_db_query final: Type={type(start_time_for_db_query)}, Value={start_time_for_db_query}") # DEBUG

        logger.info(f"BACKFILL WORKER [{tf}]: Awal query DB untuk candle fitur: {start_time_for_db_query.isoformat()}.")

        total_processed_candles = 0

        with globals().get('_backfill_progress_lock', threading.Lock()):
            globals().get('_backfill_overall_progress_status')[symbol_param][tf] = {
                "percentage_complete": 0.0,
                "current_processing_range_start": start_time_for_db_query.isoformat(),
                "current_processing_range_end": "N/A",
                "total_backfill_range_start": start_time_for_db_query.isoformat(),
                "total_backfill_range_end": (backfill_end_date or datetime.now(timezone.utc)).isoformat(),
                "is_running": True,
                "last_update_time": datetime.now(timezone.utc).isoformat()
            }

        while True:
            current_end_time_for_query = datetime.now(timezone.utc)
            logger.debug(f"BACKFILL WORKER [{tf}]: current_end_time_for_query (sebelum perbandingan): Type={type(current_end_time_for_query)}, Value={current_end_time_for_query}") # DEBUG
            logger.debug(f"BACKFILL WORKER [{tf}]: backfill_end_date (untuk perbandingan): Type={type(backfill_end_date)}, Value={backfill_end_date}") # DEBUG
            
            if backfill_end_date and current_end_time_for_query > backfill_end_date:
                current_end_time_for_query = backfill_end_date

            logger.debug(f"BACKFILL WORKER [{tf}]: current_end_time_for_query (setelah perbandingan): Type={type(current_end_time_for_query)}, Value={current_end_time_for_query}") # DEBUG

            if start_time_for_db_query >= current_end_time_for_query:
                logger.info(f"BACKFILL WORKER [{tf}]: Awal query {start_time_for_db_query.isoformat()} sudah melewati atau sama dengan akhir rentang {current_end_time_for_query.isoformat()}. Backfill untuk TF ini selesai.")
                break

            logger.info(f"BACKFILL WORKER [{tf}]: Mengambil chunk dari DB dari {start_time_for_db_query.isoformat()} hingga {current_end_time_for_query.isoformat()} (Limit: {CHUNK_SIZE}).")

            candles_data_chunk = database_manager.get_historical_candles_from_db(
                symbol_param,
                tf,
                start_time_utc=start_time_for_db_query,
                end_time_utc=current_end_time_for_query,
                limit=CHUNK_SIZE, 
                order_asc=True,
                min_open_time_utc=config_historical_start_datetime
            )

            if candles_data_chunk:
                start_time_for_db_query = candles_data_chunk[-1]['open_time_utc'] + timedelta(seconds=1)
                last_processed_time_in_chunk_for_status = candles_data_chunk[-1]['open_time_utc']
            else:
                start_time_for_db_query = current_end_time_for_query 
                logger.info(f"BACKFILL WORKER [{tf}]: Tidak ada candle baru atau candle tersisa untuk diambil dari DB dalam rentang {start_time_for_db_query.isoformat()} - {current_end_time_for_query.isoformat()}. Backfill untuk TF ini selesai.")
                break

            df_candles_raw = pd.DataFrame(candles_data_chunk)
            df_candles_raw['open_time_utc'] = pd.to_datetime(df_candles_raw['open_time_utc'])
            df_candles_raw.set_index('open_time_utc', inplace=True)
            
            critical_ohlcv_cols = ['open_price', 'high_price', 'low_price', 'close_price', 'tick_volume']
            optional_cols = ['real_volume', 'spread']

            all_cols_to_process = critical_ohlcv_cols + [col for col in optional_cols if col in df_candles_raw.columns]

            for col in all_cols_to_process:
                df_candles_raw[col] = df_candles_raw[col].apply(utils.to_decimal_or_none)
                
                if df_candles_raw[col].isnull().any():
                    logger.debug(f"BACKFILL WORKER [{tf}]: NaN terdeteksi di kolom '{col}'. Melakukan imputasi ffill/bfill.")
                    df_candles_raw[col] = df_candles_raw[col].ffill() 
                    df_candles_raw[col] = df_candles_raw[col].bfill() 
            
            initial_rows_before_final_dropna = len(df_candles_raw)
            df_candles_raw.dropna(subset=critical_ohlcv_cols, inplace=True)
            if len(df_candles_raw) < initial_rows_before_final_dropna:
                logger.warning(f"BACKFILL WORKER [{tf}]: Menghapus {initial_rows_before_final_dropna - len(df_candles_raw)} baris karena NaN persisten di kolom OHLCV kritis setelah imputasi.")

            if df_candles_raw.empty:
                logger.warning(f"BACKFILL WORKER [{tf}]: Chunk kosong setelah penanganan NaN. Lanjut ke chunk berikutnya.")
                continue 

            # --- START MODIFIKASI: Menyiapkan Dua DataFrame Utama (Decimal Renamed & Float Renamed) ---
            # 1. DataFrame untuk perhitungan yang butuh Decimal dan nama kolom pendek
            ohlc_df_decimal_renamed = df_candles_raw.copy()
            ohlc_df_decimal_renamed = ohlc_df_decimal_renamed.rename(columns={
                'open_price': 'open', 'high_price': 'high', 'low_price': 'low',
                'close_price': 'close', 'tick_volume': 'volume', 'real_volume': 'real_volume', 'spread': 'spread'
            })
            # Pastikan ini sudah Decimal seperti yang diharapkan
            for col in ['open', 'high', 'low', 'close', 'volume', 'real_volume', 'spread']:
                if col in ohlc_df_decimal_renamed.columns:
                    # Sudah Decimal dari df_candles_raw, hanya perlu penanganan np.nan jika ada
                    ohlc_df_decimal_renamed[col] = ohlc_df_decimal_renamed[col].apply(lambda x: x if not isinstance(x, float) or not np.isnan(x) else None)
                    # Konversi kembali ke Decimal jika ada yang terlewat
                    ohlc_df_decimal_renamed[col] = ohlc_df_decimal_renamed[col].apply(utils.to_decimal_or_none)
            ohlc_df_decimal_renamed.dropna(subset=['open', 'high', 'low', 'close'], inplace=True) # Final dropna

            # 2. DataFrame untuk perhitungan yang butuh Float dan nama kolom pendek
            ohlc_df_float_renamed = ohlc_df_decimal_renamed.copy() # Mulai dari Decimal yang sudah bersih
            for col in ['open', 'high', 'low', 'close', 'volume', 'real_volume', 'spread']:
                if col in ohlc_df_float_renamed.columns:
                    ohlc_df_float_renamed[col] = ohlc_df_float_renamed[col].apply(utils.to_float_or_none)
            ohlc_df_float_renamed.dropna(subset=['open', 'high', 'low', 'close'], inplace=True) # Final dropna
            
            # Validasi akhir jika DataFrame kosong setelah semua pembersihan dan renaming
            if ohlc_df_decimal_renamed.empty or ohlc_df_float_renamed.empty:
                logger.warning(f"BACKFILL WORKER [{tf}]: Salah satu DataFrame (Decimal/Float) kosong setelah pembersihan dan renaming. Melewatkan pemrosesan fitur.")
                continue # Lanjut ke chunk berikutnya

            # --- AKHIR MODIFIKASI: Menyiapkan Dua DataFrame Utama ---


            with globals().get('_dynamic_price_lock', threading.Lock()):
                min_price_limit = globals().get('_dynamic_price_min_for_analysis')
                max_price_limit = globals().get('_dynamic_price_max_for_analysis')

            is_full_historical_backfill = (backfill_start_date is None and backfill_end_date is None) or \
                                          (backfill_start_date == config_historical_start_datetime and backfill_end_date >= datetime.now(timezone.utc) - timedelta(days=7))

            if min_price_limit is None or max_price_limit is None:
                logger.warning(f"BACKFILL WORKER [{tf}]: min_price_limit atau max_price_limit belum diinisialisasi. Filter harga dinamis dinonaktifkan.")
                min_price_limit = Decimal('-1000000.0') # Nilai yang sangat rendah
                max_price_limit = Decimal('1000000.0') # Nilai yang sangat tinggi


            if not is_full_historical_backfill:
                # Filter harga dinamis diterapkan pada DataFrame yang sudah di-rename (Decimal)
                initial_len_for_feature_processing = len(ohlc_df_decimal_renamed)
                ohlc_df_decimal_renamed_filtered = ohlc_df_decimal_renamed[
                    (ohlc_df_decimal_renamed['low'] >= min_price_limit) &
                    (ohlc_df_decimal_renamed['high'] <= max_price_limit)
                ].copy()

                if len(ohlc_df_decimal_renamed_filtered) < initial_len_for_feature_processing:
                    logger.info(f"BACKFILL WORKER [{tf}]: Filter harga diterapkan. {initial_len_for_feature_processing - len(ohlc_df_decimal_renamed_filtered)} lilin di luar rentang harga dilewati untuk pemrosesan fitur.")
            else:
                logger.info(f"BACKFILL WORKER [{tf}]: Filter harga dinamis DINONAKTIFKAN untuk full historical backfill.")
                ohlc_df_decimal_renamed_filtered = ohlc_df_decimal_renamed # Gunakan seluruh DF jika tidak ada filter

            if ohlc_df_decimal_renamed_filtered.empty:
                logger.warning(f"BACKFILL WORKER [{tf}]: Chunk kosong setelah filter harga (atau tanpa filter pun kosong) untuk pemrosesan fitur. Lanjut ke chunk berikutnya.")
                continue 

            logger.info(f"BACKFILL WORKER [{tf}]: Memproses {len(ohlc_df_decimal_renamed_filtered)} lilin fitur dari {ohlc_df_decimal_renamed_filtered.index.min()} sampai {ohlc_df_decimal_renamed_filtered.index.max()}.")

            # --- MODIFIKASI: Dapatkan ATR dari ohlc_df_float_renamed ---
            current_atr_value = Decimal('0.0')
            if not ohlc_df_float_renamed.empty and config.MarketData.ATR_PERIOD > 0:
                atr_series = globals().get('_calculate_atr')(ohlc_df_float_renamed, config.MarketData.ATR_PERIOD)
                if not atr_series.empty and pd.notna(atr_series.iloc[-1]):
                    current_atr_value = utils.to_decimal_or_none(atr_series.iloc[-1])
                    logger.debug(f"BACKFILL WORKER [{tf}]: ATR terakhir untuk chunk ini: {float(current_atr_value):.5f}")
                else:
                    logger.warning(f"BACKFILL WORKER [{tf}]: Gagal menghitung ATR untuk chunk ini. Menggunakan ATR default 0.")
            else:
                logger.warning(f"BACKFILL WORKER [{tf}]: ohlc_df_float_renamed kosong atau ATR_PERIOD tidak valid, tidak dapat menghitung ATR.")
            # --- AKHIR MODIFIKASI ATR ---


            if tf == "M1":
                logger.info(f"BACKFILL WORKER [{tf}]: Melewati analisis fitur untuk M1 (hanya mengambil dan menyimpan candle).")
                pass
            else:
                if config.MarketData.ENABLE_MA_TREND_DETECTION or config.MarketData.ENABLE_EMA_CROSS_DETECTION:
                    detector_monitoring.detector_monitor.wrap_detector_function(
                        globals().get('update_moving_averages_for_single_tf'),
                        symbol_param, tf, ohlc_df_decimal_renamed_filtered # Meneruskan Decimal
                    )

                if config.MarketData.ENABLE_EMA_CROSS_DETECTION:
                    detector_monitoring.detector_monitor.wrap_detector_function(
                        globals().get('_detect_ma_crossover_signals_historically'),
                        symbol_param, tf, ohlc_df_float_renamed # Meneruskan Float
                    )

                if config.MarketData.ENABLE_MA_TREND_DETECTION:
                    logger.debug(f"BACKFILL WORKER [{tf}]: Memanggil get_trend_context_from_ma untuk deteksi tren.")
                    
                    # Trend context juga membutuhkan Decimal untuk internalnya
                    raw_trend_context = detector_monitoring.detector_monitor.wrap_detector_function(
                        globals().get('get_trend_context_from_ma'),
                        symbol_param, tf 
                    )

                    if raw_trend_context is None:
                        trend_context = {"overall_trend": "Error", "reason": "Detektor tren mengembalikan None.", "detailed_votes": {}}
                        logger.error(f"BACKFILL WORKER [{tf}]: Detektor tren mengembalikan None. Tidak dapat menyimpan event tren.")
                    else:
                        trend_context = raw_trend_context

                    logger.debug(f"BACKFILL WORKER [{tf}]: Diagnostik Trend Context: Tipe={type(trend_context)}, Nilai={trend_context}")

                    if trend_context["overall_trend"] not in ["Undefined", "Error"]:
                        latest_candle_time = None
                        latest_candle_close_price = None

                        if not ohlc_df_decimal_renamed_filtered.empty: 
                            latest_candle_time = ohlc_df_decimal_renamed_filtered.index[-1].to_pydatetime()
                            latest_candle_close_price = ohlc_df_decimal_renamed_filtered['close'].iloc[-1]
                        else:
                            logger.warning(f"BACKFILL WORKER [{tf}]: ohlc_df_decimal_renamed_filtered kosong untuk tren, mencoba fallback dari tick/candle D1 terakhir.")
                            
                            latest_tick_for_fallback = database_manager.get_latest_price_tick(symbol_param)
                            if latest_tick_for_fallback and latest_tick_for_fallback.get('time') is not None and latest_tick_for_fallback.get('last_price') is not None:
                                latest_candle_time = latest_tick_for_fallback['time']
                                latest_candle_close_price = latest_tick_for_fallback['last_price']
                            else:
                                last_d1_candle_for_fallback = database_manager.get_historical_candles_from_db(symbol_param, "D1", limit=1)
                                if last_d1_candle_for_fallback:
                                    latest_candle_time = last_d1_candle_for_fallback[0]['open_time_utc']
                                    latest_candle_close_price = last_d1_candle_for_fallback[0]['close_price']
                                else:
                                    latest_candle_time = datetime.now(timezone.utc)
                                    latest_candle_close_price = Decimal('0.0')

                        if latest_candle_time and latest_candle_close_price is not None and latest_candle_close_price != Decimal('0.0'):
                            logger.debug(f"BACKFILL WORKER [{tf}]: Mencoba menyimpan event tren '{trend_context['overall_trend']}' untuk {latest_candle_time}...")

                            try:
                                existing_trend_event = database_manager.get_market_structure_events(
                                    symbol=symbol_param,
                                    timeframe=tf,
                                    event_type="Overall Trend",
                                    start_time_utc=latest_candle_time - timedelta(minutes=utils.timeframe_to_seconds(tf) / 60 + 1),
                                    end_time_utc=latest_candle_time + timedelta(minutes=1)
                                )

                                if not existing_trend_event:
                                    database_manager.save_market_structure_events_batch([
                                        { # Mengirim sebagai list of dictionaries
                                        "symbol": symbol_param,
                                        "timeframe": tf,
                                        "event_type": "Overall Trend",
                                        "direction": trend_context["overall_trend"],
                                        "price_level": latest_candle_close_price,
                                        "event_time_utc": latest_candle_time,
                                        "reason": trend_context["reason"] # Simpan alasan juga jika kolom ada
                                        }
                                    ])
                                    logger.info(f"BACKFILL WORKER [{tf}]: Tren keseluruhan ({trend_context['overall_trend']}) disimpan untuk {latest_candle_time}.")
                                else:
                                    logger.debug(f"BACKFILL WORKER [{tf}]: Tren keseluruhan untuk {latest_candle_time} sudah ada, melewatkan penyimpanan event baru yang persis sama.")
                            except Exception as e:
                                logger.error(f"BACKFILL WORKER [{tf}]: Gagal menyimpan event tren: {e}", exc_info=True)
                        else:
                            logger.warning(f"BACKFILL WORKER [{tf}]: Tidak ada waktu/harga candle terakhir yang valid (atau harga 0) untuk menyimpan event tren. Event: {trend_context['overall_trend']}")
                    else:
                        logger.warning(f"BACKFILL WORKER [{tf}]: Tren keseluruhan tidak valid ('{trend_context['overall_trend']}') untuk disimpan. Event: {trend_context['overall_trend']}")


                if config.MarketData.ENABLE_RSI_CALCULATION:
                    detector_monitoring.detector_monitor.wrap_detector_function(
                        globals().get('_detect_and_save_rsi_historically'),
                        symbol_param, tf, ohlc_df_float_renamed # Meneruskan Float
                    )

                if config.MarketData.ENABLE_MACD_CALCULATION:
                    detector_monitoring.detector_monitor.wrap_detector_function(
                        globals().get('_detect_and_save_macd_historically'),
                        symbol_param, tf, ohlc_df_float_renamed # Meneruskan Float
                    )

                if config.MarketData.ENABLE_DIVERGENCE_DETECTION:
                    detector_monitoring.detector_monitor.wrap_detector_function(
                        globals().get('_detect_new_divergences_historically'),
                        symbol_param, tf, ohlc_df_float_renamed, current_atr_value # Meneruskan Float
                    )
                    detector_monitoring.detector_monitor.wrap_detector_function(
                        globals().get('_update_existing_divergences_status'),
                        symbol_param, tf, ohlc_df_float_renamed, current_atr_value # Meneruskan Float
                    )

                if config.MarketData.ENABLE_OB_FVG_DETECTION:
                    detector_monitoring.detector_monitor.wrap_detector_function(
                        globals().get('_detect_new_fair_value_gaps_historically'),
                        symbol_param, tf, ohlc_df_float_renamed, current_atr_value # Meneruskan Float
                    )
                    detector_monitoring.detector_monitor.wrap_detector_function(
                        globals().get('_update_existing_fair_value_gaps_status'),
                        symbol_param, tf, ohlc_df_float_renamed, current_atr_value # Meneruskan Float
                    )

                if config.MarketData.ENABLE_OB_FVG_DETECTION:
                    detector_monitoring.detector_monitor.wrap_detector_function(
                        globals().get('_detect_new_order_blocks_historically'),
                        symbol_param, tf, ohlc_df_float_renamed, current_atr_value # Meneruskan Float
                    )
                    detector_monitoring.detector_monitor.wrap_detector_function(
                        globals().get('_update_existing_order_blocks_status'),
                        symbol_param, tf, ohlc_df_float_renamed, current_atr_value # Meneruskan Float
                    )

                if config.MarketData.ENABLE_SR_DETECTION:
                    detector_monitoring.detector_monitor.wrap_detector_function(
                        globals().get('_detect_new_sr_levels_historically'),
                        symbol_param, tf, ohlc_df_float_renamed, current_atr_value # Meneruskan Float
                    )
                    detector_monitoring.detector_monitor.wrap_detector_function(
                        globals().get('_update_existing_sr_levels_status'),
                        symbol_param, tf, ohlc_df_float_renamed, current_atr_value # Meneruskan Float
                    )

                if config.MarketData.ENABLE_SR_DETECTION:
                    detector_monitoring.detector_monitor.wrap_detector_function(
                        globals().get('_detect_new_supply_demand_zones_historically'),
                        symbol_param, tf, ohlc_df_float_renamed, current_atr_value # Meneruskan Float
                    )
                    detector_monitoring.detector_monitor.wrap_detector_function(
                        globals().get('_update_existing_supply_demand_zones_status'),
                        symbol_param, tf, ohlc_df_float_renamed, current_atr_value # Meneruskan Float
                    )

                if config.MarketData.ENABLE_MARKET_STRUCTURE_DETECTION:
                    detector_monitoring.detector_monitor.wrap_detector_function(
                        globals().get('_detect_new_market_structure_events_historically'),
                        symbol_param, tf, ohlc_df_float_renamed, current_atr_value # Meneruskan Float
                    )

                if config.MarketData.ENABLE_LIQUIDITY_DETECTION:
                    detector_monitoring.detector_monitor.wrap_detector_function(
                        globals().get('_detect_new_liquidity_zones_historically'),
                        symbol_param, tf, ohlc_df_float_renamed, current_atr_value # Meneruskan Float
                    )
                    detector_monitoring.detector_monitor.wrap_detector_function(
                        globals().get('_update_existing_liquidity_zones_status'),
                        symbol_param, tf, ohlc_df_float_renamed, current_atr_value # Meneruskan Float
                    )

                if config.MarketData.ENABLE_FIBONACCI_DETECTION:
                    detector_monitoring.detector_monitor.wrap_detector_function(
                        globals().get('_detect_new_fibonacci_levels_historically'),
                        symbol_param, tf, ohlc_df_decimal_renamed_filtered, current_atr_value # Meneruskan Decimal
                    )
                    detector_monitoring.detector_monitor.wrap_detector_function(
                        globals().get('_update_existing_fibonacci_levels_status'),
                        symbol_param, tf, ohlc_df_decimal_renamed_filtered, current_atr_value # Meneruskan Decimal
                    )

                if config.MarketData.ENABLE_PREVIOUS_HIGH_LOW_DETECTION:
                    if tf == "M15": htf_for_phl = "H1"
                    elif tf == "M30": htf_for_phl = "H1"
                    elif tf == "H1": htf_for_phl = "H4"
                    elif tf == "H4": htf_for_phl = "D1"
                    elif tf == "D1": htf_for_phl = "D1"
                    else: htf_for_phl = tf
                    
                    detector_monitoring.detector_monitor.wrap_detector_function(
                        lambda s_arg, tf_arg, htf_arg_passed, candles_df_arg, atr_val_passed: \
                            globals().get('_detect_new_previous_high_low_historically')(s_arg, tf_arg, htf_arg_passed, ohlc_df_float_renamed, atr_val_passed), # Meneruskan Float
                        symbol_param, tf, htf_for_phl, ohlc_df_float_renamed, current_atr_value
                    )

                if config.MarketData.ENABLE_RSI_CALCULATION:
                    detector_monitoring.detector_monitor.wrap_detector_function(
                        globals().get('_detect_overbought_oversold_conditions_historically'),
                        symbol_param, tf, ohlc_df_float_renamed # Meneruskan Float
                    )

                if config.MarketData.ENABLE_VOLUME_PROFILE_DETECTION and tf in ["H4", "D1"]:
                    detector_monitoring.detector_monitor.wrap_detector_function(
                        lambda s_arg, tf_arg, candles_df_arg: \
                            globals().get('update_volume_profiles')(s_arg, tf_arg, df_candles_raw), # Meneruskan raw, ini akan diproses lagi di dalamnya
                        symbol_param, tf, df_candles_raw
                    )


            # start_time_for_db_query sudah dimajukan di awal loop.
            # last_processed_time_in_chunk_for_status sudah diset di awal loop.

            start_date_overall_cfg = datetime.strptime(config.MarketData.HISTORICAL_DATA_START_DATE_FULL, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            end_date_overall_current_run = backfill_end_date or datetime.now(timezone.utc)
            if end_date_overall_current_run.tzinfo is None:
                end_date_overall_current_run = end_date_overall_current_run.replace(tzinfo=timezone.utc)

            total_duration_seconds = (end_date_overall_current_run - start_date_overall_cfg).total_seconds()
            processed_duration_seconds = (last_processed_time_in_chunk_for_status - start_date_overall_cfg).total_seconds()

            percentage_complete = 0.0
            if total_duration_seconds > 0:
                percentage_complete = (processed_duration_seconds / total_duration_seconds) * 100
                percentage_complete = min(percentage_complete, 100.0)

            with globals().get('_backfill_progress_lock', threading.Lock()):
                globals().get('_backfill_overall_progress_status')[symbol_param][tf] = {
                    "percentage_complete": percentage_complete,
                    "current_processing_range_start": candles_data_chunk[0]['open_time_utc'].isoformat(),
                    "current_processing_range_end": last_processed_time_in_chunk_for_status.isoformat(),
                    "total_backfill_range_start": start_date_overall_cfg.isoformat(),
                    "total_backfill_range_end": end_date_overall_current_run.isoformat(),
                    "is_running": True,
                    "last_update_time": datetime.now(timezone.utc).isoformat()
                }
                logger.debug(f"BACKFILL PROGRESS UPDATE [{symbol_param}/{tf}]: {percentage_complete:.2f}% selesai. Range: {candles_data_chunk[0]['open_time_utc'].isoformat()} - {last_processed_time_in_chunk_for_status.isoformat()}.")


            if backfill_end_date and start_time_for_db_query > backfill_end_date:
                logger.info(f"BACKFILL WORKER [{tf}]: Mencapai akhir rentang backfill kustom ({backfill_end_date.isoformat()}). Menghentikan backfill untuk TF ini.")
                break

    except Exception as e:
        logger.critical(f"BACKFILL WORKER [{tf}]: Kesalahan KRITIS saat melakukan backfill fitur historis untuk {tf} (chunk dimulai dari {start_time_for_db_query.isoformat() if start_time_for_db_query else 'N/A'}): {e}", exc_info=True)
        import time 
        time.sleep(5)
    finally:
        final_processed_time_for_status = None
        if 'last_processed_time_in_chunk_for_status' in locals() and last_processed_time_in_chunk_for_status is not None:
            final_processed_time_for_status = last_processed_time_in_chunk_for_status
        elif start_time_for_db_query is not None:
            final_processed_time_for_status = start_time_for_db_query
        else:
            final_processed_time_for_status = datetime.now(timezone.utc)

        if final_processed_time_for_status:
            database_manager.save_feature_backfill_status(symbol_param, tf, final_processed_time_for_status)
            logger.debug(f"BACKFILL WORKER [{tf}]: Status backfill akhir disimpan untuk {final_processed_time_for_status.isoformat()}.")
        else:
            logger.warning(f"BACKFILL WORKER [{tf}]: Gagal menentukan waktu untuk menyimpan status backfill akhir.")


        with globals().get('_backfill_progress_lock', threading.Lock()):
            if symbol_param in globals().get('_backfill_overall_progress_status') and tf in globals().get('_backfill_overall_progress_status')[symbol_param]:
                globals().get('_backfill_overall_progress_status')[symbol_param][tf]["is_running"] = False
                globals().get('_backfill_overall_progress_status')[symbol_param][tf]["last_update_time"] = datetime.now(timezone.utc).isoformat()
                if globals().get('_backfill_overall_progress_status')[symbol_param][tf]["percentage_complete"] < 100.0:
                    globals().get('_backfill_overall_progress_status')[symbol_param][tf]["percentage_complete"] = 100.0
                logger.info(f"BACKFILL WORKER [{tf}]: Status backfill untuk {symbol_param} {tf} disetel ke Selesai/Tidak Berjalan.")
            else:
                logger.warning(f"BACKFILL WORKER [{tf}]: Tidak dapat memperbarui status final backfill untuk {symbol_param} {tf} karena entri tidak ditemukan di _backfill_overall_progress_status.")

    logger.info(f"BACKFILL WORKER: Backfill fitur historis untuk {symbol_param} TF {tf} selesai.")


def backfill_historical_features(symbol_param: str, timeframes_to_backfill: list = None, start_date: datetime = None, end_date: datetime = None):
    """
    Melakukan backfill deteksi dan update status untuk semua fitur teknis
    (Moving Averages, Fair Value Gaps, Order Blocks, Support/Resistance Levels,
    Supply/Demand Zones, Market Structure Events, Liquidity Zones, Fibonacci Levels,
    Divergences, Volume Profiles) untuk data historis dari database.
    Proses ini berjalan secara paralel menggunakan worker threads.
    Sekarang dapat menerima rentang tanggal opsional untuk backfill parsial.
    """
    global _dynamic_price_min_for_analysis, _dynamic_price_max_for_analysis, _dynamic_price_lock

    logger.info(f"Memulai BACKFILL fitur historis PARALEL dengan Thread Pool untuk {symbol_param}.")
    if start_date and end_date:
        logger.info(f"Backfill akan berjalan untuk rentang kustom: {start_date.isoformat()} hingga {end_date.isoformat()}.")
    else:
        logger.info("Backfill akan berjalan untuk seluruh sejarah data yang dikonfigurasi.")

    _initialize_symbol_point_value()

    if _SYMBOL_POINT_VALUE is None:
        logger.error(f"Nilai POINT untuk {symbol_param} tidak tersedia. Melewatkan backfill fitur historis.")
        return

    if timeframes_to_backfill is None:
        timeframes_to_backfill = [
            tf for tf, enabled in config.MarketData.ENABLED_TIMEFRAMES.items()
            if enabled and tf in config.MarketData.COLLECT_TIMEFRAMES
        ]
        logger.info(f"Timeframes yang diaktifkan untuk backfill: {timeframes_to_backfill}")
    
    # Ambil tanggal awal analisis dari konfigurasi
    full_historical_start_date_str = config.MarketData.HISTORICAL_DATA_START_DATE_FULL
    try:
        config_analysis_start_datetime = datetime.strptime(full_historical_start_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    except ValueError as ve:
        logger.critical(f"Format HISTORICAL_DATA_START_DATE_FULL di config.py tidak valid: {full_historical_start_date_str}. Error: {ve}", exc_info=True)
        return

    # Tentukan rentang waktu aktual untuk backfill
    actual_backfill_start_datetime = start_date if start_date else config_analysis_start_datetime
    actual_backfill_end_datetime = end_date if end_date else datetime.now(timezone.utc)

    # --- PERUBAHAN UTAMA: Menentukan Batas Harga Dinamis GLOBAL untuk SELURUH DATA DI DB ---
    with _dynamic_price_lock:
        historical_low_in_range, historical_high_in_range = _find_historical_price_extremes_in_range(
            symbol_param,
            config_analysis_start_datetime,
            datetime.now(timezone.utc)
        )
        
        _dynamic_price_min_for_analysis = Decimal('0.0')
        _dynamic_price_max_for_analysis = Decimal('99999.0')

        if historical_low_in_range is not None:
            _dynamic_price_min_for_analysis = historical_low_in_range
        else:
             logger.warning(f"Gagal menemukan harga low historis, menggunakan fallback {float(_dynamic_price_min_for_analysis)}.")

        if historical_high_in_range is not None:
            _dynamic_price_max_for_analysis = historical_high_in_range
        else:
            latest_tick = mt5_connector.get_current_tick_info(symbol_param)
            if latest_tick and latest_tick.get('high') is not None and latest_tick.get('high') > 0:
                 _dynamic_price_max_for_analysis = Decimal(str(latest_tick['high']))
            else:
                last_d1_candle = database_manager.get_historical_candles_from_db(symbol_param, "D1", limit=1)
                if last_d1_candle:
                    _dynamic_price_max_for_analysis = Decimal(str(last_d1_candle[0]['close_price']))
                else:
                    _dynamic_price_max_for_analysis = Decimal('99999.0')
            logger.warning(f"Gagal menemukan harga high historis, menggunakan harga terakhir atau fallback {float(_dynamic_price_max_for_analysis)}.")


        padding_percentage = Decimal('0.005')
        price_range = _dynamic_price_max_for_analysis - _dynamic_price_min_for_analysis
        padding = price_range * padding_percentage

        _dynamic_price_min_for_analysis -= padding
        _dynamic_price_max_for_analysis += padding


        logger.info(f"Batas harga dinamis GLOBAL untuk analisis ditetapkan: LOW: {float(_dynamic_price_min_for_analysis):.5f}, HIGH: {float(_dynamic_price_max_for_analysis):.5f}")

    # Buat antrean tugas
    task_queue = queue.Queue()
    for tf in timeframes_to_backfill:
        task_queue.put({
            'timeframe': tf,
            'start_date': actual_backfill_start_datetime,
            'end_date': actual_backfill_end_datetime
        })

    # Tentukan jumlah worker thread
    num_worker_threads = os.cpu_count() or 4
    logger.info(f"Meluncurkan {num_worker_threads} worker thread untuk backfill paralel.")

    # Buat dan mulai worker thread
    worker_threads = []
    for i in range(num_worker_threads):
        thread = threading.Thread(
            target=_backfill_worker_thread,
            args=(task_queue, symbol_param),
            name=f"BackfillWorker-{i+1}",
            daemon=True
        )
        worker_threads.append(thread)
        thread.start()

    # Tunggu semua tugas di antrean selesai diproses oleh worker
    logger.info("Koordinator backfill menunggu semua tugas diselesaikan oleh worker thread...")
    task_queue.join() 

    logger.info(f"BACKFILL fitur historis PARALEL dengan Thread Pool untuk {symbol_param} selesai total.")

    # --- START: Panggilan ke identify_key_levels_across_timeframes (MODIFIKASI BARU) ---
    try:
        logger.info(f"Memulai perhitungan confluence score lintas timeframe untuk {symbol_param} setelah backfill selesai.")
        identify_key_levels_across_timeframes(symbol_param)
        logger.info(f"Perhitungan confluence score lintas timeframe untuk {symbol_param} selesai.")
    except Exception as e:
        logger.error(f"Gagal menghitung confluence score lintas timeframe untuk {symbol_param}: {e}", exc_info=True)
    # --- END: Panggilan ke identify_key_levels_across_timeframes ---

    # Deteksi ekstrem tahunan setelah backfill utama selesai
    try:
        _detect_yearly_extremes(symbol_param, config_analysis_start_datetime)
    except Exception as e:
        logger.error(f"Gagal mendeteksi ekstrem tahunan untuk {symbol_param}: {e}", exc_info=True)

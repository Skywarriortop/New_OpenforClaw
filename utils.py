# utils.py

import math
from copy import deepcopy
from multiprocessing import cpu_count, Pool
from enum import Enum # <--- PASTIKAN BARIS INI ADA
import pandas as pd
import numpy as np
import logging
import re
import time
from decimal import Decimal
from datetime import datetime, timezone

# Konfigurasi logger untuk utils
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG) # Sesuaikan level logging sesuai kebutuhan

# Handler untuk logging ke konsol jika belum ada
if not logger.handlers:
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)


def retry_on_failure(retries=3, delay=5):
    """
    Dekorator untuk mencoba ulang fungsi jika terjadi pengecualian.
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            for i in range(retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logger.warning(f"Percobaan {i+1}/{retries} untuk fungsi '{func.__name__}' gagal: {e}")
                    if i < retries - 1:
                        time.sleep(delay)
                    else:
                        logger.error(f"Fungsi '{func.__name__}' gagal setelah {retries} percobaan.")
                        raise
        return wrapper
    return decorator


def _handle_common_none_cases(value):
    """Fungsi pembantu untuk menangani kasus None, NaN, dan Pandas NaT."""
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, pd.Series) and value.empty:
        return None
    if isinstance(value, pd.Timestamp) and pd.isna(value):
        return None
    # Menambahkan penanganan untuk numpy.ndarray yang berisi NaN atau None
    if isinstance(value, np.ndarray):
        if value.size == 0:
            return None
        if value.size == 1 and (pd.isna(value.item()) or value.item() is None):
            return None
    return value

def to_int_or_none(value):
    """Mengonversi nilai ke integer Python standar, atau None jika input None/NaN."""
    value = _handle_common_none_cases(value)
    if value is None:
        return None
    try:
        if isinstance(value, (int, np.integer)):
            return int(value)
        if isinstance(value, (float, np.floating)):
            if value.is_integer():
                return int(value)
            else: # Jika float tapi bukan integer (misal 5.1)
                logger.warning(f"Gagal mengonversi float '{value}' (tipe: {type(value)}) ke int karena bukan bilangan bulat. Mengembalikan None.")
                return None
        # --- KOREKSI UNTUK DECIMAL ---
        if isinstance(value, Decimal):
            if value.remainder_near(Decimal('1')) == Decimal('0'): # Cek apakah Decimal adalah bilangan bulat
                return int(value)
            else: # Jika Decimal tapi bukan bilangan bulat (misal Decimal('5.1'))
                logger.warning(f"Gagal mengonversi Decimal '{value}' (tipe: {type(value)}) ke int karena bukan bilangan bulat. Mengembalikan None.")
                return None
        # --- AKHIR KOREKSI ---
        # Untuk string atau tipe lain yang bisa dikonversi secara langsung
        return int(str(value))
    except (ValueError, TypeError):
        logger.warning(f"Gagal mengonversi '{value}' (tipe: {type(value)}) ke int. Mengembalikan None.")
        return None



def _get_pandas_freq_alias(timeframe_str: str) -> str:
    """
    Mengonversi alias timeframe internal ke alias frekuensi Pandas yang disarankan.
    """
    if timeframe_str.endswith('M') and len(timeframe_str) > 1: # Contoh: '15M', '30M'
        return timeframe_str[:-1] + 'min'
    if timeframe_str == 'M1': return '1min'
    if timeframe_str == 'M5': return '5min'
    if timeframe_str == 'M15': return '15min'
    if timeframe_str == 'M30': return '30min'
    if timeframe_str == 'H1': return 'H' # Pandas menggunakan 'H' untuk hourly
    if timeframe_str == 'H4': return '4H'
    if timeframe_str == 'D1': return 'D' # Pandas menggunakan 'D' untuk daily
    return timeframe_str # Kembali seperti semula jika tidak ada pemetaan khusus


def to_bool_or_none(value):
    """Mengonversi nilai ke boolean Python standar, atau None jika input None/NaN."""
    value = _handle_common_none_cases(value)
    if value is None:
        return None
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, str):
        if value.lower() in ('true', '1', 't', 'y', 'yes'):
            return True
        if value.lower() in ('false', '0', 'f', 'n', 'no'):
            return False
    logger.warning(f"Gagal mengonversi '{value}' (tipe: {type(value)}) ke bool. Mengembalikan None.")
    return None

def _get_scalar_from_possibly_ndarray(val):
    """
    Mengambil nilai skalar dari objek yang mungkin numpy.ndarray
    atau nilai Python standar, dan mengonversi NaN ke None.
    """
    if isinstance(val, np.ndarray):
        if val.size == 1:
            val = val.item()
        else:
            # Jika array memiliki lebih dari 1 elemen, ini adalah masalah desain kolom
            # Log error atau lewati kolom ini, tergantung kebutuhan Anda.
            logger.warning(f"Menyimpan numpy.ndarray dengan lebih dari satu elemen sebagai skalar. Akan dikonversi ke None. Array: {val}")
            return None
    if pd.isna(val):
        return None
    return val


def timeframe_to_seconds(timeframe_str):
    if timeframe_str == "M1": return 60
    if timeframe_str == "M5": return 300
    if timeframe_str == "M15": return 900
    if timeframe_str == "M30": return 1800
    if timeframe_str == "H1": return 3600
    if timeframe_str == "H4": return 14400
    if timeframe_str == "D1": return 86400
    return 0


def to_float_or_none(value):
    """
    Mengonversi nilai ke float. Menangani Decimal, string numerik, None, NaN, dan Series/array.
    Mengembalikan None jika input tidak valid.
    """
    value = _handle_common_none_cases(value)
    if value is None:
        return None

    if isinstance(value, (pd.Series, np.ndarray)):
        if isinstance(value, pd.Series) and not value.empty:
            value = value.iloc[0]
        elif isinstance(value, np.ndarray) and value.size > 0:
            value = value.item() if value.size == 1 else value[0]
        
        if pd.isna(value):
            return None

    try:
        return float(value)
    except (ValueError, TypeError):
        logger.warning(f"Gagal mengonversi '{value}' (tipe: {type(value)}) ke float. Mengembalikan None.")
        return None


def to_decimal_or_none(value):
    """
    Mengonversi nilai ke Decimal. Menangani None, NaN, dan Series/array,
    serta string persentase. Mengembalikan None jika input tidak valid.
    """
    value = _handle_common_none_cases(value)
    if value is None:
        return None

    # --- MULAI MODIFIKASI: Tambahkan penanganan eksplisit untuk tipe numerik NumPy ---
    # `np.integer` mencakup np.intc, np.uint64, dll.
    # `np.floating` mencakup np.float32, np.float64, dll.
    if isinstance(value, (np.integer, np.floating)):
        try:
            # Konversi ke float Python standar terlebih dahulu.
            # Ini adalah langkah perantara yang lebih aman sebelum ke Decimal.
            value = float(value) 
        except (ValueError, TypeError):
            # Jika konversi NumPy ke float pun gagal, log peringatan dan kembalikan None.
            logger.warning(f"Gagal mengonversi tipe numerik NumPy '{type(value)}' dengan nilai '{value}' ke float. Mengembalikan None.")
            return None
    # --- AKHIR MODIFIKASI ---

    if isinstance(value, (pd.Series, np.ndarray)):
        if isinstance(value, pd.Series) and not value.empty:
            value = value.iloc[0]
        elif isinstance(value, np.ndarray) and value.size > 0:
            # Ambil elemen pertama jika array memiliki banyak elemen.
            # Jika array memiliki lebih dari satu elemen dan ini bukan skenario yang diharapkan,
            # _get_scalar_from_possibly_ndarray() sudah harus menanganinya.
            value = value.item() if value.size == 1 else value[0]

        if pd.isna(value):
            return None

    if isinstance(value, str):
        # Hapus karakter '%' dan spasi dari string.
        cleaned_value = value.replace('%', '').strip()
        try:
            # Coba konversi string bersih ke Decimal.
            return Decimal(cleaned_value)
        except (ValueError, TypeError, Exception):
            logger.warning(f"Gagal mengonversi '{value}' (tipe: {type(value)}) ke Decimal setelah membersihkan string. Mengembalikan None.")
            return None

    # Penanganan untuk float Python standar.
    if isinstance(value, float):
        if math.isinf(value) or math.isnan(value): # Tangani nilai infinity dan NaN dari float.
            return None
        # Konversi float melalui string untuk **menjaga presisi** saat mengubah ke Decimal.
        return Decimal(str(value)) 

    try:
        # Coba konversi tipe lain (misalnya integer Python standar, atau objek Decimal yang sudah ada) langsung ke Decimal.
        return Decimal(value) 
    except (ValueError, TypeError, Exception):
        logger.warning(f"Gagal mengonversi '{value}' (tipe: {type(value)}) ke Decimal. Mengembalikan None.")
        return None

def to_utc_datetime_or_none(value):
    """
    Mengonversi nilai ke objek datetime timezone-aware UTC, atau None jika input tidak valid.
    Menangani string ISO, objek datetime naive/aware, Pandas Timestamp, dan timestamp Unix (int/float/numpy.int64).
    """
    value = _handle_common_none_cases(value)
    if value is None:
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        else:
            return value.astimezone(timezone.utc)
    elif isinstance(value, pd.Timestamp):
        if value.tzinfo is None:
            return value.tz_localize(timezone.utc).to_pydatetime()
        else:
            return value.tz_convert(timezone.utc).to_pydatetime()
    # >>> START KOREKSI UNTUK NUMERIC TIMESTAMP <<<
    elif isinstance(value, (int, float, np.integer, np.floating)):
        # Periksa apakah nilainya terlalu kecil untuk menjadi timestamp yang valid (misalnya, 123)
        # Ambang batas ini bisa disesuaikan, misalnya, di atas tahun 2000 (946684800)
        if value < 946684800: # Jika kurang dari 2000-01-01 00:00:00 UTC
            logger.warning(f"Timestamp numerik '{value}' (tipe: {type(value)}) terlalu kecil untuk dianggap sebagai timestamp Unix yang valid. Mengembalikan None.")
            return None
        try:
            # Heuristic: Jika angka sangat besar, mungkin milidetik
            if value > 100000000000:
                 value = value / 1000 # Konversi ke detik
            return datetime.fromtimestamp(value, tz=timezone.utc)
        except (ValueError, TypeError) as e:
            logger.warning(f"Gagal mengonversi timestamp numerik '{value}' (tipe: {type(value)}) ke datetime aware UTC: {e}. Mengembalikan None.")
            return None
    # >>> END KOREKSI UNTUK NUMERIC TIMESTAMP <<<
    elif isinstance(value, (str, np.datetime64)):
        try:
            dt_obj_pd = pd.to_datetime(value, utc=True, errors='coerce')
            if pd.notna(dt_obj_pd):
                return dt_obj_pd.to_pydatetime()
            else:
                logger.warning(f"Gagal mengonversi '{value}' (tipe: {type(value)}) ke datetime aware UTC. Mengembalikan None.")
                return None
        except (ValueError, TypeError, AttributeError):
            logger.warning(f"Gagal mengonversi '{value}' (tipe: {type(value)}) ke datetime aware UTC. Mengembalikan None.")
            return None

    logger.warning(f"Tipe tidak didukung untuk konversi datetime: '{value}' (tipe: {type(value)}). Mengembalikan None.")
    return None


def to_iso_format_or_none(dt_obj):
    """
    Mengonversi objek datetime ke string ISO (UTC), atau mengembalikan None jika input tidak valid.
    Memastikan datetime dikonversi ke UTC-aware sebelum format.
    """
    # Gunakan fungsi helper baru untuk memastikan dt_obj adalah datetime aware UTC
    dt_obj_utc = to_utc_datetime_or_none(dt_obj)
    if dt_obj_utc is None:
        return None
    try:
        return dt_obj_utc.isoformat()
    except (ValueError, TypeError):
        logger.warning(f"Gagal mengonversi '{dt_obj_utc}' (tipe: {type(dt_obj_utc)}) ke string ISO. Mengembalikan None.")
        return None


def _json_default(obj):
    """
    Fungsi bantu untuk serialisasi objek Decimal dan datetime ke JSON.
    Digunakan dalam json.dumps(default=...)
    """
    if isinstance(obj, Decimal):
        return float(obj)
    # Pastikan objek datetime dikonversi ke UTC sebelum serialisasi
    if isinstance(obj, datetime):
        return to_iso_format_or_none(obj) # Menggunakan helper baru
    raise TypeError(f"{repr(obj)} is not JSON serializable")


def _escape_markdown(text):
    """
    Meng-escape karakter khusus MarkdownV2 agar tidak diinterpretasikan sebagai formatting.
    Ini adalah daftar lengkap yang direkomendasikan oleh Telegram untuk MarkdownV2.
    Pastikan ini mencakup semua karakter yang bisa menyebabkan masalah, termasuk '!'
    Urutan penting: backslash harus di-escape duluan!
    """
    if not isinstance(text, str):
        text = str(text)

    # Escape backslash terlebih dahulu untuk menghindari double escaping pada backslash itu sendiri
    escaped_text = text.replace('\\', '\\\\')

    # Daftar karakter khusus MarkdownV2 yang perlu di-escape
    # Menambahkan '!' ke dalam daftar ini
    reserved_chars = [
        '_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!'
    ]
    
    # Escape karakter khusus lainnya
    for char in reserved_chars:
        # Hanya escape jika karakter tersebut bukan bagian dari backslash yang sudah di-escape
        escaped_text = escaped_text.replace(char, f'\\{char}')
    
    return escaped_text

def _get_available_volume_series(df_candles: pd.DataFrame, timeframe_str: str) -> pd.Series:
    """
    Mengembalikan Series Pandas dari kolom volume yang tersedia dan bermakna dari DataFrame candle.
    Prioritas: real_volume (dari df_candles.columns) > tick_volume (dari df_candles.columns).
    Jika tidak ada volume valid, kembalikan Series nol.
    
    Args:
        df_candles (pd.DataFrame): DataFrame candle yang sudah di-rename kolomnya
                                   (misalnya 'real_volume', 'tick_volume', 'open', 'high', dll.)
                                   dan dikonversi ke tipe data yang sesuai (Decimal atau float).
        timeframe_str (str): Timeframe dari data candle (untuk logging).
        
    Returns:
        pd.Series: Series Pandas yang berisi nilai volume terbaik yang tersedia.
    """
    # Pastikan df_candles memiliki indeks DatetimeIndex dan kolom numerik
    if not isinstance(df_candles.index, pd.DatetimeIndex):
        logger.error(f"[_get_available_volume_series] DataFrame input tidak memiliki DatetimeIndex untuk {timeframe_str}. Mengembalikan Series nol.")
        return pd.Series(Decimal('0.0'), index=pd.to_datetime([], utc=True))

    # Pastikan kolom volume adalah numerik (Decimal) dan bukan NaN
    # Gunakan .apply(utils.to_decimal_or_none) untuk konversi yang aman
    # dan fillna(Decimal('0.0')) untuk menanggulangi NaN setelah konversi
    
    # Cek ketersediaan dan validitas real_volume
    if 'real_volume' in df_candles.columns:
        real_vol_series = df_candles['real_volume'].apply(utils.to_decimal_or_none).fillna(Decimal('0.0')) # <-- ini akan diperbaiki oleh import utils
        if real_vol_series.sum() > 0: # Cek jika ada total volume real yang berarti
            logger.debug(f"[_get_available_volume_series] Menggunakan real_volume untuk perhitungan volume di TF {timeframe_str}.")
            return real_vol_series
    
    # Fallback ke tick_volume jika real_volume tidak tersedia atau nol
    if 'tick_volume' in df_candles.columns:
        tick_vol_series = df_candles['tick_volume'].apply(utils.to_decimal_or_none).fillna(Decimal('0.0')) # <-- ini akan diperbaiki oleh import utils
        if tick_vol_series.sum() > 0: # Cek jika ada total tick_volume yang berarti
            logger.warning(f"[_get_available_volume_series] Real volume tidak tersedia. Menggunakan tick_volume sebagai fallback untuk perhitungan volume di TF {timeframe_str}. Pertimbangkan akurasi.")
            return tick_vol_series
    
    logger.warning(f"[_get_available_volume_series] Tidak ada kolom volume yang valid atau terisi di TF {timeframe_str}. Perhitungan berbasis volume mungkin tidak akurat.")
    return pd.Series(Decimal('0.0'), index=df_candles.index) # Mengembalikan Series nol jika tidak ada yang valid


# --- ENUMERASI ---
class DistributionData(Enum):
    OHLC = 1
    OHLC_No_Avg = 2
    Open = 3
    High = 4
    Low = 5
    Close = 6
    Uniform_Distribution = 7
    Uniform_Presence = 8
    Parabolic_Distribution = 9
    Triangular_Distribution = 10
    Tick_By_Tick = 11 # <--- PASTIKAN BARIS INI ADA

class VolumeType(Enum):
    TICK = 1
    REAL = 2
    MONEY = 3

class SymbolType(Enum):
    FOREX = 1
    STOCK = 2
    CRYPTO = 3
    COMMODITY = 4
    INDEX = 5

class Timeframe(Enum):
    M1 = 1
    M5 = 5
    M15 = 15
    M30 = 30
    H1 = 60
    H4 = 240
    D1 = 1440
    W1 = 10080
    MN1 = 43200
# ml_predictor.py

import logging
import pandas as pd
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import numpy as np
import joblib
import talib
import time

# Import modul yang Anda miliki
import database_manager
import utils
from config import config
import config
# Import model Machine Learning
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report

# --- TAMBAHAN BARU UNTUK PENANGANAN IMBALANCE ---
from imblearn.over_sampling import SMOTE
# from imblearn.under_sampling import RandomUnderSampler # Jika ingin mencoba undersampling juga
# from imblearn.pipeline import Pipeline # Jika ingin chaining proses
# --- AKHIR TAMBAHAN ---

logger = logging.getLogger(__name__)
# ... sisa kode ...

logger = logging.getLogger(__name__)
# --- MODIFIKASI KONFIGURASI LOGGER ---
logger.setLevel(logging.DEBUG) # Set level logger ke DEBUG untuk melihat semua pesan

# Hapus handler yang mungkin sudah ada untuk menghindari duplikasi
for handler in logger.handlers[:]:
    logger.removeHandler(handler)
logger.propagate = False # Penting untuk mencegah duplikasi log jika root logger sudah dikonfigurasi

# Tambahkan StreamHandler untuk menampilkan log di konsol
ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

# Tambahkan FileHandler untuk mencatat log ke file ml_predictor.log
ML_PREDICTOR_LOG_FILE = "ml_predictor.log"
fh = logging.FileHandler(ML_PREDICTOR_LOG_FILE)
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)

# Atur level logging untuk pustaka yang "berisik"
logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
logging.getLogger('sqlalchemy.pool').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)
logging.getLogger('requests').setLevel(logging.WARNING)
# --- AKHIR MODIFIKASI KONFIGURASI LOGGER ---


# Path untuk menyimpan model yang terlatih
MODEL_PATH = "trained_ml_model.joblib"

def load_historical_data_for_ml(symbol: str, timeframe: str, num_candles: int) -> pd.DataFrame:
    """
    Memuat data candle historis dari database dan mengonversinya ke DataFrame.
    Kolom harga dikonversi ke float untuk keperluan ML.
    """
    logger.info(f"ML_PREDICTOR: Memuat {num_candles} candle historis untuk ML: {symbol} {timeframe}")
    candles_data_raw = database_manager.get_historical_candles_from_db(
        symbol=symbol,
        timeframe=timeframe,
        limit=num_candles,
        order_asc=True # Data berurutan waktu untuk time series
    )

    if not candles_data_raw:
        logger.warning(f"ML_PREDICTOR: Tidak ada data candle ditemukan untuk {symbol} {timeframe}.")
        return pd.DataFrame()

    df = pd.DataFrame(candles_data_raw)
    df['open_time_utc'] = pd.to_datetime(df['open_time_utc'], utc=True)
    df = df.set_index('open_time_utc').sort_index()

    # Rename dan konversi ke float untuk keperluan ML (TA-Lib, scikit-learn)
    df = df.rename(columns={
        'open_price': 'open', 'high_price': 'high', 'low_price': 'low',
        'close_price': 'close', 'tick_volume': 'volume', 'real_volume': 'real_volume'
    })
    for col in ['open', 'high', 'low', 'close', 'volume', 'real_volume']:
        if col in df.columns:
            df[col] = df[col].apply(utils.to_float_or_none)
    
    # Hapus baris dengan NaN di kolom OHLCV yang krusial
    initial_len = len(df)
    df.dropna(subset=['open', 'high', 'low', 'close', 'volume'], inplace=True)
    if len(df) < initial_len:
        logger.warning(f"ML_PREDICTOR: Dihapus {initial_len - len(df)} baris dengan NaN setelah konversi float.")

    logger.info(f"ML_PREDICTOR: Berhasil memuat {len(df)} candle untuk ML.")
    return df

def generate_features_and_labels(df_candles: pd.DataFrame, prediction_horizon: int = 1) -> tuple[pd.DataFrame, pd.Series]:
    """
    Membuat fitur dan label dari data candle untuk pelatihan model ML.
    Menggunakan periode indikator dari konfigurasi.
    """
    logger.info(f"ML_PREDICTOR: Memulai pembuatan fitur dan label. Panjang DF: {len(df_candles)}")

    # Perlu memastikan ini cukup panjang untuk periode indikator terpanjang
    min_required_candles_for_indicators = max(
        int(config.config.MarketData.MA_PERIODS_TO_CALCULATE[1]), # EMA Long Period
        config.config.AIAnalysts.RSI_PERIOD, #
        config.config.AIAnalysts.MACD_SLOW_PERIOD + config.config.AIAnalysts.MACD_SIGNAL_PERIOD, #
        config.config.MarketData.ATR_PERIOD #
    ) + prediction_horizon # Tambah horizon prediksi

    if df_candles.empty or len(df_candles) < min_required_candles_for_indicators:
        logger.warning(f"ML_PREDICTOR: Data candle tidak cukup ({len(df_candles)}) untuk pembuatan fitur. Diperlukan minimal {min_required_candles_for_indicators} lilin.")
        return pd.DataFrame(), pd.Series()

    close_prices = df_candles['close']

    # EMA Crossover
    ema_short_period = int(config.config.MarketData.MA_PERIODS_TO_CALCULATE[0]) # Ambil dari config
    ema_long_period = int(config.config.MarketData.MA_PERIODS_TO_CALCULATE[1])  # Ambil dari config
    df_candles['EMA_Short'] = close_prices.ewm(span=ema_short_period, adjust=False).mean()
    df_candles['EMA_Long'] = close_prices.ewm(span=ema_long_period, adjust=False).mean()
    df_candles['EMA_Cross'] = 0
    df_candles.loc[df_candles['EMA_Short'] > df_candles['EMA_Long'], 'EMA_Cross'] = 1
    df_candles.loc[df_candles['EMA_Short'] < df_candles['EMA_Long'], 'EMA_Cross'] = -1

    # RSI
    rsi_period = config.config.AIAnalysts.RSI_PERIOD # Ambil dari config
    df_candles['RSI'] = pd.Series(talib.RSI(close_prices.values.astype(float), timeperiod=rsi_period), index=df_candles.index)

    # MACD
    macd_fast = config.config.AIAnalysts.MACD_FAST_PERIOD     # Ambil dari config
    macd_slow = config.config.AIAnalysts.MACD_SLOW_PERIOD     # Ambil dari config
    macd_signal = config.config.AIAnalysts.MACD_SIGNAL_PERIOD # Ambil dari config
    macd, macd_signal_line, macd_hist = talib.MACD(close_prices.values.astype(float), fastperiod=macd_fast, slowperiod=macd_slow, signalperiod=macd_signal)
    df_candles['MACD_Hist'] = pd.Series(macd_hist, index=df_candles.index)

    # Volatilitas (ATR)
    atr_period = config.config.MarketData.ATR_PERIOD # Ambil dari config
    df_candles['ATR'] = pd.Series(talib.ATR(df_candles['high'].values.astype(float), df_candles['low'].values.astype(float), df_candles['close'].values.astype(float), timeperiod=atr_period), index=df_candles.index)



    # Normalisasi Fitur (Opsional tapi disarankan untuk model tertentu)
    # Misalnya, skala RSI ke 0-1, normalisasi MACD, ATR
    df_candles['RSI_Scaled'] = df_candles['RSI'] / 100.0
    df_candles['MACD_Hist_Scaled'] = df_candles['MACD_Hist'] / df_candles['close'] # Skala relatif harga
    df_candles['ATR_Scaled'] = df_candles['ATR'] / df_candles['close'] # Skala relatif harga

    # --- Pembuatan Label ---
    # Label: apakah harga akan naik (1), turun (-1), atau tetap (0) dalam `prediction_horizon` bar.
    # Misalnya, jika close harga `prediction_horizon` bar ke depan lebih tinggi dari close saat ini.
    
    # Ambil harga penutupan di masa depan sebagai dasar label
    df_candles['Future_Close'] = df_candles['close'].shift(-prediction_horizon)
    
    # Toleransi untuk "tetap" (sideways)
    # Gunakan ATR sebagai referensi untuk toleransi pergerakan
    if not df_candles['ATR'].empty and df_candles['ATR'].mean() is not np.nan: # Cek jika ATR tidak kosong/NaN
        sideways_tolerance = df_candles['ATR'].mean() * 0.1 # Contoh: 10% dari rata-rata ATR
    else:
        sideways_tolerance = 0.001 # Fallback
    
    def get_label(row):
        if pd.isna(row['Future_Close']) or pd.isna(row['close']):
            return np.nan
        if (row['Future_Close'] - row['close']) > sideways_tolerance:
            return 1 # Naik
        elif (row['Future_Close'] - row['close']) < -sideways_tolerance:
            return -1 # Turun
        else:
            return 0 # Tetap

    df_candles['Label'] = df_candles.apply(get_label, axis=1)
    # Deteksi pola Hammer
    # talib.CDLHAMMER akan mengembalikan 100 jika pola terdeteksi, -100 jika inversi, dan 0 jika tidak ada
    df_candles['CDLHAMMER'] = pd.Series(
        talib.CDLHAMMER(
            df_candles['open'].values.astype(float),
            df_candles['high'].values.astype(float),
            df_candles['low'].values.astype(float),
            df_candles['close'].values.astype(float)
        ),
        index=df_candles.index
    )
    # Ubah menjadi fitur biner: 1 jika Hammer, 0 jika tidak
    df_candles['Feature_Hammer'] = (df_candles['CDLHAMMER'] == 100).astype(int)

    # Pilih fitur yang akan digunakan untuk model ML
    features_cols = ['EMA_Cross', 'RSI_Scaled', 'MACD_Hist_Scaled', 'ATR_Scaled']
    
    # Hapus baris dengan NaN yang dihasilkan oleh indikator atau label
    df_features_labels = df_candles[features_cols + ['Label']].dropna()

    features = df_features_labels[features_cols]
    labels = df_features_labels['Label']

    logger.info(f"ML_PREDICTOR: Fitur dan label dibuat. Jumlah sampel: {len(features)}. Label distribusi:\n{labels.value_counts()}")
    return features, labels

def train_and_evaluate_model(features: pd.DataFrame, labels: pd.Series, model_type: str = 'RandomForest'):
    """
    Melatih model Machine Learning dan mengevaluasi kinerjanya.
    """
    logger.info(f"ML_PREDICTOR: Memulai pelatihan model {model_type}...")

    if features.empty or labels.empty:
        logger.warning("ML_PREDICTOR: Fitur atau label kosong. Tidak dapat melatih model.")
        return None

    # Pisahkan data menjadi set pelatihan dan pengujian
    X_train, X_test, y_train, y_test = train_test_split(features, labels, test_size=0.2, random_state=42, stratify=labels)

    # --- TAMBAHAN BARU UNTUK PENANGANAN IMBALANCE PADA DATA PELATIHAN ---
    logger.info(f"ML_PREDICTOR: Distribusi label pelatihan sebelum SMOTE:\n{y_train.value_counts()}")

    # Inisialisasi SMOTE
    # sampling_strategy='auto' akan oversample semua kelas minoritas hingga jumlah sampel mayoritas
    oversampler = SMOTE(sampling_strategy='auto', random_state=42)

    # Terapkan SMOTE hanya pada data pelatihan
    X_train_resampled, y_train_resampled = oversampler.fit_resample(X_train, y_train)

    logger.info(f"ML_PREDICTOR: Distribusi label pelatihan setelah SMOTE:\n{y_train_resampled.value_counts()}")
    # --- AKHIR TAMBAHAN ---

    model = None
    if model_type == 'RandomForest':
        # Tetap gunakan class_weight='balanced' sebagai lapisan keamanan atau bisa dihilangkan jika SMOTE sudah cukup
        model = RandomForestClassifier(n_estimators=100, random_state=42, class_weight='balanced')
    else:
        logger.error(f"ML_PREDICTOR: Tipe model '{model_type}' tidak didukung.")
        return None

    # Latih model dengan data yang sudah di-resample
    model.fit(X_train_resampled, y_train_resampled) # GUNAKAN DATA YANG DI-RESAMPLE DI SINI

    # Evaluasi model (menggunakan X_test dan y_test ASLI)
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    report = classification_report(y_test, y_pred)

    logger.info(f"ML_PREDICTOR: Pelatihan model {model_type} selesai. Akurasi: {accuracy:.2f}")
    logger.debug(f"ML_PREDICTOR: Classification Report:\n{report}")

    return model

def save_ml_model(model, filename: str = MODEL_PATH):
    """Menyimpan model ML yang terlatih ke disk."""
    try:
        joblib.dump(model, filename)
        logger.info(f"ML_PREDICTOR: Model ML berhasil disimpan ke {filename}")
    except Exception as e:
        logger.error(f"ML_PREDICTOR: Gagal menyimpan model ML: {e}", exc_info=True)

def load_ml_model(filename: str = MODEL_PATH):
    """Memuat model ML yang terlatih dari disk."""
    try:
        model = joblib.load(filename)
        logger.info(f"ML_PREDICTOR: Model ML berhasil dimuat dari {filename}")
        return model
    except FileNotFoundError:
        logger.warning(f"ML_PREDICTOR: File model tidak ditemukan: {filename}. Model perlu dilatih.")
        return None
    except Exception as e:
        logger.error(f"ML_PREDICTOR: Gagal memuat model ML: {e}", exc_info=True)
        return None

def predict_with_model(model, features_df: pd.DataFrame) -> int:
    """
    Membuat prediksi arah pergerakan harga menggunakan model ML yang terlatih.
    Args:
        model: Model ML yang sudah terlatih.
        features_df (pd.DataFrame): DataFrame berisi fitur-fitur untuk prediksi (satu baris data).
                                    Pastikan kolomnya sesuai dengan yang digunakan saat pelatihan.
    Returns:
        int: Prediksi arah (1=Naik, -1=Turun, 0=Tetap).
    """
    if model is None:
        logger.error("ML_PREDICTOR: Model ML belum dimuat atau terlatih. Tidak dapat membuat prediksi.")
        return 0 # Default ke "Tetap" jika model tidak siap

    if features_df.empty:
        logger.warning("ML_PREDICTOR: Fitur input untuk prediksi kosong.")
        return 0

    try:
        prediction = model.predict(features_df)
        logger.info(f"ML_PREDICTOR: Prediksi model: {prediction[0]}")
        return int(prediction[0])
    except Exception as e:
        logger.error(f"ML_PREDICTOR: Gagal membuat prediksi: {e}", exc_info=True)
        return 0 # Default ke "Tetap" jika ada error


if __name__ == '__main__':
    logger.info("ML_PREDICTOR: Script dimulai.")
    
    # Pastikan koneksi DB diinisialisasi untuk ml_predictor
    try:
        # Inisialisasi objek config terlebih dahulu
        my_app_config = config.Config() 
        database_manager.init_db_connection(my_app_config.Database.URL)
        database_manager.init_db_writer() # Mulai writer thread jika belum

        # Pastikan _SYMBOL_POINT_VALUE diinisialisasi untuk digunakan oleh utils atau modul lain
        # Ini penting karena generate_features_and_labels mungkin memerlukan nilai titik
        # atau ATR (yang mungkin bergantung pada nilai titik)
        # Untuk saat ini, asumsikan itu akan dihandle di market_data_processor saat data diambil.
        # Jika ada error lagi di generate_features_and_labels terkait Decimal, kita akan atasi.

    except Exception as e:
        logger.critical(f"ML_PREDICTOR: Gagal menginisialisasi database atau konfigurasi: {e}", exc_info=True)
        import sys
        sys.exit(1) # Keluar dengan error jika layanan penting gagal

    SYMBOL = "XAUUSD"
    TIMEFRAME = "H1" # Atau timeframe lain yang Anda punya data cukup
    NUM_CANDLES_FOR_TRAINING = 5000 # Jumlah candle untuk pelatihan (semakin banyak semakin baik)

    logger.info(f"ML_PREDICTOR: Memulai proses pelatihan ML untuk {SYMBOL} {TIMEFRAME} dengan {NUM_CANDLES_FOR_TRAINING} lilin.")
    for col in ['open', 'high', 'low', 'close', 'volume', 'real_volume']:
        if col in df.columns:
            df[col] = df[col].apply(utils.to_float_or_none) # Pastikan ini mengonversi ke float


    df_data = load_historical_data_for_ml(SYMBOL, TIMEFRAME, NUM_CANDLES_FOR_TRAINING)

    if not df_data.empty:
        # Pastikan data yang dikirim ke generate_features_and_labels adalah DataFrame float
        features, labels = generate_features_and_labels(df_data, prediction_horizon=1) # Prediksi 1 bar ke depan
        
        if not features.empty and not labels.empty:
            logger.info(f"ML_PREDICTOR: Melatih model dengan {len(features)} sampel.")
            trained_model = train_and_evaluate_model(features, labels, model_type='RandomForest')
            if trained_model:
                save_ml_model(trained_model)
            else:
                logger.error("ML_PREDICTOR: Gagal melatih model ML. Tidak dapat menyimpan.")
        else:
            logger.error("ML_PREDICTOR: Fitur atau label kosong setelah pembuatan. Tidak dapat melatih model.")
    else:
        logger.error("ML_PREDICTOR: Data historis kosong. Tidak dapat melatih model.")
    
    # Cleanup DB writer thread setelah selesai
    try:
        database_manager.stop_db_writer()
        logger.info("ML_PREDICTOR: Menunggu DB writer queue selesai...")
        # Beri waktu queue untuk selesai
        max_wait_time = 30
        start_wait_time = time.time()
        while not database_manager._db_write_queue.empty() and (time.time() - start_wait_time) < max_wait_time:
            time.sleep(0.1)
        if not database_manager._db_write_queue.empty():
            logger.warning("ML_PREDICTOR: DB write queue tidak kosong setelah menunggu.")
    except Exception as e:
        logger.error(f"ML_PREDICTOR: Error saat cleanup DB writer: {e}", exc_info=True)

    logger.info("ML_PREDICTOR: Proses pelatihan ML selesai.")
    import sys
    sys.exit(0)
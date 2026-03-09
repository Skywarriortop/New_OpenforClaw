# data_updater.py

import sys
import time
import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import pandas as pd
from sqlalchemy.exc import OperationalError
import glob
import os
import threading
from collections import defaultdict
import json # Tambahkan impor json karena digunakan di rule_based_signal_loop

# Impor modul lain yang dibutuhkan
import database_manager
from config import config
import mt5_connector
import market_data_processor
import notification_service
import auto_trade_manager
import fundamental_data_service
import chart_generator
import ai_analyzer
import ai_consensus_manager
import detector_monitoring
import utils
import rule_based_signal_generator

logger = logging.getLogger(__name__)

# --- INI ADALAH DEKLARASI VARIABEL GLOBAL (DIKELOLA SECARA INTERNAL OLEH MODUL INI) ---
_latest_realtime_price = Decimal('0.0')
_latest_real_time_price_timestamp_utc = None
_daily_realized_pnl = Decimal('0.0')
_last_daily_pnl_calculation_time = None

last_tick_time_msc = 0
_tick_buffer = []
_last_tick_save_time = time.time()
_TICK_SAVE_INTERVAL_SECONDS = 5
_MAX_TICK_BUFFER_SIZE = 50
_last_raw_tick_mt5 = None
# --- AKHIR DEKLARASI VARIABEL GLOBAL ---

# --- MODIFIKASI: Inisialisasi _fundamental_service_instance di luar fungsi untuk re-use ---
_fundamental_service_instance = fundamental_data_service.FundamentalDataService()
# --- AKHIR MODIFIKASI ---

# --- MODIFIKASI: Implementasi _get_instance untuk menjaga kompatibilitas jika dipanggil dengan cara lama ---
_instance = None
def _get_instance():
    global _instance
    if _instance is None:
        _instance = DataUpdaterInstance()
    return _instance

class DataUpdaterInstance:
    def get_latest_realtime_price(self, symbol: str) -> tuple[Decimal, datetime]:
        return get_latest_realtime_price(symbol)
    def calculate_and_update_daily_pnl(self, symbol: str):
        calculate_and_update_daily_pnl(symbol)
    def periodic_historical_data_update_loop(self, symbol: str, stop_event: threading.Event, _feature_backfill_completed: bool):
        periodic_historical_data_update_loop(symbol, stop_event, _feature_backfill_completed)
    def scenario_analysis_loop(self, symbol: str, stop_event: threading.Event, _feature_backfill_completed: bool):
        scenario_analysis_loop(symbol, stop_event, _feature_backfill_completed)
    def detector_health_monitoring_loop(self, symbol: str, stop_event: threading.Event, _feature_backfill_completed: bool):
        detector_health_monitoring_loop(symbol, stop_event, _feature_backfill_completed)
    def periodic_volume_profile_update_loop(self, symbol: str, stop_event: threading.Event, _feature_backfill_completed: bool):
        periodic_volume_profile_update_loop(symbol, stop_event, _feature_backfill_completed)
    def periodic_combined_advanced_detection_loop(self, symbol: str, stop_event: threading.Event, _feature_backfill_completed: bool):
        periodic_combined_advanced_detection_loop(symbol, stop_event, _feature_backfill_completed)
    def periodic_fundamental_data_update_loop(self, symbol: str, stop_event: threading.Event, _feature_backfill_completed: bool):
        periodic_fundamental_data_update_loop(symbol, stop_event, _feature_backfill_completed)
    def rule_based_signal_loop(self, symbol: str, stop_event: threading.Event, _feature_backfill_completed: bool):
        rule_based_signal_loop(symbol, stop_event, _feature_backfill_completed)
    def monthly_historical_feature_backfill_loop(self, symbol: str, stop_event: threading.Event, _feature_backfill_completed: bool):
        monthly_historical_feature_backfill_loop(symbol, stop_event, _feature_backfill_completed)
    def daily_summary_report_loop(self, symbol: str, stop_event: threading.Event, _feature_backfill_completed: bool):
        daily_summary_report_loop(symbol, stop_event, _feature_backfill_completed)
    def periodic_daily_pnl_check_loop(self, symbol: str, stop_event: threading.Event, _feature_backfill_completed: bool):
        periodic_daily_pnl_check_loop(symbol, stop_event, _feature_backfill_completed)
    def daily_cleanup_scheduler_loop(self, symbol: str, stop_event: threading.Event, _feature_backfill_completed: bool):
        daily_cleanup_scheduler_loop(symbol, stop_event, _feature_backfill_completed)
    def daily_open_prices_scheduler_loop(self, symbol: str, stop_event: threading.Event, _feature_backfill_completed: bool):
        daily_open_prices_scheduler_loop(symbol, stop_event, _feature_backfill_completed)
    def periodic_mt5_trade_data_update_loop(self, symbol: str, stop_event: threading.Event, _feature_backfill_completed: bool):
        periodic_mt5_trade_data_update_loop(symbol, stop_event, _feature_backfill_completed)
    def periodic_market_status_update_loop(self, symbol: str, stop_event: threading.Event, _feature_backfill_completed: bool):
        periodic_market_status_update_loop(symbol, stop_event, _feature_backfill_completed)
    def periodic_session_data_update_loop(self, symbol: str, stop_event: threading.Event, _feature_backfill_completed: bool):
        periodic_session_data_update_loop(symbol, stop_event, _feature_backfill_completed)
    def periodic_realtime_tick_loop(self, symbol: str, stop_event: threading.Event, _feature_backfill_completed: bool):
        periodic_realtime_tick_loop(symbol, stop_event, _feature_backfill_completed)


# --- Fungsi untuk mendapatkan harga real-time terakhir ---
def get_latest_realtime_price(symbol: str) -> tuple[Decimal, datetime]:
    """
    Mengembalikan harga real-time terakhir yang diketahui dan timestamp-nya.
    """
    global _latest_realtime_price, _latest_real_time_price_timestamp_utc
    return _latest_realtime_price, _latest_real_time_price_timestamp_utc


# --- Fungsi untuk mendapatkan P/L harian yang terealisasi ---
def get_daily_realized_pnl(symbol: str) -> Decimal:
    """
    Mengembalikan nilai P/L harian terealisasi terakhir.
    """
    global _daily_realized_pnl
    return _daily_realized_pnl

# --- FUNCTIONS FOR PERIODIC LOOPS ---

def periodic_realtime_tick_loop(symbol: str, stop_event: threading.Event, _feature_backfill_completed: bool):
    """
    Mengambil tick real-time, menghitung perubahannya, dan menyimpannya ke database.
    Loop ini berjalan secara periodik.
    """
    if symbol is None or not isinstance(symbol, str):
        logger.error("periodic_realtime_tick_loop dipanggil tanpa argumen simbol string yang valid. Menghentikan loop ini.")
        return

    global _latest_realtime_price, _latest_real_time_price_timestamp_utc
    last_tick = None

    while not stop_event.is_set():
        try:
            new_tick_data = mt5_connector.get_current_tick_info(symbol)

            if new_tick_data and new_tick_data.get('last') is not None and new_tick_data.get('time') is not None:
                last_tick = new_tick_data
                _latest_realtime_price = utils.to_decimal_or_none(last_tick['last'])
                _latest_real_time_price_timestamp_utc = utils.to_utc_datetime_or_none(last_tick['time'])
                logger.debug(f"Tick realtime updated: Price={float(_latest_realtime_price):.5f}, Time={_latest_real_time_price_timestamp_utc.isoformat()}")
            else:
                logger.warning(f"Gagal mengambil tick untuk {symbol}. mt5.symbol_info_tick() mengembalikan None atau tick tidak valid. Melewatkan pemrosesan tick ini.")
                time.sleep(float(config.Scheduler.UPDATE_INTERVALS["periodic_realtime_tick_loop"]))
                continue

            tick_timestamp_utc_dt = utils.to_utc_datetime_or_none(last_tick.get('time'))
            tick_last_price = utils.to_decimal_or_none(last_tick.get('last'))

            if tick_timestamp_utc_dt is None or tick_last_price is None:
                logger.warning(f"Tick data tidak lengkap atau tidak valid untuk {symbol}. Melewatkan pemrosesan tick ini.")
                time.sleep(float(config.Scheduler.UPDATE_INTERVALS["periodic_realtime_tick_loop"]))
                continue

            daily_open_price = database_manager.get_daily_open_price(
                date_ref=datetime.now(timezone.utc).date().isoformat(),
                symbol_param=symbol
            )

            if daily_open_price is None or daily_open_price == Decimal('0.0'):
                logger.debug("Daily Open Price belum tersedia, mencoba mengupdatenya.")
                market_data_processor.update_daily_open_prices_logic(symbol)
                daily_open_price = database_manager.get_daily_open_price(
                    date_ref=datetime.now(timezone.utc).date().isoformat(),
                    symbol_param=symbol
                )
                if daily_open_price is None or daily_open_price == Decimal('0.0'):
                    logger.warning("Gagal mendapatkan Daily Open Price setelah update. Menggunakan harga tick terakhir sebagai fallback untuk perhitungan delta/change.")
                    daily_open_price = tick_last_price

            delta_point = Decimal('0.0')
            change = Decimal('0.0')
            change_percent = Decimal('0.0')

            if daily_open_price is not None and daily_open_price != Decimal('0.0'):
                delta_point = tick_last_price - daily_open_price
                change = delta_point
                if daily_open_price != Decimal('0.0'):
                    change_percent = (change / daily_open_price) * Decimal('100.0')

            tick_timestamp_utc_ms_int = int(tick_timestamp_utc_dt.timestamp() * 1000)

            database_manager.save_price_tick(
                symbol=symbol,
                time=tick_timestamp_utc_ms_int,
                time_utc_datetime=tick_timestamp_utc_dt,
                last_price=tick_last_price,
                bid_price=utils.to_decimal_or_none(last_tick.get('bid')),
                daily_open_price=daily_open_price,
                delta_point=delta_point,
                change=change,
                change_percent=change_percent
            )

            time.sleep(float(config.Scheduler.UPDATE_INTERVALS["periodic_realtime_tick_loop"]))

        except Exception as e:
            logger.error(f"Error tak terduga di periodic_realtime_tick_loop: {e}", exc_info=True)
            time.sleep(float(config.System.RETRY_DELAY_SECONDS) * 2)
    logger.info(f"periodic_realtime_tick_loop untuk {symbol} dihentikan.")

def periodic_session_data_update_loop(symbol: str, stop_event: threading.Event, _feature_backfill_completed: bool):
    if symbol is None or not isinstance(symbol, str):
        logger.error("periodic_session_data_update_loop dipanggil tanpa argumen simbol string yang valid. Menghentikan loop ini.")
        return
    while not stop_event.is_set():
        try:
            current_mt5_datetime = datetime.now(timezone.utc)
            market_data_processor.calculate_and_update_session_data(symbol, current_mt5_datetime)
            time.sleep(float(config.Scheduler.UPDATE_INTERVALS["periodic_session_data_update_loop"]))
        except Exception as e:
            logger.error(f"Error tak terduga di periodic_session_data_update_loop: {e}", exc_info=True)
            time.sleep(float(config.System.RETRY_DELAY_SECONDS) * 2)
    logger.info(f"periodic_session_data_update_loop untuk {symbol} dihentikan.")

def periodic_market_status_update_loop(symbol: str, stop_event: threading.Event, _feature_backfill_completed: bool):
    if symbol is None or not isinstance(symbol, str):
        logger.error("periodic_market_status_update_loop dipanggil tanpa argumen simbol string yang valid. Menghentikan loop ini.")
        return
    while not stop_event.is_set():
        try:
            latest_tick_info = mt5_connector.get_current_tick_info(symbol)
            if latest_tick_info:
                logger.info(f"Status Pasar: Harga terakhir {float(latest_tick_info['last']):.5f} @ {latest_tick_info['time']}")
            else:
                logger.warning(f"data_updater: Tidak ada tick real-time yang tersedia. Menggunakan harga dan waktu default untuk {symbol}.")
            time.sleep(float(config.Scheduler.UPDATE_INTERVALS["periodic_market_status_update_loop"]))
        except Exception as e:
            logger.error(f"Error tak terduga di periodic_market_status_update_loop: {e}", exc_info=True)
            time.sleep(float(config.System.RETRY_DELAY_SECONDS) * 2)
    logger.info(f"periodic_market_status_update_loop untuk {symbol} dihentikan.")

def periodic_mt5_trade_data_update_loop(symbol: str, stop_event: threading.Event, _feature_backfill_completed: bool):
    if symbol is None or not isinstance(symbol, str):
        logger.error("periodic_mt5_trade_data_update_loop dipanggil tanpa argumen simbol string yang valid. Menghentikan loop ini.")
        return
    while not stop_event.is_set():
        try:
            database_manager.update_mt5_trade_data_periodically(symbol)
            time.sleep(float(config.Scheduler.UPDATE_INTERVALS["periodic_mt5_trade_data_update_loop"]))
        except Exception as e:
            logger.error(f"Error tak terduga di periodic_mt5_trade_data_update_loop: {e}", exc_info=True)
            time.sleep(float(config.System.RETRY_DELAY_SECONDS) * 2)
    logger.info(f"periodic_mt5_trade_data_update_loop untuk {symbol} dihentikan.")

def daily_open_prices_scheduler_loop(symbol: str, stop_event: threading.Event, _feature_backfill_completed: bool):
    if symbol is None or not isinstance(symbol, str):
        logger.error("daily_open_prices_scheduler_loop dipanggil tanpa argumen simbol string yang valid. Menghentikan loop ini.")
        return
    while not stop_event.is_set():
        try:
            market_data_processor.update_daily_open_prices_logic(symbol)
            time.sleep(float(config.Scheduler.UPDATE_INTERVALS["daily_open_prices_scheduler_loop"]))
        except Exception as e:
            logger.error(f"Error tak terduga di daily_open_prices_scheduler_loop: {e}", exc_info=True)
            time.sleep(float(config.System.RETRY_DELAY_SECONDS) * 2)
    logger.info(f"daily_open_prices_scheduler_loop untuk {symbol} dihentikan.")

def periodic_historical_data_update_loop(symbol: str, stop_event: threading.Event, _feature_backfill_completed: bool):
    if symbol is None or not isinstance(symbol, str):
        logger.error("periodic_historical_data_update_loop dipanggil tanpa argumen simbol string yang valid. Menghentikan loop ini.")
        return
    while not stop_event.is_set():
        try:
            logger.info(f"Memperbarui candle historis TERBARU dan indikator terkait untuk {symbol}...")
            
            market_data_processor._backfill_single_timeframe_features(symbol, "H1", datetime.now(timezone.utc) - timedelta(days=2), datetime.now(timezone.utc))
            market_data_processor._backfill_single_timeframe_features(symbol, "H4", datetime.now(timezone.utc) - timedelta(days=7), datetime.now(timezone.utc))
            
            time.sleep(float(config.Scheduler.UPDATE_INTERVALS["periodic_historical_data_update_loop"]))
        except Exception as e:
            logger.error(f"Error tak terduga di periodic_historical_data_update_loop: {e}", exc_info=True)
            time.sleep(float(config.System.RETRY_DELAY_SECONDS) * 2)
    logger.info(f"periodic_historical_data_update_loop untuk {symbol} dihentikan.")

def periodic_volume_profile_update_loop(symbol: str, stop_event: threading.Event, _feature_backfill_completed: bool):
    if symbol is None or not isinstance(symbol, str):
        logger.error("periodic_volume_profile_update_loop dipanggil tanpa argumen simbol string yang valid. Menghentikan loop ini.")
        return
    while not stop_event.is_set():
        try:
            logger.info(f"Memperbarui Volume Profiles untuk {symbol}...")

            candles_h4 = database_manager.get_historical_candles_from_db(symbol, "H4", limit=config.MarketData.COLLECT_TIMEFRAMES.get("H4", 5000))
            if candles_h4:
                df_candles_h4 = pd.DataFrame(candles_h4)
                df_candles_h4['open_time_utc'] = pd.to_datetime(df_candles_h4['open_time_utc'])
                df_candles_h4.set_index('open_time_utc', inplace=True)
                market_data_processor.update_volume_profiles(symbol, "H4", df_candles_h4)

            candles_d1 = database_manager.get_historical_candles_from_db(symbol, "D1", limit=config.MarketData.COLLECT_TIMEFRAMES.get("D1", 5000))
            if candles_d1:
                df_candles_d1 = pd.DataFrame(candles_d1)
                df_candles_d1['open_time_utc'] = pd.to_datetime(df_candles_d1['open_time_utc'])
                df_candles_d1.set_index('open_time_utc', inplace=True)
                market_data_processor.update_volume_profiles(symbol, "D1", df_candles_d1)

            time.sleep(float(config.Scheduler.UPDATE_INTERVALS["periodic_volume_profile_update_loop"]))
        except Exception as e:
            logger.error(f"Error tak terduga di periodic_volume_profile_update_loop: {e}", exc_info=True)
            time.sleep(float(config.System.RETRY_DELAY_SECONDS) * 2)
    logger.info(f"periodic_volume_profile_update_loop untuk {symbol} dihentikan.")

def periodic_combined_advanced_detection_loop(symbol: str, stop_event: threading.Event, _feature_backfill_completed: bool):
    if symbol is None or not isinstance(symbol, str):
        logger.error("periodic_combined_advanced_detection_loop dipanggil tanpa argumen simbol string yang valid. Menghentikan loop ini.")
        return
    while not stop_event.is_set():
        try:
            logger.info(f"Memulai loop deteksi lanjutan gabungan untuk {symbol}...")

            timeframes_to_process = config.MarketData.ENABLED_TIMEFRAMES

            for tf_name, enabled in timeframes_to_process.items():
                if not enabled: continue

                current_atr_value = Decimal('0.0')

                candles_limit = config.MarketData.COLLECT_TIMEFRAMES.get(tf_name, 500)
                if tf_name in ["M5", "M15", "M30", "H1", "H4", "D1"]:
                    candles_limit = max(candles_limit, 500)

                candles_raw_data_tf = database_manager.get_historical_candles_from_db(
                    symbol, tf_name, limit=candles_limit, end_time_utc=datetime.now(timezone.utc)
                )

                if not candles_raw_data_tf:
                    logger.warning(f"PCDL: Tidak ada data candle untuk {symbol} {tf_name}. Melewatkan deteksi lanjutan.")
                    continue

                df_candles_tf = pd.DataFrame(candles_raw_data_tf)
                df_candles_tf['open_time_utc'] = pd.to_datetime(df_candles_tf['open_time_utc'])
                df_candles_tf.set_index('open_time_utc', inplace=True)
                df_candles_tf.sort_index(inplace=True)

                df_candles_tf_processed_decimal = df_candles_tf.copy()
                df_candles_tf_processed_decimal = df_candles_tf_processed_decimal.rename(columns={
                    'open_price': 'open', 'high_price': 'high', 'low_price': 'low',
                    'close_price': 'close', 'tick_volume': 'volume',
                    'real_volume': 'real_volume', 'spread': 'spread'
                })
                for col in ['open', 'high', 'low', 'close', 'volume', 'real_volume', 'spread']:
                    if col in df_candles_tf_processed_decimal.columns:
                        df_candles_tf_processed_decimal[col] = df_candles_tf_processed_decimal[col].apply(utils.to_decimal_or_none)
                initial_decimal_len = len(df_candles_tf_processed_decimal)
                df_candles_tf_processed_decimal.dropna(subset=['open', 'high', 'low', 'close'], inplace=True)
                if len(df_candles_tf_processed_decimal) < initial_decimal_len:
                    logger.warning(f"PCDL: Dihapus {initial_decimal_len - len(df_candles_tf_processed_decimal)} baris dengan NaN di OHLC untuk {symbol} {tf_name} (setelah konversi Decimal).")
                if df_candles_tf_processed_decimal.empty:
                    logger.warning(f"PCDL: DataFrame Decimal kosong setelah pembersihan NaN untuk {symbol} {tf_name}. Melewatkan detektor lanjutan untuk TF ini.")
                    continue

                df_candles_tf_processed_float = df_candles_tf_processed_decimal.copy()
                for col in ['open', 'high', 'low', 'close', 'volume', 'real_volume', 'spread']:
                    if col in df_candles_tf_processed_float.columns:
                        df_candles_tf_processed_float[col] = df_candles_tf_processed_float[col].apply(utils.to_float_or_none)
                initial_float_len = len(df_candles_tf_processed_float)
                df_candles_tf_processed_float.dropna(subset=['open', 'high', 'low', 'close'], inplace=True)
                if len(df_candles_tf_processed_float) < initial_float_len:
                    logger.warning(f"PCDL: Dihapus {initial_float_len - len(df_candles_tf_processed_float)} baris dengan NaN di OHLC untuk {symbol} {tf_name} (setelah konversi float).")
                if df_candles_tf_processed_float.empty:
                    logger.warning(f"PCDL: DataFrame float kosong setelah pembersihan NaN untuk {symbol} {tf_name}. Melewatkan.")
                    continue

                if not df_candles_tf_processed_float.empty and config.MarketData.ATR_PERIOD > 0:
                    atr_series = market_data_processor._calculate_atr(df_candles_tf_processed_float, config.MarketData.ATR_PERIOD)
                    if not atr_series.empty and pd.notna(atr_series.iloc[-1]):
                        current_atr_value = utils.to_decimal_or_none(atr_series.iloc[-1])
                    else:
                        logger.warning(f"PCDL: Gagal menghitung ATR untuk {tf_name}. Menggunakan ATR default 0 (ATR series kosong/NaN).") # Perbaikan tf_det ke tf_name
                else:
                    logger.warning(f"PCDL: Tidak cukup data ({len(df_candles_tf_processed_float)}) atau periode ATR tidak valid ({config.MarketData.ATR_PERIOD}) untuk menghitung ATR {tf_name}. Menggunakan ATR default 0.") # Perbaikan tf_det ke tf_name

                if current_atr_value <= Decimal('0.0'):
                    logger.warning(f"PCDL: ATR untuk {tf_name} adalah nol atau tidak valid. Deteksi yang bergantung pada ATR mungkin terpengaruh.") # Perbaikan tf_det ke tf_name

                if config.MarketData.ENABLE_SR_DETECTION:
                    market_data_processor._detect_new_sr_levels_historically(symbol, tf_name, df_candles_tf_processed_decimal, current_atr_value) # Perbaikan tf_det ke tf_name
                    market_data_processor._update_existing_sr_levels_status(symbol, tf_name, df_candles_tf_processed_decimal, current_atr_value) # Perbaikan tf_det ke tf_name
                    market_data_processor._detect_new_supply_demand_zones_historically(symbol, tf_name, df_candles_tf_processed_decimal, current_atr_value) # Perbaikan tf_det ke tf_name
                    market_data_processor._update_existing_supply_demand_zones_status(symbol, tf_name, df_candles_tf_processed_decimal, current_atr_value) # Perbaikan tf_det ke tf_name

                if config.MarketData.ENABLE_OB_FVG_DETECTION:
                    market_data_processor._detect_new_order_blocks_historically(symbol, tf_name, df_candles_tf_processed_decimal, current_atr_value) # Perbaikan tf_det ke tf_name
                    market_data_processor._update_existing_order_blocks_status(symbol, tf_name, df_candles_tf_processed_decimal, current_atr_value) # Perbaikan tf_det ke tf_name
                    market_data_processor._detect_new_fair_value_gaps_historically(symbol, tf_name, df_candles_tf_processed_decimal, current_atr_value) # Perbaikan tf_det ke tf_name
                    market_data_processor._update_existing_fair_value_gaps_status(symbol, tf_name, df_candles_tf_processed_decimal, current_atr_value) # Perbaikan tf_det ke tf_name
                
                if config.MarketData.ENABLE_LIQUIDITY_DETECTION:
                    swing_results_df_liq = market_data_processor._calculate_swing_highs_lows_internal(df_candles_tf_processed_float, swing_length=config.AIAnalysts.SWING_EXT_BARS)
                    
                    if not swing_results_df_liq.empty and not swing_results_df_liq['HighLow'].isnull().all():
                        market_data_processor._detect_new_liquidity_zones_historically(symbol, tf_name, df_candles_tf_processed_decimal, current_atr_value) # Perbaikan tf_det ke tf_name
                        market_data_processor._update_existing_liquidity_zones_status(symbol, tf_name, df_candles_tf_processed_decimal, current_atr_value) # Perbaikan tf_det ke tf_name
                    else:
                        logger.debug(f"Tidak ada swing points untuk {symbol} {tf_name}. Melewatkan deteksi Likuiditas.") # Perbaikan tf_det ke tf_name

                if config.MarketData.ENABLE_FIBONACCI_DETECTION:
                    swing_results_df_fib = market_data_processor._calculate_swing_highs_lows_internal(df_candles_tf_processed_float, swing_length=config.AIAnalysts.SWING_EXT_BARS)
                    
                    if not swing_results_df_fib.empty and not swing_results_df_fib['HighLow'].isnull().all():
                        retracement_results_df = market_data_processor._calculate_retracements_internal(df_candles_tf_processed_decimal, swing_results_df_fib)
                        if not retracement_results_df.empty and not retracement_results_df['Direction'].isnull().all():
                            market_data_processor._detect_new_fibonacci_levels_historically(symbol, tf_name, df_candles_tf_processed_decimal, current_atr_value) # Perbaikan tf_det ke tf_name
                            market_data_processor._update_existing_fibonacci_levels_status(symbol, tf_name, df_candles_tf_processed_decimal, current_atr_value) # Perbaikan tf_det ke tf_name
                        else:
                            logger.debug(f"Tidak ada retracement yang terdeteksi untuk {symbol} {tf_name}. Melewatkan deteksi Fibonacci Levels.") # Perbaikan tf_det ke tf_name
                    else:
                        logger.debug(f"Tidak ada swing points untuk {symbol} {tf_name}. Melewatkan deteksi Fibonacci Levels.") # Perbaikan tf_det ke tf_name


                if config.MarketData.ENABLE_MARKET_STRUCTURE_DETECTION:
                    market_data_processor._detect_new_market_structure_events_historically(symbol, tf_name, df_candles_tf_processed_decimal, current_atr_value) # Perbaikan tf_det ke tf_name
                    phl_htf = config.MarketData.MA_TREND_TIMEFRAMES[-1] if config.MarketData.MA_TREND_TIMEFRAMES else tf_name
                    market_data_processor._detect_new_previous_high_low_historically(symbol, tf_name, phl_htf, df_candles_tf_processed_decimal, current_atr_value) # Perbaikan tf_det ke tf_name
                
                if config.MarketData.ENABLE_RSI_CALCULATION:
                    market_data_processor._detect_and_save_rsi_historically(symbol, tf_name, df_candles_tf_processed_decimal) # Perbaikan tf_det ke tf_name
                    market_data_processor._detect_overbought_oversold_conditions_historically(symbol, tf_name, df_candles_tf_processed_decimal) # Perbaikan tf_det ke tf_name

                if config.MarketData.ENABLE_MACD_CALCULATION:
                    market_data_processor._detect_and_save_macd_historically(symbol, tf_name, df_candles_tf_processed_decimal) # Perbaikan tf_det ke tf_name
                
            time.sleep(float(config.System.RETRY_DELAY_SECONDS) * 2)
        except Exception as e:
            logger.error(f"Error tak terduga di periodic_combined_advanced_detection_loop: {e}", exc_info=True)
            time.sleep(float(config.System.RETRY_DELAY_SECONDS) * 2)

def periodic_fundamental_data_update_loop(symbol: str, stop_event: threading.Event, _feature_backfill_completed: bool): # Tambahkan _feature_backfill_completed sebagai parameter
    """
    Loop periodik untuk memperbarui data fundamental (kalender dan berita) secara periodik.
    """
    interval = config.Scheduler.UPDATE_INTERVALS.get('periodic_fundamental_data_update_loop', 14400)
    _fundamental_service_instance = fundamental_data_service.FundamentalDataService() # Pindahkan inisialisasi ke sini

    while not stop_event.is_set():
        now_utc = datetime.now(timezone.utc)

        logger.info(f"Pembaruan fundamental berikutnya dijadwalkan dalam {interval / 3600:.1f} jam.")

        stopped = stop_event.wait(interval)
        if stopped:
            break

        try:
            logger.info("Menjalankan update data fundamental (scraping dan penyimpanan)...")

            comprehensive_data = _fundamental_service_instance.get_comprehensive_fundamental_data(
                days_past=7, # Bisa disesuaikan di config.py jika ingin lebih luas
                days_future=7, # Bisa disesuaikan
                min_impact_level="Low", # Bisa disesuaikan
                target_currency="USD", # Asumsi fokus USD untuk XAUUSD, bisa jadi parameter
                include_news_topics=["gold", "usd", "suku bunga", "perang", "fed", "inflation", "market", "economy", "geopolitics", "commodities", "finance", "conflict", "trade"], # Bisa disesuaikan
                read_from_cache_only=False
            )
            
            economic_events = comprehensive_data.get("economic_calendar", [])
            news_articles = comprehensive_data.get("news_article", [])

            if economic_events:
                database_manager.save_economic_events(economic_events)
                logger.info(f"Berhasil menyimpan {len(economic_events)} event kalender ekonomi ke DB.")
            else:
                logger.info("Tidak ada event kalender ekonomi baru yang ditemukan.")

            if news_articles:
                database_manager.save_news_articles(news_articles)
                logger.info(f"Berhasil menyimpan {len(news_articles)} artikel berita ke DB.")
            else:
                logger.info("Tidak ada artikel berita baru yang ditemukan.")

            if config.Telegram.SEND_FUNDAMENTAL_NOTIFICATIONS:
                if economic_events:
                    notification_service.notify_only_economic_calendar(
                        economic_events_list=economic_events,
                        min_impact="Low"
                    )
                    logger.info("Notifikasi kalender ekonomi dikirim.")
                else:
                    logger.info("Tidak ada event kalender ekonomi untuk dinotifikasi.")

                if news_articles:
                    news_to_notify = news_articles[:config.Telegram.MAX_ARTICLES_TO_NOTIFY]
                    notification_service.notify_only_news_articles(
                        news_articles_list=news_to_notify,
                        include_topics=None
                    )
                    logger.info(f"Notifikasi {len(news_to_notify)} artikel berita dikirim (dari total {len(news_articles)}).")
                else:
                    logger.info("Tidak ada artikel berita untuk dinotifikasi.")
            else:
                logger.info("Notifikasi fundamental dinonaktifkan di konfigurasi.")

            logger.info("periodic_fundamental_data_update_loop selesai.")

        except Exception as e:
            logger.error(f"Error tak terduga di periodic_fundamental_data_update_loop: {e}", exc_info=True)
            notification_service.notify_error(f"Gagal memperbarui data fundamental: {e}", "Fundamental Data Update Loop")
            time.sleep(float(config.System.RETRY_DELAY_SECONDS) * 2)


def rule_based_signal_loop(symbol: str, stop_event: threading.Event, _feature_backfill_completed: bool): # Tambahkan _feature_backfill_completed sebagai parameter
    """Loop periodik untuk generasi sinyal berbasis aturan."""
    interval = config.Scheduler.UPDATE_INTERVALS.get('rule_based_signal_loop', 60)
    
    while not stop_event.is_set():
        if _feature_backfill_completed:
            try:
                logger.info(f"Memicu generasi sinyal berbasis aturan untuk {symbol}...")
                latest_tick = database_manager.get_latest_price_tick(config.TRADING_SYMBOL)

                if not latest_tick or latest_tick.get('last_price') is None or latest_tick.get('last_price') <= 0:
                    logger.warning(f"RULE SIGNAL: Tidak ada harga tick terbaru yang valid untuk {symbol}. Melewatkan generasi sinyal.")
                    time.sleep(float(config.Scheduler.UPDATE_INTERVALS["rule_based_signal_loop"]))
                    continue

                current_time = latest_tick['time']
                current_price = Decimal(str(latest_tick['last_price']))

                rule_signal = rule_based_signal_generator.generate_signal(
                    symbol=symbol,
                    current_time=current_time,
                    current_price=current_price
                )

                if rule_signal and rule_signal['action'] in ["BUY", "SELL", "HOLD"]:
                    logger.info(f"Sinyal Berbasis Aturan: {rule_signal['action']} (Conf: {rule_signal['confidence']})")

                    database_manager.save_ai_analysis_result(
                        symbol=symbol,
                        timestamp=current_time,
                        summary=rule_signal.get('reasoning', 'Sinyal berbasis aturan.'),
                        potential_direction=rule_signal.get('potential_direction', 'Undefined'),
                        recommendation_action=rule_signal['action'],
                        entry_price=rule_signal.get('entry_price_suggestion'),
                        stop_loss=rule_signal.get('stop_loss_suggestion'),
                        take_profit=rule_signal.get('take_profit_suggestion'),
                        reasoning=rule_signal.get('reasoning', 'N/A'),
                        ai_confidence=rule_signal.get('confidence', 'High'),
                        raw_response_json=json.dumps(rule_signal, default=utils._json_default),
                        analyst_id="Rule_Based_Strategy"
                    )
                    logger.info("Sinyal berbasis aturan berhasil disimpan ke DB.")

                    notification_service.notify_signal(
                        symbol=symbol,
                        action=rule_signal['action'],
                        entry_price=rule_signal.get('entry_price_suggestion'),
                        stop_loss=rule_signal.get('stop_loss_suggestion'),
                        take_profit_suggestion=rule_signal.get('take_profit_suggestion'),
                        tp2_suggestion=rule_signal.get('tp2_suggestion'),
                        tp3_suggestion=rule_signal.get('tp3_suggestion'),
                        reasoning=rule_signal.get('reasoning'),
                        confidence=rule_signal.get('confidence'),
                        analyst_id="Rule_Based_Strategy"
                    )
                    logger.info(f"Notifikasi sinyal berbasis aturan {rule_signal['action']} (Conf: {rule_signal['confidence']}) dikirim.")

                    MIN_CONFIDENCE_FOR_TRADE_EXECUTION = "Medium"
                    confidence_levels_map = {"Low": 1, "Medium": 2, "High": 3}
                    current_signal_confidence_value = confidence_levels_map.get(rule_signal['confidence'], 1)
                    min_trade_confidence_value = confidence_levels_map.get(MIN_CONFIDENCE_FOR_TRADE_EXECUTION, 1)

                    if config.Trading.auto_trade_enabled and rule_signal['action'] in ["BUY", "SELL"]:
                        if current_signal_confidence_value >= min_trade_confidence_value:
                            tp_levels_for_atm = []
                            if rule_signal.get('take_profit_suggestion') is not None:
                                tp_levels_for_atm.append({'price': rule_signal['take_profit_suggestion'], 'volume_percentage': Decimal('0.50')})
                            if rule_signal.get('tp2_suggestion') is not None:
                                tp_levels_for_atm.append({'price': rule_signal['tp2_suggestion'], 'volume_percentage': Decimal('0.30')})
                            if rule_signal.get('tp3_suggestion') is not None:
                                tp_levels_for_atm.append({'price': rule_signal['tp3_suggestion'], 'volume_percentage': Decimal('0.20')})
                            
                            trade_result = auto_trade_manager.execute_ai_trade(
                                symbol=symbol,
                                action=rule_signal['action'],
                                volume=config.Trading.AUTO_TRADE_VOLUME,
                                entry_price=rule_signal.get('entry_price_suggestion'),
                                stop_loss=rule_signal.get('stop_loss_suggestion'),
                                take_profit=rule_signal.get('take_profit_suggestion'),
                                magic_number=config.Trading.AUTO_TRADE_MAGIC_NUMBER,
                                slippage=config.Trading.AUTO_TRADE_SLIPPAGE,
                                tp_levels_config=tp_levels_for_atm
                            )
                        else:
                            logger.info(f"Eksekusi trade untuk sinyal berbasis aturan {rule_signal['action']} dilewatkan (kepercayaan rendah untuk trade).")
            except Exception as e:
                logger.error(f"ERROR (Scheduler - Rule Based Signal): {e}", exc_info=True)
                time.sleep(float(config.System.RETRY_DELAY_SECONDS) * 2)
        else:
            logger.info("Rule-based Signal loop menunggu backfill fitur selesai...")
        time.sleep(interval)


def daily_summary_report_loop(symbol: str, stop_event: threading.Event, _feature_backfill_completed: bool): # Tambahkan _feature_backfill_completed sebagai parameter
    """Loop untuk menjadwalkan laporan ringkasan harian."""
    if symbol is None or not isinstance(symbol, str):
        logger.error("daily_summary_report_loop dipanggil tanpa argumen simbol string yang valid. Menghentikan loop ini.")
        return
    interval = config.Scheduler.UPDATE_INTERVALS.get('daily_summary_report_loop', 3600 * 24)
    while not stop_event.is_set():
        now_utc = datetime.now(timezone.utc)
        target_time = now_utc.replace(hour=23, minute=59, second=0, microsecond=0)
        
        if now_utc > target_time:
            target_time += timedelta(days=1)

        wait_seconds = (target_time - now_utc).total_seconds()
        logger.info(f"Laporan ringkasan harian berikutnya dalam {wait_seconds / 3600:.2f} jam.")
        
        if stop_event.wait(wait_seconds):
            break

        try:
            logger.info(f"Menjalankan laporan ringkasan harian untuk {symbol}...")
            notification_service.notify_daily_summary(symbol)
        except Exception as e:
            logger.error(f"Gagal menjalankan laporan ringkasan harian: {e}", exc_info=True)
            time.sleep(float(config.System.RETRY_DELAY_SECONDS) * 2)

def monthly_historical_feature_backfill_loop(symbol: str, stop_event: threading.Event, _feature_backfill_completed: bool): # Tambahkan _feature_backfill_completed sebagai parameter
    """Loop untuk menjalankan backfill fitur historis bulanan."""
    if symbol is None or not isinstance(symbol, str):
        logger.error("monthly_historical_feature_backfill_loop dipanggil tanpa argumen simbol string yang valid. Menghentikan loop ini.")
        return
    interval = config.Scheduler.UPDATE_INTERVALS.get('monthly_historical_feature_backfill_loop', 3600 * 24 * 30)
    while not stop_event.is_set():
        if _feature_backfill_completed:
            try:
                logger.info(f"Memulai backfill fitur historis bulanan untuk {symbol}...")
                market_data_processor.backfill_historical_features(symbol)
                logger.info(f"Backfill fitur historis bulanan untuk {symbol} selesai.")
            except Exception as e:
                logger.error(f"Gagal menghitung backfill fitur historis bulanan: {e}", exc_info=True)
                time.sleep(float(config.System.RETRY_DELAY_SECONDS) * 2)
        else:
            logger.info("Monthly Historical Feature Backfill loop menunggu backfill fitur awal selesai.")
        time.sleep(interval)


def scenario_analysis_loop(symbol: str, stop_event: threading.Event, _feature_backfill_completed: bool): # Tambahkan _feature_backfill_completed sebagai parameter
    """Loop untuk menjalankan analisis skenario."""
    if symbol is None or not isinstance(symbol, str):
        logger.error("scenario_analysis_loop dipanggil tanpa argumen simbol string yang valid. Menghentikan loop ini.")
        return
    interval = config.Scheduler.UPDATE_INTERVALS.get('scenario_analysis_loop', 3600)
    while not stop_event.is_set():
        if _feature_backfill_completed:
            try:
                logger.info(f"Memicu analisis skenario untuk {symbol}...")
                
                current_price_val, current_price_time = get_latest_realtime_price(symbol)
                if current_price_val is None or current_price_val == Decimal('0.0'):
                    logger.warning("Scenario Analyzer: Tidak ada harga real-time atau timestamp yang valid dari data_updater. Tidak dapat melakukan analisis skenario.")
                    time.sleep(float(config.Scheduler.UPDATE_INTERVALS["scenario_analysis_loop"]))
                    continue

                ai_consensus_manager.analyze_and_notify_scenario(
                    symbol=symbol,
                    timeframe="H1",
                    current_price=current_price_val,
                    current_time=current_price_time
                )
            except Exception as e:
                logger.error(f"ERROR (Scheduler - Scenario Analysis): {e}", exc_info=True)
                time.sleep(float(config.System.RETRY_DELAY_SECONDS) * 2)
        else:
            logger.info("Scenario Analysis loop menunggu backfill fitur selesai.")
        time.sleep(interval)


def detector_health_monitoring_loop(symbol: str, stop_event: threading.Event, _feature_backfill_completed: bool): # Tambahkan _feature_backfill_completed sebagai parameter
    """
    Loop untuk secara periodik memeriksa status kesehatan detektor
    dan mengirim notifikasi Telegram jika ada anomali.
    """
    if symbol is None or not isinstance(symbol, str):
        logger.error("detector_health_monitoring_loop dipanggil tanpa argumen simbol string yang valid. Menghentikan loop ini.")
        return
    interval = config.Scheduler.UPDATE_INTERVALS.get('detector_health_monitoring_loop', 3600)
    
    last_notified_anomalies = defaultdict(datetime)

    while not stop_event.is_set():
        try: # <--- Blok 'try' dimulai di sini
            logger.info(f"Menjalankan pemantauan kesehatan detektor untuk {symbol}...")
            
            with detector_monitoring.detector_monitor._detector_health_lock:
                current_health_status = detector_monitoring.detector_monitor.get_health_status()

            anomalies_found = []
            for detector_name, status_data in current_health_status.items():
                current_time = datetime.now(timezone.utc)
                
                telegram_cooldown = getattr(config.Telegram, 'NOTIFICATION_COOLDOWN_SECONDS', 3600)
                if (current_time - last_notified_anomalies[detector_name]).total_seconds() < telegram_cooldown:
                    continue

                if status_data["status"] == "Failed":
                    anomalies_found.append(f"🔴 Detektor *{detector_name}* GAGAL! Error: `{utils._escape_markdown(status_data.get('error_message_last_run', 'Tidak ada pesan'))}`. Cek log lebih lanjut.")
                    last_notified_anomalies[detector_name] = current_time
                elif status_data["status"] == "Warning" and status_data["consecutive_zero_detections"] >= getattr(config.Monitoring, 'DETECTOR_ZERO_DETECTIONS_THRESHOLD', 5):
                    anomalies_found.append(f"🟡 Detektor *{detector_name}* Peringatan: {status_data['consecutive_zero_detections']} run berturut-turut dengan 0 deteksi. Pesan: `{utils._escape_markdown(status_data.get('warning_message_last_run', 'Tidak ada pesan'))}`.")
                    last_notified_anomalies[detector_name] = current_time
                elif status_data["status"] == "Not Run" and \
                    (status_data["last_run_time_utc"] is None or \
                     (current_time - status_data["last_run_time_utc"]).total_seconds() > interval * 1.5):
                    anomalies_found.append(f"⚪ Detektor *{detector_name}* belum pernah berjalan atau terlalu lama tidak aktif ({status_data['status']}).")
                    last_notified_anomalies[detector_name] = current_time


            if anomalies_found:
                message = "⚠️ *Anomali Detektor Terdeteksi:*\n\n" + "\n".join(anomalies_found)
                notification_service.send_telegram_message(message)
                logger.warning(f"Mengirim notifikasi anomali detektor: {len(anomalies_found)} anomali.")
            else:
                logger.info("Tidak ada anomali detektor yang terdeteksi.")
                
        except Exception as e: # <--- TAMBAHKAN BLOK 'except' INI
            logger.error(f"Error tak terduga di detector_health_monitoring_loop: {e}", exc_info=True)
            # Anda mungkin juga ingin menambahkan time.sleep di sini
            time.sleep(float(config.System.RETRY_DELAY_SECONDS) * 2)

        time.sleep(interval)


# --- calculate_and_update_daily_pnl (Fungsi Pembantu untuk PnL) ---
def calculate_and_update_daily_pnl(symbol: str):
    """
    Menghitung profit/loss harian terealisasi dari deal history MT5
    dan memperbarui variabel global.
    """
    global _daily_realized_pnl, _last_daily_pnl_calculation_time
    try:
        current_utc_time = datetime.now(timezone.utc)
        start_of_today_utc = current_utc_time.replace(hour=0, minute=0, second=0, microsecond=0)

        deals_today = mt5_connector.get_mt5_deals_history_raw(start_of_today_utc, current_utc_time, group=symbol)

        if deals_today:
            logger.debug(f"Ditemukan {len(deals_today)} deal yang ditutup hari ini untuk perhitungan P/L harian untuk {symbol}.")
        else:
            logger.debug(f"Tidak ada deal yang ditutup hari ini untuk perhitungan P/L harian untuk {symbol}.")

        total_profit_today = Decimal('0.0')
        if deals_today:
            for deal in deals_today:
                if deal.get('entry') == mt5_connector.mt5.DEAL_ENTRY_OUT and deal.get('profit') is not None:
                    total_profit_today += utils.to_decimal_or_none(deal['profit']) 

        _daily_realized_pnl = total_profit_today
        _last_daily_pnl_calculation_time = current_utc_time
        logger.info(f"Profit/Loss Harian Terealisasi: {float(_daily_realized_pnl):.2f} (diperbarui pada {_last_daily_pnl_calculation_time.isoformat()}).")

    except Exception as e:
        logger.error(f"Gagal menghitung profit/loss harian: {e}", exc_info=True)
        _daily_realized_pnl = Decimal('0.0')
        _last_daily_pnl_calculation_time = None

# --- daily_cleanup_scheduler_loop (Sudah ada di data_updater.py) ---
def daily_cleanup_scheduler_loop(symbol: str, stop_event: threading.Event, _feature_backfill_completed: bool): # Tambahkan _feature_backfill_completed sebagai parameter
    """Loop untuk menjadwalkan pembersihan database harian."""
    if symbol is None or not isinstance(symbol, str):
        logger.error("daily_cleanup_scheduler_loop dipanggil tanpa argumen simbol string yang valid. Menghentikan loop ini.")
        return
    while not stop_event.is_set():
        try:
            logger.info(f"Memulai tugas pembersihan harian untuk {symbol}...")
            database_manager.clean_old_price_ticks(symbol_param=config.TRADING_SYMBOL, days_to_keep=config.MarketData.HISTORICAL_DATA_RETENTION_DAYS.get("M1", 7))
            database_manager.clean_old_mt5_trade_data(symbol_param=config.TRADING_SYMBOL, days_to_keep=30)
            database_manager.clean_old_historical_candles(symbol_param=config.TRADING_SYMBOL, days_to_keep_by_timeframe=config.MarketData.HISTORICAL_DATA_RETENTION_DAYS)

            chart_output_dir = "charts_output"
            days_to_keep_charts = 7

            if os.path.exists(chart_output_dir):
                import glob # Import glob di sini
                cutoff_time = datetime.now(timezone.utc) - timedelta(days=days_to_keep_charts)
                deleted_chart_count = 0
                for f in glob.glob(os.path.join(chart_output_dir, "*.png")):
                    try:
                        file_timestamp = datetime.fromtimestamp(os.path.getmtime(f), tz=timezone.utc)
                        if file_timestamp < cutoff_time:
                            os.remove(f)
                            deleted_chart_count += 1
                    except Exception as e:
                        logger.error(f"Gagal menghapus file chart {f}: {e}")
                logger.info(f"Dibersihkan {deleted_chart_count} file chart lama dari {chart_output_dir}.")
            time.sleep(float(config.Scheduler.UPDATE_INTERVALS["daily_cleanup_scheduler_loop"]))
        except Exception as e:
            logger.error(f"Error tak terduga di daily_cleanup_scheduler_loop: {e}", exc_info=True)
            time.sleep(float(config.System.RETRY_DELAY_SECONDS) * 2)

# --- monthly_historical_feature_backfill_loop ---
# Catatan: Fungsi ini sudah ada dua kali di file yang Anda berikan.
# Saya akan mempertahankan yang kedua karena itu yang paling bawah,
# tapi disarankan untuk menghapus duplikat dan hanya mempertahankan satu.
# Karena namanya sama, Python akan menggunakan definisi yang terakhir.
def monthly_historical_feature_backfill_loop(symbol: str, stop_event: threading.Event, _feature_backfill_completed: bool):
    """Loop untuk menjalankan backfill fitur historis bulanan."""
    if symbol is None or not isinstance(symbol, str):
        logger.error("monthly_historical_feature_backfill_loop dipanggil tanpa argumen simbol string yang valid. Menghentikan loop ini.")
        return
    interval = config.Scheduler.UPDATE_INTERVALS.get('monthly_historical_feature_backfill_loop', 3600 * 24 * 30)
    while not stop_event.is_set():
        if _feature_backfill_completed:
            try:
                logger.info(f"Memulai backfill fitur historis bulanan untuk {symbol}...")
                market_data_processor.backfill_historical_features(symbol)
                logger.info(f"Backfill fitur historis bulanan untuk {symbol} selesai.")
            except Exception as e:
                logger.error(f"Gagal menghitung backfill fitur historis bulanan: {e}", exc_info=True)
                time.sleep(float(config.System.RETRY_DELAY_SECONDS) * 2)
        else:
            logger.info("Monthly Historical Feature Backfill loop menunggu backfill fitur awal selesai.")
        time.sleep(interval)


def periodic_daily_pnl_check_loop(symbol: str, stop_event: threading.Event, _feature_backfill_completed: bool): # Tambahkan parameter symbol dan _feature_backfill_completed
    """Loop untuk memeriksa profit/loss harian dan mengambil tindakan."""
    interval = config.Scheduler.UPDATE_INTERVALS.get('periodic_daily_pnl_check_loop', 300)
    while not stop_event.is_set():
        try:
            logger.info("Memeriksa status Profit/Loss Harian...")
            # Panggil fungsi perhitungan P/L harian dari data_updater, meneruskan symbol
            # Perhatikan bahwa _feature_backfill_completed tidak relevan untuk fungsi ini,
            # tetapi ditambahkan untuk konsistensi parameter loop.
            calculate_and_update_daily_pnl(symbol) 
            # Panggil fungsi pemeriksaan dan tindakan P/L harian dari auto_trade_manager
            auto_trade_manager.check_daily_pnl_and_act(symbol) # Menggunakan symbol yang dilewatkan
        except Exception as e:
            logger.error(f"Gagal memeriksa status P/L harian: {e}", exc_info=True)
        stop_event.wait(interval)
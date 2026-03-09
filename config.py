import os
import threading
import MetaTrader5 as mt5
from dotenv import load_dotenv
from datetime import datetime, timezone
import sys
import logging
import logging.handlers # <--- TAMBAHKAN BARIS INI

from decimal import Decimal, getcontext

# Set presisi Decimal untuk perhitungan keuangan secara global.
getcontext().prec = 10

# Dapatkan path absolut dari direktori saat ini.
basedir = os.path.abspath(os.path.dirname(__file__))

# --- Muat Environment Variables dari file .env ---
load_dotenv(os.path.join(basedir, '.env'))

# --- BARIS DEBUG SEMENTARA YANG PERLU ANDA TAMBAHKAN DI SINI ---
print(f"DEBUG: PATH .env file: {os.path.join(basedir, '.env')}")
print(f"DEBUG: DATABASE_URL dari os.getenv (setelah load_dotenv): {os.getenv('DATABASE_URL')}")
print(f"DEBUG: OPENAI_API_KEY dari os.getenv (setelah load_dotenv): {os.getenv('OPENAI_API_KEY')}")
# --- AKHIR BARIS DEBUG ---

# Inisialisasi logger untuk modul ini.
logger = logging.getLogger(__name__)


# Pindahkan AUTO_TRADE_STATUS_FILENAME ke level modul.
AUTO_TRADE_STATUS_FILENAME = "auto_trade_status.tmp"

class Config:
    """
    Kelas dasar untuk konfigurasi aplikasi.
    Menggunakan nested classes untuk organisasi yang lebih baik dan modularitas.
    """
    APP_ENV = os.getenv('APP_ENV', 'development')
    TRADING_SYMBOL = os.getenv("TRADING_SYMBOL", "XAUUSD")
    TRADING_SYMBOL_POINT_VALUE = Decimal("0.001")
    PIP_UNIT_IN_DOLLAR = Decimal("1.0")
    PIP_SIZE = Decimal("0.1")       
    @staticmethod
    def _set_from_env(env_vars: dict, target_cls, attr_name: str, expected_type, validation_func=None, error_detail: str = "", env_var_name: str = None):
        """
        Helper method for consistent environment variable loading and validation.
        Args:
            env_vars (dict): Dictionary of all environment variables (from os.environ).
            target_cls: The class (e.g., Config.Trading, Config.Scheduler) where the attribute is to be set.
            attr_name (str): The name of the attribute to set (e.g., 'DRY_RUN_MODE_ENABLED').
            expected_type: The Python type to convert the environment variable value to (e.g., int, Decimal, bool).
            validation_func (callable, optional): A function that takes the converted value and returns True if invalid.
            error_detail (str, optional): A string to describe the validation error if validation_func returns True.
            env_var_name (str, optional): Explicit environment variable name if different from attr_name.upper().
        """
        if env_var_name is None:
            env_var_name = attr_name.upper().replace('.', '_') # Default to upper case of attr_name

        env_val = env_vars.get(env_var_name)

        if env_val is not None:
            try:
                if expected_type == bool:
                    converted_val = env_val.lower() == 'true'
                elif expected_type == Decimal:
                    converted_val = Decimal(env_val)
                else:
                    converted_val = expected_type(env_val)
                
                setattr(target_cls, attr_name, converted_val)
                
                if validation_func and validation_func(converted_val):
                    raise ValueError(f"{attr_name} ({converted_val}) must be {error_detail}.")
            except ValueError as ve:
                raise ValueError(
                    f"{target_cls.__name__}.{attr_name} from environment ('{env_var_name}') "
                    f"is not a valid {expected_type.__name__}: '{env_val}'. Error: {ve}. Check your .env file."
                )
            except Exception as e:
                raise ValueError(
                    f"Unexpected error setting {target_cls.__name__}.{attr_name} from environment ('{env_var_name}'): {e}. Check your .env file."
                )
        # Jika env_val adalah None, atribut akan tetap menggunakan nilai default yang didefinisikan di kelas.
    # --- AKHIR PENAMBAHAN METODE HELPER ---
    class Database:
        """Konfigurasi terkait database."""
        URL = None # Akan dibaca dari os.getenv di validate_config

    class Paths:
        """Konfigurasi terkait jalur file dan direktori."""
        MQL5_DATA_FILE = None # Akan dibaca dari os.getenv di validate_config

    class APIKeys:
        """Konfigurasi untuk kunci API layanan eksternal."""
        OPENAI_API_KEY = None
        TELEGRAM_BOT_TOKEN = None
        TELEGRAM_CHAT_ID = None
        MYFXBOOK_COOKIES_STRING = None
        LM_STUDIO_API_ENDPOINT = "http://localhost:1234/v1" # Default endpoint

    class Trading:
        """Konfigurasi terkait parameter trading dan manajemen posisi."""
        DEFAULT_TIMEFRAME = "M5"
        PARTIAL_TP_LEVELS = [
            {'price_multiplier': Decimal('1.0'), 'volume_percentage_to_close': Decimal('0.50'), 'move_sl_to_breakeven_after_partial': True, 'move_sl_to_price_after_partial': None},
            {'price_multiplier': Decimal('2.0'), 'volume_percentage_to_close': Decimal('1.00'), 'move_sl_to_breakeven_after_partial': True, 'move_sl_to_price_after_partial': None},
            {'price_multiplier': Decimal('2.0'), 'volume_percentage_to_close': Decimal('1.00'), 'move_sl_to_breakeven_after_partial': False, 'move_sl_to_price_after_partial': None}
        ]
        DAILY_PROFIT_TARGET_PERCENT = Decimal('0.01')
        DAILY_LOSS_LIMIT_PERCENT = Decimal('0.02')
        SPREAD_POINTS = Decimal("16")
        COMMISSION_PER_LOT = Decimal("0.00")
        MAX_TOTAL_LOTS = Decimal("1.0")
        MAX_OPEN_POSITIONS = 10
        BACKTEST_HIT_TOLERANCE_POINTS = Decimal("0.5")
        HAS_REAL_VOLUME_DATA_FROM_BROKER = False
        HAS_TICK_VOLUME_DATA_FROM_BROKER = True
        HAS_REALTIME_TICK_VOLUME_FROM_BROKER = False
        MIN_SPREAD_POINTS = Decimal("0.5")
        MAX_SPREAD_POINTS = Decimal("2.0")
        MIN_SLIPPAGE_POINTS = Decimal("0.1")
        MAX_SLIPPAGE_POINTS = Decimal("0.5")
        MAX_LAYERS_PER_DIRECTION = 5
        MIN_PROFIT_FOR_LAYERING_PIPS = Decimal("2.0")
        CONSOLIDATE_SL_ON_LAYER = True
        LAYER_VOLUME_MULTIPLIER = Decimal("1.0")
        SAME_ZONE_TOLERANCE_PIPS = Decimal("3.0")
        RISK_PERCENT_PER_TRADE = Decimal("0.01")
        MAX_POSITION_HOLD_MINUTES = 240
        AUTO_TRADE_VOLUME = Decimal("0.01")
        AUTO_TRADE_SLIPPAGE = 0
        AUTO_TRADE_MAGIC_NUMBER = 12345
        RISK_PER_TRADE_PERCENT = Decimal("1.0")
        MAX_DAILY_DRAWDOWN_PERCENT = Decimal("5.0")
        TRAILING_STOP_PIPS = Decimal("15")
        TRAILING_STOP_STEP_PIPS = Decimal("5")
        TRADING_START_HOUR_UTC = 0
        TRADING_END_HOUR_UTC = 23
        MARKET_CLOSE_BUFFER_MINUTES = 30
        MARKET_OPEN_BUFFER_MINUTES = 30
        MIN_SL_PIPS = Decimal("0")
        MIN_TP_PIPS = Decimal("0")
        SWAP_RATE_PER_LOT_BUY = Decimal("-44.0")
        SWAP_RATE_PER_LOT_SELL = Decimal("-44.0")
        DRY_RUN_MODE_ENABLED = True # Default True for safety
        SIMULATED_PNL_TRACKING_ENABLED = True # Default True

        @staticmethod
        def _read_auto_trade_status():
            file_path = os.path.join(basedir, AUTO_TRADE_STATUS_FILENAME)
            try:
                with open(file_path, 'r') as f:
                    content = f.read().strip().lower()
                    return content == 'true'
            except FileNotFoundError:
                return False
            except Exception as e:
                logger.error(f"Error reading auto-trade status file: {e}", exc_info=True)
                return False

        @staticmethod
        def _write_auto_trade_status(status: bool):
            file_path = os.path.join(basedir, AUTO_TRADE_STATUS_FILENAME)
            try:
                with open(file_path, 'w') as f:
                    f.write(str(status).lower())
            except Exception as e:
                logger.error(f"Error writing auto-trade status to file: {e}", exc_info=True)

        _auto_trade_enabled_value = _read_auto_trade_status()

        @property
        def auto_trade_enabled(self):
            return self._auto_trade_enabled_value

        @auto_trade_enabled.setter
        def auto_trade_enabled(self, value: bool):
            self._auto_trade_enabled_value = value
            self._write_auto_trade_status(value)

    class Scheduler:
        """Konfigurasi untuk interval dan status loop scheduler."""
        # Nilai default Python (akan di-override oleh .env di validate_config)
        UPDATE_INTERVALS = {
            "periodic_realtime_tick_loop": 30.0,
            "periodic_session_data_update_loop": 300.0,
            "periodic_market_status_update_loop": 60.0,
            "periodic_mt5_trade_data_update_loop": 10.0,
            "daily_cleanup_scheduler_loop": 3600*24,
            "daily_open_prices_scheduler_loop": 3600*24,
            "automatic_signal_generation_loop": 1800.0,
            "periodic_historical_data_update_loop": 3600.0,
            "periodic_volume_profile_update_loop": 3600.0,
            "periodic_combined_advanced_detection_loop": 300.0,
            "periodic_fundamental_data_update_loop": 3600.0,
            "rule_based_signal_loop": 3600.0,
            "daily_summary_report_loop": 3600*24,
            "monthly_historical_feature_backfill_loop": 3600*24*30,
            "scenario_analysis_loop": 3600.0,
            "detector_health_monitoring_loop": 60.0,
            "periodic_daily_pnl_check_loop": 300.0,
            "aggressive_signal_loop": 300.0, # Pastikan float
            "llm_self_diagnosis_loop": 3600.0 * 24 # Default 24 jam
        }

        ENABLED_LOOPS = {
            "periodic_realtime_tick_loop": True,
            "periodic_session_data_update_loop": True,
            "periodic_market_status_update_loop": True,
            "periodic_mt5_trade_data_update_loop": True,
            "daily_cleanup_scheduler_loop": False,
            "daily_open_prices_scheduler_loop": True,
            "automatic_signal_generation_loop": False,
            "periodic_historical_data_update_loop": True,
            "periodic_volume_profile_update_loop": True,
            "periodic_combined_advanced_detection_loop": True,
            "periodic_fundamental_data_update_loop": True,
            "rule_based_signal_loop": True,
            "daily_summary_report_loop": True,
            "monthly_historical_feature_backfill_loop": True,
            "scenario_analysis_loop": True,
            "detector_health_monitoring_loop": True,
            "periodic_daily_pnl_check_loop": True,
            "aggressive_signal_loop": True,
            "llm_self_diagnosis_loop": True # default False
        }

        _data_update_thread_restart_lock = threading.Lock()
        _data_update_stop_event = threading.Event()
        _data_update_threads = []
        AUTO_DATA_UPDATE_ENABLED = True # Default True

    class Sessions:
        """Konfigurasi untuk jam buka/tutup sesi pasar utama (UTC)."""
        ASIA_SESSION_START_HOUR_UTC = 0
        ASIA_SESSION_END_HOUR_UTC = 9
        EUROPE_SESSION_START_HOUR_UTC = 7
        EUROPE_SESSION_END_HOUR_UTC = 16
        NEWYORK_SESSION_START_HOUR_UTC = 12
        NEWYORK_SESSION_END_HOUR_UTC = 21
        TIMEZONE_FOR_DST_CHECK = "America/New_York"

    class MarketData:
        """
        Konfigurasi terkait pengumpulan data pasar, timeframe, dan parameter detektor.
        """
        COLLECT_TIMEFRAMES = {"M1": 1000, "M5": 5000, "M15": 5000, "M30": 5000, "H1": 5000, "H4": 5000, "D1": 5000}
        ENABLED_TIMEFRAMES = {"M1": False, "M5": True, "M15": True, "M30": True, "H1": True, "H4": True, "D1": True}
        MA_PERIODS_TO_CALCULATE = [Decimal('20'), Decimal('50'), Decimal('100')]
        MA_TYPES_TO_CALCULATE = ["SMA", "EMA"]
        HISTORICAL_DATA_RETENTION_DAYS = {"M1": 7, "M5": 14, "M15": 30, "M30": 60, "H1": 90, "H4": 180, "D1": 0}
        HISTORICAL_DATA_START_DATE_FULL = "2024-01-01"
        _market_status_data_lock = threading.Lock()
        market_status_data = {"session_status": [], "overlap_status": "Not Determined", "current_utc_time": datetime.now(timezone.utc).isoformat(), "detailed_sessions": []}
        TIMEZONE_FOR_DST_CHECK = "America/New_York"
        ATR_PERIOD = 14
        ATR_MULTIPLIER_FOR_TOLERANCE = Decimal("0.5")
        ENABLE_SR_DETECTION = True
        SR_LOOKBACK_CANDLES = 200
        SR_ZONE_ATR_MULTIPLIER = Decimal("0.5")
        MIN_SR_STRENGTH = 1
        ENABLE_OB_FVG_DETECTION = True
        FVG_MIN_ATR_MULTIPLIER = Decimal("0.2")
        OB_MIN_VOLUME_MULTIPLIER = Decimal("1.5")
        OB_FVG_MITIGATION_LOOKBACK_CANDLES = 50
        ENABLE_LIQUIDITY_DETECTION = True
        LIQUIDITY_CANDLE_RANGE_PERCENT = Decimal("0.5")
        ENABLE_FIBONACCI_DETECTION = True
        FIBO_RETRACTION_LEVELS = [Decimal('0.236'), Decimal('0.382'), Decimal('0.5'), Decimal('0.618'), Decimal('0.786')]
        ENABLE_MARKET_STRUCTURE_DETECTION = True
        BOS_CHOCH_MIN_PIPS_CONFIRMATION = Decimal("10.0")
        ENABLE_SWING_DETECTION = True
        SWING_LOOKBACK_CANDLES = 50
        ENABLE_DIVERGENCE_DETECTION = True
        RSI_DIVERGENCE_PERIODS = 14
        MACD_DIVERGENCE_FAST_PERIOD = 12
        MACD_DIVERGENCE_SLOW_PERIOD = 26
        MACD_DIVERGENCE_SIGNAL_PERIOD = 9
        ENABLE_RSI_CALCULATION = True
        RSI_PERIOD = 14
        RSI_OVERBOUGHT_LEVEL = Decimal("70.0")
        RSI_OVERSOLD_LEVEL = Decimal("30.0")
        ENABLE_MACD_CALCULATION = True
        MACD_FAST_PERIOD = 12
        MACD_SLOW_PERIOD = 26
        MACD_SIGNAL_PERIOD = 9
        ENABLE_EMA_CROSS_DETECTION = True
        EMA_FAST_PERIOD = 50
        EMA_SLOW_PERIOD = 200
        ENABLE_MA_TREND_DETECTION = True
        MA_TREND_PERIODS = [Decimal('20'), Decimal('50'), Decimal('200')]
        MA_TREND_TIMEFRAMES = ["M15", "M30", "H1", "H4", "D1"]
        MA_TREND_TIMEFRAME_WEIGHTS = {"M15": 1, "M30": 1, "H1": 1, "H4": 2, "D1": 3}
        ENABLE_VOLUME_PROFILE_DETECTION = True
        ENABLE_PREVIOUS_HIGH_LOW_DETECTION = True
        CONFLUENCE_PROXIMITY_TOLERANCE_PIPS = Decimal("10.0")
        CONFLUENCE_SCORE_PER_LEVEL = 1
        OB_CONSOLIDATION_TOLERANCE_POINTS = Decimal("25.0")
        OB_SHOULDER_LENGTH = 3
        FVG_MIN_CANDLE_BODY_PERCENT_FOR_STRENGTH = Decimal("0.7")
        FVG_VOLUME_FACTOR_FOR_STRENGTH = Decimal("0.5")

    class AIAnalysts:
        """Konfigurasi untuk modul analisis AI."""
        MAX_FUNDAMENTAL_EVENTS_FOR_LLM = 2  # Default: Batas jumlah event untuk prompt LLM
        MAX_FUNDAMENTAL_ARTICLES_FOR_LLM = 1 # Default: Batas jumlah artikel untuk prompt LLM
        MAX_FUNDAMENTAL_EVENTS_FOR_LLM_DAYS_LOOKBACK = 1 # Default: Berapa hari ke belakang untuk event
        MAX_FUNDAMENTAL_ARTICLES_FOR_LLM_DAYS_LOOKBACK = 1 # Default: Berapa hari ke belakang untuk artikel

        OPENAI_MODEL_NAME = "gpt-4o"
        OPENAI_TEMPERATURE = 0.7
        AI_MIN_ANALYSTS_FOR_CONSENSUS = 2
        FVG_MIN_DOLLARS = Decimal("0.0005")
        SWING_EXT_BARS = 1
        RSI_PERIOD = 14
        RSI_OVERBOUGHT = Decimal("70.0")
        RSI_OVERSOLD = Decimal("30.0")
        MACD_FAST_PERIOD = 12
        MACD_SLOW_PERIOD = 26
        MACD_SIGNAL_PERIOD = 9
        TREND_MA_SHORT_PERIOD = 20
        TREND_MA_MEDIUM_PERIOD = 50
        TREND_MA_LONG_PERIOD = 200
        MAX_FVG_DISPLAY = 3
        MAX_OB_DISPLAY = 3
        MAX_KEY_LEVEL_DISPLAY = 5
        MAX_SR_DISPLAY = 3
        MAX_LIQUIDITY_DISPLAY = 0
        MAX_DIVERGENCE_DISPLAY = 0
        CHART_BUFFER_PERCENTAGE = 0.05
        CHART_MIN_STRENGTH_SCORE = 1
        CHART_INCLUDE_KEY_LEVELS_ONLY = False
        CHART_MAX_FVGS_TO_PLOT = 5
        CHART_MAX_SR_TO_PLOT = 15
        CHART_MAX_OB_TO_PLOT = 10
        CHART_MAX_MS_TO_PLOT = 10
        CHART_MAX_LIQ_TO_PLOT = 8
        CHART_MAX_FIB_TO_PLOT = 10
        CHART_MAX_FIB_SETS_TO_PLOT = 5
        ANALYSIS_CONFIGS = {
            "Technical_Trend": {"enabled": True, "persona_prompt": "Anda adalah seorang analis tren teknikal pasar keuangan yang ahli dalam mengidentifikasi arah pergerakan harga jangka menengah hingga panjang menggunakan moving averages dan struktur pasar yang besar. Fokus pada bias tren (bullish/bearish/sideways) dan konfirmasi tren. Berikan alasan yang jelas berdasarkan konfluensi MA (misalnya 50 SMA di atas 200 SMA) dan Break of Structure (BoS) atau Change of Character (ChoCh) di timeframe tinggi (H4, D1).", "confidence_threshold": "Moderate", "relevant_timeframes": ["H4", "D1"]},
            "Technical_Levels": {"enabled": True, "persona_prompt": "Anda adalah seorang analis level kunci teknikal pasar keuangan yang ahli dalam mengidentifikasi area Supply/Demand, Order Blocks (OB), Fair Value Gaps (FVG), Support/Resistance (S&R), dan Liquidity Zones di timeframe H1 dan H4. Fokus pada di mana harga bereaksi terhadap level-level penting. Berikan rekomendasi trading berdasarkan reaksi harga terhadap level tersebut (misalnya, harga kembali ke OB Bullish, mengisi FVG Bearish).", "confidence_threshold": "Moderate", "relevant_timeframes": ["H1", "H4"]},
            "Technical_Momentum": {"enabled": True, "persona_prompt": "Anda adalah seorang analis momentum teknikal pasar keuangan yang ahli dalam mengidentifikasi divergensi (RSI, MACD) dan pola candlestick yang kuat di timeframe M15 dan H1. Fokus pada perubahan momentum dan potensi pembalikan atau kelanjutan tren. Berikan rekomendasi trading berdasarkan sinyal momentum (misalnya, divergensi bullish RSI, pola engulfing candle).", "confidence_threshold": "Moderate", "relevant_timeframes": ["M15", "H1"]},
            "Fundamental_Analyst": {"enabled": True, "persona_prompt": "Anda adalah seorang analis fundamental makroekonomi yang ahli dalam memahami dampak berita ekonomi (NFP, CPI, suku bunga, FOMC) dan peristiwa geopolitik (perang, konflik, kebijakan perdagangan) pada pasar XAUUSD dan USD. Fokus pada interpretasi data fundamental dan implikasinya terhadap arah harga aset. Berikan analisis dan rekomendasi berdasarkan pandangan fundamental Anda.", "confidence_threshold": "High", "relevant_timeframes": ["Daily"]}
        }

    class RuleBasedStrategy:
        """Konfigurasi untuk strategi trading berbasis aturan."""
        DEFAULT_SL_PIPS = Decimal("10")
        TP1_PIPS = Decimal("5")
        TP2_PIPS = Decimal("10")
        TP3_PIPS = Decimal("50")
        RULE_SR_TOLERANCE_POINTS = Decimal("15")
        RULE_EQUAL_LEVEL_TOLERANCE_POINTS = Decimal("15")
        RULE_OB_CONSOLIDATION_TOLERANCE_POINTS = Decimal("25")
        DEFAULT_SL_TP_RATIO = Decimal("1.5")
        RULE_SR_TOLERANCE_POINTS = Decimal("15")
        EMA_SHORT_PERIOD = 9
        EMA_LONG_PERIOD = 21
        LOOKBACK_CANDLES_LTF = 200
        LOOKBACK_CANDLES_HTF = 100
        CANDLE_BODY_MIN_RATIO = Decimal("0.4")
        CANDLE_MIN_SIZE_PIPS = Decimal("10.0")
        STRUCTURE_OFFSET_PIPS = Decimal("10.0")
        OB_SHOULDER_LENGTH = 3
        SR_STRENGTH_RETEST_WINDOW_CANDLES = 50
        SR_STRENGTH_BREAK_TOLERANCE_MULTIPLIER = Decimal("1.5")
        OB_MIN_IMPULSIVE_CANDLE_BODY_PERCENT = Decimal("0.5")
        OB_MIN_IMPULSIVE_MOVE_MULTIPLIER = Decimal("3.0")
        OB_VOLUME_FACTOR_MULTIPLIER = Decimal("0.5")
        FVG_MIN_CANDLE_BODY_PERCENT_FOR_STRENGTH = Decimal("0.7")
        FVG_VOLUME_FACTOR_FOR_STRENGTH = Decimal("0.5")
        SD_MIN_IMPULSIVE_MOVE_ATR_MULTIPLIER = Decimal("1.5")
        DIVERGENCE_PRICE_TOLERANCE_ATR_MULTIPLIER = Decimal("0.2")
        MS_BREAK_ATR_MULTIPLIER = Decimal("0.2")
        CONFLUENCE_PROXIMITY_TOLERANCE_PIPS = Decimal("10.0")
        CONFLUENCE_SCORE_PER_LEVEL = 1
        SIGNAL_RULES = [
            {"name": "Bullish Rejection from Demand Zone (High Conf.)", "action": "BUY", "aggressiveness_level": "High-Probability", "priority": 1, "enabled": True, "timeframes": ["H1", "H4"], "conditions": ["TREND_H4_BULLISH", "PRICE_IN_ACTIVE_DEMAND_ZONE", "DEMAND_ZONE_STRENGTH_GE_2", "CANDLE_REJECTION_BULLISH_H1", "RSI_H1_OVERSOLD_OR_NOT_OVERBOUGHT", "MACD_H1_BULLISH_CROSS_OR_POSITIVE"], "entry_price_logic": "DEMAND_ZONE_TOP", "stop_loss_logic": "BELOW_DEMAND_ZONE_LOW", "take_profit_logic": "AT_NEXT_SUPPLY_ZONE", "risk_reward_ratio_min": Decimal('1.5')},
            {"name": "Bearish Rejection from Supply Zone (Moderate)", "action": "SELL", "aggressiveness_level": "Moderate", "priority": 2, "enabled": True, "timeframes": ["H1"], "conditions": ["TREND_H1_BEARISH", "PRICE_IN_ACTIVE_SUPPLY_ZONE", "SUPPLY_ZONE_STRENGTH_GE_1", "CANDLE_REJECTION_BEARISH_H1", "RSI_H1_OVERBOUGHT_OR_NOT_OVERSOLD"], "entry_price_logic": "SUPPLY_ZONE_BOTTOM", "stop_loss_logic": "ABOVE_SUPPLY_ZONE_HIGH", "take_profit_logic": "AT_NEXT_DEMAND_ZONE", "risk_reward_ratio_min": Decimal('1.0')},
            {"name": "Trend Continuation - Pullback to OB (Aggressive)", "action": "BUY", "aggressiveness_level": "Aggressive", "priority": 3, "enabled": False, "timeframes": ["M15"], "conditions": ["TREND_M15_BULLISH", "PRICE_IN_ACTIVE_BULLISH_OB", "OB_MITIGATED_ONCE"], "entry_price_logic": "OB_MIDPOINT", "stop_loss_logic": "BELOW_OB_LOW", "take_profit_logic": "FIXED_RR_1_5", "risk_reward_ratio_min": Decimal('1.5')},
        ]

    class Telegram:
        """Konfigurasi untuk notifikasi Telegram."""
        NOTIFICATION_COOLDOWN_SECONDS = 14400
        MAX_EVENTS_TO_NOTIFY = 10
        MAX_ARTICLES_TO_NOTIFY = 10
        NOTIF_MAX_LEVELS_PER_TYPE_PER_TF = 3
        NOTIF_MAX_FVG_PER_TF = 3
        NOTIF_MAX_OB_PER_TF = 3
        NOTIF_MAX_KEY_LEVELS_PER_TF = 5
        NOTIF_MAX_RESISTANCE_PER_TF = 3
        NOTIF_MAX_SUPPORT_PER_TF = 3
        NOTIF_MAX_FIBO_PER_TF = 0
        NOTIF_MAX_SWING_PER_TF = 1
        SEND_SIGNAL_NOTIFICATIONS = True
        SEND_TRADE_NOTIFICATIONS = True
        SEND_ACCOUNT_NOTIFICATIONS = True
        SEND_DAILY_SUMMARY = True
        SEND_ERROR_NOTIFICATIONS = True
        SEND_APP_STATUS_NOTIFICATIONS = True
        SEND_FUNDAMENTAL_NOTIFICATIONS = True
        SEND_INDIVIDUAL_ANALYST_SIGNALS = False
        # --- TAMBAHAN BARU ---
        MIN_IMPACT_NOTIF_LEVEL = "Medium" # Default: Hanya notifikasi Medium dan High impact
        # --- AKHIR TAMBAHAN ---

    class System:
        """Konfigurasi terkait pengaturan sistem umum dan penanganan error."""
        MAX_RETRIES = 5
        RETRY_DELAY_SECONDS = Decimal("0.5")
        DATABASE_BATCH_SIZE = 1000
        LOG_LEVEL = "DEBUG"     # <--- UBAH INI MENJADI STRING "DEBUG" (atau "INFO", "WARNING", dll.)
        LOG_FILE = "app.log"

    class Monitoring:
        """Konfigurasi untuk pemantauan kesehatan detektor."""
        DETECTOR_ZERO_DETECTIONS_THRESHOLD = 3
        DETECTOR_HEALTH_CHECK_INTERVAL_SECONDS = 3600

    @classmethod
    def validate_config(cls):
        """
        Validasi semua konfigurasi setelah dimuat dari environment variables.
        Ini memastikan tipe data yang benar dan nilai yang masuk akal untuk setiap parameter.
        Metode ini dipanggil saat modul config dimuat.
        """
        # --- PASTIKAN BARIS INI ADA DI AWAL FUNGSI validate_config ---
        env_vars = {k: os.getenv(k) for k in os.environ}
        # --- AKHIR BAGIAN KRITIS ---

        # Validasi PARTIAL_TP_LEVELS (saat ini hardcoded, validasi memastikan strukturnya benar)
        if not isinstance(cls.Trading.PARTIAL_TP_LEVELS, list):
            raise ValueError("PARTIAL_TP_LEVELS must be a list.")
        for i, tp_level in enumerate(cls.Trading.PARTIAL_TP_LEVELS):
            if not isinstance(tp_level, dict):
                raise ValueError(f"PARTIAL_TP_LEVELS[{i}] must be a dictionary.")
            if 'price_multiplier' not in tp_level or not isinstance(tp_level['price_multiplier'], Decimal):
                raise ValueError(f"PARTIAL_TP_LEVELS[{i}] missing or invalid 'price_multiplier' (must be Decimal).")
            if tp_level['price_multiplier'] <= 0:
                raise ValueError(f"PARTIAL_TP_LEVELS[{i}] 'price_multiplier' must be greater than 0.")
            if 'volume_percentage_to_close' not in tp_level or not isinstance(tp_level['volume_percentage_to_close'], Decimal):
                raise ValueError(f"PARTIAL_TP_LEVELS[{i}] missing or invalid 'volume_percentage_to_close' (must be Decimal).")
            if not (Decimal('0.0') <= tp_level['volume_percentage_to_close'] <= Decimal('1.0')):
                raise ValueError(f"PARTIAL_TP_LEVELS[{i}] 'volume_percentage_to_close' must be between 0.0 and 1.0.")
            if 'move_sl_to_breakeven_after_partial' not in tp_level or not isinstance(tp_level['move_sl_to_breakeven_after_partial'], bool):
                raise ValueError(f"PARTIAL_TP_LEVELS[{i}] missing or invalid 'move_sl_to_breakeven_after_partial' (must be boolean).")
            if 'move_sl_to_price_after_partial' in tp_level and tp_level['move_sl_to_price_after_partial'] is not None and not isinstance(tp_level['move_sl_to_price_after_partial'], Decimal):
                raise ValueError(f"PARTIAL_TP_LEVELS[{i}] 'move_sl_to_price_after_partial' must be Decimal or None.")


        # Validasi dan set Daily Profit/Loss Limit
        cls._set_from_env(env_vars, cls.Trading, 'DAILY_PROFIT_TARGET_PERCENT', Decimal, lambda x: x < 0, "non-negative")
        cls._set_from_env(env_vars, cls.Trading, 'DAILY_LOSS_LIMIT_PERCENT', Decimal, lambda x: x < 0, "non-negative")

        # Validasi dan set TRADING_SYMBOL_POINT_VALUE
        cls._set_from_env(env_vars, cls, 'TRADING_SYMBOL_POINT_VALUE', Decimal)

        # Validasi dan set PIP_UNIT_IN_DOLLAR
        cls._set_from_env(env_vars, cls, 'PIP_UNIT_IN_DOLLAR', Decimal)

        # Validasi dan set MA_TREND_PERIODS (dari string dipisahkan koma di .env)
        ma_trend_periods_env = env_vars.get("MA_TREND_PERIODS")
        if ma_trend_periods_env:
            try:
                cls.MarketData.MA_TREND_PERIODS = [Decimal(p.strip()) for p in ma_trend_periods_env.split(',')]
            except Exception:
                raise ValueError("MA_TREND_PERIODS must be a comma-separated list of valid decimal numbers (e.g., '20,50,200').")

        # Validasi dan set FIBO_RETRACTION_LEVELS (dari string dipisahkan koma di .env)
        fibo_retraction_levels_env = env_vars.get("FIBO_RETRACTION_LEVELS")
        if fibo_retraction_levels_env:
            try:
                cls.MarketData.FIBO_RETRACTION_LEVELS = [Decimal(r.strip()) for r in fibo_retraction_levels_env.split(',')]
            except Exception:
                raise ValueError("FIBO_RETRACTION_LEVELS must be a comma-separated list of valid decimal numbers (e.g., '0.236,0.382,0.5').")
        # --- Tambahan Validasi Khusus untuk Layering/Konsolidasi ---
        cls._set_from_env(env_vars, cls.Trading, 'MAX_LAYERS_PER_DIRECTION', int, lambda x: x < 0, "non-negative")
        cls._set_from_env(env_vars, cls.Trading, 'MIN_PROFIT_FOR_LAYERING_PIPS', Decimal, lambda x: x < 0, "non-negative")
        cls._set_from_env(env_vars, cls.Trading, 'LAYER_VOLUME_MULTIPLIER', Decimal, lambda x: x <= 0, "greater than 0")
        cls._set_from_env(env_vars, cls.Trading, 'SAME_ZONE_TOLERANCE_PIPS', Decimal, lambda x: x < 0, "non-negative")
        
        cls._set_from_env(env_vars, cls.APIKeys, 'LM_STUDIO_API_ENDPOINT', str)
        if not cls.APIKeys.LM_STUDIO_API_ENDPOINT:
            logger.warning("APIKeys.LM_STUDIO_API_ENDPOINT tidak ditemukan di .env atau kosong. LM Studio API mungkin tidak berfungsi.")

        # Loop validasi dan penimpaan nilai dari .env ke properti kelas yang sesuai.
        attrs_to_validate_from_env = {
            # Class Config (root)
            'TRADING_SYMBOL': str,
            'DEFAULT_TIMEFRAME': str,
            # Class Trading
            'AUTO_TRADE_VOLUME': Decimal,
            'BACKTEST_HIT_TOLERANCE_POINTS': Decimal,
            'MAX_TOTAL_LOTS': Decimal,
            'MAX_OPEN_POSITIONS': int,
            'AUTO_TRADE_SLIPPAGE': int,
            'AUTO_TRADE_MAGIC_NUMBER': int,
            'RISK_PER_TRADE_PERCENT': Decimal,
            'MAX_DAILY_DRAWDOWN_PERCENT': Decimal,
            'TRAILING_STOP_PIPS': Decimal,
            'TRAILING_STOP_STEP_PIPS': Decimal,
            'TRADING_START_HOUR_UTC': int,
            'TRADING_END_HOUR_UTC': int,
            'MARKET_CLOSE_BUFFER_MINUTES': int,
            'MARKET_OPEN_BUFFER_MINUTES': int,
            'MIN_SL_PIPS': Decimal,
            'MIN_TP_PIPS': Decimal,
            'SWAP_RATE_PER_LOT_BUY': Decimal,
            'SWAP_RATE_PER_LOT_SELL': Decimal,
            'DRY_RUN_MODE_ENABLED': bool,
            'SIMULATED_PNL_TRACKING_ENABLED': bool,

            # Class Scheduler (khusus AUTO_DATA_UPDATE_ENABLED yang ada di level ini)
            'AUTO_DATA_UPDATE_ENABLED': bool,

            # Class Sessions
            'ASIA_SESSION_START_HOUR_UTC': int,
            'ASIA_SESSION_END_HOUR_UTC': int,
            'EUROPE_SESSION_START_HOUR_UTC': int,
            'EUROPE_SESSION_END_HOUR_UTC': int,
            'NEWYORK_SESSION_START_HOUR_UTC': int,
            'NEWYORK_SESSION_END_HOUR_UTC': int,
            'TIMEZONE_FOR_DST_CHECK': str,

            # Class MarketData (Detektor & Umum)
            'ATR_PERIOD': int,
            'ATR_MULTIPLIER_FOR_TOLERANCE': Decimal,
            'SR_LOOKBACK_CANDLES': int,
            'SR_ZONE_ATR_MULTIPLIER': Decimal,
            'MIN_SR_STRENGTH': int,
            'ENABLE_OB_FVG_DETECTION': bool,
            'FVG_MIN_ATR_MULTIPLIER': Decimal,
            'OB_MIN_VOLUME_MULTIPLIER': Decimal,
            'OB_FVG_MITIGATION_LOOKBACK_CANDLES': int,
            'LIQUIDITY_CANDLE_RANGE_PERCENT': Decimal,
            'BOS_CHOCH_MIN_PIPS_CONFIRMATION': Decimal,
            'SWING_LOOKBACK_CANDLES': int,
            'RSI_DIVERGENCE_PERIODS': int,
            'MACD_DIVERGENCE_FAST_PERIOD': int,
            'MACD_DIVERGENCE_SLOW_PERIOD': int,
            'MACD_DIVERGENCE_SIGNAL_PERIOD': int,
            'ENABLE_SR_DETECTION': bool,
            'ENABLE_OB_FVG_DETECTION': bool,
            'ENABLE_LIQUIDITY_DETECTION': bool,
            'ENABLE_FIBONACCI_DETECTION': bool,
            'ENABLE_MARKET_STRUCTURE_DETECTION': bool,
            'ENABLE_SWING_DETECTION': bool,
            'ENABLE_DIVERGENCE_DETECTION': bool,
            'ENABLE_RSI_CALCULATION': bool,
            'ENABLE_MACD_CALCULATION': bool,
            'ENABLE_EMA_CROSS_DETECTION': bool,
            'ENABLE_MA_TREND_DETECTION': bool,
            'ENABLE_VOLUME_PROFILE_DETECTION': bool,
            'ENABLE_PREVIOUS_HIGH_LOW_DETECTION': bool,
            'CONFLUENCE_PROXIMITY_TOLERANCE_PIPS': Decimal,
            'CONFLUENCE_SCORE_PER_LEVEL': int,
            'OB_CONSOLIDATION_TOLERANCE_POINTS': Decimal,
            'OB_SHOULDER_LENGTH': int,
            'FVG_MIN_CANDLE_BODY_PERCENT_FOR_STRENGTH': Decimal,
            'FVG_VOLUME_FACTOR_FOR_STRENGTH': Decimal,
            'HISTORICAL_DATA_START_DATE_FULL': str,
            'RSI_PERIOD': int,
            'RSI_OVERBOUGHT_LEVEL': Decimal,
            'RSI_OVERSOLD_LEVEL': Decimal,
            'MACD_FAST_PERIOD': int,
            'MACD_SLOW_PERIOD': int,
            'MACD_SIGNAL_PERIOD': int,


            # Class AIAnalysts
            'OPENAI_MODEL_NAME': str,
            'OPENAI_TEMPERATURE': float,
            'AI_MIN_ANALYSTS_FOR_CONSENSUS': int,
            'FVG_MIN_DOLLARS': Decimal,
            'SWING_EXT_BARS': int,
            'RSI_OVERBOUGHT': Decimal,
            'RSI_OVERSOLD': Decimal,
            'MAX_FVG_DISPLAY': int,
            'MAX_OB_DISPLAY': int,
            'MAX_KEY_LEVEL_DISPLAY': int,
            'MAX_SR_DISPLAY': int,
            'MAX_LIQUIDITY_DISPLAY': int,
            'MAX_DIVERGENCE_DISPLAY': int,
            'CHART_BUFFER_PERCENTAGE': float,
            'CHART_MIN_STRENGTH_SCORE': int,
            'CHART_INCLUDE_KEY_LEVELS_ONLY': bool,
            'CHART_MAX_FVGS_TO_PLOT': int,
            'CHART_MAX_SR_TO_PLOT': int,
            'CHART_MAX_OB_TO_PLOT': int,
            'CHART_MAX_MS_TO_PLOT': int,
            'CHART_MAX_LIQ_TO_PLOT': int,
            'CHART_MAX_FIB_TO_PLOT': int,
            'CHART_MAX_FIB_SETS_TO_PLOT': int,

            # Class RuleBasedStrategy
            'DEFAULT_SL_PIPS': Decimal,
            'TP1_PIPS': Decimal,
            'TP2_PIPS': Decimal,
            'TP3_PIPS': Decimal,
            'RULE_SR_TOLERANCE_POINTS': Decimal,
            'RULE_EQUAL_LEVEL_TOLERANCE_POINTS': Decimal,
            'RULE_OB_CONSOLIDATION_TOLERANCE_POINTS': Decimal,
            'EMA_SHORT_PERIOD': int,
            'EMA_LONG_PERIOD': int,
            'LOOKBACK_CANDLES_LTF': int,
            'LOOKBACK_CANDLES_HTF': int,
            'CANDLE_BODY_MIN_RATIO': Decimal,
            'CANDLE_MIN_SIZE_PIPS': Decimal,
            'STRUCTURE_OFFSET_PIPS': Decimal,
            'SR_STRENGTH_RETEST_WINDOW_CANDLES': int,
            'SR_STRENGTH_BREAK_TOLERANCE_MULTIPLIER': Decimal,
            'OB_MIN_IMPULSIVE_CANDLE_BODY_PERCENT': Decimal,
            'OB_MIN_IMPULSIVE_MOVE_MULTIPLIER': Decimal,
            'OB_VOLUME_FACTOR_MULTIPLIER': Decimal,
            'FVG_MIN_CANDLE_BODY_PERCENT_FOR_STRENGTH': Decimal,
            'FVG_VOLUME_FACTOR_FOR_STRENGTH': Decimal,
            'SD_MIN_IMPULSIVE_MOVE_ATR_MULTIPLIER': Decimal,
            'DIVERGENCE_PRICE_TOLERANCE_ATR_MULTIPLIER': Decimal,
            'MS_BREAK_ATR_MULTIPLIER': Decimal,

            # Class Telegram
            'TELEGRAM_NOTIFICATION_COOLDOWN_SECONDS': int,
            'MAX_EVENTS_TO_NOTIFY': int,
            'MAX_ARTICLES_TO_NOTIFY': int,
            'NOTIF_MAX_LEVELS_PER_TYPE_PER_TF': int,
            'NOTIF_MAX_FVG_PER_TF': int,
            'NOTIF_MAX_OB_PER_TF': int,
            'NOTIF_MAX_KEY_LEVELS_PER_TF': int,
            'NOTIF_MAX_RESISTANCE_PER_TF': int,
            'NOTIF_MAX_SUPPORT_PER_TF': int,
            'NOTIF_MAX_FIBO_PER_TF': int,
            'NOTIF_MAX_SWING_PER_TF': int,
            'SEND_SIGNAL_NOTIFICATIONS': bool,
            'SEND_TRADE_NOTIFICATIONS': bool,
            'SEND_ACCOUNT_NOTIFICATIONS': bool,
            'SEND_DAILY_SUMMARY': bool,
            'SEND_ERROR_NOTIFICATIONS': bool,
            'SEND_APP_STATUS_NOTIFICATIONS': bool,
            'SEND_FUNDAMENTAL_NOTIFICATIONS': bool,
            'SEND_INDIVIDUAL_ANALYST_SIGNALS': bool,

            # Class System
            'MAX_RETRIES': int,
            'RETRY_DELAY_SECONDS': Decimal,
            'DATABASE_BATCH_SIZE': int,

            # Class Monitoring
            'DETECTOR_ZERO_DETECTIONS_THRESHOLD': int,
            'DETECTOR_HEALTH_CHECK_INTERVAL_SECONDS': int,
        }
        
        # Mulai validasi properti kelas
        # Proses semua variabel lingkungan di awal untuk akses cepat
        
        # Validasi properti Config (level root)
        cls._set_from_env(env_vars, cls, 'TRADING_SYMBOL', str)
        cls._set_from_env(env_vars, cls, 'TRADING_SYMBOL_POINT_VALUE', Decimal)
        cls._set_from_env(env_vars, cls, 'PIP_UNIT_IN_DOLLAR', Decimal)
        cls._set_from_env(env_vars, cls, 'PIP_SIZE', Decimal)
        
        # Validasi Database
        cls._set_from_env(env_vars, cls.Database, 'URL', str, env_var_name='DATABASE_URL')
        if cls.Database.URL is None:
             raise ValueError("DATABASE_URL must be set in your .env file.")

        # Validasi Paths
        cls._set_from_env(env_vars, cls.Paths, 'MQL5_DATA_FILE', str, env_var_name='MQL5_DATA_FILE_PATH')
        if cls.Paths.MQL5_DATA_FILE is None:
             logger.warning("MQL5_DATA_FILE_PATH tidak disetel di .env. MT5 mungkin tidak bisa diinisialisasi jika tidak ada default.")

        # Validasi APIKeys
        cls._set_from_env(env_vars, cls.APIKeys, 'OPENAI_API_KEY', str)
        cls._set_from_env(env_vars, cls.APIKeys, 'TELEGRAM_BOT_TOKEN', str)
        cls._set_from_env(env_vars, cls.APIKeys, 'TELEGRAM_CHAT_ID', str)
        cls._set_from_env(env_vars, cls.APIKeys, 'MYFXBOOK_COOKIES_STRING', str)
        cls._set_from_env(env_vars, cls.APIKeys, 'LM_STUDIO_API_ENDPOINT', str)
        if not cls.APIKeys.LM_STUDIO_API_ENDPOINT:
            logger.warning("APIKeys.LM_STUDIO_API_ENDPOINT tidak ditemukan di .env atau kosong. LM Studio API mungkin tidak berfungsi.")

        # Validasi Trading
        cls._set_from_env(env_vars, cls.Trading, 'DEFAULT_TIMEFRAME', str)
        cls._set_from_env(env_vars, cls.Trading, 'DAILY_PROFIT_TARGET_PERCENT', Decimal, lambda x: x < 0, "non-negative")
        cls._set_from_env(env_vars, cls.Trading, 'DAILY_LOSS_LIMIT_PERCENT', Decimal, lambda x: x < 0, "non-negative")
        cls._set_from_env(env_vars, cls.Trading, 'SPREAD_POINTS', Decimal)
        cls._set_from_env(env_vars, cls.Trading, 'COMMISSION_PER_LOT', Decimal)
        cls._set_from_env(env_vars, cls.Trading, 'MAX_TOTAL_LOTS', Decimal)
        cls._set_from_env(env_vars, cls.Trading, 'MAX_OPEN_POSITIONS', int)
        cls._set_from_env(env_vars, cls.Trading, 'BACKTEST_HIT_TOLERANCE_POINTS', Decimal)
        cls._set_from_env(env_vars, cls.Trading, 'HAS_REAL_VOLUME_DATA_FROM_BROKER', bool)
        cls._set_from_env(env_vars, cls.Trading, 'HAS_TICK_VOLUME_DATA_FROM_BROKER', bool)
        cls._set_from_env(env_vars, cls.Trading, 'HAS_REALTIME_TICK_VOLUME_FROM_BROKER', bool)
        cls._set_from_env(env_vars, cls.Trading, 'MIN_SPREAD_POINTS', Decimal)
        cls._set_from_env(env_vars, cls.Trading, 'MAX_SPREAD_POINTS', Decimal)
        cls._set_from_env(env_vars, cls.Trading, 'MIN_SLIPPAGE_POINTS', Decimal)
        cls._set_from_env(env_vars, cls.Trading, 'MAX_SLIPPAGE_POINTS', Decimal)
        cls._set_from_env(env_vars, cls.Trading, 'MAX_LAYERS_PER_DIRECTION', int)
        cls._set_from_env(env_vars, cls.Trading, 'MIN_PROFIT_FOR_LAYERING_PIPS', Decimal)
        cls._set_from_env(env_vars, cls.Trading, 'CONSOLIDATE_SL_ON_LAYER', bool)
        cls._set_from_env(env_vars, cls.Trading, 'LAYER_VOLUME_MULTIPLIER', Decimal)
        cls._set_from_env(env_vars, cls.Trading, 'SAME_ZONE_TOLERANCE_PIPS', Decimal)
        cls._set_from_env(env_vars, cls.Trading, 'RISK_PERCENT_PER_TRADE', Decimal)
        cls._set_from_env(env_vars, cls.Trading, 'MAX_DAILY_DRAWDOWN_PERCENT', Decimal)
        cls._set_from_env(env_vars, cls.Trading, 'TRAILING_STOP_PIPS', Decimal)
        cls._set_from_env(env_vars, cls.Trading, 'TRAILING_STOP_STEP_PIPS', Decimal)
        cls._set_from_env(env_vars, cls.Trading, 'TRADING_START_HOUR_UTC', int)
        cls._set_from_env(env_vars, cls.Trading, 'TRADING_END_HOUR_UTC', int)
        cls._set_from_env(env_vars, cls.Trading, 'MARKET_CLOSE_BUFFER_MINUTES', int)
        cls._set_from_env(env_vars, cls.Trading, 'MARKET_OPEN_BUFFER_MINUTES', int)
        cls._set_from_env(env_vars, cls.Trading, 'MIN_SL_PIPS', Decimal)
        cls._set_from_env(env_vars, cls.Trading, 'MIN_TP_PIPS', Decimal)
        cls._set_from_env(env_vars, cls.Trading, 'SWAP_RATE_PER_LOT_BUY', Decimal)
        cls._set_from_env(env_vars, cls.Trading, 'SWAP_RATE_PER_LOT_SELL', Decimal)
        cls._set_from_env(env_vars, cls.Trading, 'DRY_RUN_MODE_ENABLED', bool)
        cls._set_from_env(env_vars, cls.Trading, 'SIMULATED_PNL_TRACKING_ENABLED', bool)
        
        # Validasi AUTO_TRADE_ENABLED dari file status
        cls.Trading._auto_trade_enabled_value = cls.Trading._read_auto_trade_status()


        # Validasi Scheduler (khusus AUTO_DATA_UPDATE_ENABLED)
        cls._set_from_env(env_vars, cls.Scheduler, 'AUTO_DATA_UPDATE_ENABLED', bool)

        # Validasi Sessions
        cls._set_from_env(env_vars, cls.Sessions, 'ASIA_SESSION_START_HOUR_UTC', int)
        cls._set_from_env(env_vars, cls.Sessions, 'ASIA_SESSION_END_HOUR_UTC', int)
        cls._set_from_env(env_vars, cls.Sessions, 'EUROPE_SESSION_START_HOUR_UTC', int)
        cls._set_from_env(env_vars, cls.Sessions, 'EUROPE_SESSION_END_HOUR_UTC', int)
        cls._set_from_env(env_vars, cls.Sessions, 'NEWYORK_SESSION_START_HOUR_UTC', int)
        cls._set_from_env(env_vars, cls.Sessions, 'NEWYORK_SESSION_END_HOUR_UTC', int)
        cls._set_from_env(env_vars, cls.Sessions, 'TIMEZONE_FOR_DST_CHECK', str)

        # Validasi MarketData (Detektor & Umum)
        cls._set_from_env(env_vars, cls.MarketData, 'ATR_PERIOD', int)
        cls._set_from_env(env_vars, cls.MarketData, 'ATR_MULTIPLIER_FOR_TOLERANCE', Decimal)
        cls._set_from_env(env_vars, cls.MarketData, 'SR_LOOKBACK_CANDLES', int)
        cls._set_from_env(env_vars, cls.MarketData, 'SR_ZONE_ATR_MULTIPLIER', Decimal)
        cls._set_from_env(env_vars, cls.MarketData, 'MIN_SR_STRENGTH', int)
        cls._set_from_env(env_vars, cls.MarketData, 'ENABLE_OB_FVG_DETECTION', bool)
        cls._set_from_env(env_vars, cls.MarketData, 'FVG_MIN_ATR_MULTIPLIER', Decimal)
        cls._set_from_env(env_vars, cls.MarketData, 'OB_MIN_VOLUME_MULTIPLIER', Decimal)
        cls._set_from_env(env_vars, cls.MarketData, 'OB_FVG_MITIGATION_LOOKBACK_CANDLES', int)
        cls._set_from_env(env_vars, cls.MarketData, 'ENABLE_LIQUIDITY_DETECTION', bool)
        cls._set_from_env(env_vars, cls.MarketData, 'LIQUIDITY_CANDLE_RANGE_PERCENT', Decimal)
        cls._set_from_env(env_vars, cls.MarketData, 'ENABLE_FIBONACCI_DETECTION', bool)
        
        # FIBO_RETRACTION_LEVELS: Penanganan khusus untuk list
        fibo_retraction_levels_env = env_vars.get("FIBO_RETRACTION_LEVELS")
        if fibo_retraction_levels_env:
            try:
                cls.MarketData.FIBO_RETRACTION_LEVELS = [Decimal(r.strip()) for r in fibo_retraction_levels_env.split(',')]
            except Exception:
                raise ValueError("FIBO_RETRACTION_LEVELS must be a comma-separated list of valid decimal numbers (e.g., '0.236,0.382,0.5').")
        
        cls._set_from_env(env_vars, cls.MarketData, 'ENABLE_MARKET_STRUCTURE_DETECTION', bool)
        cls._set_from_env(env_vars, cls.MarketData, 'BOS_CHOCH_MIN_PIPS_CONFIRMATION', Decimal)
        cls._set_from_env(env_vars, cls.MarketData, 'ENABLE_SWING_DETECTION', bool)
        cls._set_from_env(env_vars, cls.MarketData, 'SWING_LOOKBACK_CANDLES', int)
        cls._set_from_env(env_vars, cls.MarketData, 'ENABLE_DIVERGENCE_DETECTION', bool)
        cls._set_from_env(env_vars, cls.MarketData, 'RSI_DIVERGENCE_PERIODS', int)
        cls._set_from_env(env_vars, cls.MarketData, 'MACD_DIVERGENCE_FAST_PERIOD', int)
        cls._set_from_env(env_vars, cls.MarketData, 'MACD_DIVERGENCE_SLOW_PERIOD', int)
        cls._set_from_env(env_vars, cls.MarketData, 'MACD_DIVERGENCE_SIGNAL_PERIOD', int)
        cls._set_from_env(env_vars, cls.MarketData, 'ENABLE_RSI_CALCULATION', bool)
        cls._set_from_env(env_vars, cls.MarketData, 'RSI_PERIOD', int)
        cls._set_from_env(env_vars, cls.MarketData, 'RSI_OVERBOUGHT_LEVEL', Decimal)
        cls._set_from_env(env_vars, cls.MarketData, 'RSI_OVERSOLD_LEVEL', Decimal)
        cls._set_from_env(env_vars, cls.MarketData, 'ENABLE_MACD_CALCULATION', bool)
        cls._set_from_env(env_vars, cls.MarketData, 'MACD_FAST_PERIOD', int)
        cls._set_from_env(env_vars, cls.MarketData, 'MACD_SLOW_PERIOD', int)
        cls._set_from_env(env_vars, cls.MarketData, 'MACD_SIGNAL_PERIOD', int)
        cls._set_from_env(env_vars, cls.MarketData, 'ENABLE_EMA_CROSS_DETECTION', bool)
        cls._set_from_env(env_vars, cls.MarketData, 'EMA_FAST_PERIOD', int)
        cls._set_from_env(env_vars, cls.MarketData, 'EMA_SLOW_PERIOD', int)
        cls._set_from_env(env_vars, cls.MarketData, 'ENABLE_MA_TREND_DETECTION', bool)
        
        # MA_TREND_PERIODS: Penanganan khusus untuk list
        ma_trend_periods_env = env_vars.get("MA_TREND_PERIODS")
        if ma_trend_periods_env:
            try:
                cls.MarketData.MA_TREND_PERIODS = [Decimal(p.strip()) for p in ma_trend_periods_env.split(',')]
            except Exception:
                raise ValueError("MA_TREND_PERIODS must be a comma-separated list of valid decimal numbers (e.g., '20,50,200').")
        
        # MA_TREND_TIMEFRAMES dan MA_TREND_TIMEFRAME_WEIGHTS: Perlu penanganan khusus jika dari string
        # Ini adalah contoh bagaimana Anda akan memparsing list/dict dari env jika disimpan sebagai string JSON/koma
        # Misalnya: MA_TREND_TIMEFRAMES='M15,M30,H1'
        ma_trend_timeframes_env = env_vars.get("MA_TREND_TIMEFRAMES")
        if ma_trend_timeframes_env:
            cls.MarketData.MA_TREND_TIMEFRAMES = [tf.strip() for tf in ma_trend_timeframes_env.split(',')]
        # Misalnya: MA_TREND_TIMEFRAME_WEIGHTS='{"M15":1,"M30":1}' (perlu json.loads)
        ma_trend_tf_weights_env = env_vars.get("MA_TREND_TIMEFRAME_WEIGHTS")
        if ma_trend_tf_weights_env:
            try:
                cls.MarketData.MA_TREND_TIMEFRAME_WEIGHTS = json.loads(ma_trend_tf_weights_env)
            except json.JSONDecodeError:
                raise ValueError("MA_TREND_TIMEFRAME_WEIGHTS must be a valid JSON string for a dictionary.")


        cls._set_from_env(env_vars, cls.MarketData, 'ENABLE_VOLUME_PROFILE_DETECTION', bool)
        cls._set_from_env(env_vars, cls.MarketData, 'ENABLE_PREVIOUS_HIGH_LOW_DETECTION', bool)
        cls._set_from_env(env_vars, cls.MarketData, 'CONFLUENCE_PROXIMITY_TOLERANCE_PIPS', Decimal)
        cls._set_from_env(env_vars, cls.MarketData, 'CONFLUENCE_SCORE_PER_LEVEL', int)
        cls._set_from_env(env_vars, cls.MarketData, 'OB_CONSOLIDATION_TOLERANCE_POINTS', Decimal)
        cls._set_from_env(env_vars, cls.MarketData, 'OB_SHOULDER_LENGTH', int)
        cls._set_from_env(env_vars, cls.MarketData, 'FVG_MIN_CANDLE_BODY_PERCENT_FOR_STRENGTH', Decimal)
        cls._set_from_env(env_vars, cls.MarketData, 'FVG_VOLUME_FACTOR_FOR_STRENGTH', Decimal)
        cls._set_from_env(env_vars, cls.MarketData, 'HISTORICAL_DATA_START_DATE_FULL', str)
        
        # Validasi AIAnalysts
        cls._set_from_env(env_vars, cls.AIAnalysts, 'OPENAI_MODEL_NAME', str)
        cls._set_from_env(env_vars, cls.AIAnalysts, 'MAX_FUNDAMENTAL_EVENTS_FOR_LLM', int)
        cls._set_from_env(env_vars, cls.AIAnalysts, 'MAX_FUNDAMENTAL_ARTICLES_FOR_LLM', int)
        cls._set_from_env(env_vars, cls.AIAnalysts, 'MAX_FUNDAMENTAL_EVENTS_FOR_LLM_DAYS_LOOKBACK', int)
        cls._set_from_env(env_vars, cls.AIAnalysts, 'MAX_FUNDAMENTAL_ARTICLES_FOR_LLM_DAYS_LOOKBACK', int)

        cls._set_from_env(env_vars, cls.AIAnalysts, 'OPENAI_TEMPERATURE', float)
        cls._set_from_env(env_vars, cls.AIAnalysts, 'AI_MIN_ANALYSTS_FOR_CONSENSUS', int)
        cls._set_from_env(env_vars, cls.AIAnalysts, 'FVG_MIN_DOLLARS', Decimal)
        cls._set_from_env(env_vars, cls.AIAnalysts, 'SWING_EXT_BARS', int)
        cls._set_from_env(env_vars, cls.AIAnalysts, 'RSI_PERIOD', int)
        cls._set_from_env(env_vars, cls.AIAnalysts, 'RSI_OVERBOUGHT', Decimal)
        cls._set_from_env(env_vars, cls.AIAnalysts, 'RSI_OVERSOLD', Decimal)
        cls._set_from_env(env_vars, cls.AIAnalysts, 'MACD_FAST_PERIOD', int)
        cls._set_from_env(env_vars, cls.AIAnalysts, 'MACD_SLOW_PERIOD', int)
        cls._set_from_env(env_vars, cls.AIAnalysts, 'MACD_SIGNAL_PERIOD', int)
        cls._set_from_env(env_vars, cls.AIAnalysts, 'TREND_MA_SHORT_PERIOD', int)
        cls._set_from_env(env_vars, cls.AIAnalysts, 'TREND_MA_MEDIUM_PERIOD', int)
        cls._set_from_env(env_vars, cls.AIAnalysts, 'TREND_MA_LONG_PERIOD', int)
        cls._set_from_env(env_vars, cls.AIAnalysts, 'MAX_FVG_DISPLAY', int)
        cls._set_from_env(env_vars, cls.AIAnalysts, 'MAX_OB_DISPLAY', int)
        cls._set_from_env(env_vars, cls.AIAnalysts, 'MAX_KEY_LEVEL_DISPLAY', int)
        cls._set_from_env(env_vars, cls.AIAnalysts, 'MAX_SR_DISPLAY', int)
        cls._set_from_env(env_vars, cls.AIAnalysts, 'MAX_LIQUIDITY_DISPLAY', int)
        cls._set_from_env(env_vars, cls.AIAnalysts, 'MAX_DIVERGENCE_DISPLAY', int)
        cls._set_from_env(env_vars, cls.AIAnalysts, 'CHART_BUFFER_PERCENTAGE', float)
        cls._set_from_env(env_vars, cls.AIAnalysts, 'CHART_MIN_STRENGTH_SCORE', int)
        cls._set_from_env(env_vars, cls.AIAnalysts, 'CHART_INCLUDE_KEY_LEVELS_ONLY', bool)
        cls._set_from_env(env_vars, cls.AIAnalysts, 'CHART_MAX_FVGS_TO_PLOT', int)
        cls._set_from_env(env_vars, cls.AIAnalysts, 'CHART_MAX_SR_TO_PLOT', int)
        cls._set_from_env(env_vars, cls.AIAnalysts, 'CHART_MAX_OB_TO_PLOT', int)
        cls._set_from_env(env_vars, cls.AIAnalysts, 'CHART_MAX_MS_TO_PLOT', int)
        cls._set_from_env(env_vars, cls.AIAnalysts, 'CHART_MAX_LIQ_TO_PLOT', int)
        cls._set_from_env(env_vars, cls.AIAnalysts, 'CHART_MAX_FIB_TO_PLOT', int)
        cls._set_from_env(env_vars, cls.AIAnalysts, 'CHART_MAX_FIB_SETS_TO_PLOT', int)

        # Validasi RuleBasedStrategy
        cls._set_from_env(env_vars, cls.RuleBasedStrategy, 'DEFAULT_SL_PIPS', Decimal)
        cls._set_from_env(env_vars, cls.RuleBasedStrategy, 'TP1_PIPS', Decimal)
        cls._set_from_env(env_vars, cls.RuleBasedStrategy, 'TP2_PIPS', Decimal)
        cls._set_from_env(env_vars, cls.RuleBasedStrategy, 'TP3_PIPS', Decimal)
        cls._set_from_env(env_vars, cls.RuleBasedStrategy, 'RULE_SR_TOLERANCE_POINTS', Decimal)
        cls._set_from_env(env_vars, cls.RuleBasedStrategy, 'RULE_EQUAL_LEVEL_TOLERANCE_POINTS', Decimal)
        cls._set_from_env(env_vars, cls.RuleBasedStrategy, 'RULE_OB_CONSOLIDATION_TOLERANCE_POINTS', Decimal)
        cls._set_from_env(env_vars, cls.RuleBasedStrategy, 'DEFAULT_SL_TP_RATIO', Decimal) # DEFAULT_SL_TP_RATIO from .env file (not RULE_DEFAULT_TP_FACTOR)
        cls._set_from_env(env_vars, cls.RuleBasedStrategy, 'EMA_SHORT_PERIOD', int)
        cls._set_from_env(env_vars, cls.RuleBasedStrategy, 'EMA_LONG_PERIOD', int)
        cls._set_from_env(env_vars, cls.RuleBasedStrategy, 'LOOKBACK_CANDLES_LTF', int)
        cls._set_from_env(env_vars, cls.RuleBasedStrategy, 'LOOKBACK_CANDLES_HTF', int)
        cls._set_from_env(env_vars, cls.RuleBasedStrategy, 'CANDLE_BODY_MIN_RATIO', Decimal)
        cls._set_from_env(env_vars, cls.RuleBasedStrategy, 'CANDLE_MIN_SIZE_PIPS', Decimal)
        cls._set_from_env(env_vars, cls.RuleBasedStrategy, 'STRUCTURE_OFFSET_PIPS', Decimal)
        cls._set_from_env(env_vars, cls.RuleBasedStrategy, 'SR_STRENGTH_RETEST_WINDOW_CANDLES', int)
        cls._set_from_env(env_vars, cls.RuleBasedStrategy, 'SR_STRENGTH_BREAK_TOLERANCE_MULTIPLIER', Decimal)
        cls._set_from_env(env_vars, cls.RuleBasedStrategy, 'OB_MIN_IMPULSIVE_CANDLE_BODY_PERCENT', Decimal)
        cls._set_from_env(env_vars, cls.RuleBasedStrategy, 'OB_MIN_IMPULSIVE_MOVE_MULTIPLIER', Decimal)
        cls._set_from_env(env_vars, cls.RuleBasedStrategy, 'OB_VOLUME_FACTOR_MULTIPLIER', Decimal)
        cls._set_from_env(env_vars, cls.RuleBasedStrategy, 'FVG_MIN_CANDLE_BODY_PERCENT_FOR_STRENGTH', Decimal)
        cls._set_from_env(env_vars, cls.RuleBasedStrategy, 'FVG_VOLUME_FACTOR_FOR_STRENGTH', Decimal)
        cls._set_from_env(env_vars, cls.RuleBasedStrategy, 'SD_MIN_IMPULSIVE_MOVE_ATR_MULTIPLIER', Decimal)
        cls._set_from_env(env_vars, cls.RuleBasedStrategy, 'DIVERGENCE_PRICE_TOLERANCE_ATR_MULTIPLIER', Decimal)
        cls._set_from_env(env_vars, cls.RuleBasedStrategy, 'MS_BREAK_ATR_MULTIPLIER', Decimal)
        
        # SIGNAL_RULES: Harus dikonversi list of dicts. Jika disimpan sebagai JSON string.
        signal_rules_env = env_vars.get("SIGNAL_RULES_JSON") # Misal: store sebagai JSON string
        if signal_rules_env:
            try:
                cls.RuleBasedStrategy.SIGNAL_RULES = json.loads(signal_rules_env)
            except json.JSONDecodeError:
                raise ValueError("SIGNAL_RULES_JSON must be a valid JSON string for a list of dictionaries.")

        # Validasi Telegram
        cls._set_from_env(env_vars, cls.Telegram, 'NOTIFICATION_COOLDOWN_SECONDS', int)
        cls._set_from_env(env_vars, cls.Telegram, 'MAX_EVENTS_TO_NOTIFY', int)
        cls._set_from_env(env_vars, cls.Telegram, 'MAX_ARTICLES_TO_NOTIFY', int)
        cls._set_from_env(env_vars, cls.Telegram, 'NOTIF_MAX_LEVELS_PER_TYPE_PER_TF', int)
        cls._set_from_env(env_vars, cls.Telegram, 'NOTIF_MAX_FVG_PER_TF', int)
        cls._set_from_env(env_vars, cls.Telegram, 'NOTIF_MAX_OB_PER_TF', int)
        cls._set_from_env(env_vars, cls.Telegram, 'NOTIF_MAX_KEY_LEVELS_PER_TF', int)
        cls._set_from_env(env_vars, cls.Telegram, 'NOTIF_MAX_RESISTANCE_PER_TF', int)
        cls._set_from_env(env_vars, cls.Telegram, 'NOTIF_MAX_SUPPORT_PER_TF', int)
        cls._set_from_env(env_vars, cls.Telegram, 'NOTIF_MAX_FIBO_PER_TF', int)
        cls._set_from_env(env_vars, cls.Telegram, 'NOTIF_MAX_SWING_PER_TF', int)
        cls._set_from_env(env_vars, cls.Telegram, 'SEND_SIGNAL_NOTIFICATIONS', bool)
        cls._set_from_env(env_vars, cls.Telegram, 'SEND_TRADE_NOTIFICATIONS', bool)
        cls._set_from_env(env_vars, cls.Telegram, 'SEND_ACCOUNT_NOTIFICATIONS', bool)
        cls._set_from_env(env_vars, cls.Telegram, 'SEND_DAILY_SUMMARY', bool)
        cls._set_from_env(env_vars, cls.Telegram, 'SEND_ERROR_NOTIFICATIONS', bool)
        cls._set_from_env(env_vars, cls.Telegram, 'SEND_APP_STATUS_NOTIFICATIONS', bool)
        cls._set_from_env(env_vars, cls.Telegram, 'SEND_FUNDAMENTAL_NOTIFICATIONS', bool)
        cls._set_from_env(env_vars, cls.Telegram, 'SEND_INDIVIDUAL_ANALYST_SIGNALS', bool)
        cls._set_from_env(env_vars, cls.Telegram, 'MIN_IMPACT_NOTIF_LEVEL', str)
        if cls.Telegram.MIN_IMPACT_NOTIF_LEVEL.lower() not in ["low", "medium", "high"]:
            raise ValueError("Telegram.MIN_IMPACT_NOTIF_LEVEL must be 'Low', 'Medium', or 'High'.")

        # Validasi System
        cls._set_from_env(env_vars, cls.System, 'MAX_RETRIES', int)
        cls._set_from_env(env_vars, cls.System, 'RETRY_DELAY_SECONDS', Decimal)
        cls._set_from_env(env_vars, cls.System, 'DATABASE_BATCH_SIZE', int)
        cls._set_from_env(env_vars, cls.System, 'LOG_LEVEL', str)
        cls._set_from_env(env_vars, cls.System, 'LOG_FILE', str)

        # Validasi Monitoring
        cls._set_from_env(env_vars, cls.Monitoring, 'DETECTOR_ZERO_DETECTIONS_THRESHOLD', int)
        cls._set_from_env(env_vars, cls.Monitoring, 'DETECTOR_HEALTH_CHECK_INTERVAL_SECONDS', int)

        # Validasi khusus untuk Scheduler.UPDATE_INTERVALS (nilai-nilai float) dan ENABLED_LOOPS (boolean)
        # Ini adalah bagian yang paling rentan terhadap kesalahan penamaan variabel lingkungan.
        # Kita akan iterasi melalui semua atribut di Scheduler.UPDATE_INTERVALS dan ENABLED_LOOPS
        # dan secara eksplisit mencoba membacanya dari env.

        # UPDATE_INTERVALS
        for attr_name in list(cls.Scheduler.UPDATE_INTERVALS.keys()):
            env_var_name = attr_name.upper() # Nama variabel lingkungan adalah UPPERCASE dari nama atribut
            env_val = env_vars.get(env_var_name)
            if env_val is not None:
                try:
                    cls.Scheduler.UPDATE_INTERVALS[attr_name] = float(env_val)
                except ValueError:
                    raise ValueError(
                        f"Scheduler.UPDATE_INTERVALS['{attr_name}'] from environment ('{env_var_name}') "
                        f"is not a valid float: '{env_val}'. Check .env file."
                    )

        # ENABLED_LOOPS
        for attr_name in list(cls.Scheduler.ENABLED_LOOPS.keys()):
            env_var_name = f"ENABLE_{attr_name.upper()}" # Nama variabel lingkungan adalah ENABLE_UPPERCASE_DARI_NAMA_ATRIBUT
            env_val = env_vars.get(env_var_name)
            if env_val is not None:
                if env_val.lower() not in ['true', 'false']:
                    raise ValueError(
                        f"Scheduler.ENABLED_LOOPS['{attr_name}'] from environment ('{env_var_name}') "
                        f"must be 'true' or 'false': '{env_val}'. Check .env file."
                    )
                cls.Scheduler.ENABLED_LOOPS[attr_name] = env_val.lower() == 'true'

        # Konfigurasi `logging` berdasarkan LOG_LEVEL dan LOG_FILE dari System
        logging_level_str = getattr(cls.System, 'LOG_LEVEL', 'DEBUG').upper()
        log_level_map = {
            'CRITICAL': logging.CRITICAL, 'ERROR': logging.ERROR,
            'WARNING': logging.WARNING, 'INFO': logging.INFO,
            'DEBUG': logging.DEBUG, 'NOTSET': logging.NOTSET
        }
        logging_level = log_level_map.get(logging_level_str, logging.DEBUG)
        
        # Atur root logger
        logging.basicConfig(level=logging_level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                            handlers=[logging.StreamHandler(sys.stdout)])
        
        # Tambahkan file handler jika LOG_FILE disetel
        log_file_path = getattr(cls.System, 'LOG_FILE', None)
        if log_file_path:
            file_handler = logging.handlers.RotatingFileHandler(
                log_file_path, maxBytes=10*1024*1024, backupCount=5, encoding='utf-8'
            )
            file_handler.setLevel(logging_level)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            file_handler.setFormatter(formatter)
            logging.getLogger().addHandler(file_handler) # Tambahkan ke root logger
            logger.info(f"Logging output juga akan ditulis ke: {log_file_path}")

        # Atur level logging untuk pustaka yang "berisik"
        logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
        logging.getLogger('sqlalchemy.pool').setLevel(logging.WARNING)
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logging.getLogger('requests').setLevel(logging.WARNING)


# Instansiasi objek Config dan panggil validasi saat modul dimuat.
# Objek 'config' ini yang akan diimpor oleh modul lain.
config = Config()
config.validate_config()
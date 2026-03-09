# aggressive_signal_generator.py

import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal, getcontext
import pandas as pd
import numpy as np
import os
import sys

# Import config (sebagai modul, bukan instance)
import config # Pastikan ini diimpor
import database_manager
import utils
import market_data_processor

logger = logging.getLogger(__name__)
# UBAH LEVEL LOGGING UNTUK DEBUGGING
logger.setLevel(logging.DEBUG)

getcontext().prec = 10

class AggressiveSignalGenerator:
    def __init__(self):
        logger.info("AggressiveSignalGenerator initialized. Focused purely on market signal identification.")

        # Inisialisasi parameter dari config.Config
        # Parameter terkait SL/TP masih ada karena ini bagian dari 'sugesti' sinyal
        self.min_sl_pips = config.Config.Trading.MIN_SL_PIPS
        self.min_tp_pips = config.Config.Trading.MIN_TP_PIPS
        self.pip_unit_in_dollar = config.Config.PIP_UNIT_IN_DOLLAR # Nilai 1 pip dalam USD (misal 0.01)
        self.pip_size = config.Config.PIP_SIZE                     # Ukuran 1 pip dalam unit harga (misal 0.01)
        self.trading_symbol_point_value = config.Config.TRADING_SYMBOL_POINT_VALUE # Mengambil dari config

        # Parameter detektor dan indikator dari config.Config.MarketData
        self.atr_period = config.Config.MarketData.ATR_PERIOD
        self.atr_multiplier_for_tolerance = config.Config.MarketData.ATR_MULTIPLIER_FOR_TOLERANCE

        self.enable_sr_detection = config.Config.MarketData.ENABLE_SR_DETECTION
        self.sr_lookback_candles = config.Config.MarketData.SR_LOOKBACK_CANDLES
        self.sr_zone_atr_multiplier = config.Config.MarketData.SR_ZONE_ATR_MULTIPLIER
        self.min_sr_strength = config.Config.MarketData.MIN_SR_STRENGTH

        self.enable_ob_fvg_detection = config.Config.MarketData.ENABLE_OB_FVG_DETECTION
        self.fvg_min_atr_multiplier = config.Config.MarketData.FVG_MIN_ATR_MULTIPLIER
        self.ob_min_volume_multiplier = config.Config.MarketData.OB_MIN_VOLUME_MULTIPLIER
        self.ob_fvg_mitigation_lookback_candles = config.Config.MarketData.OB_FVG_MITIGATION_LOOKBACK_CANDLES

        self.enable_liquidity_detection = config.Config.MarketData.ENABLE_LIQUIDITY_DETECTION
        self.liquidity_candle_range_percent = config.Config.MarketData.LIQUIDITY_CANDLE_RANGE_PERCENT

        self.enable_fibonacci_detection = config.Config.MarketData.ENABLE_FIBONACCI_DETECTION
        self.fibo_retraction_levels = config.Config.MarketData.FIBO_RETRACTION_LEVELS

        self.enable_market_structure_detection = config.Config.MarketData.ENABLE_MARKET_STRUCTURE_DETECTION
        self.bos_choch_min_pips_confirmation = config.Config.MarketData.BOS_CHOCH_MIN_PIPS_CONFIRMATION

        self.enable_swing_detection = config.Config.MarketData.ENABLE_SWING_DETECTION
        self.swing_lookback_candles = config.Config.MarketData.SWING_LOOKBACK_CANDLES

        self.enable_divergence_detection = config.Config.MarketData.ENABLE_DIVERGENCE_DETECTION
        
        rsi_divergence_periods_from_config = config.Config.MarketData.RSI_DIVERGENCE_PERIODS
        if isinstance(rsi_divergence_periods_from_config, int):
            self.rsi_divergence_periods = [rsi_divergence_periods_from_config]
        else:
            self.rsi_divergence_periods = rsi_divergence_periods_from_config

        self.macd_divergence_fast_period = config.Config.MarketData.MACD_DIVERGENCE_FAST_PERIOD
        self.macd_divergence_slow_period = config.Config.MarketData.MACD_DIVERGENCE_SLOW_PERIOD
        self.macd_divergence_signal_period = config.Config.MarketData.MACD_DIVERGENCE_SIGNAL_PERIOD

        self.enable_rsi_calculation = config.Config.MarketData.ENABLE_RSI_CALCULATION
        self.rsi_period = config.Config.MarketData.RSI_PERIOD
        self.rsi_overbought_level = config.Config.MarketData.RSI_OVERBOUGHT_LEVEL
        self.rsi_oversold_level = config.Config.MarketData.RSI_OVERSOLD_LEVEL

        self.enable_macd_calculation = config.Config.MarketData.ENABLE_MACD_CALCULATION
        self.macd_fast_period = config.Config.MarketData.MACD_FAST_PERIOD
        self.macd_slow_period = config.Config.MarketData.MACD_SLOW_PERIOD
        self.macd_signal_period = config.Config.MarketData.MACD_SIGNAL_PERIOD

        self.enable_ema_cross_detection = config.Config.MarketData.ENABLE_EMA_CROSS_DETECTION
        self.ema_fast_period = config.Config.MarketData.EMA_FAST_PERIOD
        self.ema_slow_period = config.Config.MarketData.EMA_SLOW_PERIOD

        self.enable_ma_trend_detection = config.Config.MarketData.ENABLE_MA_TREND_DETECTION
        self.ma_trend_periods = config.Config.MarketData.MA_TREND_PERIODS
        self.ma_trend_timeframes = config.Config.MarketData.MA_TREND_TIMEFRAMES
        self.ma_trend_timeframe_weights = config.Config.MarketData.MA_TREND_TIMEFRAME_WEIGHTS

        self.enable_volume_profile_detection = config.Config.MarketData.ENABLE_VOLUME_PROFILE_DETECTION
        self.enable_previous_high_low_detection = config.Config.MarketData.ENABLE_PREVIOUS_HIGH_LOW_DETECTION

        self.confluence_proximity_tolerance_pips = config.Config.MarketData.CONFLUENCE_PROXIMITY_TOLERANCE_PIPS
        self.confluence_score_per_level = config.Config.MarketData.CONFLUENCE_SCORE_PER_LEVEL

        self.ob_consolidation_tolerance_points = config.Config.MarketData.OB_CONSOLIDATION_TOLERANCE_POINTS
        self.ob_shoulder_length = config.Config.MarketData.OB_SHOULDER_LENGTH

        self.fvg_min_candle_body_percent_for_strength = config.Config.MarketData.FVG_MIN_CANDLE_BODY_PERCENT_FOR_STRENGTH
        self.fvg_volume_factor_for_strength = config.Config.MarketData.FVG_VOLUME_FACTOR_FOR_STRENGTH

        self.tp1_rr_ratio = Decimal('1.0')
        self.tp2_rr_ratio = Decimal('2.0')
        self.tp3_rr_ratio = Decimal('3.0')

        self.default_sl_pips = config.Config.RuleBasedStrategy.DEFAULT_SL_PIPS
        self.tp1_pips = config.Config.RuleBasedStrategy.TP1_PIPS
        self.tp2_pips = config.Config.RuleBasedStrategy.TP2_PIPS
        self.tp3_pips = config.Config.RuleBasedStrategy.TP3_PIPS
        
        # MODIFIKASI: Menggunakan getattr untuk kompatibilitas mundur
        self.price_confirmation_threshold_pips = getattr(
            config.Config.RuleBasedStrategy,
            'PRICE_CONFIRMATION_THRESHOLD_PIPS',
            Decimal('5.0') # Nilai default yang aman jika tidak ditemukan di config.py
        )
        logger.debug(f"AGGR_SIGNAL: PRICE_CONFIRMATION_THRESHOLD_PIPS diatur ke: {float(self.price_confirmation_threshold_pips)}")
        self.confirmation_candle_lookback = 1 # Hanya cek candle terakhir untuk konfirmasi


        self.momentum_lookback = 3
        self.recent_divergence_window_hours = 4

        self.condition_weights = {
            # Bullish Conditions
            "Bullish Rejection + Breakout": Decimal('2.5'),
            "Momentum Bullish (3 candle lalu)": Decimal('1.0'), 
            "EMA (50/200) Bullish Crossover": Decimal('2.0'),   
            "RSI (14) Oversold (30.00)": Decimal('0.5'),
            "Divergensi Bullish (RSI) terbaru.": Decimal('3.0'), 
            "Divergensi Bullish (MACD) terbaru.": Decimal('3.5'), 
            "Harga di Bullish OB": Decimal('2.5'), 
            "Harga di Demand Zone": Decimal('2.5'), 
            "FVG Bullish Belum Terisi": Decimal('1.5'), 
            "BoS Bullish Terbaru": Decimal('2.0'), 
            "ChoCh Bullish Terbaru": Decimal('2.5'), 
            "Previous Low Broken (Bullish)": Decimal('1.5'), 
            "Harga di Fibonacci Level Bullish": Decimal('1.0'), 
            "Harga menyapu Equal Lows (Bullish)": Decimal('3.0'),
            "Harga di Bullish OB (Terkonfirmasi)": Decimal('3.5'),
            "Harga di Demand Zone (Terkonfirmasi)": Decimal('3.5'),
            "FVG Bullish Belum Terisi (Terkonfirmasi)": Decimal('2.5'),

            # Bearish Conditions
            "Bearish Rejection + Breakdown": Decimal('2.5'),
            "Momentum Bearish (3 candle lalu)": Decimal('1.0'), 
            "EMA (50/200) Bearish Crossover": Decimal('2.0'),   
            "RSI (14) Overbought (70.00)": Decimal('0.5'),
            "Divergensi Bearish (RSI) terbaru.": Decimal('3.0'), 
            "Divergensi Bearish (MACD) terbaru.": Decimal('3.5'), 
            "Harga di Bearish OB": Decimal('2.5'), 
            "Harga di Supply Zone": Decimal('2.5'), 
            "FVG Bearish Belum Terisi": Decimal('1.5'), 
            "BoS Bearish Terbaru": Decimal('2.0'), 
            "ChoCh Bearish Terbaru": Decimal('2.5'), 
            "Previous High Broken (Bearish)": Decimal('1.5'), 
            "Harga di Fibonacci Level Bearish": Decimal('1.0'),
            "Harga menyapu Equal Highs (Bearish)": Decimal('3.0'),
            "Harga di Bearish OB (Terkonfirmasi)": Decimal('3.5'),
            "Harga di Supply Zone (Terkonfirmasi)": Decimal('3.5'),
            "FVG Bearish Belum Terisi (Terkonfirmasi)": Decimal('2.5')
        }

    def _prepare_df_for_mdp_calc_float(self, candles_df: pd.DataFrame, min_candles_for_indicators: int) -> pd.DataFrame:
        logger.debug("AGGR_SIGNAL: _prepare_df_for_mdp_calc_float dipanggil.")
        if candles_df.empty:
            logger.debug("AGGR_SIGNAL: _prepare_df_for_mdp_calc_float: Input candles_df kosong.")
            return pd.DataFrame()

        df_for_mdp_calc_float = pd.DataFrame(index=candles_df.index)

        cols_to_convert_to_float = ['open', 'high', 'low', 'close', 'volume', 'real_volume', 'spread']
        for col in cols_to_convert_to_float:
            if col in candles_df.columns:
                df_for_mdp_calc_float[col] = candles_df[col].apply(utils.to_float_or_none)
            else:
                df_for_mdp_calc_float[col] = np.nan
                logger.debug(f"AGGR_SIGNAL: _prepare_df_for_mdp_calc_float: Kolom '{col}' tidak ada di candles_df, diisi dengan NaN.")

        df_for_mdp_calc_float.dropna(subset=['open', 'high', 'low', 'close'], inplace=True)
        
        return df_for_mdp_calc_float

    def _check_price_confirmation(self, candles_df: pd.DataFrame, current_candle_idx: int, level_price: Decimal, direction: str, tolerance_pips: Decimal) -> bool:
        """
        Memeriksa konfirmasi harga dari candle terakhir setelah menyentuh level kunci.
        Untuk BUY (Bullish): cari penolakan dari bawah atau penutupan kuat di atas.
        Untuk SELL (Bearish): cari penolakan dari atas atau penutupan kuat di bawah.
        """
        if len(candles_df) < current_candle_idx + 1:
            logger.debug(f"AGGR_SIGNAL: Konfirmasi harga: Tidak cukup lilin untuk candle saat ini ({len(candles_df)} vs {current_candle_idx+1}).")
            return False

        current_candle = candles_df.iloc[current_candle_idx]
        prev_candle = candles_df.iloc[current_candle_idx - 1] if current_candle_idx > 0 else None

        current_open = utils.to_decimal_or_none(current_candle['open'])
        current_high = utils.to_decimal_or_none(current_candle['high'])
        current_low = utils.to_decimal_or_none(current_candle['low'])
        current_close = utils.to_decimal_or_none(current_candle['close'])

        if any(x is None for x in [current_open, current_high, current_low, current_close]):
            logger.debug("AGGR_SIGNAL: Konfirmasi harga: Data OHLC lilin saat ini tidak valid (None).")
            return False

        tolerance_value = tolerance_pips * self.pip_size # Konversi pips ke unit harga

        # Konfirmasi Bullish (misal: setelah menyentuh Demand Zone/Bullish OB/FVG)
        # Harga bergerak di bawah level, lalu ditolak dan ditutup bullish.
        if direction == "BUY":
            # Opsi 1: Pin Bar / Hammer (penolakan dari bawah level)
            if (current_low < level_price - tolerance_value and # Low menembus di bawah level
                current_close > current_open and                # Lilin bullish
                current_close >= level_price - tolerance_value and # Ditutup kembali di atas atau di dalam toleransi level
                (current_high - current_close) < (current_close - current_low) # Ekor bawah lebih panjang dari ekor atas/body atas
                ):
                logger.debug(f"AGGR_SIGNAL: Konfirmasi BUY: Pin Bar/Hammer di level {float(level_price):.5f}.")
                return True
            # Opsi 2: Bullish Engulfing (membungkus lilin sebelumnya, menunjukkan kekuatan)
            if prev_candle is not None:
                prev_open = utils.to_decimal_or_none(prev_candle['open'])
                prev_close = utils.to_decimal_or_none(prev_candle['close'])

                if (prev_open is not None and prev_close is not None and
                    current_open < prev_close and # Buka di bawah penutupan sebelumnya
                    current_close > prev_open and # Tutup di atas pembukaan sebelumnya
                    current_close > current_open): # Lilin bullish
                    logger.debug(f"AGGR_SIGNAL: Konfirmasi BUY: Bullish Engulfing di level {float(level_price):.5f}.")
                    return True
            # Opsi 3: Close di atas level setelah menyentuh (konfirmasi rebound)
            # Candle saat ini harus menyentuh level atau masuk ke dalamnya, lalu ditutup di atas.
            # Ini lebih kuat jika body besar atau ekor bawah panjang.
            if current_close > level_price + tolerance_value and current_low <= level_price + tolerance_value:
                logger.debug(f"AGGR_SIGNAL: Konfirmasi BUY: Penutupan kuat di atas level {float(level_price):.5f} setelah menyentuh.")
                return True

        # Konfirmasi Bearish (misal: setelah menyentuh Supply Zone/Bearish OB/FVG)
        # Harga bergerak di atas level, lalu ditolak dan ditutup bearish.
        elif direction == "SELL":
            # Opsi 1: Shooting Star / Inverted Hammer (penolakan dari atas level)
            if (current_high > level_price + tolerance_value and # High menembus di atas level
                current_close < current_open and                  # Lilin bearish
                current_close <= level_price + tolerance_value and # Ditutup kembali di bawah atau di dalam toleransi level
                (current_close - current_low) < (current_high - current_close) # Ekor atas lebih panjang dari ekor bawah/body bawah
                ):
                logger.debug(f"AGGR_SIGNAL: Konfirmasi SELL: Shooting Star/Inverted Hammer di level {float(level_price):.5f}.")
                return True
            # Opsi 2: Bearish Engulfing
            if prev_candle is not None:
                prev_open = utils.to_decimal_or_none(prev_candle['open'])
                prev_close = utils.to_decimal_or_none(prev_candle['close'])

                if (prev_open is not None and prev_close is not None and
                    current_open > prev_close and # Buka di atas penutupan sebelumnya
                    current_close < prev_open and # Tutup di bawah pembukaan sebelumnya
                    current_close < current_open): # Lilin bearish
                    logger.debug(f"AGGR_SIGNAL: Konfirmasi SELL: Bearish Engulfing di level {float(level_price):.5f}.")
                    return True
            # Opsi 3: Close di bawah level setelah menyentuh (konfirmasi rebound)
            # Candle saat ini harus menyentuh level atau masuk ke dalamnya, lalu ditutup di bawah.
            # Ini lebih kuat jika body besar atau ekor atas panjang.
            if current_close < level_price - tolerance_value and current_high >= level_price - tolerance_value:
                logger.debug(f"AGGR_SIGNAL: Konfirmasi SELL: Penutupan kuat di bawah level {float(level_price):.5f} setelah menyentuh.")
                return True
        return False


    def generate_signal(self, symbol: str, current_time: datetime, current_price: Decimal) -> dict:
        logger.debug(f"AGGR_SIGNAL: Menganalisis {symbol} pada {current_time.strftime('%Y-%m-%d %H:%M:%S')} (Harga: {float(current_price):.5f}).")

        # Inisialisasi sinyal default sebagai HOLD
        signal_data = {
            "action": "HOLD",
            "entry_price_suggestion": None,
            "stop_loss_suggestion": None,
            "take_profit_levels_suggestion": [],
            "strength": 0,
            "reason": [],
            "timestamp": current_time,
            "signal_type": "Aggressive",
            "confidence": "Low",
            "potential_direction": "Sideways",
            "tp2_suggestion": None,
            "tp3_suggestion": None
        }

        # Validasi self.trading_symbol_point_value
        if self.trading_symbol_point_value is None or self.trading_symbol_point_value == Decimal('0.0'):
            logger.error(f"AGGR_SIGNAL: Nilai POINT untuk {symbol} tidak tersedia atau 0. Tidak dapat melanjutkan generasi sinyal.")
            signal_data["reason"].append("Nilai POINT simbol tidak valid.")
            return signal_data

        timeframe = config.Config.Trading.DEFAULT_TIMEFRAME
        
        # Perhitungan min_candles_for_indicators yang lebih komprehensif
        min_candles_for_indicators = max(
            self.momentum_lookback + 1,
            self.ema_slow_period if self.enable_ema_cross_detection else 0,
            self.rsi_period if self.enable_rsi_calculation else 0,
            (self.macd_slow_period + self.macd_signal_period) if self.enable_macd_calculation else 0,
            self.atr_period + 5 if self.atr_period else 0,
            self.sr_lookback_candles if self.enable_sr_detection else 0,
            self.ob_fvg_mitigation_lookback_candles if self.enable_ob_fvg_detection else 0,
            self.swing_lookback_candles if self.enable_swing_detection else 0,
            float(max(self.rsi_divergence_periods)) if self.enable_divergence_detection and self.rsi_divergence_periods else 0,
            float((self.macd_divergence_slow_period + self.macd_divergence_signal_period)) if self.enable_divergence_detection else 0,
            float(max(self.ma_trend_periods)) if self.enable_ma_trend_detection and self.ma_trend_periods else 0,
            5 # Minimal candle untuk analisis dasar
        ) + 10 # Buffer tambahan
        logger.debug(f"AGGR_SIGNAL DEBUG: min_candles_for_indicators dihitung sebagai: {min_candles_for_indicators}")

        # Pengambilan Data Candle
        candles_data = database_manager.get_historical_candles_from_db(
            symbol=symbol,
            timeframe=timeframe,
            end_time_utc=current_time,
            limit=min_candles_for_indicators,
            order_asc=True
        )
        
        actual_candles_count = len(candles_data) if candles_data else 0
        logger.debug(f"AGGR_SIGNAL DEBUG: Jumlah candle {timeframe} yang diterima dari DB: {actual_candles_count}")

        if not candles_data or actual_candles_count < min_candles_for_indicators:
            logger.warning(f"AGGR_SIGNAL: TIDAK CUKUP CANDLE {timeframe} ({actual_candles_count}) untuk analisis indikator (butuh {min_candles_for_indicators}). Kembali ke HOLD.")
            signal_data["reason"].append(f"Tidak cukup data candle ({actual_candles_count} dari {min_candles_for_indicators})")
            return signal_data

        candles_df = pd.DataFrame(candles_data)
        candles_df['open_time_utc'] = pd.to_datetime(candles_df['open_time_utc'])
        candles_df.set_index('open_time_utc', inplace=True)
        candles_df.sort_index(inplace=True)
        
        # Rename kolom ke nama pendek standar
        candles_df = candles_df.rename(columns={
            'open_price': 'open', 'high_price': 'high', 'low_price': 'low',
            'close_price': 'close', 'tick_volume': 'volume', 'real_volume': 'real_volume', 'spread': 'spread'
        })

        initial_df_len = len(candles_df)
        candles_df.dropna(subset=['open', 'high', 'low', 'close'], inplace=True)
        if len(candles_df) < initial_df_len:
            logger.warning(f"AGGR_SIGNAL: Dihapus {initial_df_len - len(candles_df)} baris dengan NaN di kolom OHLC.")
        
        if candles_df.empty:
            logger.debug("AGGR_SIGNAL: DataFrame kosong setelah pembersihan NaN. Kembali ke HOLD.")
            signal_data["reason"].append("DataFrame kosong setelah pembersihan NaN.")
            return signal_data

        df_for_mdp_calc_float = self._prepare_df_for_mdp_calc_float(candles_df, min_candles_for_indicators)

        if df_for_mdp_calc_float.empty or len(df_for_mdp_calc_float) < min_candles_for_indicators:
            logger.warning(f"AGGR_SIGNAL: DataFrame kosong atau tidak cukup data setelah pembersihan NaN untuk MDP Calc. Kembali ke HOLD.")
            signal_data["reason"].append("DataFrame kosong/tidak cukup data setelah pembersihan NaN untuk MDP.")
            return signal_data


        current_price_dec = utils.to_decimal_or_none(current_price)
        if current_price_dec is None:
            logger.error(f"AGGR_SIGNAL: Harga saat ini ({current_price}) tidak valid. Melewatkan generasi sinyal.")
            signal_data["reason"].append("Harga saat ini tidak valid.")
            return signal_data

        # Hitung ATR saat ini (akan digunakan untuk toleransi dinamis)
        current_atr_series = market_data_processor._calculate_atr(df_for_mdp_calc_float, self.atr_period)
        current_atr_value = Decimal('0.0')
        if not current_atr_series.empty and pd.notna(current_atr_series.iloc[-1]):
            current_atr_value = utils.to_decimal_or_none(current_atr_series.iloc[-1])
        
        # Penentuan toleransi dinamis berdasarkan ATR, fallback jika ATR tidak valid
        atr_tolerance_points = Decimal('0.0')
        if current_atr_value > Decimal('0.0'):
            atr_tolerance_points = current_atr_value * self.atr_multiplier_for_tolerance
        else:
            atr_tolerance_points = config.Config.RuleBasedStrategy.RULE_SR_TOLERANCE_POINTS * self.trading_symbol_point_value
            logger.warning(f"AGGR_SIGNAL: ATR tidak valid atau nol ({current_atr_value}). Menggunakan toleransi default dari config.Config.RuleBasedStrategy.RULE_SR_TOLERANCE_POINTS: {float(atr_tolerance_points):.5f}.")


        last_candle_idx_pos = len(candles_df) - 1 # Index posisi dari candle terakhir
        last_candle = candles_df.iloc[last_candle_idx_pos]
        prev_candle = candles_df.iloc[last_candle_idx_pos - 1] if len(candles_df) >= 2 else None
        
        current_close_dec = last_candle['close']
        current_open_dec = last_candle['open']
        current_high_dec = last_candle['high']
        current_low_dec = last_candle['low']

        prev_close_dec = Decimal('0.0')
        prev_open_dec = Decimal('0.0')
        prev_high_dec = Decimal('0.0')
        prev_low_dec = Decimal('0.0')

        bullish_conditions_met = []
        bearish_conditions_met = []

        # --- DETEKSI SINYAL MURNI BERDASARKAN INDIKATOR & POLA ---

        # 1. Rejection & Breakout/Breakdown
        if prev_candle is not None:
           prev_close_dec = prev_candle['close'] if pd.notna(prev_candle['close']) else Decimal('0.0')
           prev_open_dec = prev_candle['open'] if pd.notna(prev_candle['open']) else Decimal('0.0')
           prev_high_dec = prev_candle['high'] if pd.notna(prev_candle['high']) else Decimal('0.0')
           prev_low_dec = prev_candle['low'] if pd.notna(prev_candle['low']) else Decimal('0.0')

           if current_open_dec < current_close_dec and \
              prev_close_dec > current_open_dec and \
              current_close_dec > prev_open_dec and \
              current_price_dec > prev_high_dec:
               bullish_conditions_met.append("Bullish Rejection + Breakout")
           if current_open_dec > current_close_dec and \
              prev_close_dec < current_open_dec and \
              current_close_dec < prev_open_dec and \
              current_price_dec < prev_low_dec:
               bearish_conditions_met.append("Bearish Rejection + Breakdown")

        # 2. Momentum
        if len(candles_df) > self.momentum_lookback:
            past_close_dec = candles_df['close'].iloc[-(self.momentum_lookback + 1)]
            if current_close_dec > past_close_dec:
                bullish_conditions_met.append(f"Momentum Bullish ({self.momentum_lookback} candle lalu)")
            elif current_close_dec < past_close_dec:
                bearish_conditions_met.append(f"Momentum Bearish ({self.momentum_lookback} candle lalu)")
            logger.debug(f"AGGR_SIGNAL DEBUG: Momentum check: Current Close={float(current_close_dec):.5f}, Past Close ({self.momentum_lookback} ago)={float(past_close_dec):.5f}")
        else:
            logger.debug(f"AGGR_SIGNAL: Tidak cukup data untuk analisis momentum ({len(candles_df)}).")

        # 3. EMA Cross
        if self.enable_ema_cross_detection and len(df_for_mdp_calc_float) >= max(self.ema_fast_period, self.ema_slow_period):
            try:
                ema_fast_series = market_data_processor._calculate_ema_internal(df_for_mdp_calc_float['close'], self.ema_fast_period)
                ema_slow_series = market_data_processor._calculate_ema_internal(df_for_mdp_calc_float['close'], self.ema_slow_period)

                if not ema_fast_series.empty and not ema_slow_series.empty and \
                   len(ema_fast_series) > 1 and len(ema_slow_series) > 1:
                    
                    current_ema_fast = utils.to_decimal_or_none(ema_fast_series.iloc[-1])
                    current_ema_slow = utils.to_decimal_or_none(ema_slow_series.iloc[-1])
                    prev_ema_fast = utils.to_decimal_or_none(ema_fast_series.iloc[-2])
                    prev_ema_slow = utils.to_decimal_or_none(ema_slow_series.iloc[-2])

                    if current_ema_fast is not None and current_ema_slow is not None and \
                       prev_ema_fast is not None and prev_ema_slow is not None:
                        if current_ema_fast > current_ema_slow and prev_ema_fast <= prev_ema_slow:
                            bullish_conditions_met.append(f"EMA ({self.ema_fast_period}/{self.ema_slow_period}) Bullish Crossover")
                        elif current_ema_fast < current_ema_slow and prev_ema_fast >= prev_ema_slow:
                            bearish_conditions_met.append(f"EMA ({self.ema_fast_period}/{self.ema_slow_period}) Bearish Crossover")
            except Exception as e:
                logger.warning(f"AGGR_SIGNAL: Error calculating EMA using market_data_processor: {e}")

        # 4. RSI (Overbought/Oversold)
        if self.enable_rsi_calculation and len(df_for_mdp_calc_float) >= self.rsi_period:
            try:
                rsi_series = market_data_processor._calculate_rsi(df_for_mdp_calc_float, self.rsi_period)
                if not rsi_series.empty and pd.notna(rsi_series.iloc[-1]):
                    current_rsi = utils.to_decimal_or_none(rsi_series.iloc[-1])
                    if current_rsi is not None:
                        logger.debug(f"AGGR_SIGNAL DEBUG: RSI Check: Current RSI={float(current_rsi):.2f}. Oversold={float(self.rsi_oversold_level):.2f}, Overbought={float(self.rsi_overbought_level):.2f}")
                        if current_rsi < self.rsi_oversold_level:
                            bullish_conditions_met.append(f"RSI ({self.rsi_period}) Oversold ({float(self.rsi_oversold_level):.2f})")
                        elif current_rsi > self.rsi_overbought_level:
                            bearish_conditions_met.append(f"RSI ({self.rsi_period}) Overbought ({float(self.rsi_overbought_level):.2f})")
                else:
                    logger.debug("AGGR_SIGNAL: RSI series kosong atau tidak cukup data.")
            except Exception as e:
                logger.warning(f"AGGR_SIGNAL: Error calculating RSI: {e}")

        # 5. Divergensi (Ambil dari DB, asumsikan sudah dihitung & disimpan oleh proses lain)
        if self.enable_divergence_detection:
            recent_divergences = database_manager.get_divergences(
                symbol=symbol,
                timeframe=timeframe,
                is_active=True,
                end_time_utc=current_time,
                start_time_utc=current_time - timedelta(hours=self.recent_divergence_window_hours),
                limit=5
            )
            logger.debug(f"AGGR_SIGNAL DEBUG: Jumlah divergensi terbaru ditemukan: {len(recent_divergences)}")
            if recent_divergences:
                for div in recent_divergences:
                    if (current_time - div.get('price_point_time_utc', datetime.min)).total_seconds() <= timedelta(hours=self.recent_divergence_window_hours).total_seconds():
                        if "Bullish" in div.get('divergence_type', '') and div.get('indicator_type', '') == "RSI":
                            bullish_conditions_met.append(f"Divergensi Bullish (RSI) terbaru.")
                        elif "Bullish" in div.get('divergence_type', '') and div.get('indicator_type', '') == "MACD":
                            bullish_conditions_met.append(f"Divergensi Bullish (MACD) terbaru.")
                        elif "Bearish" in div.get('divergence_type', '') and div.get('indicator_type', '') == "RSI":
                            bearish_conditions_met.append(f"Divergensi Bearish (RSI) terbaru.")
                        elif "Bearish" in div.get('divergence_type', '') and div.get('indicator_type', '') == "MACD":
                            bearish_conditions_met.append(f"Divergensi Bearish (MACD) terbaru.")
        
        # --- Detektor Tingkat Lanjut dari Database ---

        # Jendela waktu untuk mencari detektor yang relevan
        lookback_window_hours_for_levels = 6 # Misalnya, cari level/event dalam 6 jam terakhir
        lookback_window_start_time = current_time - timedelta(hours=lookback_window_hours_for_levels)

        # A. Order Blocks (OB)
        if self.enable_ob_fvg_detection:
            recent_obs = database_manager.get_order_blocks(
                symbol=symbol,
                timeframe=timeframe,
                is_mitigated=False, # Hanya yang belum dimitigasi
                end_time_utc=current_time,
                start_time_utc=lookback_window_start_time,
                limit=5
            )
            for ob in recent_obs:
                ob_top_dec = utils.to_decimal_or_none(ob['ob_top_price'])
                ob_bottom_dec = utils.to_decimal_or_none(ob['ob_bottom_price'])
                
                if ob_top_dec is None or ob_bottom_dec is None: continue

                # Cek apakah harga saat ini berada di dalam OB atau sangat dekat
                if ob['type'] == "Bullish":
                    if current_price_dec >= ob_bottom_dec - atr_tolerance_points and current_price_dec <= ob_top_dec + atr_tolerance_points:
                        bullish_conditions_met.append(f"Harga di Bullish OB")
                        # Cek konfirmasi harga untuk sinyal "sniper"
                        if self._check_price_confirmation(candles_df, last_candle_idx_pos, ob_bottom_dec, "BUY", self.price_confirmation_threshold_pips):
                            bullish_conditions_met.append(f"Harga di Bullish OB (Terkonfirmasi)")
                elif ob['type'] == "Bearish":
                    if current_price_dec <= ob_top_dec + atr_tolerance_points and current_price_dec >= ob_bottom_dec - atr_tolerance_points:
                        bearish_conditions_met.append(f"Harga di Bearish OB")
                        # Cek konfirmasi harga untuk sinyal "sniper"
                        if self._check_price_confirmation(candles_df, last_candle_idx_pos, ob_top_dec, "SELL", self.price_confirmation_threshold_pips):
                            bearish_conditions_met.append(f"Harga di Bearish OB (Terkonfirmasi)")


        # B. Fair Value Gaps (FVG)
        if self.enable_ob_fvg_detection:
            recent_fvgs = database_manager.get_fair_value_gaps(
                symbol=symbol,
                timeframe=timeframe,
                is_filled=False, # Hanya yang belum terisi
                end_time_utc=current_time,
                start_time_utc=lookback_window_start_time,
                limit=5
            )
            for fvg in recent_fvgs:
                fvg_top_dec = utils.to_decimal_or_none(fvg['fvg_top_price'])
                fvg_bottom_dec = utils.to_decimal_or_none(fvg['fvg_bottom_price'])
                
                if fvg_top_dec is None or fvg_bottom_dec is None: continue

                if fvg['type'] == "Bullish Imbalance":
                    if current_price_dec >= fvg_bottom_dec - atr_tolerance_points and current_price_dec <= fvg_top_dec + atr_tolerance_points:
                        bullish_conditions_met.append(f"FVG Bullish Belum Terisi")
                        if self._check_price_confirmation(candles_df, last_candle_idx_pos, fvg_bottom_dec, "BUY", self.price_confirmation_threshold_pips):
                            bullish_conditions_met.append(f"FVG Bullish Belum Terisi (Terkonfirmasi)")
                elif fvg['type'] == "Bearish Imbalance":
                    if current_price_dec <= fvg_top_dec + atr_tolerance_points and current_price_dec >= fvg_bottom_dec - atr_tolerance_points:
                        bearish_conditions_met.append(f"FVG Bearish Belum Terisi")
                        if self._check_price_confirmation(candles_df, last_candle_idx_pos, fvg_top_dec, "SELL", self.price_confirmation_threshold_pips):
                            bearish_conditions_met.append(f"FVG Bearish Belum Terisi (Terkonfirmasi)")

        # C. Supply/Demand Zones
        if self.enable_sr_detection: # S&D biasanya bagian dari SR
            recent_sds = database_manager.get_supply_demand_zones(
                symbol=symbol,
                timeframe=timeframe,
                is_mitigated=False, # Hanya yang belum dimitigasi
                end_time_utc=current_time,
                start_time_utc=lookback_window_start_time,
                limit=5
            )
            for sd in recent_sds:
                sd_top_dec = utils.to_decimal_or_none(sd['zone_top_price'])
                sd_bottom_dec = utils.to_decimal_or_none(sd['zone_bottom_price'])

                if sd_top_dec is None or sd_bottom_dec is None: continue

                if sd['zone_type'] == "Demand":
                    if current_price_dec >= sd_bottom_dec - atr_tolerance_points and current_price_dec <= sd_top_dec + atr_tolerance_points:
                        bullish_conditions_met.append(f"Harga di Demand Zone")
                        if self._check_price_confirmation(candles_df, last_candle_idx_pos, sd_bottom_dec, "BUY", self.price_confirmation_threshold_pips):
                            bullish_conditions_met.append(f"Harga di Demand Zone (Terkonfirmasi)")
                elif sd['zone_type'] == "Supply":
                    if current_price_dec <= sd_top_dec + atr_tolerance_points and current_price_dec >= sd_bottom_dec - atr_tolerance_points:
                        bearish_conditions_met.append(f"Harga di Supply Zone")
                        if self._check_price_confirmation(candles_df, last_candle_idx_pos, sd_top_dec, "SELL", self.price_confirmation_threshold_pips):
                            bearish_conditions_met.append(f"Harga di Supply Zone (Terkonfirmasi)")

        # D. Market Structure Events (BoS/ChoCh, Previous High/Low)
        if self.enable_market_structure_detection:
            # Hitung durasi jendela dalam menit sebagai float
            ms_event_window_minutes = float(self.bos_choch_min_pips_confirmation / self.pip_size) * 2

            recent_ms_events = database_manager.get_market_structure_events(
                symbol=symbol,
                timeframe=timeframe,
                event_type=['Break of Structure', 'Change of Character', 'Previous High Broken', 'Previous Low Broken'],
                end_time_utc=current_time,
                start_time_utc=current_time - timedelta(minutes=ms_event_window_minutes),
                limit=3
            )
            for event in recent_ms_events:
                if event['event_type'] == "Break of Structure":
                    if event['direction'] == "Bullish":
                        bullish_conditions_met.append("BoS Bullish Terbaru")
                    elif event['direction'] == "Bearish":
                        bearish_conditions_met.append("BoS Bearish Terbaru")
                elif event['event_type'] == "Change of Character":
                    if event['direction'] == "Bullish":
                        bullish_conditions_met.append("ChoCh Bullish Terbaru")
                    elif event['direction'] == "Bearish":
                        bearish_conditions_met.append("ChoCh Bearish Terbaru")
                elif event['event_type'] == "Previous High Broken":
                    bearish_conditions_met.append("Previous High Broken (Bearish)")
                elif event['event_type'] == "Previous Low Broken":
                    bullish_conditions_met.append("Previous Low Broken (Bullish)")

        # E. Fibonacci Levels
        if self.enable_fibonacci_detection:
            recent_fib_levels = database_manager.get_fibonacci_levels(
                symbol=symbol,
                timeframe=timeframe,
                is_active=True, # Hanya level aktif
                end_time_utc=current_time,
                start_time_utc=lookback_window_start_time, # Menggunakan start_time_utc
                limit=5
            )
            for fib_level in recent_fib_levels:
                fib_price_dec = utils.to_decimal_or_none(fib_level['price_level'])
                if fib_price_dec is None: continue
                
                # Cek apakah harga saat ini dekat dengan level Fibonacci
                if abs(current_price_dec - fib_price_dec) <= atr_tolerance_points:
                    if fib_level.get('retracement_direction') == 1: # Bullish retracement (uptrend leg, pullback)
                        bullish_conditions_met.append(f"Harga di Fibonacci Level Bullish")
                    elif fib_level.get('retracement_direction') == -1: # Bearish retracement (downtrend leg, pullback)
                        bearish_conditions_met.append(f"Harga di Fibonacci Level Bearish")

        # F. Liquidity Zones (NEW)
        if self.enable_liquidity_detection:
            recent_liq_zones = database_manager.get_liquidity_zones(
                symbol=symbol,
                timeframe=timeframe,
                is_tapped=False, # Hanya yang belum disapu
                end_time_utc=current_time,
                start_time_utc=lookback_window_start_time,
                limit=3
            )
            for liq_zone in recent_liq_zones:
                liq_price_dec = utils.to_decimal_or_none(liq_zone['price_level'])
                if liq_price_dec is None: continue

                # Deteksi "Liquidity Sweep" - harga menembus level likuiditas dan kemudian berbalik
                if liq_zone['zone_type'] == "Equal Lows": # Potensi sweep Equal Lows untuk BUY
                    # Cek apakah low candle saat ini menembus liq_price_dec
                    # DAN ada konfirmasi harga Bullish.
                    if current_low_dec < liq_price_dec - atr_tolerance_points and \
                       self._check_price_confirmation(candles_df, last_candle_idx_pos, liq_price_dec, "BUY", self.price_confirmation_threshold_pips):
                        bullish_conditions_met.append("Harga menyapu Equal Lows (Bullish)")
                elif liq_zone['zone_type'] == "Equal Highs": # Potensi sweep Equal Highs untuk SELL
                    # Cek apakah high candle saat ini menembus liq_price_dec
                    # DAN ada konfirmasi harga Bearish.
                    if current_high_dec > liq_price_dec + atr_tolerance_points and \
                       self._check_price_confirmation(candles_df, last_candle_idx_pos, liq_price_dec, "SELL", self.price_confirmation_threshold_pips):
                        bearish_conditions_met.append("Harga menyapu Equal Highs (Bearish)")

        # --- Penentuan Aksi Agresif Berdasarkan Kombinasi Sinyal (Dengan Pembobotan) ---
        total_bullish_strength = Decimal('0.0')
        for cond in bullish_conditions_met:
            total_bullish_strength += self.condition_weights.get(cond, Decimal('0.0'))

        total_bearish_strength = Decimal('0.0')
        for cond in bearish_conditions_met:
            total_bearish_strength += self.condition_weights.get(cond, Decimal('0.0'))

        action = "HOLD"
        confidence = "Low"
        reasoning_text = []
        potential_direction = "Sideways"
        signal_strength_final = Decimal('0.0')

        logger.debug(f"AGGR_SIGNAL DEBUG: Total Bullish Strength: {float(total_bullish_strength):.2f}, Bearish Strength: {float(total_bearish_strength):.2f}")
        logger.debug(f"AGGR_SIGNAL DEBUG: Bullish Conditions Met: {bullish_conditions_met}")
        logger.debug(f"AGGR_SIGNAL DEBUG: Bearish Conditions Met: {bearish_conditions_met}")

        if total_bullish_strength > total_bearish_strength and total_bullish_strength > Decimal('0.0'):
            action = "BUY"
            potential_direction = "Bullish"
            reasoning_text.append("⬆️ Sinyal BUY Agresif:")
            reasoning_text.extend(bullish_conditions_met)
            signal_strength_final = total_bullish_strength

            if total_bullish_strength >= Decimal('7.0'):
                confidence = "High"
            elif total_bullish_strength >= Decimal('4.0'):
                confidence = "Medium"
            else:
                confidence = "Low"

        elif total_bearish_strength > total_bullish_strength and total_bearish_strength > Decimal('0.0'):
            action = "SELL"
            potential_direction = "Bearish"
            reasoning_text.append("⬇️ Sinyal SELL Agresif:")
            reasoning_text.extend(bearish_conditions_met)
            signal_strength_final = total_bearish_strength

            if total_bearish_strength >= Decimal('7.0'):
                confidence = "High"
            elif total_bearish_strength >= Decimal('4.0'):
                confidence = "Medium"
            else:
                confidence = "Low"
        else:
            action = "HOLD"
            confidence = "Low"
            reasoning_text.append("↔️ Kondisi sinyal tidak cukup kuat atau seimbang.")
            potential_direction = "Sideways"
            signal_strength_final = Decimal('0.0')

        # --- Filter Kondisi Pasar Tambahan (Tren Multi-Timeframe) ---
        # Ini akan dilakukan HANYA JIKA sinyal awal BUKAN HOLD
        if self.enable_ma_trend_detection and action != "HOLD":
            higher_timeframe_for_trend = "H1" # Ambil H1 sebagai higher TF untuk tren, sesuai config
            # market_data_processor.get_trend_context_from_ma hanya menerima symbol dan timeframe.
            # Jadi, kita tidak perlu mempassing candles_df lagi.
            trend_context = market_data_processor.get_trend_context_from_ma(symbol, higher_timeframe_for_trend)

            if trend_context and trend_context['overall_trend'] != "Undefined" and trend_context['overall_trend'] != "Error":
                if (action == "BUY" and trend_context['overall_trend'] == "Bearish") or \
                   (action == "SELL" and trend_context['overall_trend'] == "Bullish"):
                    reasoning_text.insert(0, f"Tren {higher_timeframe_for_trend} ({trend_context['overall_trend']}) berlawanan, meniadakan sinyal.")
                    action = "HOLD"
                    confidence = "Low"
                    potential_direction = "Sideways"
                elif trend_context['overall_trend'] == "Ranging":
                    reasoning_text.append(f"Tren {higher_timeframe_for_trend} Ranging, mengurangi kepercayaan sinyal.")
                    if confidence == "High": confidence = "Medium"
                    elif confidence == "Medium": confidence = "Low"
            else:
                logger.warning(f"AGGR_SIGNAL: Gagal mendapatkan konteks tren dari {higher_timeframe_for_trend}. Filter tren dilewati.")

        # Hitung SL/TP (dari config.RuleBasedStrategy)
        entry_price_sugg_dec = utils.to_decimal_or_none(current_price_dec)

        if entry_price_sugg_dec is None:
            logger.error("AGGR_SIGNAL: entry_price_sugg tidak valid (None). Tidak dapat menghitung SL/TP.")
            signal_data["reason"].append("Gagal menghitung SL/TP karena harga entry tidak valid.")
            signal_data["action"] = "HOLD"
            signal_data["confidence"] = "Low"
            signal_data["potential_direction"] = "Sideways"
            return signal_data

        sl_sugg = None
        tp1_sugg = None
        tp2_sugg = None
        tp3_sugg = None

        # --- Perhitungan SL/TP Adaptif ---
        sl_search_window_hours = 6
        sl_search_window_start_time = current_time - timedelta(hours=sl_search_window_hours)

        if action == "BUY":
            # Opsi 1: Cari Swing Low Terdekat
            recent_swing_lows = database_manager.get_market_structure_events(
                symbol=symbol,
                timeframe=timeframe,
                event_type='Swing Low',
                end_time_utc=current_time,
                start_time_utc=sl_search_window_start_time,
                limit=2
            )
            # Filter swing low yang valid (lebih rendah dari harga entri)
            valid_swing_lows = [utils.to_decimal_or_none(s['price_level']) for s in recent_swing_lows if utils.to_decimal_or_none(s['price_level']) is not None and utils.to_decimal_or_none(s['price_level']) < entry_price_sugg_dec]
            if valid_swing_lows:
                best_sl_level_dec = min(valid_swing_lows)
                sl_sugg = best_sl_level_dec - atr_tolerance_points # Sedikit di bawah swing low
                logger.debug(f"AGGR_SIGNAL: SL adaptif dari Swing Low: {float(sl_sugg):.5f}")

            # Opsi 2: Cari Order Block Bullish terdekat yang belum dimitigasi
            if (sl_sugg is None or sl_sugg >= entry_price_sugg_dec) and self.enable_ob_fvg_detection:
                recent_bullish_obs = database_manager.get_order_blocks(
                    symbol=symbol, timeframe=timeframe, type="Bullish", is_mitigated=False,
                    end_time_utc=current_time, start_time_utc=sl_search_window_start_time, limit=1
                )
                if recent_bullish_obs:
                    ob_bottom = utils.to_decimal_or_none(recent_bullish_obs[0]['ob_bottom_price'])
                    if ob_bottom is not None:
                        temp_sl = ob_bottom - atr_tolerance_points
                        if temp_sl < entry_price_sugg_dec:
                           sl_sugg = temp_sl
                           logger.debug(f"AGGR_SIGNAL: SL adaptif dari Bullish OB: {float(sl_sugg):.5f}")

            # Opsi 3: Cari Demand Zone terdekat yang belum dimitigasi
            if (sl_sugg is None or sl_sugg >= entry_price_sugg_dec) and self.enable_sr_detection:
                recent_demand_zones = database_manager.get_supply_demand_zones(
                    symbol=symbol, timeframe=timeframe, zone_type="Demand", is_mitigated=False,
                    end_time_utc=current_time, start_time_utc=sl_search_window_start_time, limit=1
                )
                if recent_demand_zones:
                    dz_bottom = utils.to_decimal_or_none(recent_demand_zones[0]['zone_bottom_price'])
                    if dz_bottom is not None:
                        temp_sl = dz_bottom - atr_tolerance_points
                        if temp_sl < entry_price_sugg_dec:
                            sl_sugg = temp_sl
                            logger.debug(f"AGGR_SIGNAL: SL adaptif dari Demand Zone: {float(sl_sugg):.5f}")

            # Fallback SL: Jika tidak ada SL adaptif yang ditemukan atau SL adaptif di atas entry (invalid)
            if sl_sugg is None or sl_sugg >= entry_price_sugg_dec:
                sl_sugg = entry_price_sugg_dec - self.default_sl_pips * self.pip_unit_in_dollar
                logger.debug(f"AGGR_SIGNAL: Menggunakan SL default: {float(sl_sugg):.5f}")
            
            # Hitung TP berdasarkan R:R dari SL adaptif
            risk_distance = abs(entry_price_sugg_dec - sl_sugg)
            if risk_distance <= Decimal('0.0'): # Pastikan risk_distance valid
                logger.warning(f"AGGR_SIGNAL: Risk distance untuk BUY adalah nol atau negatif ({float(risk_distance):.5f}). Tidak dapat menghitung TP. Kembali ke HOLD.")
                signal_data["reason"].append("Risk distance nol atau negatif untuk BUY.")
                signal_data["action"] = "HOLD"
                signal_data["confidence"] = "Low"
                signal_data["potential_direction"] = "Sideways"
                return signal_data

            tp1_sugg = entry_price_sugg_dec + risk_distance * self.tp1_rr_ratio
            tp2_sugg = entry_price_sugg_dec + risk_distance * self.tp2_rr_ratio
            tp3_sugg = entry_price_sugg_dec + risk_distance * self.tp3_rr_ratio

        # Jika sinyal SELL
        elif action == "SELL":
            # Opsi 1: Cari Swing High Terdekat
            recent_swing_highs = database_manager.get_market_structure_events(
                symbol=symbol,
                timeframe=timeframe,
                event_type='Swing High',
                end_time_utc=current_time,
                start_time_utc=sl_search_window_start_time,
                limit=2
            )
            # Filter swing high yang valid (lebih tinggi dari harga entri)
            valid_swing_highs = [utils.to_decimal_or_none(s['price_level']) for s in recent_swing_highs if utils.to_decimal_or_none(s['price_level']) is not None and utils.to_decimal_or_none(s['price_level']) > entry_price_sugg_dec]
            if valid_swing_highs:
                best_sl_level_dec = max(valid_swing_highs)
                sl_sugg = best_sl_level_dec + atr_tolerance_points # Sedikit di atas swing high
                logger.debug(f"AGGR_SIGNAL: SL adaptif dari Swing High: {float(sl_sugg):.5f}")
            
            # Opsi 2: Cari Order Block Bearish terdekat yang belum dimitigasi
            if (sl_sugg is None or sl_sugg <= entry_price_sugg_dec) and self.enable_ob_fvg_detection:
                recent_bearish_obs = database_manager.get_order_blocks(
                    symbol=symbol, timeframe=timeframe, type="Bearish", is_mitigated=False,
                    end_time_utc=current_time, start_time_utc=sl_search_window_start_time, limit=1
                )
                if recent_bearish_obs:
                    ob_top = utils.to_decimal_or_none(recent_bearish_obs[0]['ob_top_price'])
                    if ob_top is not None:
                        temp_sl = ob_top + atr_tolerance_points
                        if temp_sl > entry_price_sugg_dec:
                            sl_sugg = temp_sl
                            logger.debug(f"AGGR_SIGNAL: SL adaptif dari Bearish OB: {float(sl_sugg):.5f}")

            # Opsi 3: Cari Supply Zone terdekat yang belum dimitigasi
            if (sl_sugg is None or sl_sugg <= entry_price_sugg_dec) and self.enable_sr_detection:
                recent_supply_zones = database_manager.get_supply_demand_zones(
                    symbol=symbol, timeframe=timeframe, zone_type="Supply", is_mitigated=False,
                    end_time_utc=current_time, start_time_utc=sl_search_window_start_time, limit=1
                )
                if recent_supply_zones:
                    sz_top = utils.to_decimal_or_none(recent_supply_zones[0]['zone_top_price'])
                    if sz_top is not None:
                        temp_sl = sz_top + atr_tolerance_points
                        if temp_sl > entry_price_sugg_dec:
                            sl_sugg = temp_sl
                            logger.debug(f"AGGR_SIGNAL: SL adaptif dari Supply Zone: {float(sz_top):.5f}")

            # Fallback SL: Jika tidak ada SL adaptif yang ditemukan atau SL adaptif di bawah entry (invalid)
            if sl_sugg is None or sl_sugg <= entry_price_sugg_dec:
                sl_sugg = entry_price_sugg_dec + self.default_sl_pips * self.pip_unit_in_dollar
                logger.debug(f"AGGR_SIGNAL: Menggunakan SL default: {float(sl_sugg):.5f}")

            # Hitung TP berdasarkan R:R dari SL adaptif
            risk_distance = abs(entry_price_sugg_dec - sl_sugg)
            if risk_distance <= Decimal('0.0'): # Pastikan risk_distance valid
                logger.warning(f"AGGR_SIGNAL: Risk distance untuk SELL adalah nol atau negatif ({float(risk_distance):.5f}). Tidak dapat menghitung TP. Kembali ke HOLD.")
                signal_data["reason"].append("Risk distance nol atau negatif untuk SELL.")
                signal_data["action"] = "HOLD"
                signal_data["confidence"] = "Low"
                signal_data["potential_direction"] = "Sideways"
                return signal_data

            tp1_sugg = entry_price_sugg_dec - risk_distance * self.tp1_rr_ratio
            tp2_sugg = entry_price_sugg_dec - risk_distance * self.tp2_rr_ratio
            tp3_sugg = entry_price_sugg_dec - risk_distance * self.tp3_rr_ratio

        # Quantize SL/TP for precision
        precision = Decimal(str(self.pip_size))
        if sl_sugg is not None: sl_sugg = sl_sugg.quantize(precision)
        if tp1_sugg is not None: tp1_sugg = tp1_sugg.quantize(precision)
        if tp2_sugg is not None: tp2_sugg = tp2_sugg.quantize(precision)
        if tp3_sugg is not None: tp3_sugg = tp3_sugg.quantize(precision)

        # Final check for SL/TP validity based on min_sl_pips / min_tp_pips
        if action != "HOLD" :
            if sl_sugg is None or tp1_sugg is None:
                logger.warning(f"AGGR_SIGNAL: SL atau TP1 tidak dapat dihitung. Kembali ke HOLD. Action: {action}")
                action = "HOLD"
                reasoning_text.insert(0, "Gagal menghitung SL/TP.")
                confidence = "Low"
                potential_direction = "Sideways"
            else:
                sl_distance_pips = abs(entry_price_sugg_dec - sl_sugg) / self.pip_size
                tp1_distance_pips = abs(entry_price_sugg_dec - tp1_sugg) / self.pip_size

                if sl_distance_pips < self.min_sl_pips or tp1_distance_pips < self.min_tp_pips:
                    logger.warning(f"AGGR_SIGNAL: SL ({float(sl_distance_pips):.2f} pips) atau TP1 ({float(tp1_distance_pips):.2f} pips) terlalu kecil. Min SL: {float(self.min_sl_pips)}, Min TP: {float(self.min_tp_pips)}. Kembali ke HOLD.")
                    action = "HOLD"
                    reasoning_text.insert(0, f"SL/TP terlalu kecil (SL:{float(sl_distance_pips):.2f}, TP:{float(tp1_distance_pips):.2f})")
                    confidence = "Low"
                    potential_direction = "Sideways"

        # Update final signal_data
        signal_data.update({
            "action": action,
            "potential_direction": potential_direction,
            "entry_price_suggestion": float(entry_price_sugg_dec) if entry_price_sugg_dec is not None else None,
            "stop_loss_suggestion": float(sl_sugg) if sl_sugg is not None else None,
            "take_profit_suggestion": float(tp1_sugg) if tp1_sugg is not None else None,
            "reason": reasoning_text,
            "confidence": confidence,
            "tp2_suggestion": float(tp2_sugg) if tp2_sugg is not None else None,
            "tp3_suggestion": float(tp3_sugg) if tp3_sugg is not None else None,
            "strength": float(signal_strength_final),
            "timestamp": current_time,
            "signal_type": "Aggressive"
        })

        # Perhitungan TP levels untuk auto_trade_manager (menggunakan RR default dari aggressive_signal_generator)
        tp_levels_for_atm = []
        if signal_data["action"] != "HOLD" and signal_data["entry_price_suggestion"] is not None and signal_data["stop_loss_suggestion"] is not None:
            entry_price_for_atm = Decimal(str(signal_data["entry_price_suggestion"]))
            stop_loss_for_atm = Decimal(str(signal_data["stop_loss_suggestion"]))

            risk_distance_in_price_units = abs(entry_price_for_atm - stop_loss_for_atm)
            
            if self.pip_size == Decimal('0.0'):
                logger.error("AGGR_SIGNAL: PIP_SIZE adalah nol, tidak dapat menghitung risk_distance_pips_for_atm. TP levels ATM tidak dihitung.")
            else:
                risk_distance_pips_for_atm = risk_distance_in_price_units / self.pip_size

                if risk_distance_pips_for_atm > 0:
                    if self.tp1_rr_ratio > 0 and tp1_sugg is not None:
                        tp_levels_for_atm.append({'price': float(tp1_sugg), 'volume_percentage': Decimal('0.50'), 'move_sl_to_breakeven_after_partial': False, 'move_sl_to_price_after_partial': None})
                    if self.tp2_rr_ratio > 0 and tp2_sugg is not None:
                        tp_levels_for_atm.append({'price': float(tp2_sugg), 'volume_percentage': Decimal('0.30'), 'move_sl_to_breakeven_after_partial': True, 'move_sl_to_price_after_partial': None})
                    if self.tp3_rr_ratio > 0 and tp3_sugg is not None:
                        tp_levels_for_atm.append({'price': float(tp3_sugg), 'volume_percentage': Decimal('0.20'), 'move_sl_to_breakeven_after_partial': False, 'move_sl_to_price_after_partial': None})
                else:
                    logger.warning("AGGR_SIGNAL: Risk distance nol atau negatif, TP levels untuk ATM tidak dihitung.")
        
        signal_data["take_profit_levels_suggestion"] = tp_levels_for_atm 

        logger.info(f"AGGR_SIGNAL: Sinyal dihasilkan untuk {symbol} pada {current_time}: {signal_data['action']} (Conf: {signal_data['confidence']}).")
        if signal_data['action'] != 'HOLD':
            logger.info(f"AGGR_SIGNAL: Entry: {float(signal_data['entry_price_suggestion']):.5f}, SL: {float(signal_data['stop_loss_suggestion']):.5f}.")
            for i, tp in enumerate(signal_data['take_profit_levels_suggestion']):
                logger.info(f"AGGR_SIGNAL: TP{i+1}: {float(tp['price']):.5f} ({float(tp['volume_percentage']*100):.0f}% volume).")

        return signal_data

# Inisialisasi generator sinyal (ini penting agar bisa langsung dipakai saat diimpor)
aggressive_signal_generator_instance = AggressiveSignalGenerator()

# Fungsi publik untuk diakses oleh modul lain
def generate_signal(symbol: str, current_time: datetime, current_price: Decimal) -> dict:
    return aggressive_signal_generator_instance.generate_signal(symbol, current_time, current_price)
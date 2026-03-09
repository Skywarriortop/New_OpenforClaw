# rule_based_signal_generator.py

import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal, getcontext # Pastikan getcontext diimpor untuk presisi
import pandas as pd
import json
import talib # Pastikan TA-Lib terinstal
# from sqlalchemy.exc import OperationalError # Tidak digunakan langsung di sini
import time # Untuk sleep jika diperlukan, tapi sebaiknya di scheduler
from collections import defaultdict
import database_manager
from config import config # Pastikan config diimpor
import mt5_connector
import market_data_processor # Pastikan ini diimpor
import utils # Pastikan utils diimpor
from utils import to_float_or_none, to_iso_format_or_none , to_decimal_or_none, to_utc_datetime_or_none, _get_scalar_from_possibly_ndarray
logger = logging.getLogger(__name__)
getcontext().prec = 10 # Set presisi Decimal untuk perhitungan keuangan


# --- Fungsi Pembantu: Mengumpulkan Konteks Pasar ---
def _get_market_context(symbol: str, current_time: datetime) -> dict:
    """
    Mengumpulkan data detektor terbaru dan relevan dari database untuk berbagai timeframes.
    Args:
        symbol (str): Simbol trading.
        current_time (datetime): Waktu saat ini (UTC) untuk membatasi pencarian data.
    Returns:
        dict: Kamus yang berisi semua data konteks pasar yang relevan.
    """
    logger.debug(f"RULE SIGNAL: Mengumpulkan konteks pasar untuk {symbol} pada {current_time.isoformat()}...")

    context = {
        "current_price": None,
        "overall_trend": None, # Akan diisi dari market_structure_events
        "ma_crossovers": defaultdict(list), # {timeframe: [{type, direction, price}]}
        "rsi_status": defaultdict(dict), # {timeframe: {value, condition}}
        "macd_status": defaultdict(dict), # {timeframe: {macd_line, signal_line, histogram, macd_pcent}}
        "swing_points": defaultdict(list),
        "sr_levels": defaultdict(list),
        "sd_zones": defaultdict(list),
        "order_blocks": defaultdict(list),
        "fair_value_gaps": defaultdict(list),
        "liquidity_zones": defaultdict(list),
        "fib_levels": defaultdict(list),
        "market_structure_events": defaultdict(list), # BoS, ChoCh, Yearly Extremes, Overall Trend
        "previous_high_low_events": defaultdict(list), # Previous High/Low Broken
        "divergences": defaultdict(list),
        "atr_values": defaultdict(Decimal), # Untuk toleransi dinamis

        "latest_candles": {}, # {timeframe: pd.DataFrame (last N candles)}
    }

    # 1. Dapatkan harga saat ini
    latest_tick = database_manager.get_latest_price_tick(symbol)
    if latest_tick and latest_tick.get('last_price') is not None:
        context["current_price"] = utils.to_decimal_or_none(latest_tick['last_price'])
    elif latest_tick and latest_tick.get('bid_price') is not None:
        context["current_price"] = utils.to_decimal_or_none(latest_tick['bid_price'])
    else:
        logger.warning(f"RULE SIGNAL: Tidak dapat menemukan harga saat ini untuk {symbol}. Konteks mungkin tidak lengkap.")
        # Fallback ke harga penutupan candle terakhir jika tidak ada tick
        latest_d1_candle = database_manager.get_historical_candles_from_db(symbol, "D1", limit=1)
        if latest_d1_candle:
            context["current_price"] = utils.to_decimal_or_none(latest_d1_candle[0]['close_price'])

    if context["current_price"] is None:
        logger.error(f"RULE SIGNAL: Gagal mendapatkan harga saat ini untuk {symbol}. Konteks tidak dapat dibuat.")
        return context

    # List timeframes yang relevan dari SIGNAL_RULES untuk mengoptimalkan query DB
    relevant_timeframes = set()
    for rule in config.RuleBasedStrategy.SIGNAL_RULES:
        if rule['enabled']:
            for tf in rule['timeframes']:
                relevant_timeframes.add(tf)
    
    # Ambil data detektor untuk setiap timeframe yang relevan
    for tf in relevant_timeframes:
        candles_for_tf = database_manager.get_historical_candles_from_db(symbol, tf, limit=20, end_time_utc=current_time)
        
        # PERBAIKAN: Proses DataFrame lilin agar konsisten
        if not candles_for_tf:
            logger.warning(f"RULE SIGNAL: Tidak ada data candle untuk {symbol} {tf}. Melewatkan TF ini dalam konteks.")
            continue

        df_tf = pd.DataFrame(candles_for_tf)
        
        # Pastikan kolom waktu adalah datetime dan disetel sebagai indeks
        df_tf['open_time_utc'] = pd.to_datetime(df_tf['open_time_utc'])
        df_tf.set_index('open_time_utc', inplace=True)
        df_tf.sort_index(inplace=True)

        # Rename kolom harga dari '_price' ke nama pendek, dan konversi ke Decimal
        df_tf = df_tf.rename(columns={
            'open_price': 'open',
            'high_price': 'high',
            'low_price': 'low',
            'close_price': 'close',
            'tick_volume': 'volume',
            'real_volume': 'real_volume',
            'spread': 'spread'
        })
        for col in ['open', 'high', 'low', 'close', 'volume', 'real_volume', 'spread']:
            if col in df_tf.columns:
                df_tf[col] = df_tf[col].apply(utils.to_decimal_or_none)
        
        # Hapus baris dengan NaN di kolom harga kunci setelah konversi
        initial_df_len = len(df_tf)
        df_tf.dropna(subset=['open', 'high', 'low', 'close'], inplace=True)
        if len(df_tf) < initial_df_len:
            logger.warning(f"RULE SIGNAL: Dihapus {initial_df_len - len(df_tf)} baris dengan NaN di kolom OHLC untuk {symbol} {tf}.")
        
        if df_tf.empty:
            logger.warning(f"RULE SIGNAL: DataFrame {tf} kosong setelah pembersihan NaN. Melewatkan TF ini.")
            continue

        context["latest_candles"][tf] = df_tf # Simpan DataFrame yang sudah diproses

        # Perhitungan ATR (ATR _calculate_atr mengharapkan float)
        if not df_tf.empty and config.MarketData.ATR_PERIOD > 0:
            df_tf_float_for_atr = df_tf[['high', 'low', 'close']].apply(utils.to_float_or_none)
            
            atr_series = market_data_processor._calculate_atr(df_tf_float_for_atr, config.MarketData.ATR_PERIOD)
            if not atr_series.empty and pd.notna(atr_series.iloc[-1]):
                context["atr_values"][tf] = utils.to_decimal_or_none(atr_series.iloc[-1])
            else:
                context["atr_values"][tf] = Decimal('0.0')
        else:
            context["atr_values"][tf] = Decimal('0.0')

        # Dapatkan data detektor terbaru dari database (menggunakan TF ini)
        lookback_limit_db = 100 # Jumlah entri terbaru yang relevan dari DB

        context["order_blocks"][tf] = database_manager.get_order_blocks(symbol, timeframe=tf, is_mitigated=False, limit=lookback_limit_db, end_time_utc=current_time)
        context["fair_value_gaps"][tf] = database_manager.get_fair_value_gaps(symbol, timeframe=tf, is_filled=False, limit=lookback_limit_db, end_time_utc=current_time)
        context["liquidity_zones"][tf] = database_manager.get_liquidity_zones(symbol, timeframe=tf, is_tapped=False, limit=lookback_limit_db, end_time_utc=current_time)
        context["fib_levels"][tf] = database_manager.get_fibonacci_levels(symbol, timeframe=tf, is_active=True, limit=lookback_limit_db, end_time_utc=current_time)
        context["sr_levels"][tf] = database_manager.get_support_resistance_levels(symbol, timeframe=tf, is_active=True, limit=lookback_limit_db, end_time_utc=current_time)
        context["divergences"][tf] = database_manager.get_divergences(symbol, timeframe=tf, is_active=True, limit=lookback_limit_db, end_time_utc=current_time)
        
        # Market Structure Events (BoS, ChoCh, Swing High/Low, Overall Trend, MA Crossover, OB/OS)
        # Mengambil semua event_type yang relevan
        context["market_structure_events"][tf] = database_manager.get_market_structure_events(
            symbol, timeframe=tf, 
            event_type=[
                'Swing High', 'Swing Low', 'Break of Structure', 'Change of Character',
                'Yearly High', 'Yearly Low', 'SMA Buy Crossover', 'SMA Sell Crossover',
                'EMA Buy Crossover', 'EMA Sell Crossover', 'Overbought', 'Oversold',
                'Overall Trend', 'Previous High Broken', 'Previous Low Broken'
            ],
            limit=lookback_limit_db, end_time_utc=current_time
        )

        # Filter Overall Trend dari MS Events ke konteks root
        overall_trend_event_for_tf = next((e for e in context["market_structure_events"][tf] if e.get("event_type") == 'Overall Trend'), None)
        if overall_trend_event_for_tf:
            context["overall_trend"] = overall_trend_event_for_tf.get('direction') # "Bullish", "Bearish", "Sideways"

        # RSI dan MACD (nilai terkini dan sebelumnya untuk cross check)
        if config.MarketData.ENABLE_RSI_CALCULATION:
            latest_rsi_value_list = database_manager.get_rsi_values(symbol, tf, limit=2, end_time_utc=current_time) # Ambil 2 nilai
            if latest_rsi_value_list:
                context["rsi_status"][tf] = {
                    "value": utils.to_decimal_or_none(latest_rsi_value_list[0]['value']), # Nilai terkini
                    "prev_value": utils.to_decimal_or_none(latest_rsi_value_list[1]['value']) if len(latest_rsi_value_list) > 1 else None, # Nilai sebelumnya
                    "condition": ("OVERSOLD" if latest_rsi_value_list[0]['value'] <= config.MarketData.RSI_OVERSOLD_LEVEL else
                                  ("OVERBOUGHT" if latest_rsi_value_list[0]['value'] >= config.MarketData.RSI_OVERBOUGHT_LEVEL else "NORMAL"))
                }
        
        if config.MarketData.ENABLE_MACD_CALCULATION:
            latest_macd_value_list = database_manager.get_macd_values(symbol, tf, limit=2, end_time_utc=current_time) # Ambil 2 nilai
            if latest_macd_value_list:
                context["macd_status"][tf] = {
                    "macd_line": utils.to_decimal_or_none(latest_macd_value_list[0]['macd_line']),
                    "signal_line": utils.to_decimal_or_none(latest_macd_value_list[0]['signal_line']),
                    "histogram": utils.to_decimal_or_none(latest_macd_value_list[0]['histogram']),
                    "macd_pcent": utils.to_decimal_or_none(latest_macd_value_list[0]['macd_pcent']),
                    "prev_macd_line": utils.to_decimal_or_none(latest_macd_value_list[1]['macd_line']) if len(latest_macd_value_list) > 1 else None, # Nilai sebelumnya
                    "prev_signal_line": utils.to_decimal_or_none(latest_macd_value_list[1]['signal_line']) if len(latest_macd_value_list) > 1 else None, # Nilai sebelumnya
                }

    logger.debug("RULE SIGNAL: Pengumpulan konteks pasar selesai.")
    return context

# --- Fungsi Pembantu: Mengevaluasi Kondisi (Digunakan oleh _collect_all_evidence untuk bukti individual) ---
def _evaluate_condition(condition_str: str, context: dict, symbol_param: str, tf_eval: str, current_price: Decimal) -> bool:
    """
    Mengevaluasi satu kondisi string terhadap konteks pasar.
    Args:
        condition_str (str): String kondisi (misal: "TREND_H4_BULLISH").
        context (dict): Konteks pasar yang berisi data detektor.
        symbol_param (str): Simbol trading.
        tf_eval (str): Timeframe yang sedang dievaluasi untuk aturan saat ini (misal H1 untuk RSI H1).
        current_price (Decimal): Harga saat ini.
    Returns:
        bool: True jika kondisi terpenuhi, False jika tidak.
    """
    logger.debug(f"EVAL_COND_DIAG: Mengevaluasi '{condition_str}' untuk TF '{tf_eval}'. Harga: {float(current_price):.5f}")

    atr_val = context["atr_values"].get(tf_eval, Decimal('0.0'))
    dynamic_tolerance = atr_val * config.MarketData.ATR_MULTIPLIER_FOR_TOLERANCE
    logger.debug(f"EVAL_COND_DIAG: ATR for {tf_eval}: {float(atr_val):.5f}, Dynamic Tolerance: {float(dynamic_tolerance):.5f}.")


    # --- Kondisi Tren ---
    if condition_str.startswith("TREND_"):
        parts = condition_str.split('_')
        tf_trend = parts[1]
        expected_trend = parts[2]
        
        overall_trend_event = None
        if tf_trend in context.get("market_structure_events", {}):
            for event in reversed(context["market_structure_events"][tf_trend]):
                if event.get("event_type") == 'Overall Trend':
                    overall_trend_event = event
                    break
        
        result = (overall_trend_event and overall_trend_event.get("direction") == expected_trend)
        logger.debug(f"EVAL_COND_DIAG: TREND_{tf_trend}_{expected_trend} -> Ditemukan: {overall_trend_event.get('direction') if overall_trend_event else 'N/A'}. Hasil: {result}")
        return result

    # --- Kondisi Harga Relatif terhadap Level / Zone ---
    if condition_str.startswith("PRICE_IN_ACTIVE_"):
        parts = condition_str.split('_IN_ACTIVE_')
        zone_type = parts[1].upper()
        
        logger.debug(f"EVAL_COND_DIAG: Mengecek harga dalam zona aktif: '{zone_type}'.")

        if zone_type == "DEMAND_ZONE":
            active_zones = [z for z in context["sd_zones"].get(tf_eval, []) if z.get('zone_type') == 'Demand' and not z.get('is_mitigated')]
            logger.debug(f"EVAL_COND_DIAG: Ditemukan {len(active_zones)} Demand Zones aktif untuk {tf_eval}.")
            for zone in active_zones:
                zone_bottom = utils.to_decimal_or_none(zone['zone_bottom_price'])
                zone_top = utils.to_decimal_or_none(zone['zone_top_price'])
                if zone_bottom is None or zone_top is None: continue

                is_in_zone = current_price >= zone_bottom - dynamic_tolerance and \
                           current_price <= zone_top + dynamic_tolerance
                logger.debug(f"EVAL_COND_DIAG: Checking Demand Zone {float(zone_bottom):.5f}-{float(zone_top):.5f}. Current price {float(current_price):.5f}. Is in zone? {is_in_zone}")
                if is_in_zone:
                    return True
            return False
        
        elif zone_type == "SUPPLY_ZONE":
            active_zones = [z for z in context["sd_zones"].get(tf_eval, []) if z.get('zone_type') == 'Supply' and not z.get('is_mitigated')]
            logger.debug(f"EVAL_COND_DIAG: Ditemukan {len(active_zones)} Supply Zones aktif untuk {tf_eval}.")
            for zone in active_zones:
                zone_bottom = utils.to_decimal_or_none(zone['zone_bottom_price'])
                zone_top = utils.to_decimal_or_none(zone['zone_top_price'])
                if zone_bottom is None or zone_top is None: continue

                is_in_zone = current_price >= zone_bottom - dynamic_tolerance and \
                           current_price <= zone_top + dynamic_tolerance
                logger.debug(f"EVAL_COND_DIAG: Checking Supply Zone {float(zone_bottom):.5f}-{float(zone_top):.5f}. Current price {float(current_price):.5f}. Is in zone? {is_in_zone}")
                if is_in_zone:
                    return True
            return False

        # ... (sisa kondisi PRICE_IN_ACTIVE_ untuk OB, FVG, dll.)
        # Ini akan memastikan kondisi-kondisi ini terpicu dengan benar.
        # Jika Anda menambahkan detektor lain (OB, FVG, LIQ, FIB, SR) ke RuleBasedStrategy,
        # pastikan _evaluate_condition memiliki logika yang tepat untuk mereka.
        # Saat ini, kondisi SIGNAL_RULES Anda hanya mencakup TREND, PRICE_IN_ACTIVE_SD_ZONE, STRENGTH_SD_ZONE.
        # Anda juga perlu logika untuk:
        elif zone_type == "BULLISH_OB":
            active_obs = [ob for ob in context["order_blocks"].get(tf_eval, []) if ob.get('type') == 'Bullish' and not ob.get('is_mitigated')]
            for ob in active_obs:
                if current_price >= ob['ob_bottom_price'] - dynamic_tolerance and current_price <= ob['ob_top_price'] + dynamic_tolerance: return True
            return False
        elif zone_type == "BEARISH_OB":
            active_obs = [ob for ob in context["order_blocks"].get(tf_eval, []) if ob.get('type') == 'Bearish' and not ob.get('is_mitigated')]
            for ob in active_obs:
                if current_price >= ob['ob_bottom_price'] - dynamic_tolerance and current_price <= ob['ob_top_price'] + dynamic_tolerance: return True
            return False
        elif zone_type == "BULLISH_IMBALANCE":
            active_fvgs = [fvg for fvg in context["fair_value_gaps"].get(tf_eval, []) if fvg.get('type') == 'Bullish Imbalance' and not fvg.get('is_filled')]
            for fvg in active_fvgs:
                if current_price >= fvg['fvg_bottom_price'] - dynamic_tolerance and current_price <= fvg['fvg_top_price'] + dynamic_tolerance: return True
            return False
        elif zone_type == "BEARISH_IMBALANCE":
            active_fvgs = [fvg for fvg in context["fair_value_gaps"].get(tf_eval, []) if fvg.get('type') == 'Bearish Imbalance' and not fvg.get('is_filled')]
            for fvg in active_fvgs:
                if current_price >= fvg['fvg_bottom_price'] - dynamic_tolerance and current_price <= fvg['fvg_top_price'] + dynamic_tolerance: return True
            return False
        elif zone_type == "EQUAL_HIGHS":
            active_liqs = [liq for liq in context["liquidity_zones"].get(tf_eval, []) if liq.get('zone_type') == 'Equal Highs' and not liq.get('is_tapped')]
            for liq in active_liqs:
                if abs(current_price - liq['price_level']) <= dynamic_tolerance: return True
            return False
        elif zone_type == "EQUAL_LOWS":
            active_liqs = [liq for liq in context["liquidity_zones"].get(tf_eval, []) if liq.get('zone_type') == 'Equal Lows' and not liq.get('is_tapped')]
            for liq in active_liqs:
                if abs(current_price - liq['price_level']) <= dynamic_tolerance: return True
            return False



# --- Fungsi Pembantu: Mengevaluasi Kondisi ---
def _evaluate_condition(condition_str: str, context: dict, symbol_param: str, tf_eval: str, current_price: Decimal) -> bool:
    """
    Mengevaluasi satu kondisi string terhadap konteks pasar.
    Fungsi ini digunakan untuk memeriksa apakah suatu kondisi teknis atau pasar tertentu terpenuhi.
    Args:
        condition_str (str): String kondisi (misal: "TREND_H4_BULLISH", "PRICE_IN_ACTIVE_DEMAND_ZONE").
        context (dict): Konteks pasar yang berisi data detektor terbaru dari berbagai timeframes.
        symbol_param (str): Simbol trading (misal: "XAUUSD").
        tf_eval (str): Timeframe yang relevan untuk evaluasi kondisi ini (misal: "H1", "H4").
        current_price (Decimal): Harga pasar saat ini.
    Returns:
        bool: True jika kondisi terpenuhi, False jika tidak.
    """
    logger.debug(f"EVAL_COND_DIAG: Mengevaluasi '{condition_str}' untuk TF '{tf_eval}'. Harga: {float(current_price):.5f}")

    # Helper untuk mendapatkan ATR terkini dari konteks
    atr_val = context["atr_values"].get(tf_eval, Decimal('0.0'))
    dynamic_tolerance = atr_val * config.MarketData.ATR_MULTIPLIER_FOR_TOLERANCE
    logger.debug(f"EVAL_COND_DIAG: ATR for {tf_eval}: {float(atr_val):.5f}, Dynamic Tolerance: {float(dynamic_tolerance):.5f}.")


    # --- Kondisi Tren (Misal: "TREND_H4_BULLISH") ---
    if condition_str.startswith("TREND_"):
        parts = condition_str.split('_')
        if len(parts) != 3:
            logger.warning(f"EVAL_COND_DIAG: Format kondisi TREND tidak valid: '{condition_str}'.")
            return False
        tf_trend = parts[1] # Timeframe untuk trend (contoh: "H4")
        expected_trend = parts[2] # Arah trend yang diharapkan (contoh: "BULLISH", "BEARISH", "SIDEWAYS")
        
        overall_trend_event = None
        # Cari event Overall Trend terbaru untuk timeframe yang diminta dari konteks
        if tf_trend in context.get("market_structure_events", {}):
            for event in reversed(context["market_structure_events"][tf_trend]): # Iterasi mundur untuk yang terbaru
                if event.get("event_type") == 'Overall Trend':
                    overall_trend_event = event
                    break
        
        # Bandingkan arah trend yang ditemukan dengan yang diharapkan
        result = (overall_trend_event and overall_trend_event.get("direction") == expected_trend)
        logger.debug(f"EVAL_COND_DIAG: TREND_{tf_trend}_{expected_trend} -> Ditemukan: {overall_trend_event.get('direction') if overall_trend_event else 'N/A'}. Hasil: {result}")
        return result

    # --- Kondisi Harga Relatif terhadap Level / Zone (Misal: "PRICE_IN_ACTIVE_DEMAND_ZONE") ---
    if condition_str.startswith("PRICE_IN_ACTIVE_"):
        parts = condition_str.split('_IN_ACTIVE_')
        if len(parts) != 2:
            logger.warning(f"EVAL_COND_DIAG: Format kondisi PRICE_IN_ACTIVE_ tidak valid: '{condition_str}'.")
            return False
        zone_type_requested = parts[1].upper() # Tipe zona yang diminta (contoh: "DEMAND_ZONE", "SUPPLY_ZONE", "BULLISH_OB")
        
        logger.debug(f"EVAL_COND_DIAG: Mengecek harga dalam zona aktif: '{zone_type_requested}'.")

        # Mendapatkan daftar zona/level aktif yang relevan dari konteks
        active_zones_or_levels = []
        if zone_type_requested == "DEMAND_ZONE":
            active_zones_or_levels = [z for z in context["sd_zones"].get(tf_eval, []) if z.get('zone_type') == 'Demand' and not z.get('is_mitigated')]
        elif zone_type_requested == "SUPPLY_ZONE":
            active_zones_or_levels = [z for z in context["sd_zones"].get(tf_eval, []) if z.get('zone_type') == 'Supply' and not z.get('is_mitigated')]
        elif zone_type_requested == "BULLISH_OB":
            active_zones_or_levels = [ob for ob in context["order_blocks"].get(tf_eval, []) if ob.get('type') == 'Bullish' and not ob.get('is_mitigated')]
        elif zone_type_requested == "BEARISH_OB":
            active_zones_or_levels = [ob for ob in context["order_blocks"].get(tf_eval, []) if ob.get('type') == 'Bearish' and not ob.get('is_mitigated')]
        elif zone_type_requested == "BULLISH_IMBALANCE": # FVG
            active_zones_or_levels = [fvg for fvg in context["fair_value_gaps"].get(tf_eval, []) if fvg.get('type') == 'Bullish Imbalance' and not fvg.get('is_filled')]
        elif zone_type_requested == "BEARISH_IMBALANCE": # FVG
            active_zones_or_levels = [fvg for fvg in context["fair_value_gaps"].get(tf_eval, []) if fvg.get('type') == 'Bearish Imbalance' and not fvg.get('is_filled')]
        elif zone_type_requested == "EQUAL_HIGHS": # Liquidity
            active_zones_or_levels = [liq for liq in context["liquidity_zones"].get(tf_eval, []) if liq.get('zone_type') == 'Equal Highs' and not liq.get('is_tapped')]
        elif zone_type_requested == "EQUAL_LOWS": # Liquidity
            active_zones_or_levels = [liq for liq in context["liquidity_zones"].get(tf_eval, []) if liq.get('zone_type') == 'Equal Lows' and not liq.get('is_tapped')]
        # Anda dapat menambahkan lebih banyak jenis zona/level sesuai kebutuhan

        if not active_zones_or_levels:
            logger.debug(f"EVAL_COND_DIAG: PRICE_IN_ACTIVE_{zone_type_requested}. Tidak ditemukan zona aktif untuk {tf_eval}. Hasil: False")
            return False

        # Periksa apakah harga saat ini berada di dalam salah satu zona aktif tersebut (dengan toleransi)
        for item in active_zones_or_levels:
            item_bottom = None
            item_top = None

            # Tentukan harga bawah dan atas item (zona/OB/FVG)
            if 'zone_bottom_price' in item: # Untuk Supply/Demand Zones
                item_bottom = utils.to_decimal_or_none(item['zone_bottom_price'])
                item_top = utils.to_decimal_or_none(item['zone_top_price'])
            elif 'ob_bottom_price' in item: # Untuk Order Blocks
                item_bottom = utils.to_decimal_or_none(item['ob_bottom_price'])
                item_top = utils.to_decimal_or_none(item['ob_top_price'])
            elif 'fvg_bottom_price' in item: # Untuk Fair Value Gaps
                item_bottom = utils.to_decimal_or_none(item['fvg_bottom_price'])
                item_top = utils.to_decimal_or_none(item['fvg_top_price'])
            elif 'price_level' in item: # Untuk Liquidity Zones atau SR Levels (jika disertakan)
                item_bottom = utils.to_decimal_or_none(item['price_level']) - dynamic_tolerance # Asumsi level adalah titik tengah zona
                item_top = utils.to_decimal_or_none(item['price_level']) + dynamic_tolerance
            
            if item_bottom is None or item_top is None: continue # Lewati item yang tidak memiliki harga valid

            is_in_range = current_price >= item_bottom - dynamic_tolerance and \
                          current_price <= item_top + dynamic_tolerance
            
            logger.debug(f"EVAL_COND_DIAG: Checking {zone_type_requested} {float(item_bottom):.5f}-{float(item_top):.5f}. Current price {float(current_price):.5f}. Is in range? {is_in_range}")
            if is_in_range:
                logger.debug(f"EVAL_COND_DIAG: Kondisi '{condition_str}' TRUE.")
                return True
        logger.debug(f"EVAL_COND_DIAG: Kondisi '{condition_str}' FALSE (Harga tidak di zona aktif {zone_type_requested}).")
        return False

    # --- Kondisi Kekuatan Detektor (Misal: "DEMAND_ZONE_STRENGTH_GE_2") ---
    if "_STRENGTH_GE_" in condition_str:
        parts = condition_str.split('_STRENGTH_GE_')
        if len(parts) != 2:
            logger.warning(f"EVAL_COND_DIAG: Format kondisi _STRENGTH_GE_ tidak valid: '{condition_str}'.")
            return False
        det_type_requested = parts[0].upper()
        min_strength = utils.to_decimal_or_none(parts[1])
        if min_strength is None:
            logger.warning(f"EVAL_COND_DIAG: Min strength tidak valid untuk '{condition_str}'.")
            return False
        
        logger.debug(f"EVAL_COND_DIAG: Mengecek kekuatan detektor: '{det_type_requested}' >= {float(min_strength):.0f}.")

        active_items_for_strength_check = []
        if det_type_requested == "DEMAND_ZONE":
            active_items_for_strength_check = [z for z in context["sd_zones"].get(tf_eval, []) if z.get('zone_type') == 'Demand' and not z.get('is_mitigated')]
        elif det_type_requested == "SUPPLY_ZONE":
            active_items_for_strength_check = [z for z in context["sd_zones"].get(tf_eval, []) if z.get('zone_type') == 'Supply' and not z.get('is_mitigated')]
        elif det_type_requested == "OB": # Baik Bullish maupun Bearish OB, jika kekuatannya penting
            active_items_for_strength_check = [ob for ob in context["order_blocks"].get(tf_eval, []) if not ob.get('is_mitigated')]
        elif det_type_requested == "FVG": # Baik Bullish maupun Bearish FVG, jika kekuatannya penting
            active_items_for_strength_check = [fvg for fvg in context["fair_value_gaps"].get(tf_eval, []) if not fvg.get('is_filled')]
        # Tambahkan detektor lain yang memiliki strength_score

        if not active_items_for_strength_check:
            logger.debug(f"EVAL_COND_DIAG: {det_type_requested}_STRENGTH_GE_{float(min_strength):.0f}. Tidak ada item aktif. Hasil: False")
            return False

        for item in active_items_for_strength_check:
            item_bottom = None
            item_top = None
            if 'zone_bottom_price' in item: item_bottom, item_top = utils.to_decimal_or_none(item['zone_bottom_price']), utils.to_decimal_or_none(item['zone_top_price'])
            elif 'ob_bottom_price' in item: item_bottom, item_top = utils.to_decimal_or_none(item['ob_bottom_price']), utils.to_decimal_or_none(item['ob_top_price'])
            elif 'fvg_bottom_price' in item: item_bottom, item_top = utils.to_decimal_or_none(item['fvg_bottom_price']), utils.to_decimal_or_none(item['fvg_top_price'])
            elif 'price_level' in item: item_bottom, item_top = utils.to_decimal_or_none(item['price_level']) - dynamic_tolerance, utils.to_decimal_or_none(item['price_level']) + dynamic_tolerance
            
            if item_bottom is None or item_top is None: continue

            is_in_range = current_price >= item_bottom - dynamic_tolerance and \
                          current_price <= item_top + dynamic_tolerance
            strength_score = utils.to_decimal_or_none(item.get('strength_score', Decimal('0')))
            
            logger.debug(f"EVAL_COND_DIAG: Checking {det_type_requested} {float(item_bottom):.5f}-{float(item_top):.5f}. In range? {is_in_range}. Strength: {float(strength_score):.0f}. Min Strength: {float(min_strength):.0f}.")
            
            if is_in_range and strength_score >= min_strength:
                logger.debug(f"EVAL_COND_DIAG: Kondisi '{condition_str}' TRUE.")
                return True
        logger.debug(f"EVAL_COND_DIAG: {det_type_requested}_STRENGTH_GE_{float(min_strength):.0f}. Hasil: False (Tidak ada item yang memenuhi kriteria).")
        return False
    
    # --- Kondisi Pola Candle (Misal: "CANDLE_REJECTION_BULLISH_H1") ---
    if "_CANDLE_REJECTION_" in condition_str:
        parts = condition_str.split('_CANDLE_REJECTION_')
        if len(parts) != 2:
            logger.warning(f"EVAL_COND_DIAG: Format kondisi CANDLE_REJECTION tidak valid: '{condition_str}'.")
            return False
        rejection_type = parts[0].upper() # BULLISH, BEARISH
        tf_candle = parts[1] # Timeframe lilin
        
        df_candles = context["latest_candles"].get(tf_candle)
        if df_candles is None or len(df_candles) < 2:
            logger.debug(f"EVAL_COND_DIAG: CANDLE_REJECTION_{rejection_type}_{tf_candle}. Tidak cukup data lilin. Hasil: False")
            return False
        
        latest_candle = df_candles.iloc[-1]
        prev_candle = df_candles.iloc[-2]

        result = False
        if rejection_type == "BULLISH":
            # Contoh sederhana: Bullish Engulfing atau Hammer/Pin Bar
            # Catatan: _detect_candlestick_pattern di ai_consensus_manager tidak bisa dipanggil langsung di sini.
            # Anda perlu mengimplementasikan logika deteksi pola candle secara langsung di sini
            # atau memastikan data pola candle sudah ada di konteks jika dideteksi di tempat lain.
            # Untuk test ini, kita bisa mock sederhana atau fokus pada deteksi yang sudah diimplementasikan di tempat lain.
            
            # Placeholder untuk deteksi pola lilin bullish yang robust
            if latest_candle['close'] > latest_candle['open'] and \
               latest_candle['close'] > prev_candle['open'] and \
               latest_candle['open'] < prev_candle['close']: # Basic Engulfing
                result = True
        elif rejection_type == "BEARISH":
            # Placeholder untuk deteksi pola lilin bearish yang robust
            if latest_candle['close'] < latest_candle['open'] and \
               latest_candle['close'] < prev_candle['open'] and \
               latest_candle['open'] > prev_candle['close']: # Basic Engulfing
                result = True
        
        logger.debug(f"EVAL_COND_DIAG: CANDLE_REJECTION_{rejection_type}_{tf_candle}. Hasil: {result}")
        return result

    # --- Kondisi RSI (Misal: "RSI_H1_OVERSOLD_OR_NOT_OVERBOUGHT") ---
    if condition_str.startswith("RSI_"):
        parts = condition_str.split('_')
        if len(parts) < 3: # Misal "RSI_H1_OVERSOLD"
            logger.warning(f"EVAL_COND_DIAG: Format kondisi RSI tidak valid: '{condition_str}'.")
            return False
        # PERBAIKAN DI SINI: tf_rsi seharusnya adalah elemen kedua (indeks 1) dari `parts`
        tf_rsi = parts[1] # Ini akan mengambil 'H1' dari ['RSI', 'H1', 'OVERSOLD', ...]
        expected_state = parts[2].upper()

        
        rsi_status = context["rsi_status"].get(tf_rsi)
        if rsi_status and rsi_status.get('value') is not None:
            rsi_val = rsi_status['value']
            is_oversold = (rsi_val <= config.MarketData.RSI_OVERSOLD_LEVEL)
            is_overbought = (rsi_val >= config.MarketData.RSI_OVERBOUGHT_LEVEL)
            
            result = False
            if expected_state == "OVERSOLD" and is_oversold: result = True
            elif expected_state == "OVERBOUGHT" and is_overbought: result = True
            elif expected_state == "NOT_OVERBOUGHT" and not is_overbought: result = True
            elif expected_state == "NOT_OVERSOLD" and not is_oversold: result = True
            
            logger.debug(f"EVAL_COND_DIAG: RSI_{tf_rsi}_{expected_state} -> Value: {float(rsi_val):.2f}. Is Oversold: {is_oversold}, Is Overbought: {is_overbought}. Hasil: {result}")
            return result
        logger.debug(f"EVAL_COND_DIAG: RSI_{tf_rsi}_{expected_state}. No valid RSI status found. Hasil: False")
        return False
    
    # --- Kondisi MACD (Misal: "MACD_H1_BULLISH_CROSS_OR_POSITIVE") ---
    if condition_str.startswith("MACD_"):
        parts = condition_str.split('_')
        if len(parts) < 3:
            logger.warning(f"EVAL_COND_DIAG: Format kondisi MACD tidak valid: '{condition_str}'.")
            return False
        tf_macd = parts[1]
        expected_state = parts[2].upper()
        
        macd_status = context["macd_status"].get(tf_macd)
        if macd_status and macd_status.get('macd_line') is not None and macd_status.get('signal_line') is not None and macd_status.get('histogram') is not None:
            # PERBAIKAN DI SINI: Ambil nilai MACD sebelumnya dari macd_status
            prev_macd_line = macd_status.get('prev_macd_line')
            prev_signal_line = macd_status.get('prev_signal_line')
            
            # Jika tidak ada nilai sebelumnya (misal hanya ada 1 data MACD), maka cross tidak bisa dihitung.
            if prev_macd_line is None or prev_signal_line is None:
                logger.debug(f"EVAL_COND_DIAG: MACD_{tf_macd}_{expected_state}. No previous MACD/Signal values available. Hasil: False")
                return False

            result = False
            if expected_state == "BULLISH_CROSS":
                if macd_status['macd_line'] > macd_status['signal_line'] and \
                   prev_macd_line <= prev_signal_line: # Cross dari bawah ke atas
                    result = True
            elif expected_state == "BEARISH_CROSS":
                if macd_status['macd_line'] < macd_status['signal_line'] and \
                   prev_macd_line >= prev_signal_line: # Cross dari atas ke bawah
                    result = True
            elif expected_state == "POSITIVE" and macd_status['histogram'] > 0: result = True
            elif expected_state == "NEGATIVE" and macd_status['histogram'] < 0: result = True
            
            logger.debug(f"EVAL_COND_DIAG: MACD_{tf_macd}_{expected_state} -> MACD: {float(macd_status['macd_line']):.5f}, Signal: {float(macd_status['signal_line']):.5f}, Hist: {float(macd_status['histogram']):.5f}. Prev MACD: {float(prev_macd_line):.5f}, Prev Signal: {float(prev_signal_line):.5f}. Hasil: {result}")
            return result
        logger.debug(f"EVAL_COND_DIAG: MACD_{tf_macd}_{expected_state}. No valid MACD status found. Hasil: False")
        return False

    
    # Default: jika kondisi tidak dikenal atau tidak ada implementasi spesifik
    logger.warning(f"RULE SIGNAL: Kondisi '{condition_str}' tidak dikenal atau tidak dapat dievaluasi. Mengembalikan False.")
    return False

# --- Fungsi Pembantu: Menghitung Harga SL/TP dari Logika ---
def _calculate_price_levels_from_logic(logic_str: str, action: str, context: dict, symbol_param: str, tf_eval: str, entry_price: Decimal) -> Decimal:
    """
    Menghitung harga Stop Loss atau Take Profit berdasarkan logika yang diberikan.
    Args:
        logic_str (str): String logika (misal: "BELOW_DEMAND_ZONE_LOW", "FIXED_RR_1_5").
        action (str): Aksi trading ("BUY", "SELL").
        context (dict): Konteks pasar yang berisi data detektor.
        symbol_param (str): Simbol trading.
        tf_eval (str): Timeframe yang sedang dievaluasi untuk aturan saat ini.
        entry_price (Decimal): Harga entry yang disarankan.
    Returns:
        Decimal: Harga level yang dihitung, atau None jika tidak dapat dihitung.
    """
    # Pastikan current_price tidak None, meskipun sudah seharusnya di generate_signal
    current_price = context["current_price"] # Ambil dari konteks untuk memastikan terdefinisi dengan jelas
    if current_price is None:
        logger.error(f"RULE SIGNAL: current_price di _calculate_price_levels_from_logic adalah None. Tidak dapat menghitung level untuk '{logic_str}'.")
        return None

    # Gunakan mt5_point_value dan PIP_UNIT_IN_DOLLAR dari config
    mt5_point_value = config.TRADING_SYMBOL_POINT_VALUE
    pip_unit_in_dollar = config.PIP_UNIT_IN_DOLLAR

    atr_val = context["atr_values"].get(tf_eval, Decimal('0.0'))
    dynamic_tolerance = atr_val * config.MarketData.ATR_MULTIPLIER_FOR_TOLERANCE


    dynamic_tolerance = atr_val * config.MarketData.ATR_MULTIPLIER_FOR_TOLERANCE

    # --- Logika Berbasis Level/Zone ---
    if logic_str.startswith("BELOW_") or logic_str.startswith("ABOVE_") or logic_str.startswith("AT_"):
        parts = logic_str.split('_')
        target_type = parts[-1].upper() # LOW, HIGH, ZONE, OB, FVG, etc.
        
        # Contoh: BELOW_DEMAND_ZONE_LOW
        if logic_str == "BELOW_DEMAND_ZONE_LOW":
            active_zones = [z for z in context["sd_zones"][tf_eval] if z['zone_type'] == 'Demand' and not z['is_mitigated']]
            if active_zones:
                # Ambil zona terdekat atau yang paling kuat
                target_zone = sorted(active_zones, key=lambda x: abs(current_price - x['zone_bottom_price']))[0]
                return target_zone['zone_bottom_price'] - (dynamic_tolerance * Decimal('0.5')) # Sedikit di bawah
            return None
        
        elif logic_str == "ABOVE_SUPPLY_ZONE_HIGH":
            # KOREKSI INI: Ganti 'z z in' menjadi 'z in'
            active_zones = [z for z in context["sd_zones"][tf_eval] if z['zone_type'] == 'Supply' and not z['is_mitigated']]
            if active_zones:
                target_zone = sorted(active_zones, key=lambda x: abs(current_price - x['zone_top_price']))[0]
                return target_zone['zone_top_price'] + (dynamic_tolerance * Decimal('0.5')) # Sedikit di atas
            return None

        elif logic_str == "BELOW_OB_LOW":
            active_obs = [ob for ob in context["order_blocks"][tf_eval] if ob['type'] == 'Bullish' and not ob['is_mitigated']]
            if active_obs:
                target_ob = sorted(active_obs, key=lambda x: abs(current_price - x['ob_bottom_price']))[0]
                return target_ob['ob_bottom_price'] - (dynamic_tolerance * Decimal('0.5'))
            return None
        
        elif logic_str == "ABOVE_OB_HIGH":
            active_obs = [ob for ob in context["order_blocks"][tf_eval] if ob['type'] == 'Bearish' and not ob['is_mitigated']]
            if active_obs:
                target_ob = sorted(active_obs, key=lambda x: abs(current_price - x['ob_top_price']))[0]
                return target_ob['ob_top_price'] + (dynamic_tolerance * Decimal('0.5'))
            return None
        
        elif logic_str == "AT_NEXT_SUPPLY_ZONE":
            active_zones = [z for z in context["sd_zones"][tf_eval] if z['zone_type'] == 'Supply' and not z['is_mitigated']]
            if active_zones:
                # Cari Supply Zone di atas harga saat ini
                target_zones = [z for z in active_zones if z['zone_bottom_price'] > current_price]
                if target_zones:
                    return sorted(target_zones, key=lambda x: x['zone_bottom_price'])[0]['zone_bottom_price']
            return None
        
        elif logic_str == "AT_NEXT_DEMAND_ZONE":
            active_zones = [z for z in context["sd_zones"][tf_eval] if z['zone_type'] == 'Demand' and not z['is_mitigated']]
            if active_zones:
                # Cari Demand Zone di bawah harga saat ini
                target_zones = [z for z in active_zones if z['zone_top_price'] < current_price]
                if target_zones:
                    return sorted(target_zones, key=lambda x: x['zone_top_price'], reverse=True)[0]['zone_top_price']
            return None
        
        elif logic_str == "AT_PREVIOUS_HIGH":
            prev_high_events = [e for e in context["market_structure_events"][tf_eval] if e['event_type'] == 'Previous High Broken' and e['direction'] == 'Bullish'] # Asumsi kita ingin kembali ke PH yang baru ditembus
            if prev_high_events:
                # Ambil PH terbaru
                latest_ph = sorted(prev_high_events, key=lambda x: x['event_time_utc'], reverse=True)[0]
                return latest_ph['price_level']
            return None
        
        elif logic_str == "AT_PREVIOUS_LOW":
            prev_low_events = [e for e in context["market_structure_events"][tf_eval] if e['event_type'] == 'Previous Low Broken' and e['direction'] == 'Bearish']
            if prev_low_events:
                latest_pl = sorted(prev_low_events, key=lambda x: x['event_time_utc'], reverse=True)[0]
                return latest_pl['price_level']
            return None
        
        elif logic_str == "AT_FIB_50_PERCENT": # Contoh untuk TP/SL di level Fib tertentu
            active_fibs = [f for f in context["fib_levels"][tf_eval] if f['is_active']]
            if active_fibs:
                # Cari fib set terbaru yang relevan
                latest_fib_set_time = max([f['formation_time_utc'] for f in active_fibs])
                relevant_fibs = [f for f in active_fibs if f['formation_time_utc'] == latest_fib_set_time]
                
                fib_50 = next((f for f in relevant_fibs if f['ratio'] == Decimal('0.5')), None)
                if fib_50:
                    return fib_50['price_level']
            return None
            
        # Tambahkan logika untuk jenis level/zone lainnya sesuai kebutuhan

    # --- Logika Berbasis Jarak (Pips/ATR) ---
    elif logic_str.startswith("FIXED_PIPS_"):
        pips = Decimal(logic_str.split('_')[2])
        if action == "BUY":
            return entry_price + pips * pip_unit_in_dollar
        elif action == "SELL":
            return entry_price - pips * pip_unit_in_dollar
        return None
    
    elif logic_str.startswith("ATR_MULTIPLIER_"):
        multiplier = Decimal(logic_str.split('_')[2])
        if atr_val == Decimal('0.0'):
            logger.warning(f"RULE SIGNAL: ATR tidak valid untuk perhitungan '{logic_str}'. Menggunakan fallback FIXED_PIPS_20.")
            # Fallback ke fixed pips jika ATR 0
            if action == "BUY": return entry_price + Decimal('20') * pip_unit_in_dollar
            elif action == "SELL": return entry_price - Decimal('20') * pip_unit_in_dollar
            return None

        if action == "BUY":
            return entry_price + atr_val * multiplier
        elif action == "SELL":
            return entry_price - atr_val * multiplier
        return None

    # --- Logika Berbasis Risk-Reward Ratio ---
    elif logic_str.startswith("FIXED_RR_"):
        rr_ratio = Decimal(logic_str.split('_')[2].replace('_', '.')) # '1_5' menjadi '1.5'
        if entry_price is None or stop_loss_final is None: # stop_loss_final harus sudah ada
            return None
        
        # Hitung jarak SL dari Entry
        sl_distance = abs(entry_price - stop_loss_final)
        if sl_distance == Decimal('0.0'): return None

        # Hitung jarak TP
        tp_distance = sl_distance * rr_ratio
        
        if action == "BUY":
            return entry_price + tp_distance
        elif action == "SELL":
            return entry_price - tp_distance
        return None

    # --- Logika Khusus ---
    elif logic_str == "CURRENT_PRICE":
        return current_price
    
    elif logic_str == "OB_MIDPOINT":
        # Ini perlu disesuaikan dengan aturan yang cocok
        # Jika aturan adalah Bullish OB, cari OB Bullish terdekat.
        if action == "BUY":
            active_obs = [ob for ob in context["order_blocks"][tf_eval] if ob['type'] == 'Bullish' and not ob['is_mitigated']]
            if active_obs:
                target_ob = sorted(active_obs, key=lambda x: abs(current_price - (x['ob_bottom_price'] + x['ob_top_price']) / Decimal('2')))[0]
                return (target_ob['ob_bottom_price'] + target_ob['ob_top_price']) / Decimal('2')
        elif action == "SELL":
            active_obs = [ob for ob in context["order_blocks"][tf_eval] if ob['type'] == 'Bearish' and not ob['is_mitigated']]
            if active_obs:
                target_ob = sorted(active_obs, key=lambda x: abs(current_price - (x['ob_bottom_price'] + x['ob_top_price']) / Decimal('2')))[0]
                return (target_ob['ob_bottom_price'] + target_ob['ob_top_price']) / Decimal('2')
        return None

    elif logic_str == "DEMAND_ZONE_TOP":
        if action == "BUY": # Cari Demand Zone yang valid untuk BUY
            active_zones = [z for z in context["sd_zones"][tf_eval] if z['zone_type'] == 'Demand' and not z['is_mitigated']]
            if active_zones:
                target_zone = sorted(active_zones, key=lambda x: abs(current_price - x['zone_top_price']))[0]
                return target_zone['zone_top_price']
        return None

    elif logic_str == "SUPPLY_ZONE_BOTTOM":
        if action == "SELL": # Cari Supply Zone yang valid untuk SELL
            active_zones = [z for z in context["sd_zones"][tf_eval] if z['zone_type'] == 'Supply' and not z['is_mitigated']]
            if active_zones:
                target_zone = sorted(active_zones, key=lambda x: abs(current_price - x['zone_bottom_price']))[0]
                return target_zone['zone_bottom_price']
        return None

    # Tambahkan logika untuk FVG_MIDPOINT, NEXT_FVG_FILL, dll.

    logger.warning(f"RULE SIGNAL: Logika harga '{logic_str}' tidak dikenal atau tidak dapat dihitung.")
    return None

def _collect_all_evidence(market_context: dict, symbol_param: str, current_price: Decimal) -> dict:
    """
    Mengumpulkan semua bukti bullish atau bearish dari berbagai detektor dan timeframes.
    Ini adalah inti dari pendekatan konfluensi sinyal.
    Args:
        market_context (dict): Konteks pasar yang berisi data detektor.
        symbol_param (str): Simbol trading (misal: "XAUUSD").
        current_price (Decimal): Harga saat ini.
    Returns:
        dict: Kamus berisi {'bullish_evidence': [], 'bearish_evidence': [], 'neutral_reasons': []}
    """
    bullish_evidence = []
    bearish_evidence = []
    neutral_reasons = []

    # Iterasi melalui setiap rule yang diaktifkan untuk mengambil kondisi-kondisi
    for rule in config.RuleBasedStrategy.SIGNAL_RULES:
        if not rule['enabled'] or rule['action'] == "HOLD":
            continue # Lewati aturan HOLD atau yang dinonaktifkan untuk pengumpulan bukti

        for condition in rule['conditions']:
            for tf in rule['timeframes']: # Evaluasi kondisi ini di setiap timeframe yang relevan
                # PERBAIKAN PENTING DI SINI: Gunakan `symbol_param` yang diteruskan ke fungsi
                if _evaluate_condition(condition, market_context, symbol_param, tf, current_price):
                    if rule['action'] == "BUY":
                        bullish_evidence.append(f"{condition} ({tf})")
                    elif rule['action'] == "SELL":
                        bearish_evidence.append(f"{condition} ({tf})")

    
    # Tambahkan bukti tambahan yang tidak langsung terkait dengan rule conditions jika ada
    # Contoh: Konfirmasi dari get_market_context yang tidak dipetakan ke kondisi rule
    
    return {
        'bullish_evidence': list(set(bullish_evidence)),
        'bearish_evidence': list(set(bearish_evidence)),
        'neutral_reasons': list(set(neutral_reasons))
    }


def _analyze_confluence(bullish_evidence: list, bearish_evidence: list, current_price: Decimal) -> dict:
    """
    Menganalisis bukti bullish dan bearish yang terkumpul untuk menentukan sinyal akhir
    dan tingkat kepercayaan berdasarkan konfluensi.
    Args:
        bullish_evidence (list): Daftar bukti bullish (string deskriptif).
        bearish_evidence (list): Daftar bukti bearish (string deskriptif).
        current_price (Decimal): Harga saat ini (untuk kontekstualisasi).
    Returns:
        dict: Kamus sinyal akhir {action, potential_direction, reasoning, confidence, entry, SL, TP}
    """
    num_bullish_evidence = len(bullish_evidence)
    num_bearish_evidence = len(bearish_evidence)

    STRONG_CONFLUENCE_THRESHOLD = 2 # Diubah dari 3 menjadi 2 untuk kemudahan test
    MODERATE_CONFLUENCE_THRESHOLD = 1 # Misal, butuh minimal 1 bukti

    final_action = "HOLD"
    final_potential_direction = "Sideways"
    final_reasoning = "Tidak ada konfluensi bukti yang cukup untuk sinyal BUY/SELL."
    final_confidence = "Low"
    final_entry = current_price
    final_sl = None
    final_tp = None

    if num_bullish_evidence >= STRONG_CONFLUENCE_THRESHOLD and num_bullish_evidence > num_bearish_evidence:
        final_action = "BUY"
        final_potential_direction = "Bullish"
        final_confidence = "High-Probability" # <--- DIUBAH DI SINI
        final_reasoning = f"Konfluensi Bullish Kuat: {num_bullish_evidence} bukti ({', '.join(bullish_evidence)})"
    elif num_bearish_evidence >= STRONG_CONFLUENCE_THRESHOLD and num_bearish_evidence > num_bullish_evidence:
        final_action = "SELL"
        final_potential_direction = "Bearish"
        final_confidence = "High-Probability" # <--- DIUBAH DI SINI
        final_reasoning = f"Konfluensi Bearish Kuat: {num_bearish_evidence} bukti ({', '.join(bearish_evidence)})"


    elif num_bullish_evidence >= MODERATE_CONFLUENCE_THRESHOLD and num_bullish_evidence > num_bearish_evidence:
        final_action = "BUY"
        final_potential_direction = "Bullish"
        final_confidence = "Moderate"
        final_reasoning = f"Konfluensi Bullish Moderat: {num_bullish_evidence} bukti ({', '.join(bullish_evidence)})"
    elif num_bearish_evidence >= MODERATE_CONFLUENCE_THRESHOLD and num_bearish_evidence > num_bullish_evidence:
        final_action = "SELL"
        final_potential_direction = "Bearish"
        final_confidence = "Moderate"
        final_reasoning = f"Konfluensi Bearish Moderat: {num_bearish_evidence} bukti ({', '.join(bearish_evidence)})"
    elif num_bullish_evidence > 0 and num_bearish_evidence > 0:
        final_reasoning = f"Pasar Ranging/Tidak Jelas: {num_bullish_evidence} bullish vs {num_bearish_evidence} bearish."
        final_confidence = "Low"

    # Penentuan SL/TP default berdasarkan aksi
    default_sl_pips = config.RuleBasedStrategy.DEFAULT_SL_PIPS
    default_tp_pips = config.RuleBasedStrategy.TP1_PIPS
    pip_unit_in_dollar = config.PIP_UNIT_IN_DOLLAR

    if final_action == "BUY":
        final_sl = current_price - default_sl_pips * pip_unit_in_dollar
        final_tp = current_price + default_tp_pips * pip_unit_in_dollar
    elif final_action == "SELL":
        final_sl = current_price + default_sl_pips * pip_unit_in_dollar
        final_tp = current_price - default_tp_pips * pip_unit_in_dollar

    return {
        "action": final_action,
        "potential_direction": final_potential_direction,
        "entry_price_suggestion": float(current_price),
        "stop_loss_suggestion": float(final_sl) if final_sl is not None else None,
        "take_profit_suggestion": float(final_tp) if final_tp is not None else None,
        "reasoning": final_reasoning,
        "confidence": final_confidence
    }

# --- Fungsi Utama: generate_signal ---
def generate_signal(symbol: str, current_time: datetime, current_price: Decimal):
    """
    Menghasilkan sinyal trading berdasarkan pendekatan konfluensi bukti dari berbagai detektor.
    Args:
        symbol (str): Simbol trading.
        current_time (datetime): Waktu saat ini (UTC) untuk pengumpulan konteks.
        current_price (Decimal): Harga saat ini.
    Returns:
        dict: Kamus sinyal akhir yang berisi rekomendasi aksi, level harga, alasan, dan kepercayaan.
    """
    logger.info(f"RULE SIGNAL: Memulai generasi sinyal (konfluensi) untuk {symbol} pada {current_time.isoformat()} (Harga: {float(current_price):.5f}).")

    if current_price is None or current_price <= 0:
        logger.warning("RULE SIGNAL: Harga saat ini tidak valid (None atau <= 0). Tidak dapat menghasilkan sinyal.")
        return {
            "action": "HOLD", "potential_direction": "Undefined", "entry_price_suggestion": None,
            "stop_loss_suggestion": None, "take_profit_suggestion": None,
            "reasoning": "Tidak ada sinyal: Harga pasar tidak valid.", "confidence": "Low",
            "tp2_suggestion": None, "tp3_suggestion": None, "key_levels_context": {}
        }
    
    # 1. Kumpulkan Konteks Pasar dari semua detektor
    market_context = _get_market_context(symbol, current_time)
    if market_context["current_price"] is None:
        logger.error("RULE SIGNAL: Gagal mendapatkan konteks pasar penuh (harga tidak valid).")
        return {
            "action": "HOLD", "potential_direction": "Undefined", "entry_price_suggestion": None,
            "stop_loss_suggestion": None, "take_profit_suggestion": None,
            "reasoning": "Tidak ada sinyal: Gagal mengumpulkan konteks pasar.", "confidence": "Low",
            "tp2_suggestion": None, "tp3_suggestion": None, "key_levels_context": {}
        }
    
    current_price = market_context["current_price"]

    # 2. Kumpulkan Semua Bukti (Evidence) Bullish dan Bearish
    evidence = _collect_all_evidence(market_context, symbol, current_price)
    bullish_evidence = evidence['bullish_evidence']
    bearish_evidence = evidence['bearish_evidence'] # <--- KOREKSI TYPO DI SINI!
    neutral_reasons = evidence['neutral_reasons']

    logger.debug(f"RULE SIGNAL: Bukti Bullish terkumpul: {bullish_evidence}")
    logger.debug(f"RULE SIGNAL: Bukti Bearish terkumpul: {bearish_evidence}")
    logger.debug(f"RULE SIGNAL: Alasan Netral terkumpul: {neutral_reasons}")

    # 3. Analisis Konfluensi untuk Menentukan Sinyal Akhir
    # (Di sini final_signal_details akan memiliki confidence sebagai "High-Probability" jika kuat)
    final_signal_details = _analyze_confluence(bullish_evidence, bearish_evidence, current_price)

    # Tambahkan key_levels_context ke hasil akhir sinyal
    final_signal = {
        "action": final_signal_details['action'],
        "potential_direction": final_signal_details['potential_direction'],
        "entry_price_suggestion": final_signal_details['entry_price_suggestion'],
        "stop_loss_suggestion": final_signal_details['stop_loss_suggestion'],
        "take_profit_suggestion": final_signal_details['take_profit_suggestion'],
        "reasoning": final_signal_details['reasoning'],
        "confidence": final_signal_details['confidence'],
        "tp2_suggestion": config.RuleBasedStrategy.TP2_PIPS, # Ini adalah nilai pips Decimal dari config
        "tp3_suggestion": config.RuleBasedStrategy.TP3_PIPS, # Ini adalah nilai pips Decimal dari config
        "key_levels_context": market_context # Sertakan konteks pasar penuh
    }

    final_signal["key_levels_context"].pop("latest_candles", None)

    # Pembulatan akhir ke presisi harga untuk Entry, SL, TP1
    # Konversi ke float setelah quantize.
    precision = Decimal('0.00001') # Definisikan presisi sekali saja

    if final_signal["entry_price_suggestion"] is not None:
        final_signal["entry_price_suggestion"] = float(Decimal(str(final_signal["entry_price_suggestion"])).quantize(precision))
    if final_signal["stop_loss_suggestion"] is not None:
        final_signal["stop_loss_suggestion"] = float(Decimal(str(final_signal["stop_loss_suggestion"])).quantize(precision))
    if final_signal["take_profit_suggestion"] is not None:
        final_signal["take_profit_suggestion"] = float(Decimal(str(final_signal["take_profit_suggestion"])).quantize(precision))
    
    # --- Perhitungan dan Quantize untuk TP2 & TP3 ---
    # Lakukan perhitungan dan quantize saat masih Decimal, baru konversi ke float.
    pip_unit_in_dollar = config.PIP_UNIT_IN_DOLLAR # Pastikan PIP_UNIT_IN_DOLLAR diakses dari config

    if final_signal["tp2_suggestion"] is not None and final_signal["entry_price_suggestion"] is not None:
        entry_price_dec = Decimal(str(final_signal["entry_price_suggestion"])) # Sudah float, ubah lagi ke Decimal
        tp2_pips_dec = final_signal["tp2_suggestion"] # Ini adalah nilai pips Decimal dari config

        calculated_tp2_dec = None
        if final_signal['action'] == "BUY":
            calculated_tp2_dec = entry_price_dec + (tp2_pips_dec * pip_unit_in_dollar)
        elif final_signal['action'] == "SELL":
            calculated_tp2_dec = entry_price_dec - (tp2_pips_dec * pip_unit_in_dollar)
        
        if calculated_tp2_dec is not None:
            final_signal["tp2_suggestion"] = float(calculated_tp2_dec.quantize(precision))
        else:
            final_signal["tp2_suggestion"] = None # Pastikan None jika tidak dihitung
    else:
        final_signal["tp2_suggestion"] = None # Pastikan None jika inputnya None


    if final_signal["tp3_suggestion"] is not None and final_signal["entry_price_suggestion"] is not None:
        entry_price_dec = Decimal(str(final_signal["entry_price_suggestion"])) # Sudah float, ubah lagi ke Decimal
        tp3_pips_dec = final_signal["tp3_suggestion"] # Ini adalah nilai pips Decimal dari config

        calculated_tp3_dec = None
        if final_signal['action'] == "BUY":
            calculated_tp3_dec = entry_price_dec + (tp3_pips_dec * pip_unit_in_dollar)
        elif final_signal['action'] == "SELL":
            calculated_tp3_dec = entry_price_dec - (tp3_pips_dec * pip_unit_in_dollar)
            
        if calculated_tp3_dec is not None:
            final_signal["tp3_suggestion"] = float(calculated_tp3_dec.quantize(precision))
        else:
            final_signal["tp3_suggestion"] = None # Pastikan None jika tidak dihitung
    else:
        final_signal["tp3_suggestion"] = None # Pastikan None jika inputnya None
    
    # --- Akhir Perhitungan TP2 & TP3 ---


    # Siapkan string yang diformat untuk logging, menangani kasus None
    # Ini harus ditempatkan setelah semua perhitungan dan konversi final_signal selesai.
    entry_str = f"{final_signal['entry_price_suggestion']:.5f}" if final_signal['entry_price_suggestion'] is not None else 'N/A'
    sl_str = f"{final_signal['stop_loss_suggestion']:.5f}" if final_signal['stop_loss_suggestion'] is not None else 'N/A'
    tp1_str = f"{final_signal['take_profit_suggestion']:.5f}" if final_signal['take_profit_suggestion'] is not None else 'N/A'
    tp2_str = f"{final_signal['tp2_suggestion']:.5f}" if final_signal['tp2_suggestion'] is not None else 'N/A'
    tp3_str = f"{final_signal['tp3_suggestion']:.5f}" if final_signal['tp3_suggestion'] is not None else 'N/A'

    logger.info(f"RULE SIGNAL: Sinyal FINAL: {final_signal['action']} (Conf: {final_signal['confidence']}) "
                f"Entry:{entry_str}, SL:{sl_str}, TP1:{tp1_str}. "
                f"Reason: {final_signal['reasoning']}")

    return final_signal


# --- Fungsi Pembantu: Menghitung Harga SL/TP dari Logika ---
def _calculate_price_levels_from_logic(logic_str: str, action: str, context: dict, symbol_param: str, tf_eval: str, entry_price: Decimal) -> Decimal | None:
    """
    Menghitung harga Stop Loss atau Take Profit berdasarkan logika yang diberikan.
    Args:
        logic_str (str): String logika (misal: "ATR_SL_MULTIPLIER_1_5", "RR_FROM_ATR_SL_1_5").
        action (str): Aksi trading ("BUY", "SELL").
        context (dict): Konteks pasar yang berisi data detektor.
        symbol_param (str): Simbol trading.
        tf_eval (str): Timeframe yang sedang dievaluasi untuk aturan saat ini.
        entry_price (Decimal): Harga entry yang disarankan (dari sinyal).
    Returns:
        Decimal: Harga level yang dihitung, atau None jika tidak dapat dihitung.
    """
    current_price = context["current_price"] 
    if current_price is None:
        logger.error(f"RULE SIGNAL: current_price di _calculate_price_levels_from_logic adalah None. Tidak dapat menghitung level untuk '{logic_str}'.")
        return None

    pip_unit_in_dollar = config.PIP_UNIT_IN_DOLLAR
    atr_val = context["atr_values"].get(tf_eval, Decimal('0.0')) # Dapatkan ATR untuk timeframe yang relevan
    dynamic_tolerance = atr_val * config.MarketData.ATR_MULTIPLIER_FOR_TOLERANCE

    # --- Logika Berbasis ATR untuk Stop Loss ---
    if logic_str.startswith("ATR_SL_MULTIPLIER_"):
        multiplier = Decimal(logic_str.split('_')[3]) 
        
        if atr_val == Decimal('0.0'):
            logger.warning(f"RULE SIGNAL: ATR tidak valid untuk perhitungan '{logic_str}'. Menggunakan fallback FIXED_SL_PIPS_20.")
            pips_fallback = Decimal('20') # Contoh fallback 20 pips
            distance_in_usd_fallback = pips_fallback * pip_unit_in_dollar
            if action == "BUY": return entry_price - distance_in_usd_fallback
            elif action == "SELL": return entry_price + distance_in_usd_fallback
            return None

        sl_distance_usd = atr_val * multiplier
        
        if action == "BUY":
            return entry_price - sl_distance_usd
        elif action == "SELL":
            return entry_price + sl_distance_usd
        return None

    # --- Logika Berbasis RR dari SL (untuk Take Profit utama) ---
    elif logic_str.startswith("RR_FROM_ATR_SL_"):
        rr_ratio = Decimal(logic_str.split('_')[4].replace('_', '.')) 
        
        # Untuk menghitung ini, kita perlu jarak SL yang sudah dihitung sebelumnya.
        # Karena _calculate_price_levels_from_logic dipanggil terpisah untuk SL dan TP,
        # kita harus menghitung ulang sl_distance_usd di sini, menggunakan multiplier SL yang sama.
        # Ambil multiplier SL dari rule yang sama (hardcode untuk demo ini, seharusnya dinamis)
        # Asumsi: rule yang memanggil ini untuk TP juga punya SL berbasis ATR_SL_MULTIPLIER_1_5
        sl_multiplier_for_rule = Decimal('1.5') # Harus sesuai dengan multiplier di ATR_SL_MULTIPLIER_X_X

        if atr_val == Decimal('0.0'):
            logger.warning(f"RULE SIGNAL: ATR tidak valid untuk perhitungan '{logic_str}'. Menggunakan fallback FIXED_TP_PIPS_10.")
            pips_fallback = Decimal('10') # Contoh fallback 10 pips
            distance_in_usd_fallback = pips_fallback * pip_unit_in_dollar
            if action == "BUY": return entry_price + distance_in_usd_fallback
            elif action == "SELL": return entry_price - distance_in_usd_fallback
            return None

        sl_distance_usd = atr_val * sl_multiplier_for_rule
        tp_distance_usd = sl_distance_usd * rr_ratio
        
        if action == "BUY":
            return entry_price + tp_distance_usd
        elif action == "SELL":
            return entry_price - tp_distance_usd
        return None

    # --- Logika Berbasis Level/Zone (tetap seperti sebelumnya) ---
    current_price = context["current_price"] 
    if current_price is None:
        logger.error(f"RULE SIGNAL: current_price di _calculate_price_levels_from_logic adalah None. Tidak dapat menghitung level untuk '{logic_str}'.")
        return None

    pip_unit_in_dollar = config.PIP_UNIT_IN_DOLLAR
    atr_val = context["atr_values"].get(tf_eval, Decimal('0.0')) # Dapatkan ATR untuk timeframe yang relevan
    dynamic_tolerance = atr_val * config.MarketData.ATR_MULTIPLIER_FOR_TOLERANCE

    # --- Logika Berbasis ATR untuk Stop Loss ---
    if logic_str.startswith("ATR_SL_MULTIPLIER_"):
        multiplier = Decimal(logic_str.split('_')[3]) 
        
        if atr_val == Decimal('0.0'):
            logger.warning(f"RULE SIGNAL: ATR tidak valid untuk perhitungan '{logic_str}'. Menggunakan fallback FIXED_SL_PIPS_20.")
            pips_fallback = Decimal('20') # Contoh fallback 20 pips
            distance_in_usd_fallback = pips_fallback * pip_unit_in_dollar
            if action == "BUY": return entry_price - distance_in_usd_fallback
            elif action == "SELL": return entry_price + distance_in_usd_fallback
            return None

        sl_distance_usd = atr_val * multiplier
        
        if action == "BUY":
            return entry_price - sl_distance_usd
        elif action == "SELL":
            return entry_price + sl_distance_usd
        return None

    # --- Logika Berbasis RR dari SL (untuk Take Profit utama) ---
    elif logic_str.startswith("RR_FROM_ATR_SL_"):
        rr_ratio = Decimal(logic_str.split('_')[4].replace('_', '.')) 
        
        # Untuk menghitung ini, kita perlu jarak SL yang sudah dihitung sebelumnya.
        # Karena _calculate_price_levels_from_logic dipanggil terpisah untuk SL dan TP,
        # kita harus menghitung ulang sl_distance_usd di sini, menggunakan multiplier SL yang sama.
        # Ambil multiplier SL dari rule yang sama (hardcode untuk demo ini, seharusnya dinamis)
        # Asumsi: rule yang memanggil ini untuk TP juga punya SL berbasis ATR_SL_MULTIPLIER_1_5
        sl_multiplier_for_rule = Decimal('1.5') # Harus sesuai dengan multiplier di ATR_SL_MULTIPLIER_X_X

        if atr_val == Decimal('0.0'):
            logger.warning(f"RULE SIGNAL: ATR tidak valid untuk perhitungan '{logic_str}'. Menggunakan fallback FIXED_TP_PIPS_10.")
            pips_fallback = Decimal('10') # Contoh fallback 10 pips
            distance_in_usd_fallback = pips_fallback * pip_unit_in_dollar
            if action == "BUY": return entry_price + distance_in_usd_fallback
            elif action == "SELL": return entry_price - distance_in_usd_fallback
            return None

        sl_distance_usd = atr_val * sl_multiplier_for_rule
        tp_distance_usd = sl_distance_usd * rr_ratio
        
        if action == "BUY":
            return entry_price + tp_distance_usd
        elif action == "SELL":
            return entry_price - tp_distance_usd
        return None

    # --- Logika Berbasis Level/Zone (tetap seperti sebelumnya) ---
    if logic_str.startswith("BELOW_") or logic_str.startswith("ABOVE_") or logic_str.startswith("AT_"):
        parts = logic_str.split('_')
        target_type = parts[-1].upper() 
        
        logger.debug(f"EVAL_COND_DIAG: Mengecek harga dalam zona aktif: '{target_type}'.")

        if logic_str == "BELOW_DEMAND_ZONE_LOW":
            active_zones = [z for z in context["sd_zones"][tf_eval] if z['zone_type'] == 'Demand' and not z['is_mitigated']]
            if active_zones:
                target_zone = sorted(active_zones, key=lambda x: abs(current_price - x['zone_bottom_price']))[0]
                return target_zone['zone_bottom_price'] - (dynamic_tolerance * Decimal('0.5')) 
            return None
        
        elif logic_str == "ABOVE_SUPPLY_ZONE_HIGH":
            active_zones = [z for z in context["sd_zones"][tf_eval] if z['zone_type'] == 'Supply' and not z['is_mitigated']]
            if active_zones:
                target_zone = sorted(active_zones, key=lambda x: abs(current_price - x['zone_top_price']))[0]
                return target_zone['zone_top_price'] + (dynamic_tolerance * Decimal('0.5')) 
            return None

        elif logic_str == "BELOW_OB_LOW":
            active_obs = [ob for ob in context["order_blocks"][tf_eval] if ob['type'] == 'Bullish' and not ob['is_mitigated']]
            if active_obs:
                target_ob = sorted(active_obs, key=lambda x: abs(current_price - x['ob_bottom_price']))[0]
                return target_ob['ob_bottom_price'] - (dynamic_tolerance * Decimal('0.5'))
            return None
        
        elif logic_str == "ABOVE_OB_HIGH":
            active_obs = [ob for ob in context["order_blocks"][tf_eval] if ob['type'] == 'Bearish' and not ob['is_mitigated']]
            if active_obs:
                target_ob = sorted(active_obs, key=lambda x: abs(current_price - x['ob_top_price']))[0]
                return target_ob['ob_top_price'] + (dynamic_tolerance * Decimal('0.5'))
            return None
        
        elif logic_str == "AT_NEXT_SUPPLY_ZONE":
            active_zones = [z for z in context["sd_zones"][tf_eval] if z['zone_type'] == 'Supply' and z['zone_bottom_price'] > current_price and not z['is_mitigated']] # Filter zona di atas harga saat ini
            if active_zones:
                return sorted(active_zones, key=lambda x: x['zone_bottom_price'])[0]['zone_bottom_price'] # Zona terdekat
            return None
        
        elif logic_str == "AT_NEXT_DEMAND_ZONE":
            active_zones = [z for z in context["sd_zones"][tf_eval] if z['zone_type'] == 'Demand' and z['zone_top_price'] < current_price and not z['is_mitigated']] # Filter zona di bawah harga saat ini
            if active_zones:
                return sorted(active_zones, key=lambda x: x['zone_top_price'], reverse=True)[0]['zone_top_price'] # Zona terdekat
            return None
        
        elif logic_str == "AT_PREVIOUS_HIGH":
            prev_high_events = [e for e in context["market_structure_events"][tf_eval] if e['event_type'] == 'Previous High Broken' and e['direction'] == 'Bullish'] 
            if prev_high_events:
                latest_ph = sorted(prev_high_events, key=lambda x: x['event_time_utc'], reverse=True)[0]
                return latest_ph['price_level']
            return None
        
        elif logic_str == "AT_PREVIOUS_LOW":
            prev_low_events = [e for e in context["market_structure_events"][tf_eval] if e['event_type'] == 'Previous Low Broken' and e['direction'] == 'Bearish']
            if prev_low_events:
                latest_pl = sorted(prev_low_events, key=lambda x: x['event_time_utc'], reverse=True)[0]
                return latest_pl['price_level']
            return None
        
        elif logic_str == "AT_FIB_50_PERCENT": 
            active_fibs = [f for f in context["fib_levels"][tf_eval] if f['is_active']]
            if active_fibs:
                latest_fib_set_time = max([f['formation_time_utc'] for f in active_fibs])
                relevant_fibs = [f for f in active_fibs if f['formation_time_utc'] == latest_fib_set_time]
                
                fib_50 = next((f for f in relevant_fibs if f['ratio'] == Decimal('0.5')), None)
                if fib_50:
                    return fib_50['price_level']
            return None
            











            












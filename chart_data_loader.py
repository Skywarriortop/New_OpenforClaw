# chart_data_loader.py

import logging
from datetime import datetime, timezone, timedelta
import pandas as pd
from decimal import Decimal

import database_manager
import utils
from utils import to_float_or_none
from config import config

logger = logging.getLogger(__name__)

def load_all_chart_overlays(
    chart_symbol: str,
    chart_timeframe: str,
    start_time_for_features: datetime,
    end_time_for_features: datetime,
    price_min_filter: Decimal,
    price_max_filter: Decimal,
    include_ma_param: bool,
    include_sr_param: bool,
    include_sd_ob_param: bool,
    include_fvg_liq_param: bool,
    include_ms_fib_param: bool,
    include_signals_param: bool,
    include_volume_profile_param: bool,
    min_strength_score_param: int = 0,
    include_key_levels_only_param: bool = False
):
    sr_levels_data = []
    sd_zones_data = []
    order_blocks_data = []
    fair_value_gaps_data = []
    market_structure_events_data = []
    liquidity_zones_data = []
    fibonacci_levels_data = []
    ma_data_dict = {}
    signals_to_plot = None
    ms_events_data = []
    volume_profiles_data = []

    logger.info(f"CHART_DATA_LOADER: Memulai memuat overlay dengan rentang waktu {start_time_for_features} - {end_time_for_features}")
    if price_min_filter is not None and price_max_filter is not None:
        logger.info(f"CHART_DATA_LOADER: Filter harga aktif: {to_float_or_none(price_min_filter):.2f} - {to_float_or_none(price_max_filter):.2f}")
    else:
        logger.info("CHART_DATA_LOADER: Filter harga TIDAK aktif.")

    level_fetch_limit = config.AIAnalysts.CHART_MAX_SR_TO_PLOT * 2
    if level_fetch_limit == 0:
        level_fetch_limit = 100
    logger.debug(f"CHART_DATA_LOADER: Menggunakan level_fetch_limit: {level_fetch_limit}")

    if include_ma_param:
        try:
            logger.debug("CHART_DATA_LOADER: Memuat data Moving Averages.")
            sma50_raw = database_manager.get_moving_averages(chart_symbol, chart_timeframe, 'SMA', 50,
                                                              start_time_utc=start_time_for_features, end_time_utc=end_time_for_features)
            if sma50_raw:
                df_ma_50 = pd.DataFrame(sma50_raw).sort_values(by='timestamp_utc', ascending=True)
                df_ma_50['timestamp_utc'] = pd.to_datetime(df_ma_50['timestamp_utc'])
                df_ma_50 = df_ma_50.set_index('timestamp_utc')
                ma_data_dict["SMA 50"] = df_ma_50['value']
                logger.debug(f"CHART_DATA_LOADER: Loaded SMA 50 points: {len(df_ma_50)}. Contoh terakhir: {to_float_or_none(ma_data_dict['SMA 50'].iloc[-1]):.5f}")

            sma200_raw = database_manager.get_moving_averages(chart_symbol, chart_timeframe, 'SMA', 200,
                                                               start_time_utc=start_time_for_features, end_time_utc=end_time_for_features)
            if sma200_raw:
                df_ma_200 = pd.DataFrame(sma200_raw).sort_values(by='timestamp_utc', ascending=True)
                df_ma_200['timestamp_utc'] = pd.to_datetime(df_ma_200['timestamp_utc'])
                df_ma_200 = df_ma_200.set_index('timestamp_utc')
                ma_data_dict["SMA 200"] = df_ma_200['value']
                logger.debug(f"CHART_DATA_LOADER: Loaded SMA 200 points: {len(df_ma_200)}. Contoh terakhir: {to_float_or_none(ma_data_dict['SMA 200'].iloc[-1]):.5f}")
        except Exception as e:
            logger.error(f"Gagal memuat MA data: {e}", exc_info=True)
            ma_data_dict = {}

    if include_sr_param:
        try:
            logger.debug("CHART_DATA_LOADER: Memuat data Support/Resistance Levels.")
            sr_levels_data = database_manager.get_support_resistance_levels(
                symbol=chart_symbol, timeframe=chart_timeframe, is_active=True,
                start_time_utc=start_time_for_features, end_time_utc=end_time_for_features,
                min_price_level=price_min_filter, max_price_level=price_max_filter,
                min_strength_score=min_strength_score_param, is_key_level=include_key_levels_only_param,
                limit=level_fetch_limit
            )
        except Exception as e:
            logger.error(f"Gagal memuat SR levels: {e}", exc_info=True)
            sr_levels_data = []

    if include_sd_ob_param:
        try:
            logger.debug("CHART_DATA_LOADER: Memuat data Supply/Demand Zones dan Order Blocks.")
            sd_zones_data = database_manager.get_supply_demand_zones(
                symbol=chart_symbol, timeframe=chart_timeframe, is_mitigated=False,
                start_time_utc=start_time_for_features, end_time_for_features=end_time_for_features,
                min_price_level=price_min_filter, max_price_level=price_max_filter,
                min_strength_score=min_strength_score_param, is_key_level=include_key_levels_only_param,
                limit=level_fetch_limit
            )
            order_blocks_data = database_manager.get_order_blocks(
                symbol=chart_symbol, timeframe=chart_timeframe, is_mitigated=False,
                start_time_utc=start_time_for_features, end_time_for_features=end_time_for_features,
                min_price_level=price_min_filter, max_price_level=price_max_filter,
                min_strength_score=min_strength_score_param,
                limit=level_fetch_limit
            )
            if sd_zones_data:
                logger.debug(f"CHART_DATA_LOADER: Loaded {len(sd_zones_data)} SD zones. Contoh: Type={sd_zones_data[0]['zone_type']}, Top={to_float_or_none(sd_zones_data[0]['zone_top_price']):.5f}")
            if order_blocks_data:
                logger.debug(f"CHART_DATA_LOADER: Loaded {len(order_blocks_data)} OBs. Contoh: Type={order_blocks_data[0]['type']}, Top={to_float_or_none(order_blocks_data[0]['ob_top_price']):.5f}")
        except Exception as e:
            logger.error(f"Gagal memuat SD/OB data: {e}", exc_info=True)
            sd_zones_data = []
            order_blocks_data = []

    if include_fvg_liq_param:
        try:
            logger.debug("CHART_DATA_LOADER: Memuat data Fair Value Gaps dan Liquidity Zones.")
            fair_value_gaps_data = database_manager.get_fair_value_gaps(
                symbol=chart_symbol, timeframe=chart_timeframe, is_filled=False,
                start_time_utc=start_time_for_features, end_time_for_features=end_time_for_features,
                min_price_level=price_min_filter, max_price_level=price_max_filter,
                limit=level_fetch_limit
            )
            liquidity_zones_data = database_manager.get_liquidity_zones(
                symbol=chart_symbol, timeframe=chart_timeframe, is_tapped=False,
                start_time_utc=start_time_for_features, end_time_for_features=end_time_for_features,
                min_price_level=price_min_filter, max_price_level=price_max_filter,
                limit=level_fetch_limit
            )
            if fair_value_gaps_data:
                logger.debug(f"CHART_DATA_LOADER: Loaded {len(fair_value_gaps_data)} FVG. Contoh: Type={fair_value_gaps_data[0]['type']}, Bottom={to_float_or_none(fair_value_gaps_data[0]['fvg_bottom_price']):.5f}")
            if liquidity_zones_data:
                logger.debug(f"CHART_DATA_LOADER: Loaded {len(liquidity_zones_data)} Liquidity zones. Contoh: Type={liquidity_zones_data[0]['zone_type']}, Price={to_float_or_none(liquidity_zones_data[0]['price_level']):.5f}")
        except Exception as e:
            logger.error(f"Gagal memuat FVG/Liq data: {e}", exc_info=True)
            fair_value_gaps_data = []
            liquidity_zones_data = []

    if include_ms_fib_param:
        try:
            logger.debug("CHART_DATA_LOADER: Memuat data Market Structure Events dan Fibonacci Levels.")
            
            ms_event_types_to_load = ['Break of Structure', 'Change of Character']
            if not include_key_levels_only_param:
                ms_event_types_to_load.extend(['Swing High', 'Swing Low'])
            
            all_ms_events_raw = database_manager.get_market_structure_events(
                symbol=chart_symbol, timeframe=chart_timeframe,
                start_time_utc=start_time_for_features, end_time_for_features=end_time_for_features,
                event_type=ms_event_types_to_load,
                min_price_level=price_min_filter, max_price_level=price_max_filter,
                limit=level_fetch_limit
            )
            
            ms_events_limit = 10
            ms_events_data = sorted(all_ms_events_raw, key=lambda x: x['event_time_utc'], reverse=True)[:ms_events_limit]
            if ms_events_data:
                logger.debug(f"CHART_DATA_LOADER: Loaded {len(ms_events_data)} MS events (limited). Contoh: Type={ms_events_data[0]['event_type']}, Price={to_float_or_none(ms_events_data[0]['price_level']):.5f}")

            fib_ratios_to_load = None
            if include_key_levels_only_param:
                fib_ratios_to_load = [Decimal('0.5'), Decimal('0.618'), Decimal('0.786')]
                logger.debug("CHART_DATA_LOADER: Fibonacci difilter hanya rasio utama karena include_key_levels_only=True.")
            
            all_fib_levels_raw = database_manager.get_fibonacci_levels(
                symbol=chart_symbol, timeframe=chart_timeframe, is_active=True,
                limit=level_fetch_limit,
                start_time_utc=start_time_for_features, end_time_for_features=end_time_for_features,
                min_price_level=price_min_filter, max_price_level=price_max_filter,
                ratios_to_include=fib_ratios_to_load
            )

            fib_sets = {}
            for fib_lvl in all_fib_levels_raw:
                set_key = (fib_lvl['high_price_ref'], fib_lvl['low_price_ref'])
                if set_key not in fib_sets:
                    fib_sets[set_key] = {
                        'start_time_ref_utc': fib_lvl['start_time_ref_utc'],
                        'levels': []
                    }
                fib_sets[set_key]['levels'].append(fib_lvl)
            
            sorted_fib_sets = sorted(fib_sets.items(), key=lambda item: item[1]['start_time_ref_utc'], reverse=True)
            
            fib_sets_limit = 5
            fibonacci_levels_data = []
            for i, (set_key, set_info) in enumerate(sorted_fib_sets):
                if i >= fib_sets_limit:
                    break
                fibonacci_levels_data.extend(set_info['levels'])
            if fibonacci_levels_data:
                logger.debug(f"CHART_DATA_LOADER: Loaded {len(fibonacci_levels_data)} Fibonacci levels (from {len(fib_sets)} sets, limited). Contoh: Ratio={to_float_or_none(fibonacci_levels_data[0]['ratio']):.3f}, Price={to_float_or_none(fibonacci_levels_data[0]['price_level']):.5f}")
        except Exception as e:
            logger.error(f"Gagal memuat MS/Fib data: {e}", exc_info=True)
            ms_events_data = []
            fibonacci_levels_data = []

    if include_signals_param:
        try:
            logger.debug("CHART_DATA_LOADER: Memuat data AI Signals.")
            signals_to_plot = database_manager.get_ai_analysis_results(
                symbol=chart_symbol,
                start_time_utc=start_time_for_features,
                end_time_for_features=end_time_for_features,
                analyst_id="Consensus_Executor"
            )
            if signals_to_plot:
                logger.debug(f"CHART_DATA_LOADER: Loaded {len(signals_to_plot)} AI signals. Contoh: Action={signals_to_plot[0]['recommendation_action']}, Entry={to_float_or_none(signals_to_plot[0]['entry_price']):.5f}")
        except Exception as e:
            logger.error(f"Gagal memuat AI signals: {e}", exc_info=True)
            signals_to_plot = None

    if include_volume_profile_param:
        try:
            logger.debug("CHART_DATA_LOADER: Memuat data Volume Profiles.")
            volume_profiles_data = database_manager.get_volume_profiles(
                symbol=chart_symbol,
                timeframe=chart_timeframe,
                limit=1,
                start_time_utc=start_time_for_features,
                end_time_for_features=end_time_for_features
            )
            if volume_profiles_data:
                logger.debug(f"CHART_DATA_LOADER: Loaded {len(volume_profiles_data)} Volume Profiles. Contoh: POC={to_float_or_none(volume_profiles_data[0]['poc_price']):.5f}")
            else:
                logger.debug(f"CHART_DATA_LOADER: Tidak ada Volume Profiles ditemukan untuk {chart_symbol} {chart_timeframe} dalam rentang yang diminta.")
        except Exception as e:
            logger.error(f"Gagal memuat Volume Profiles: {e}", exc_info=True)
            volume_profiles_data = []

    return {
        "ma_data": ma_data_dict,
        "sr_levels_data": sr_levels_data,
        "sd_zones_data": sd_zones_data,
        "ob_data": order_blocks_data,
        "fvgs_data": fair_value_gaps_data,
        "liquidity_zones_data": liquidity_zones_data,
        "fibonacci_levels_data": fibonacci_levels_data,
        "ms_events_data": ms_events_data,
        "signals_data": signals_to_plot,
        "volume_profile_data": volume_profiles_data
    }
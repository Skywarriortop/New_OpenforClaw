# scheduler.py

import threading
import time
import sys
from datetime import datetime, timezone, timedelta
from config import config
import logging
from collections import defaultdict
import data_updater # Pastikan ini diimpor
import ai_consensus_manager
import rule_based_signal_generator
import aggressive_signal_generator
import notification_service
import database_manager
import market_data_processor
import detector_monitoring
import fundamental_data_service
import utils
import json
from decimal import Decimal
import chart_generator
import pandas as pd
import mt5_connector
import llm_self_diagnoser # <--- PASTIKAN INI DIIMPOR
import auto_trade_manager
from openclaw_agent import OpenClawAgent

logger = logging.getLogger(__name__)
openclaw_agent = OpenClawAgent()

_feature_backfill_completed = False

def _json_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")


# --- Scheduler Task Functions (Self-Looping) ---
# Hanya definisikan fungsi-fungsi loop yang memang milik scheduler.py di sini.
# Fungsi lainnya akan dipanggil dari data_updater.py.
def daily_summary_report_loop(stop_event: threading.Event, symbol_param: str):
    """
    Loop periodik untuk mengirimkan laporan ringkasan harian.
    """
    interval = config.Scheduler.UPDATE_INTERVALS.get('daily_summary_report_loop', float(3600 * 24))
    
    while not stop_event.is_set():
        now = datetime.now(timezone.utc)
        next_run_time = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        time_to_wait = (next_run_time - now).total_seconds()

        if time_to_wait < 0 or time_to_wait > interval:
             time_to_wait = 10
             
        logger.info(f"Daily Summary loop akan menunggu {time_to_wait:.0f} detik hingga waktu laporan berikutnya.")
        stop_event.wait(time_to_wait)

        if stop_event.is_set():
            break

        try:
            logger.info(f"Memicu laporan ringkasan harian untuk {symbol_param}...")
            notification_service.notify_daily_summary(symbol_param)
`            logger.info(f"Laporan ringkasan harian untuk {symbol_param} berhasil dikirim.")

        except Exception as e:
            logger.error(f"ERROR (Scheduler - Daily Summary Report): {e}", exc_info=True)
        
        stop_event.wait(interval - min(interval, time_to_wait))

def automatic_signal_generation_loop(stop_event: threading.Event):
    """Loop untuk generasi sinyal otomatis dari AI."""
    interval = config.Scheduler.UPDATE_INTERVALS.get('automatic_signal_generation_loop', 300)
    while not stop_event.is_set():
        if _feature_backfill_completed:
            try:
                logger.info("Memanggil analisis pasar AI (multiple analis & konsensus)...")
                
                current_price_from_tick, current_price_timestamp = data_updater.get_latest_realtime_price(config.TRADING_SYMBOL)
                
                if current_price_from_tick is None or current_price_from_tick <= Decimal('0.0'):
                    logger.warning("Harga real-time tidak valid atau belum tersedia. Melewatkan generasi sinyal AI.")
                    stop_event.wait(interval)
                    continue

                ai_consensus_manager.analyze_market_and_get_consensus(
                    config.TRADING_SYMBOL,
                    current_price=current_price_from_tick,
                    current_price_timestamp=current_price_timestamp
                )

            except Exception as e:
                logger.error(f"ERROR (Scheduler - AI Signal): {e}", exc_info=True)
        else:
            logger.info("AI Signal loop menunggu backfill fitur selesai...")
        stop_event.wait(interval)


def aggressive_signal_loop(stop_event: threading.Event, symbol_param: str):
    """Loop periodik untuk generasi sinyal agresif."""
    # Baris ini dihapus karena symbol_param sekarang datang dari argumen fungsi
    # symbol_param = config.TRADING_SYMBOL
    
    interval = config.Scheduler.UPDATE_INTERVALS.get('aggressive_signal_loop', 60)

    while not stop_event.is_set():
        if _feature_backfill_completed:
            try:
                logger.info(f"Memicu generasi sinyal agresif untuk {symbol_param}...")
                latest_tick = database_manager.get_latest_price_tick(symbol_param)

                if not latest_tick or latest_tick.get('last_price') is None or latest_tick.get('last_price') <= 0:
                    logger.warning(f"Tidak ada tick terakhir yang valid untuk memicu sinyal agresif untuk {symbol_param}.")
                    stop_event.wait(interval)
                    continue

                current_time = latest_tick['time']
                current_price = Decimal(str(latest_tick['last_price']))

                aggressive_signal = aggressive_signal_generator.generate_signal(
                    symbol_param, current_time, current_price
                )

                if aggressive_signal and aggressive_signal['action'] in ["BUY", "SELL", "HOLD"]:
                    logger.info(f"Sinyal Agresif: {aggressive_signal['action']} (Conf: {aggressive_signal['confidence']})")

                    database_manager.save_ai_analysis_result(
                        symbol=symbol_param,
                        timestamp=current_time,
                        summary=aggressive_signal.get('reasoning', 'Sinyal agresif.'),
                        potential_direction=aggressive_signal.get('potential_direction', 'Undefined'),
                        recommendation_action=aggressive_signal['action'],
                        entry_price=aggressive_signal.get('entry_price_suggestion'),
                        stop_loss=aggressive_signal.get('stop_loss_suggestion'),
                        take_profit=aggressive_signal.get('take_profit_suggestion'),
                        reasoning=aggressive_signal.get('reasoning', 'N/A'),
                        ai_confidence=aggressive_signal.get('confidence', 'High'),
                        raw_response_json=json.dumps(aggressive_signal, default=_json_default),
                        analyst_id="Aggressive_Strategy"
                    )
                    logger.info("Sinyal agresif berhasil disimpan ke DB.")

                    notification_service.notify_signal(
                        symbol=symbol_param,
                        action=aggressive_signal['action'],
                        entry_price=aggressive_signal.get('entry_price_suggestion'),
                        stop_loss=aggressive_signal.get('stop_loss_suggestion'),
                        take_profit_levels_suggestion=aggressive_signal.get('take_profit_levels_suggestion'),
                        reasoning=aggressive_signal.get('reasoning'),
                        confidence=aggressive_signal.get('confidence'),
                        analyst_id="Aggressive_Strategy"
                    )
                    logger.info(f"Notifikasi sinyal agresif {aggressive_signal['action']} (Conf: {aggressive_signal['confidence']}) dikirim.")

                    MIN_CONFIDENCE_FOR_TRADE_EXECUTION = "Medium"
                    confidence_levels_map = {"Low": 1, "Medium": 2, "High": 3}
                    current_signal_confidence_value = confidence_levels_map.get(aggressive_signal['confidence'], 1)
                    min_trade_confidence_value = confidence_levels_map.get(MIN_CONFIDENCE_FOR_TRADE_EXECUTION, 1)

                    if config.Trading.auto_trade_enabled and aggressive_signal['action'] in ["BUY", "SELL"]:
                        if current_signal_confidence_value >= min_trade_confidence_value:
                            tp_levels_for_atm = []
                            if aggressive_signal.get('take_profit_suggestion') is not None:
                                tp_levels_for_atm.append({'price': aggressive_signal['take_profit_suggestion'], 'volume_percentage': Decimal('0.50')})
                            if aggressive_signal.get('tp2_suggestion') is not None:
                                tp_levels_for_atm.append({'price': aggressive_signal['tp2_suggestion'], 'volume_percentage': Decimal('0.30')})
                            if aggressive_signal.get('tp3_suggestion') is not None:
                                tp_levels_for_atm.append({'price': aggressive_signal['tp3_suggestion'], 'volume_percentage': Decimal('0.20')})
                            
                            trade_result = auto_trade_manager.execute_ai_trade(
                                symbol=symbol_param,
                                action=aggressive_signal['action'],
                                volume=config.Trading.AUTO_TRADE_VOLUME,
                                entry_price=aggressive_signal.get('entry_price_suggestion'),
                                stop_loss=aggressive_signal.get('stop_loss_suggestion'),
                                take_profit=aggressive_signal.get('take_profit_suggestion'),
                                magic_number=config.Trading.AUTO_TRADE_MAGIC_NUMBER,
                                slippage=config.Trading.AUTO_TRADE_SLIPPAGE,
                                tp_levels_config=tp_levels_for_atm
                            )
                        else:
                            logger.info(f"Eksekusi trade untuk sinyal agresif {aggressive_signal['action']} dilewatkan (kepercayaan rendah untuk trade).")
            except Exception as e:
                logger.error(f"ERROR (Scheduler - Aggressive Signal): {e}", exc_info=True)
        else:
            logger.info("Aggressive Signal loop menunggu backfill fitur selesai...")
        stop_event.wait(interval)


# FUNGSI BARU UNTUK LOOP DIAGNOSA MANDIRI LLM
def llm_self_diagnosis_loop(stop_event: threading.Event, symbol_param: str):
    """Loop periodik untuk memicu diagnosa mandiri LLM."""
    interval = config.Scheduler.UPDATE_INTERVALS.get('llm_self_diagnosis_loop', float(3600 * 24))
    review_period_minutes = int(interval / 60)

    while not stop_event.is_set():
        if _feature_backfill_completed:
            try:
                logger.info(f"Memicu LLM self-diagnosis untuk {symbol_param} selama {review_period_minutes} menit terakhir.")
                llm_self_diagnoser.run_llm_self_diagnosis(symbol_param, review_period_minutes)
            except Exception as e:
                logger.error(f"ERROR (Scheduler - LLM Self-Diagnosis): {e}", exc_info=True)
        else:
            logger.info("LLM Self-Diagnosis loop menunggu backfill fitur selesai...")
        stop_event.wait(interval)

def openclaw_analysis_loop(stop_event: threading.Event):

    interval = 3600 * 6  # setiap 6 jam

    while not stop_event.is_set():

        try:

            logger.info("Menjalankan OpenClaw system analysis")

            result = openclaw_agent.analyze_system()

            logger.info("OpenClaw result:")
            logger.info(result)

        except Exception as e:

            logger.error(f"ERROR (OpenClaw Analysis): {e}", exc_info=True)

        stop_event.wait(interval)

def _start_data_update_threads(initial_run=Fals`e):
    global _feature_backfill_completed

    TASKS = {
        'periodic_realtime_tick_loop': data_updater.periodic_realtime_tick_loop,
        'periodic_session_data_update_loop': data_updater.periodic_session_data_update_loop,
        'periodic_market_status_update_loop': data_updater.periodic_market_status_update_loop,
        'periodic_mt5_trade_data_update_loop': data_updater.periodic_mt5_trade_data_update_loop,
        'daily_cleanup_scheduler_loop': data_updater.daily_cleanup_scheduler_loop,
        'daily_open_prices_scheduler_loop': data_updater.daily_open_prices_scheduler_loop,
        'periodic_historical_data_update_loop': data_updater.periodic_historical_data_update_loop,
        'periodic_fundamental_data_update_loop': fundamental_data_service.periodic_fundamental_data_update_loop,
        'daily_summary_report_loop': daily_summary_report_loop, # Sudah benar
        'rule_based_signal_loop': data_updater.rule_based_signal_loop,
        'automatic_signal_generation_loop': automatic_signal_generation_loop,
        'scenario_analysis_loop': data_updater.scenario_analysis_loop,
        'detector_health_monitoring_loop': detector_monitoring.detector_health_monitoring_loop,
        "periodic_daily_pnl_check_loop": data_updater.periodic_daily_pnl_check_loop,
        "periodic_volume_profile_update_loop": data_updater.periodic_volume_profile_update_loop,
        "periodic_combined_advanced_detection_loop": data_updater.periodic_combined_advanced_detection_loop,
        "monthly_historical_feature_backfill_loop": data_updater.monthly_historical_feature_backfill_loop,
        "aggressive_signal_loop": aggressive_signal_loop, # Sudah benar
        "llm_self_diagnosis_loop": llm_self_diagnosis_loop,
    }

    with config.Scheduler._data_update_thread_restart_lock:
        if not initial_run:
            logger.info("Menghentikan thread data update yang sedang berjalan...")
            config.Scheduler._data_update_stop_event.set()
            
            for thread in config.Scheduler._data_update_threads:
                thread.join(timeout=2.0)

            config.Scheduler._data_update_threads = []
            config.Scheduler._data_update_stop_event.clear()
            logger.info("Semua thread telah dihentikan.")

        if config.Scheduler.AUTO_DATA_UPDATE_ENABLED:
            logger.info("Memulai thread data update...")
             
            threads_to_start = []
            stop_event = config.Scheduler._data_update_stop_event

            for task_name, is_enabled in config.Scheduler.ENABLED_LOOPS.items():
                if is_enabled and task_name in TASKS:
                    task_function = TASKS[task_name]
                    
                    args_for_task = ()
                    # MODIFIKASI: Sesuaikan argumen untuk setiap fungsi
                    if task_name in ['periodic_realtime_tick_loop', 'periodic_session_data_update_loop', 'periodic_market_status_update_loop',
                                    'periodic_mt5_trade_data_update_loop', 'daily_cleanup_scheduler_loop',
                                    'daily_open_prices_scheduler_loop', 'periodic_historical_data_update_loop',
                                    'periodic_volume_profile_update_loop', 'periodic_combined_advanced_detection_loop',
                                    'rule_based_signal_loop', 'scenario_analysis_loop',
                                    'periodic_daily_pnl_check_loop', 'monthly_historical_feature_backfill_loop',
                                    'periodic_fundamental_data_update_loop', # Pindahkan ke sini
                                    'detector_health_monitoring_loop']: # Pindahkan ke sini
                        args_for_task = (config.TRADING_SYMBOL, stop_event, _feature_backfill_completed,)
                    elif task_name in ['automatic_signal_generation_loop', 'aggressive_signal_loop', 'daily_summary_report_loop', 'llm_self_diagnosis_loop']:
                        args_for_task = (stop_event, config.TRADING_SYMBOL,)
                    else:
                        logger.warning(f"TASK LOADER: Tanda tangan argumen untuk '{task_name}' tidak diketahui. Melewatkan.")
                        continue
                    
                    thread = threading.Thread(target=task_function, args=args_for_task, daemon=True)
                    threads_to_start.append(thread)
                    logger.info(f"Menjadwalkan tugas '{task_name}'.")

            for t in threads_to_start:
                t.start()
            
            config.Scheduler._data_update_threads = threads_to_start
            logger.info(f"{len(threads_to_start)} thread berhasil dimulai.")
        else:
            logger.info("Auto-Data Update dinonaktifkan, tidak ada thread yang dimulai.")




def _stop_data_update_threads():
    with config.Scheduler._data_update_thread_restart_lock:
        config.Scheduler._data_update_stop_event.set()
        for thread in config.Scheduler._data_update_threads:
            thread.join(timeout=5)
        config.Scheduler._data_update_threads = []
        config.Scheduler._data_update_stop_event.clear()
        logger.info("Semua thread scheduler telah dihentikan dan dibersihkan.")
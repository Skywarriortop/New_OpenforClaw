# api_routes.py - Modul ini mendefinisikan semua rute API untuk aplikasi Flask,
#                 memungkinkan interaksi antara frontend/pengguna dan logika backend bot.

from flask import Flask, Response, send_from_directory, request, jsonify
from flask_cors import CORS
import os
import sys
import threading
import logging
import re
from datetime import datetime, timezone, timedelta
from flask_socketio import SocketIO, emit
import pandas as pd
from decimal import Decimal
from utils import to_float_or_none, to_iso_format_or_none , to_decimal_or_none, to_utc_datetime_or_none, _get_scalar_from_possibly_ndarray
# Impor modul lain yang dibutuhkan oleh API routes
import scheduler
import ai_analyzer
import notification_service
import database_manager
import mt5_connector
import config
import ai_consensus_manager
import backtest_manager
import chart_generator
import utils # Modul utilitas kustom Anda
import config
import chart_data_loader
import market_data_processor
import detector_monitoring
import fundamental_data_service
import aggressive_signal_generator
# --- Deklarasi Logger untuk Modul Ini ---
logger = logging.getLogger(__name__)

# --- Inisialisasi Flask-SocketIO secara global ---
# Ini penting agar SocketIO dapat diakses dari mana saja di aplikasi Flask,
# termasuk dari background threads.
socketio = SocketIO(cors_allowed_origins="*", async_mode='threading')

# --- Variabel Global untuk Log Dasbor (In-memory) ---
_dashboard_logs = []
_MAX_DASHBOARD_LOGS = 100 # Batas maksimal entri log yang disimpan di memori

# --- Variabel Kontrol untuk Emitter Progress WebSocket ---
# Digunakan untuk mengirimkan update progress backfill ke frontend secara real-time.
_progress_emitter_stop_event = threading.Event()
_progress_emitter_thread = None

def _backfill_progress_emitter_worker():
    """
    Worker thread yang secara periodik mengirimkan status progress backfill
    ke klien WebSocket (frontend).
    """
    while not _progress_emitter_stop_event.is_set():
        try:
            # Mengambil salinan status progress dari market_data_processor dengan aman
            with market_data_processor._backfill_progress_lock:
                progress_data = market_data_processor._backfill_overall_progress_status.copy()

            if progress_data:
                # Mengirim data progress yang sudah dikonversi ke format respons API
                socketio.emit('backfill_progress_update', _convert_to_api_response_format(progress_data), namespace='/')
                logger.debug("Emitting backfill progress update via WebSocket.")

        except Exception as e:
            logger.error(f"Error in backfill progress emitter worker: {e}", exc_info=True)
        finally:
            # Menunggu sebentar sebelum mengirim update berikutnya
            _progress_emitter_stop_event.wait(5) # Emit setiap 5 detik

def start_backfill_progress_emitter():
    """Memulai background thread untuk mengirimkan progress backfill via WebSocket."""
    global _progress_emitter_thread
    if _progress_emitter_thread is None or not _progress_emitter_thread.is_alive():
        _progress_emitter_stop_event.clear() # Pastikan event clear sebelum memulai
        _progress_emitter_thread = threading.Thread(target=_backfill_progress_emitter_worker, name="BackfillProgressEmitter", daemon=True)
        _progress_emitter_thread.start()
        logger.info("Backfill progress emitter thread started.")

def stop_backfill_progress_emitter():
    """Menghentikan background thread pengirim progress backfill."""
    global _progress_emitter_thread
    if _progress_emitter_thread and _progress_emitter_thread.is_alive():
        logger.info("Stopping backfill progress emitter thread...")
        _progress_emitter_stop_event.set() # Memberi sinyal untuk berhenti
        _progress_emitter_thread.join(timeout=2) # Menunggu thread berhenti
        if _progress_emitter_thread.is_alive():
            logger.warning("Backfill progress emitter thread did not stop in time.")
        _progress_emitter_thread = None

def _convert_to_api_response_format(data):
    """
    Fungsi bantu rekursif untuk mengonversi objek Decimal ke float dan datetime ke string ISO
    agar data dapat diserialisasi ke JSON untuk respons API.
    Dapat menangani dictionary atau list of dictionaries.
    """
    if isinstance(data, list):
        return [_convert_to_api_response_format(item) for item in data]
    elif isinstance(data, dict):
        converted_item = {}
        for key, value in data.items():
            if isinstance(value, Decimal):
                converted_item[key] = to_float_or_none(value) # Menggunakan fungsi utilitas to_float_or_none
            elif isinstance(value, datetime):
                converted_item[key] = to_iso_format_or_none(value) # Menggunakan fungsi utilitas to_iso_format_or_none
            elif isinstance(value, (dict, list)): # Rekursif untuk struktur bersarang
                converted_item[key] = _convert_to_api_response_format(value)
            else:
                converted_item[key] = value
        return converted_item
    else:
        return data # Mengembalikan nilai non-dict/list apa adanya

class DashboardLogHandler(logging.Handler):
    """
    Custom logging handler untuk menangkap log dan menyimpannya di memori
    agar dapat ditampilkan di dasbor frontend.
    """
    def __init__(self):
        super().__init__()
        self.logs = _dashboard_logs
        self.max_logs = _MAX_DASHBOARD_LOGS

    def emit(self, record):
        """Menambahkan record log yang diformat ke daftar log in-memory."""
        msg = self.format(record)
        self.logs.append(msg)
        # Menjaga jumlah log agar tidak melebihi batas maksimal
        if len(self.logs) > self.max_logs:
            del self.logs[0] # Hapus log terlama jika batas terlampaui

# Variabel global untuk menyimpan instance konfigurasi aplikasi.
# Ini akan diisi oleh fungsi create_app.
my_app_config = None

def create_app(app_config_instance):
    """
    Fungsi utama untuk membuat dan menginisialisasi aplikasi Flask.
    Menerima instance konfigurasi aplikasi untuk digunakan di seluruh rute.
    """
    global my_app_config
    my_app_config = app_config_instance

    # Menentukan jalur ke direktori frontend yang dibangun (dist)
    current_backend_dir = os.path.abspath(os.path.dirname(__file__))
    FRONTEND_DIST_PATH = os.path.join(current_backend_dir, 'my_frontend', 'dist')

    # Debugging logs untuk memverifikasi jalur frontend
    print(f"DEBUG: Lokasi script backend (current_backend_dir): {current_backend_dir}")
    print(f"DEBUG: FRONTEND_DIST_PATH yang dihitung Flask: {FRONTEND_DIST_PATH}")
    print(f"DEBUG: Apakah FRONTEND_DIST_PATH ini ada? {os.path.exists(FRONTEND_DIST_PATH)}")
    if os.path.exists(FRONTEND_DIST_PATH) and os.path.isdir(FRONTEND_DIST_PATH):
        print(f"DEBUG: Isi folder FRONTEND_DIST_PATH: {os.listdir(FRONTEND_DIST_PATH)}")
        print(f"DEBUG: Apakah index.html ada di FRONTEND_DIST_PATH? {os.path.exists(os.path.join(FRONTEND_DIST_PATH, 'index.html'))}")
    else:
        print("DEBUG: FRONTEND_DIST_PATH TIDAK DITEMUKAN ATAU BUKAN FOLDER. Ini adalah masalah utama!")

    # Inisialisasi aplikasi Flask
    app = Flask(__name__, static_folder=FRONTEND_DIST_PATH, static_url_path='/')
    CORS(app) # Mengaktifkan CORS untuk mengizinkan permintaan dari frontend
    socketio.init_app(app) # Menginisialisasi SocketIO dengan aplikasi Flask

    # --- Rute untuk Melayani Frontend ---
    @app.route('/')
    def serve_frontend_root():
        """Melayani file index.html sebagai root aplikasi frontend."""
        return send_from_directory(app.static_folder, 'index.html')

    @app.route('/<path:path>')
    def serve_frontend_assets(path):
        """Melayani aset statis frontend (CSS, JS, gambar, dll.)."""
        return send_from_directory(app.static_folder, path)

    @app.route('/favicon.ico')
    def favicon():
        """Melayani favicon."""
        return send_from_directory(app.static_folder, 'favicon.ico')

    # --- Rute API untuk Query Data Database ---
    @app.route('/api/data/query', methods=['GET'])
    def database_query_builder():
        """
        Endpoint serbaguna untuk mengambil berbagai jenis data dari database.
        Parameter query: type, symbol, timeframe, limit, start_date, end_date.
        """
        data_type = request.args.get('type')
        symbol = data.get('symbol', my_app_config.TRADING_SYMBOL) # Pastikan ini 'TRADING_SYMBOL'
        timeframe = request.args.get('timeframe')
        limit = request.args.get('limit', type=int)

        start_date_str = request.args.get('start_date')
        end_date_str = request.args.get('end_date')

        start_time_utc = None
        end_time_utc = None

        try:
            if start_date_str:
                start_time_utc = datetime.strptime(start_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc, hour=0, minute=0, second=0, microsecond=0)
            if end_date_str:
                end_time_utc = datetime.strptime(end_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc, hour=23, minute=59, second=59, microsecond=999999)
        except ValueError as e:
            logger.error(f"API Error: Invalid date format for query: {e}")
            return jsonify({"status": "error", "message": "Invalid date format. UseYYYY-MM-DD."}), 400

        logger.info(f"API: Menerima permintaan query data '{data_type}' untuk {symbol}, TF {timeframe}, dari {start_date_str} sampai {end_date_str}.")

        results = []
        status_message = "success"

        if not data_type:
            return jsonify({"status": "error", "message": "Parameter 'type' is required (e.g., candles, sr_levels, economic_events). Please refer to API documentation for available types."}), 400

        if not symbol:
            return jsonify({"status": "error", "message": "Symbol parameter is required for this data type."}), 400

        try:
            if data_type == 'candles':
                if not timeframe:
                    return jsonify({"status": "error", "message": "Timeframe is required for 'candles' type."}), 400
                results = database_manager.get_historical_candles_from_db(
                    symbol_param=symbol,
                    timeframe_str=timeframe,
                    limit=limit,
                    start_time_utc=start_time_utc,
                    end_time_utc=end_time_utc
                )

            elif data_type == 'daily_open_price':
                date_param = request.args.get('date')
                if not date_param:
                    return jsonify({"status": "error", "message": "Parameter 'date' is required for 'daily_open_price' type (YYYY-MM-DD)."}), 400

                daily_open_price_val = database_manager.get_daily_open_price(
                    date_ref=date_param,
                    symbol_param=symbol,
                )
                if daily_open_price_val is not None:
                    results = [{
                        "date": date_param,
                        "symbol": symbol,
                        "open_price": daily_open_price_val
                    }]
                else:
                    status_message = "no_data_found"
                    logger.warning(f"API: Tidak ada daily open price ditemukan untuk {symbol} pada {date_param}.")

            elif data_type == 'session_candles':
                trade_date_param = request.args.get('trade_date')
                if not trade_date_param:
                    return jsonify({"status": "error", "message": "Parameter 'trade_date' is required for 'session_candles' type."}), 400

                results = database_manager.get_session_candle_data_for_date(
                    trade_date=trade_date_param,
                    symbol_param=symbol,
                    start_time_utc=start_time_utc,
                    end_time_utc=end_time_utc
                )
                if not results:
                    status_message = "no_data_found"
                    logger.warning(f"API: Tidak ada session candles ditemukan untuk {trade_date_param} (Symbol: {symbol}).")

            elif data_type == 'session_swings':
                trade_date_param = request.args.get('trade_date')
                if not trade_date_param:
                    return jsonify({"status": "error", "message": "Parameter 'trade_date' is required for 'session_swings' type."}), 400

                results = database_manager.get_session_swing_data_for_date(
                    trade_date=trade_date_param,
                    symbol_param=symbol,
                    start_time_utc=start_time_utc,
                    end_time_utc=end_time_utc
                )
                if not results:
                    status_message = "no_data_found"
                    logger.warning(f"API: Tidak ada session swings ditemukan untuk {trade_date_param} (Symbol: {symbol}).")

            elif data_type == 'sr_levels':
                is_active_str = request.args.get('is_active')
                is_active = None if is_active_str is None else (is_active_str.lower() == 'true')
                results = database_manager.get_support_resistance_levels(
                    symbol=symbol,
                    timeframe=timeframe,
                    is_active=is_active,
                    limit=limit,
                    start_time_utc=start_time_utc,
                    end_time_utc=end_time_utc
                )

            elif data_type == 'sd_zones':
                is_mitigated_str = request.args.get('is_mitigated')
                is_mitigated = None if is_mitigated_str is None else (is_mitigated_str.lower() == 'true')
                results = database_manager.get_supply_demand_zones(
                    symbol=symbol,
                    timeframe=timeframe,
                    is_mitigated=is_mitigated,
                    limit=limit,
                    start_time_utc=start_time_utc,
                    end_time_utc=end_time_utc
                )

            elif data_type == 'order_blocks':
                is_mitigated_str = request.args.get('is_mitigated')
                is_mitigated = None if is_mitigated_str is None else (is_mitigated_str.lower() == 'true')
                results = database_manager.get_order_blocks(
                    symbol=symbol,
                    timeframe=timeframe,
                    is_mitigated=is_mitigated,
                    limit=limit,
                    start_time_utc=start_time_utc,
                    end_time_utc=end_time_utc
                )

            elif data_type == 'fair_value_gaps':
                is_filled_str = request.args.get('is_filled')
                is_filled = None if is_filled_str is None else (is_filled_str.lower() == 'true')
                results = database_manager.get_fair_value_gaps(
                    symbol=symbol,
                    timeframe=timeframe,
                    is_filled=is_filled,
                    limit=limit,
                    start_time_utc=start_time_utc,
                    end_time_utc=end_time_utc
                )

            elif data_type == 'market_structure_events':
                event_type = request.args.get('event_type')
                direction = request.args.get('direction')
                results = database_manager.get_market_structure_events(
                    symbol=symbol,
                    timeframe=timeframe,
                    event_type=event_type,
                    direction=direction,
                    limit=limit,
                    start_time_utc=start_time_utc,
                    end_time_utc=end_time_utc
                )

            elif data_type == 'liquidity_zones':
                is_tapped_str = request.args.get('is_tapped')
                is_tapped = None if is_tapped_str is None else (is_tapped_str.lower() == 'true')
                results = database_manager.get_liquidity_zones(
                    symbol=symbol,
                    timeframe=timeframe,
                    is_tapped=is_tapped,
                    limit=limit,
                    start_time_utc=start_time_utc,
                    end_time_utc=end_time_utc
                )

            elif data_type == 'fibonacci_levels':
                is_active_str = request.args.get('is_active')
                is_active = None if is_active_str is None else (is_active_str.lower() == 'true')
                results = database_manager.get_fibonacci_levels(
                    symbol=symbol,
                    timeframe=timeframe,
                    is_active=is_active,
                    limit=limit,
                    start_time_utc=start_time_utc,
                    end_time_utc=end_time_utc
                )

            elif data_type == 'moving_averages':
                ma_type = request.args.get('ma_type')
                period = request.args.get('period', type=int)
                results = database_manager.get_moving_averages(
                    symbol=symbol,
                    timeframe=timeframe,
                    ma_type=ma_type,
                    period=period,
                    limit=limit,
                    start_time_utc=start_time_utc,
                    end_time_utc=end_time_utc
                )

            elif data_type == 'divergences':
                is_active_str = request.args.get('is_active')
                is_active = None if is_active_str is None else (is_active_str.lower() == 'true')
                indicator_type = request.args.get('indicator_type')
                divergence_type = request.args.get('divergence_type')
                results = database_manager.get_divergences(
                    symbol=symbol,
                    timeframe=timeframe,
                    is_active=is_active,
                    indicator_type=indicator_type,
                    divergence_type=divergence_type,
                    limit=limit,
                    start_time_utc=start_time_utc,
                    end_time_utc=end_time_utc
                )

            elif data_type == 'volume_profiles':
                results = database_manager.get_volume_profiles(
                    symbol=symbol,
                    timeframe=timeframe,
                    limit=limit,
                    start_time_utc=start_time_utc,
                    end_time_utc=end_time_utc
                )

            elif data_type == 'ai_analysis_results':
                analyst_id = request.args.get('analyst_id')
                results = database_manager.get_ai_analysis_results(
                    symbol=symbol,
                    start_time_utc=start_time_utc,
                    end_time_utc=end_time_utc,
                    limit=limit,
                    analyst_id=analyst_id
                )

            elif data_type == 'economic_events' or data_type == 'news_articles':
                min_impact = request.args.get('min_impact', default='Low')
                currency = request.args.get('currency')
                topics_str = request.args.get('topics')
                topics = [t.strip().lower() for t in topics_str.split(',')] if topics_str else []

                all_fundamental = database_manager.get_upcoming_events_from_db(
                    start_time_utc=start_time_utc,
                    end_time_utc=end_time_utc,
                    min_impact=min_impact,
                    target_currency=currency
                )
                if data_type == 'economic_events':
                    results = [item for item in all_fundamental if item['type'] == 'economic_calendar']
                else: # data_type == 'news_articles'
                    news_raw = [item for item in all_fundamental if item['type'] == 'news_article']
                    if topics:
                        results = [
                            article for article in news_raw
                            if any(t in (article.get('name', '') + ' ' + article.get('summary', '')).lower() for t in topics)
                        ]
                    else:
                        results = news_raw

            elif data_type == 'price_history':
                results = database_manager.get_price_history(
                    symbol_param=symbol,
                    limit=limit,
                    start_time_utc=start_time_utc,
                    end_time_utc=end_time_utc
                )

            else:
                return jsonify({"status": "error", "message": f"Invalid data type '{data_type}'. Please refer to API documentation for available types."}), 400

            if not results and status_message == "success":
                status_message = "no_data_found"
                logger.warning(f"API: Tidak ada data ditemukan untuk type '{data_type}' dengan filter yang diberikan.")

            final_results = _convert_to_api_response_format(results)
            return jsonify({"status": status_message, "data": final_results}), 200

        except Exception as e:
            logger.error(f"API Error: Gagal memproses query untuk type '{data_type}': {e}", exc_info=True)
            return jsonify({"status": "error", "message": f"Failed to process query for '{data_type}': {e}"}), 500

    # --- Rute API untuk Kontrol Scheduler ---
    @app.route('/api/scheduler/restart', methods=['POST'])
    def restart_scheduler():
        """
        Endpoint untuk me-restart semua thread scheduler.
        Berguna untuk menerapkan perubahan konfigurasi tanpa mematikan aplikasi.
        """
        try:
            logger.info("API: Menerima permintaan untuk me-restart scheduler.")
            scheduler._start_data_update_threads(initial_run=False) # Memanggil fungsi restart di modul scheduler
            return jsonify({"status": "success", "message": "Scheduler restart initiated."}), 200
        except Exception as e:
            logger.error(f"API ERROR: Gagal me-restart scheduler: {e}", exc_info=True)
            notification_service.notify_error(f"Gagal restart scheduler via API: {e}", "API Scheduler")
            return jsonify({"status": "error", "message": str(e)}), 500

    # --- Rute API untuk Memicu Analisis AI ---
    @app.route('/api/analyze_ai', methods=['POST'])
    def trigger_ai_analysis():
        """
        Endpoint untuk memicu siklus analisis AI secara manual.
        Analisis dijalankan di background thread.
        """
        timeframe = request.json.get('timeframe', 'H1') # Timeframe opsional untuk logging/konteks
        logger.info(f"API: Menerima permintaan untuk memicu analisis AI untuk timeframe: {timeframe}")
        
        def run_analysis_in_background():
            """Fungsi pembantu untuk menjalankan analisis AI di background."""
            try:
                ai_consensus_manager.analyze_market_and_get_consensus()
                logger.info(f"Analisis AI untuk timeframe {timeframe} selesai dipicu di background.")
            except Exception as e:
                logger.error(f"Error saat menjalankan analisis AI di background: {e}", exc_info=True)
                notification_service.notify_error(f"Error saat menjalankan analisis AI via API: {e}", "API AI Trigger")
        
        thread = threading.Thread(target=run_analysis_in_background)
        thread.daemon = True # Thread akan berhenti saat aplikasi utama berhenti
        thread.start()

        return jsonify({"status": "success", "message": f"Analisis AI untuk {timeframe} sedang dipicu. Hasil akan datang di Telegram."}), 200

    # --- Rute API untuk Memicu Analisis Fundamental ---
    @app.route('/api/analyze_fundamental_daily', methods=['POST'])
    def trigger_fundamental_analysis_daily():
        """
        Endpoint untuk memicu analisis fundamental harian secara manual.
        Mengumpulkan data fundamental dari berbagai scraper dan mengirim notifikasi.
        """
        try:
            # Periksa apakah loop fundamental diaktifkan di konfigurasi
            if not my_app_config.Scheduler.ENABLED_LOOPS.get("periodic_fundamental_data_update_loop", False):
                logger.warning("API: Permintaan analisis fundamental manual dilewati karena 'periodic_fundamental_data_update_loop' dinonaktifkan di config.py.")
                return jsonify({"status": "skipped", "message": "Analisis fundamental dinonaktifkan dalam konfigurasi."}), 200

            data = request.get_json()
            days_past = data.get('days_past', 1)
            days_future = data.get('days_future', 1)
            min_impact = data.get('min_impact', 'Medium')
            include_news_topics = data.get('include_news_topics')
            include_ai_analysis = data.get('include_ai_analysis', True)
            read_from_cache_only = data.get('read_from_cache_only', False)

            logger.info(f"API: Menerima permintaan untuk memicu analisis fundamental harian. Days past: {days_past}, Days future: {days_future}, Impact: {min_impact}, Include AI: {include_ai_analysis}, Read from Cache Only: {read_from_cache_only}")

            def run_fundamental_analysis_in_background():
                """Fungsi pembantu untuk menjalankan analisis fundamental di background."""
                try:
                    fund_service = fundamental_data_service.FundamentalDataService()
                    # Memanggil fungsi untuk menyimpan dan menotifikasi data fundamental
                    fund_service.save_and_notify_fundamental_data(
                        symbol=my_app_config.TRADING_SYMBOL,
                        days_past=days_past,
                        days_future=days_future,
                        min_impact_level=min_impact,
                        target_currency=None, # Asumsi target currency diatur di config atau tidak difilter
                        include_news_topics=include_news_topics,
                        include_ai_analysis=include_ai_analysis,
                        read_from_cache_only=read_from_cache_only
                    )
                    logger.info("Analisis fundamental harian selesai dipicu di background.")
                except Exception as e:
                    logger.error(f"Error saat menjalankan analisis fundamental di background: {e}", exc_info=True)
                    notification_service.notify_error(f"Error saat menjalankan analisis fundamental via API: {e}", "API Funda Trigger")
            
            thread = threading.Thread(target=run_fundamental_analysis_in_background)
            thread.daemon = True
            thread.start()

            return jsonify({"status": "success", "message": "Analisis fundamental harian sedang dipicu. Hasil akan datang di Telegram."}), 200
        except Exception as e:
            logger.error(f"API ERROR: Gagal memicu analisis fundamental harian: {e}", exc_info=True)
            notification_service.notify_error(f"Gagal memicu analisis fundamental harian via API: {e}", "API Funda Trigger")
            return jsonify({"status": "error", "message": str(e)}), 500

    @app.route('/api/analyze_economic_calendar', methods=['POST'])
    def trigger_economic_calendar_analysis():
        """
        Endpoint untuk memicu pengumpulan dan notifikasi kalender ekonomi saja.
        """
        try:
            data = request.get_json()
            days_past = data.get('days_past', 0)
            days_future = data.get('days_future', 0)
            start_date_str = data.get('start_date')
            end_date_str = data.get('end_date')
            min_impact = data.get('min_impact', 'Low')
            read_from_cache_only = data.get('read_from_cache_only', False)

            logger.info(f"API: Menerima permintaan analisis kalender. Days past: {days_past}, Days future: {days_future}, Impact: {min_impact}, Cache Only: {read_from_cache_only}, Date Range: {start_date_str} - {end_date_str}")

            def run_calendar_analysis_in_background():
                """Fungsi pembantu untuk menjalankan analisis kalender di background."""
                try:
                    fund_service = fundamental_data_service.FundamentalDataService()
                    economic_events = fund_service.get_economic_calendar_only(
                        days_past=days_past,
                        days_future=days_future,
                        start_date=start_date_str,
                        end_date=end_date_str,
                        min_impact_level=min_impact,
                        read_from_cache_only=read_from_cache_only
                    )
                    
                    if economic_events:
                        database_manager.save_economic_events(economic_events) # Simpan ke DB
                        logger.info(f"Berhasil menyimpan {len(economic_events)} event kalender ke DB dari API.")
                    else:
                        logger.info("Tidak ada event kalender untuk disimpan ke DB dari API.")

                    notification_service.notify_only_economic_calendar(
                        economic_events_list=economic_events,
                        min_impact=min_impact
                    )
                    logger.info("Analisis kalender selesai dipicu di background.")

                except Exception as e:
                    logger.error(f"Error saat menjalankan analisis kalender di background: {e}", exc_info=True)
                    notification_service.notify_error(f"Error saat menjalankan analisis kalender via API: {e}", "API Calendar Trigger")
            
            thread = threading.Thread(target=run_calendar_analysis_in_background)
            thread.daemon = True
            thread.start()

            return jsonify({"status": "success", "message": "Analisis kalender dipicu. Hasil akan datang di Telegram."}), 200
        except Exception as e:
            logger.error(f"API ERROR: Gagal memicu analisis kalender: {e}", exc_info=True)
            return jsonify({"status": "error", "message": str(e)}), 500

    @app.route('/api/analyze_news_only', methods=['POST'])
    def trigger_news_analysis():
        """
        Endpoint untuk memicu pengumpulan dan notifikasi artikel berita saja.
        """
        try:
            data = request.get_json()
            days_past = data.get('days_past', 0)
            days_future = data.get('days_future', 0)
            start_date_str = data.get('start_date')
            end_date_str = data.get('end_date')
            include_news_topics = data.get('include_news_topics')
            read_from_cache_only = data.get('read_from_cache_only', False)

            logger.info(f"API: Menerima permintaan analisis berita. Days past: {days_past}, Days future: {days_future}, Topics: {include_news_topics}, Cache Only: {read_from_cache_only}, Date Range: {start_date_str} - {end_date_str}")

            def run_news_analysis_in_background():
                """Fungsi pembantu untuk menjalankan analisis berita di background."""
                try:
                    fund_service = fundamental_data_service.FundamentalDataService()
                    news_articles = fund_service.get_news_articles_only(
                        days_past=days_past,
                        days_future=days_future,
                        start_date=start_date_str,
                        end_date=end_date_str,
                        include_news_topics=include_news_topics,
                        read_from_cache_only=read_from_cache_only
                    )
                    
                    if news_articles:
                        database_manager.save_news_articles(news_articles) # Simpan ke DB
                        logger.info(f"Berhasil menyimpan {len(news_articles)} artikel berita ke DB dari API.")
                    else:
                        logger.info("Tidak ada artikel berita untuk disimpan ke DB dari API.")

                    notification_service.notify_only_news_articles(
                        news_articles_list=news_articles,
                        include_topics=include_news_topics
                    )
                    logger.info("Analisis berita selesai dipicu di background.")

                except Exception as e:
                    logger.error(f"Error saat menjalankan analisis berita di background: {e}", exc_info=True)
                    notification_service.notify_error(f"Error saat menjalankan analisis berita via API: {e}", "API News Trigger")
            
            thread = threading.Thread(target=run_news_analysis_in_background)
            thread.daemon = True
            thread.start()

            return jsonify({"status": "success", "message": "Analisis berita dipicu. Hasil akan datang di Telegram."}), 200
        except Exception as e:
            logger.error(f"API ERROR: Gagal memicu analisis berita: {e}", exc_info=True)
            return jsonify({"status": "error", "message": str(e)}), 500

    # --- Rute API untuk Status Bot ---
    @app.route('/api/status', methods=['GET'])
    def get_bot_status():
        """
        Endpoint untuk mendapatkan status umum bot, termasuk info akun MT5,
        tick harga terakhir, status auto-trade, dan hasil analisis AI terbaru.
        """
        mt5_account_info_raw = database_manager.get_mt5_account_info_from_db()
        latest_price_tick_raw = database_manager.get_latest_price_tick(my_app_config.TRADING_SYMBOL)
        latest_ai_result_raw = database_manager.get_ai_analysis_results(my_app_config.TRADING_SYMBOL, limit=1, analyst_id="Consensus_Executor")

        status_data = {
            "app_status": "Running",
            "mt5_account_info": _convert_to_api_response_format(mt5_account_info_raw),
            "latest_price_tick": _convert_to_api_response_format(latest_price_tick_raw),
            "auto_trade_enabled": bool(my_app_config.Trading.auto_trade_enabled),
            "scheduler_enabled_loops": dict(my_app_config.Scheduler.ENABLED_LOOPS),
            "last_ai_analysis_time_utc": None,
            "last_ai_analysis_recommendation": None,
            "market_status_data": my_app_config.MarketData.market_status_data,
        }

        if latest_ai_result_raw:
            converted_ai_result = _convert_to_api_response_format(latest_ai_result_raw[0])
            status_data["last_ai_analysis_time_utc"] = converted_ai_result.get('timestamp')
            status_data["last_ai_analysis_recommendation"] = converted_ai_result.get('recommendation_action')
            status_data["last_ai_analysis_confidence"] = converted_ai_result.get('ai_confidence')
            status_data["last_ai_analysis_reasoning"] = converted_ai_result.get('reasoning')

        return jsonify(status_data), 200

    @app.route('/api/market_session_status', methods=['GET'])
    def get_market_session_status():
        """
        Endpoint untuk mendapatkan status sesi pasar (Asia, Eropa, New York)
        dan informasi Daylight Saving Time (DST).
        """
        dst_status = {
            "is_dst_active": False,
            "dst_offset_hours": 0,
            "dst_range_months": "N/A",
            "timezone_name": my_app_config.MarketData.TIMEZONE_FOR_DST_CHECK,
            "standard_offset_hours": 0
        }
        current_utc_time_for_session_status = datetime.now(timezone.utc)
        today_date_for_session_status = current_utc_time_for_session_status.date().isoformat()

        try:
            # Mencoba mengimpor pytz untuk perhitungan DST yang akurat
            try:
                import pytz
                target_tz = pytz.timezone(my_app_config.MarketData.TIMEZONE_FOR_DST_CHECK)
                # Localize waktu UTC saat ini ke timezone target
                localized_time = target_tz.localize(current_utc_time_for_session_status.replace(tzinfo=None))

                # Hitung offset standar (non-DST) dengan mengambil waktu di bulan Januari
                time_in_jan = datetime(current_utc_time_for_session_status.year, 1, 1, 12, 0, 0)
                localized_jan = target_tz.localize(time_in_jan.replace(tzinfo=None))
                standard_offset_td = localized_jan.tzinfo.utcoffset(localized_jan)
                dst_status["standard_offset_hours"] = int(standard_offset_td.total_seconds() / 3600)
                logger.debug(f"DST Check: Timezone={my_app_config.MarketData.TIMEZONE_FOR_DST_CHECK}, Standard Offset={dst_status['standard_offset_hours']}")

                # Periksa apakah DST aktif saat ini
                if localized_time.dst() != timedelta(0):
                    dst_status["is_dst_active"] = True
                    dst_status["dst_offset_hours"] = int(localized_time.dst().total_seconds() / 3600)

                # Menentukan rentang bulan DST (contoh sederhana)
                if target_tz.zone in ['America/New_York', 'Europe/London', 'America/Chicago', 'America/Los_Angeles']:
                     dst_status["dst_range_months"] = "Maret - November"
                elif target_tz.zone in ['Australia/Sydney', 'America/Santiago']:
                     dst_status["dst_range_months"] = "Oktober - April"
                else:
                     dst_status["dst_range_months"] = "Bervariasi (Tidak dikenal)"
            except ImportError:
                logger.warning("pytz is not installed. DST calculation will be skipped.")
                dst_status["dst_range_months"] = "N/A (pytz not installed)"
                dst_status["error"] = "pytz not installed"
            except Exception as e_dst:
                logger.error(f"Gagal menghitung status DST di /api/market_session_status: {e_dst}", exc_info=True)
                dst_status = {
                    "is_dst_active": False,
                    "dst_offset_hours": 0,
                    "dst_range_months": "Error (Cek Log)",
                    "timezone_name": my_app_config.MarketData.TIMEZONE_FOR_DST_CHECK,
                    "standard_offset_hours": 0,
                    "error": str(e_dst)
                }
            
        except Exception as e:
            logger.error(f"Gagal menghitung status DST di /api/market_session_status: {e}", exc_info=True)
            dst_status = {
                "is_dst_active": False,
                "dst_offset_hours": 0,
                "dst_range_months": "Error (Cek Log)",
                "timezone_name": my_app_config.MarketData.TIMEZONE_FOR_DST_CHECK,
                "standard_offset_hours": 0,
                "error": str(e)
            }
        
        market_session_data_response = {
            "market_status_data": my_app_config.MarketData.market_status_data,
            "current_date_iso": today_date_for_session_status,
            "dst_info": dst_status,
            "current_utc_time_from_tick": my_app_config.MarketData.market_status_data.get('current_utc_time', datetime.now(timezone.utc).isoformat())
        }
        return jsonify(market_session_data_response), 200

    # --- Rute API untuk Progress Backfill ---
    @app.route('/api/backfill_progress', methods=['GET'])
    def get_backfill_progress():
        """
        Endpoint untuk mendapatkan status progress backfill fitur historis.
        Data diambil dari market_data_processor dan dikonversi ke format API.
        """
        with market_data_processor._backfill_progress_lock:
            progress_data = market_data_processor._backfill_overall_progress_status.copy()

            return jsonify(_convert_to_api_response_format(progress_data)), 200

    # --- Rute API untuk Memicu Backfill Historis ---
    @app.route('/api/backfill_historical_features', methods=['POST'])
    def trigger_backfill_historical_features():
        """
        Endpoint API untuk memicu backfill fitur historis.
        Menerima parameter start_date, end_date (opsional, YYYY-MM-DD), dan timeframes (opsional).
        Proses backfill dijalankan di background thread.
        """
        try:
            data = request.get_json()
            start_date_str = data.get('start_date')
            end_date_str = data.get('end_date')
            requested_timeframes = data.get('timeframes') # List of strings, e.g., ["H1", "D1"]
            
            start_date_dt = None
            end_date_dt = None

            # Parsing tanggal jika disediakan
            if start_date_str and end_date_str:
                try:
                    start_date_dt = datetime.strptime(start_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc, hour=0, minute=0, second=0, microsecond=0)
                    end_date_dt = datetime.strptime(end_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc, hour=23, minute=59, second=59, microsecond=999999)
                    logger.info(f"API: Menerima permintaan backfill rentang kustom: {start_date_str} hingga {end_date_str}.")
                except ValueError as e:
                    return jsonify({"status": "error", "message": f"Invalid date format for start_date/end_date. Use YYYY-MM-DD. Error: {e}"}), 400
            else:
                logger.info("API: Menerima permintaan backfill penuh (tanpa rentang tanggal spesifik).")

            # Menentukan timeframe yang akan di-backfill
            timeframes_to_backfill_final = []
            if requested_timeframes:
                # Memfilter timeframe yang diminta agar hanya yang valid dan diaktifkan di konfigurasi
                for tf in requested_timeframes:
                    if tf in my_app_config.MarketData.COLLECT_TIMEFRAMES and \
                       my_app_config.MarketData.ENABLED_TIMEFRAMES.get(tf, False):
                        timeframes_to_backfill_final.append(tf)
                    else:
                        logger.warning(f"Timeframe '{tf}' diminta untuk backfill tetapi tidak valid atau dinonaktifkan di konfigurasi. Dilewati.")
                if not timeframes_to_backfill_final:
                    # Mengembalikan error jika tidak ada timeframe yang valid untuk diproses
                    return jsonify({"status": "error", "message": "No valid or enabled timeframes provided for backfill."}), 400
                logger.info(f"API: Memproses backfill hanya untuk timeframes yang diminta: {timeframes_to_backfill_final}")
            else:
                # Jika tidak ada timeframe yang diminta, backfill semua timeframe yang diaktifkan
                for tf in list(my_app_config.MarketData.COLLECT_TIMEFRAMES.keys()):
                    if my_app_config.MarketData.ENABLED_TIMEFRAMES.get(tf, False):
                        timeframes_to_backfill_final.append(tf)
                logger.info(f"API: Memproses backfill untuk semua timeframes yang diaktifkan: {timeframes_to_backfill_final}")

            # Fungsi yang akan dijalankan di background thread
            def run_backfill_in_background(symbol_param, tfs_to_backfill, custom_start_date=None, custom_end_date=None):
                """Fungsi pembantu untuk menjalankan proses backfill di background."""
                # Impor modul di dalam fungsi thread untuk menghindari masalah circular import atau scope
                import notification_service
                import market_data_processor
                import database_manager

                try:
                    # Jika rentang tanggal kustom diberikan, hapus status backfill yang ada
                    # Ini akan memaksa backfill untuk memulai dari awal rentang yang ditentukan
                    if custom_start_date is not None:
                        logger.info(f"Memaksa backfill dari {custom_start_date.isoformat()} dengan menghapus status backfill yang ada.")
                        for tf_to_clear in tfs_to_backfill:
                            database_manager.delete_feature_backfill_status(symbol_param, tf_to_clear)
                            logger.info(f"Status backfill dihapus untuk {symbol_param} {tf_to_clear}.")
                    
                    # Memulai proses backfill fitur historis
                    if custom_start_date and custom_end_date:
                        logger.info(f"Memulai proses backfill untuk {symbol_param} TF: {tfs_to_backfill} pada rentang: {custom_start_date.date()} hingga {custom_end_date.date()}.")
                        market_data_processor.backfill_historical_features(
                            symbol_param, 
                            timeframes_to_backfill=tfs_to_backfill,
                            start_date=custom_start_date,
                            end_date=custom_end_date
                        )
                        # Mengirim notifikasi Telegram setelah backfill selesai
                        notification_service.send_telegram_message(
                            f"✅ *Backfill Historis {symbol_param} Selesai!*\n"
                            f"TF: `{', '.join(tfs_to_backfill)}`\n"
                            f"Rentang: `{custom_start_date.strftime('%Y-%m-%d')}` sampai `{custom_end_date.strftime('%Y-%m-%d')}`\n"
                            f"Semua data historis dan fitur telah diproses.", disable_notification=False)
                    else:
                        logger.info(f"Memulai proses backfill penuh untuk {symbol_param} TF: {tfs_to_backfill}.")
                        market_data_processor.backfill_historical_features(
                            symbol_param, 
                            timeframes_to_backfill=tfs_to_backfill
                        )
                        # Mengirim notifikasi Telegram setelah backfill selesai
                        notification_service.send_telegram_message(
                            f"✅ *Backfill Historis Lengkap {symbol_param} Selesai!*\n"
                            f"TF: `{', '.join(tfs_to_backfill)}`\n"
                            f"Semua data historis dan fitur telah diproses. Bot akan melanjutkan operasi penuh.", disable_notification=False)

                except Exception as e:
                    # Menangani error yang terjadi selama proses backfill
                    logger.error(f"Error saat menjalankan backfill di background: {e}", exc_info=True)
                    notification_service.notify_error(f"Gagal menjalankan backfill via API: {e}", "API Backfill Trigger")

            # Membuat dan memulai background thread untuk menjalankan backfill
            thread = threading.Thread(target=run_backfill_in_background, 
                                    args=(my_app_config.TRADING_SYMBOL, timeframes_to_backfill_final, start_date_dt, end_date_dt))
            thread.daemon = True # Menjadikan thread daemon agar berhenti saat program utama berhenti
            thread.start()

            # Mengembalikan respons sukses ke klien API
            return jsonify({"status": "success", "message": f"Backfill fitur historis untuk {my_app_config.TRADING_SYMBOL} sedang dipicu di background."}), 200

        except Exception as e:
            # Menangani error yang terjadi saat memproses permintaan API awal
            logger.error(f"API ERROR: Gagal memicu backfill: {e}", exc_info=True)
            notification_service.notify_error(f"Gagal memicu backfill via API: {e}", "API Backfill Trigger")
            return jsonify({"status": "error", "message": str(e)}), 500

    # --- Rute API untuk Mengubah Status Auto-Trade ---
    @app.route('/api/set_autotrade_status', methods=['POST'])
    def set_autotrade_status():
        """
        Endpoint API untuk mengubah status auto-trade (mengaktifkan/menonaktifkan).
        Menerima status boolean (true/false) dalam body request JSON.
        Mengirim notifikasi ke Telegram setelah perubahan status.
        """
        try:
            data = request.get_json()
            new_status = data.get('status')

            # Validasi input: pastikan 'status' adalah boolean
            if new_status is None or not isinstance(new_status, bool):
                return jsonify({"status": "error", "message": "Invalid 'status' parameter. Must be boolean (true/false)."}), 400

            # Mengakses instance konfigurasi global dan memperbarui status auto-trade
            # Properti auto_trade_enabled di kelas Config.Trading memiliki setter yang akan menulis ke file
            my_app_config.Trading.auto_trade_enabled = new_status
            logger.info(f"API: Status auto-trade diubah menjadi: {new_status}")

            # Mengirim notifikasi ke Telegram tentang perubahan status
            notification_service.send_telegram_message(
                f"⚙️ *Auto\\-Trade Status Diubah:*\n\nStatus otomatis telah diubah menjadi `{str(new_status)}`.",
                disable_notification=False # Notifikasi ini penting, jadi jangan dinonaktifkan
            )
            
            # Mengembalikan respons sukses ke klien API
            return jsonify({"status": "success", "message": f"Auto-trade status updated to {new_status}"}), 200

        except Exception as e:
            # Menangani error yang terjadi selama proses
            logger.error(f"API ERROR: Gagal mengubah status auto-trade: {e}", exc_info=True)
            # Mengirim notifikasi error ke Telegram
            notification_service.notify_error(f"Gagal mengubah status auto-trade: {e}", "API AutoTrade")
            # Mengembalikan respons error ke klien API
            return jsonify({"status": "error", "message": str(e)}), 500

    # --- Rute API untuk Memicu Backtest ---
    @app.route('/api/backtest/run', methods=['POST'])
    def trigger_backtest():
        """
        Memicu eksekusi backtest di background thread.
        Menerima parameter start_date, end_date, timeframe, run_ai_strategy, plot_results,
        dan signal_generator_type.
        """
        data = request.get_json()
        if not data:
            return jsonify({"status": "error", "message": "No JSON data provided."}), 400

        symbol = data.get('symbol', my_app_config.TRADING_SYMBOL)
        start_date_str = data.get('start_date')
        end_date_str = data.get('end_date')
        timeframe = data.get('timeframe', 'H1')
        run_ai_strategy = data.get('run_ai_strategy', True) # Default AI Strategy (ini untuk BacktestManager)
        plot_results = data.get('plot_results', True)
        
        # MENAMBAHKAN PARAMETER signal_generator_type dari request JSON
        # Default-nya adalah "aggressive" jika tidak disediakan dalam request
        signal_generator_type = data.get('signal_generator_type', 'aggressive') # <-- DITAMBAHKAN/DIUBAH

        if not start_date_str or not end_date_str:
            return jsonify({"status": "error", "message": "start_date and end_date are required."}), 400

        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc).replace(hour=23, minute=59, second=59)
        except ValueError:
            return jsonify({"status": "error", "message": "Invalid date format. Use YYYY-MM-DD."}), 400

        logger.info(f"API: Menerima permintaan backtest untuk {symbol} {timeframe} dari {start_date_str} hingga {end_date_str} dengan generator sinyal: {signal_generator_type}.") # <-- DIUBAH PESAN LOG

        # Jalankan backtest di background thread
        # Meneruskan signal_generator_type sebagai argumen
        thread = threading.Thread(target=_run_backtest_in_background, args=(symbol, start_date, end_date, timeframe, run_ai_strategy, plot_results, signal_generator_type)) # <-- signal_generator_type DITAMBAHKAN KE ARGS
        thread.daemon = True
        thread.start()

        return jsonify({"status": "success", "message": f"Backtest untuk {symbol} {timeframe} sedang dipicu dengan generator sinyal: {signal_generator_type}."}), 200 # <-- DIUBAH PESAN


    
    def _run_backtest_in_background(symbol, start_date, end_date, timeframe, run_ai_strategy, plot_results, signal_generator_type):
        try:
            # Debugging: Log parameter yang diterima
            logger.debug(f"DEBUG API_ROUTES: Memicu _run_backtest_in_background. Symbol: {symbol}, Start: {start_date}, End: {end_date}, Signal Type: {signal_generator_type}, Plot: {plot_results}")

            # Inisialisasi BacktestManager
            manager = backtest_manager.BacktestManager(symbol)
            logger.debug(f"DEBUG API_ROUTES: BacktestManager diinisialisasi untuk symbol: {manager.symbol}. Initial Balance: {manager.initial_balance}")

            # Panggilan utama ke backtest_manager
            results = manager.run_backtest(start_date, end_date, signal_generator_type=signal_generator_type, plot_results=plot_results)
            
            logger.debug(f"DEBUG API_ROUTES: manager.run_backtest selesai. Hasil: {results is not None} (Ada hasil) {results.get('status') if results else 'N/A Status'}")

            if results:
                logger.debug(f"DEBUG API_ROUTES: Hasil backtest tidak kosong. Memproses notifikasi.")
                # Logika notifikasi teks hasil backtest
                message = f"✅ *Backtest Selesai:* {utils._escape_markdown(results.get('message', 'N/A'))}\n" \
                        f"Strategi: \\\`{utils._escape_markdown(results.get('strategy_name', 'N/A'))}\\\`\n" \
                        f"Periode: \\\`{utils._escape_markdown(results.get('start_date', 'N/A'))} - {utils._escape_markdown(results.get('end_date', 'N/A'))}\\\`\n" \
                        f"Saldo Awal: \\\`{utils._escape_markdown(f'{results.get('initial_balance', 0.0):.2f}')}\\\`\n" \
                        f"Saldo Akhir: \\\`{utils._escape_markdown(f'{results.get('final_balance', 0.0):.2f}')}\\\`\n" \
                        f"Profit Bersih: \\\`{utils._escape_markdown(f'{results.get('net_profit', 0.0):.2f}')}\\\`\n" \
                        f"Total Trades: \\\`{utils._escape_markdown(str(results.get('total_trades', 0)))}\\\`\n" \
                        f"Win Rate: \\\`{utils._escape_markdown(f'{results.get('win_rate', 'N/A'):.2f}%')}\\\`\n" \
                        f"Max Drawdown: \\\`{utils._escape_markdown(f'{results.get('max_drawdown_percent', 0.0):.2f}%')}\\\`\n" \
                        f"Profit Factor: \\\`{utils._escape_markdown(str(results.get('profit_factor', 'N/A')))}\\\`\n" \
                        f"ROIC: \`{utils._escape_markdown(results.get('roic_percent', 'N/A'))}\`\n" \
                        f"Sharpe Ratio: \\\`{utils._escape_markdown(str(results.get('sharpe_ratio', 'N/A')))}\\\`\n" \
                        f"Sortino Ratio: \\\`{utils._escape_markdown(str(results.get('sortino_ratio', 'N/A')))}\\\`\n" \
                        f"Calmar Ratio: \\\`{utils._escape_markdown(str(results.get('calmar_ratio', 'N/A')))}\\\`\n" \
                        f"SL Hit: \\\`{utils._escape_markdown(str(results.get('sl_hit_count', 0)))}\\\`\n" \
                        f"BE Hit: \\\`{utils._escape_markdown(str(results.get('be_hit_count', 0)))}\\\`\n" \
                        f"TP1 Hit: \\\`{utils._escape_markdown(str(results.get('tp1_hit_count', 0)))}\\\`\n" \
                        f"TP2 Hit: \\\`{utils._escape_markdown(str(results.get('tp2_hit_count', 0)))}\\\`\n" \
                        f"TP3 Hit: \\\`{utils._escape_markdown(str(results.get('tp3_hit_count', 0)))}\\\`\n" \
                        f"TSL Hit: \\\`{utils._escape_markdown(str(results.get('tsl_hit_count', 0)))}\\\`\n" \
                        f"Closed by Opposite Signal: \\\`{utils._escape_markdown(str(results.get('closed_by_opposite_signal_count', 0)))}\\\`"
                
                notification_service.send_telegram_message(message)

                # --- Debugging dan Pengiriman Chart Kinerja (Equity/Drawdown) ---
                chart_path_from_results = results.get('chart_filepath') # Ini adalah Equity/Drawdown Chart
                logger.debug(f"DEBUG API_ROUTES: chart_filepath (Equity/Drawdown) dari hasil backtest: {chart_path_from_results}")

                if chart_path_from_results:
                    logger.debug(f"DEBUG API_ROUTES: chart_filepath (Equity/Drawdown) valid. Memanggil send_telegram_photo.")
                    if os.path.exists(chart_path_from_results):
                        notification_service.send_telegram_photo(chart_path_from_results, caption="Equity Curve & P/L Distribution")
                    else:
                        logger.error(f"DEBUG API_ROUTES: File chart (Equity/Drawdown) TIDAK DITEMUKAN di disk: {chart_path_from_results}. Tidak dapat mengirim.")
                        notification_service.send_telegram_message(
                            utils._escape_markdown(f"Backtest selesai, tetapi file chart Equity/Drawdown tidak ditemukan di lokasi: \\`{chart_path_from_results}\\\`. Periksa izin folder atau proses pembuatan chart.")
                        )
                else:
                    logger.warning("DEBUG API_ROUTES: chart_filepath (Equity/Drawdown) tidak ada atau None. Chart tidak dikirim.")
                    notification_service.send_telegram_message(
                        utils._escape_markdown("Backtest selesai, tetapi chart Equity/Drawdown tidak dapat dibuat atau ditemukan. Periksa log untuk detail.")
                    )
                
                # --- MULAI MODIFIKASI: Pengiriman Chart Candlestick dengan Trade Events ---
                candlestick_chart_path = results.get('candlestick_chart_filepath') # Ambil path chart candlestick yang baru
                logger.debug(f"DEBUG API_ROUTES: candlestick_chart_filepath dari hasil backtest: {candlestick_chart_path}")

                if candlestick_chart_path:
                    logger.debug(f"DEBUG API_ROUTES: candlestick_chart_filepath valid. Memanggil send_telegram_photo untuk chart candlestick.")
                    if os.path.exists(candlestick_chart_path):
                        notification_service.send_telegram_photo(candlestick_chart_path, caption="Candlestick Chart with Simulated Trades")
                    else:
                        logger.error(f"DEBUG API_ROUTES: File chart candlestick TIDAK DITEMUKAN di disk: {candlestick_chart_path}. Tidak dapat mengirim.")
                        notification_service.send_telegram_message(
                            utils._escape_markdown(f"Backtest selesai, tetapi file chart Candlestick tidak ditemukan di lokasi: \\`{candlestick_chart_path}\\\`. Periksa izin folder atau proses pembuatan chart.")
                        )
                else:
                    logger.warning("DEBUG API_ROUTES: candlestick_chart_filepath tidak ada atau None. Chart candlestick tidak dikirim.")
                    notification_service.send_telegram_message(
                        utils._escape_markdown("Backtest selesai, tetapi chart Candlestick dengan trade events tidak dapat dibuat atau ditemukan. Periksa log untuk detail.")
                    )
                # --- AKHIR MODIFIKASI ---

            else: # Ini terpicu jika 'results' dari manager.run_backtest adalah None atau dictionary kosong
                logger.error("DEBUG API_ROUTES: Hasil backtest kosong atau None. Tidak melanjutkan ke notifikasi chart.")
                notification_service.notify_error(
                    utils._escape_markdown("Backtest gagal dijalankan: Hasil backtest kosong atau None."),
                    "Backtest Error"
                )

        except Exception as e:
            logger.error(f"DEBUG API_ROUTES: Error tak terduga di _run_backtest_in_background: {e}", exc_info=True)
            notification_service.notify_error(
                utils._escape_markdown(f"Backtest gagal: {e}"),
                "Backtest Error"
            )

    # --- Rute API untuk Membuat Grafik ---
    @app.route('/api/generate_chart', methods=['POST'])
    def generate_chart():
        """
        Endpoint API untuk memicu pembuatan grafik candlestick dengan berbagai overlay.
        Menerima parameter: symbol, timeframe, num_candles, include_signals,
        dan berbagai flag include_ (ma, sr, sd_ob, fvg_liq, ms_fib, volume_profile).
        Juga menerima start_date dan end_date opsional untuk rentang kustom.
        Proses pembuatan grafik dijalankan di background thread.
        """
        try:
            data = request.get_json()
            symbol = data.get('symbol', my_app_config.TRADING_SYMBOL) # Pastikan ini 'TRADING_SYMBOL'
            timeframe = data.get('timeframe', 'H1')
            num_candles = data.get('num_candles', 100)
            include_signals = data.get('include_signals', False)

            # Parameter baru untuk charting bertahap (overlays)
            include_ma = data.get('include_ma', False)
            include_sr = data.get('include_sr', False)
            include_sd_ob = data.get('include_sd_ob', False)
            include_fvg_liq = data.get('include_fvg_liq', False)
            include_ms_fib = data.get('include_ms_fib', False)
            include_volume_profile = data.get('include_volume_profile', False) # Termasuk Volume Profile

            # Parameter tanggal kustom dari request API
            request_start_date_str = data.get('start_date')
            request_end_date_str = data.get('end_date')

            logger.info(f"API: Menerima permintaan pembuatan grafik untuk {symbol} TF {timeframe}, {num_candles} lilin. Sinyal: {include_signals}, MA: {include_ma}, SR: {include_sr}, SD/OB: {include_sd_ob}, FVG/Liq: {include_fvg_liq}, MS/Fib: {include_ms_fib}, VP: {include_volume_profile}")

            # Fungsi yang akan dijalankan di background thread untuk pembuatan grafik
            def run_chart_generation_in_background(chart_symbol, chart_timeframe, chart_num_candles, include_signals_param,
                                                 include_ma_param, include_sr_param, include_sd_ob_param, include_fvg_liq_param,
                                                 include_ms_fib_param, include_volume_profile_param, # Pastikan parameter ini ada
                                                 request_start_date_str_param, request_end_date_str_param):
                """Fungsi pembantu untuk menjalankan pembuatan grafik di background."""
                # Impor modul di dalam fungsi thread untuk menghindari masalah circular import atau scope
                import chart_generator
                import notification_service
                import database_manager
                import pandas as pd
                from datetime import datetime, timezone, timedelta
                import chart_data_loader # Diperlukan untuk memuat data overlay

                query_start_time_utc = None
                query_end_time_utc = None

                # Menentukan rentang waktu aktual untuk query database
                if request_start_date_str_param and request_end_date_str_param:
                    try:
                        query_start_time_utc = datetime.strptime(request_start_date_str_param, '%Y-%m-%d').replace(tzinfo=timezone.utc, hour=0, minute=0, second=0, microsecond=0)
                        query_end_time_utc = datetime.strptime(request_end_date_str_param, '%Y-%m-%d').replace(tzinfo=timezone.utc, hour=23, minute=59, second=59, microsecond=999999)
                        logger.info(f"CHART_DEBUG: Menggunakan rentang tanggal kustom dari API: {query_start_time_utc} - {query_end_time_utc}")
                    except ValueError as e:
                        logger.error(f"CHART_ERROR: Format tanggal kustom tidak valid: {e}. Menggunakan default num_candles.")
                        query_start_time_utc = None # Reset agar menggunakan logika num_candles
                        query_end_time_utc = None # Reset agar menggunakan logika num_candles
                
                # Jika tidak ada rentang tanggal kustom yang valid, gunakan num_candles dari waktu sekarang
                if query_start_time_utc is None:
                    query_end_time_utc = datetime.now(timezone.utc)
                    
                    # Mengambil candle sementara untuk menentukan waktu mulai berdasarkan num_candles
                    temp_candles_for_range_calc = database_manager.get_historical_candles_from_db(
                        chart_symbol, chart_timeframe, limit=chart_num_candles, order_asc=False, end_time_utc=query_end_time_utc
                    )
                    
                    if temp_candles_for_range_calc:
                        # Candle terakhir (indeks -1) dari order_asc=False adalah yang paling lama
                        # Candle pertama (indeks 0) dari order_asc=False adalah yang paling baru
                        query_start_time_utc = datetime.fromisoformat(temp_candles_for_range_calc[-1]['open_time_utc'])
                        query_end_time_utc = datetime.fromisoformat(temp_candles_for_range_calc[0]['open_time_utc'])
                        logger.info(f"CHART_DEBUG: Menggunakan rentang num_candles terakhir: {query_start_time_utc} - {query_end_time_utc}")
                    else:
                        logger.warning(f"CHART_DEBUG: Tidak ada candle ditemukan untuk {chart_symbol} {chart_timeframe} dengan num_candles={chart_num_candles}. Menggunakan default rentang 1 hari ke belakang.")
                        query_start_time_utc = datetime.now(timezone.utc) - timedelta(days=1)
                        query_end_time_utc = datetime.now(timezone.utc)

                # Sekarang, gunakan query_start_time_utc dan query_end_time_utc yang sudah final
                start_time_for_features = query_start_time_utc
                end_time_for_features = query_end_time_utc

                candles_for_chart = database_manager.get_historical_candles_from_db(
                    chart_symbol, chart_timeframe,
                    start_time_utc=start_time_for_features,
                    end_time_utc=end_time_for_features,
                    order_asc=True, # Penting untuk charting agar lilin berurutan
                    limit=chart_num_candles # Batasi jumlah lilin yang diambil jika rentang terlalu lebar
                )

                if not candles_for_chart:
                    logger.warning(f"Tidak ada data candle untuk {chart_symbol} {chart_timeframe} dalam rentang {start_time_for_features} - {end_time_for_features}. Tidak bisa membuat grafik.")
                    notification_service.notify_error(f"Gagal membuat grafik: Tidak ada candle untuk {chart_symbol} {chart_timeframe} dalam rentang yang diminta.", "Chart Generation")
                    return

                # Debug log untuk data candle yang diterima
                logger.info(f"DEBUG CHART DATA: Candles received for plotting: {len(candles_for_chart)}")
                if candles_for_chart:
                    logger.info(f"DEBUG CHART DATA: First candle time: {candles_for_chart[0]['open_time_utc']}")
                    logger.info(f"DEBUG CHART DATA: Last candle time: {candles_for_chart[-1]['open_time_utc']}")
                    logger.info(f"DEBUG CHART DATA: First candle price (Open/Close): {candles_for_chart[0]['open_price']}/{candles_for_chart[0]['close_price']}")
                    logger.info(f"DEBUG CHART DATA: Last candle price (Open/Close): {candles_for_chart[-1]['open_price']}/{candles_for_chart[-1]['close_price']}")

                # Menentukan filter harga untuk tunneling overlay
                price_min_filter = None
                price_max_filter = None
                if candles_for_chart:
                    last_candle_data = candles_for_chart[-1]
                    latest_close_price = float(last_candle_data['close_price'])
                    
                    if latest_close_price is not None and latest_close_price > 0:
                        buffer_percentage = 0.05 # 5% dari harga saat ini sebagai buffer (dapat dikonfigurasi)
                        price_min_filter = latest_close_price * (1 - buffer_percentage)
                        price_max_filter = latest_close_price * (1 + buffer_percentage)
                        logger.info(f"CHART_DEBUG: Filtering overlays to price range: {price_min_filter:.2f} - {price_max_filter:.2f} (based on current price {latest_close_price:.2f})")
                    else:
                        logger.warning("CHART_DEBUG: Gagal mendapatkan harga terakhir dari candles_for_chart untuk filter overlay. Filter harga tidak diterapkan.")
                else:
                    logger.warning("CHART_DEBUG: Tidak ada data candle untuk menentukan harga terakhir. Filter overlay harga tidak diterapkan.")

                # Memuat semua data overlay menggunakan chart_data_loader
                overlays_data = chart_data_loader.load_all_chart_overlays(
                    chart_symbol=chart_symbol,
                    chart_timeframe=chart_timeframe,
                    start_time_for_features=start_time_for_features,
                    end_time_for_features=end_time_for_features,
                    price_min_filter=Decimal(str(price_min_filter)) if price_min_filter is not None else None,
                    price_max_filter=Decimal(str(price_max_filter)) if price_max_filter is not None else None,
                    include_ma_param=include_ma_param,
                    include_sr_param=include_sr_param,
                    include_sd_ob_param=include_sd_ob_param,
                    include_fvg_liq_param=include_fvg_liq_param,
                    include_ms_fib_param=include_ms_fib_param,
                    # --- TAMBAHKAN BARIS INI ---
                    include_signals_param=include_signals, # <-- Tambahkan ini
                    # --- AKHIR TAMBAHAN ---
                    include_volume_profile_param=include_volume_profile_param
                )

                
                # Bongkar dictionary hasil ke variabel lokal
                ma_data_dict = overlays_data["ma_data"]
                sr_levels_data = overlays_data["sr_levels_data"]
                sd_zones_data = overlays_data["sd_zones_data"]
                order_blocks_data = overlays_data["ob_data"]
                fair_value_gaps_data = overlays_data["fvgs_data"]
                market_structure_events_data = overlays_data["ms_events_data"]
                liquidity_zones_data = overlays_data["liquidity_zones_data"]
                fibonacci_levels_data = overlays_data["fibonacci_levels_data"]
                signals_to_plot = overlays_data["signals_data"]
                volume_profiles_data = overlays_data["volume_profile_data"] # Data Volume Profile

                caption = utils._escape_markdown(f"Grafik: {chart_symbol} {chart_timeframe} ({chart_num_candles} lilin)")

                # Memanggil chart_generator untuk membuat grafik
                chart_filepath = chart_generator.generate_candlestick_chart(
                    symbol=chart_symbol,
                    timeframe=chart_timeframe,
                    candles_data=candles_for_chart,
                    signals_data=signals_to_plot,
                    ma_data=ma_data_dict,
                    fvgs_data=fair_value_gaps_data,
                    sr_levels_data=sr_levels_data,
                    ob_data=order_blocks_data,
                    ms_events_data=market_structure_events_data,
                    liquidity_zones_data=liquidity_zones_data,
                    fibonacci_levels_data=fibonacci_levels_data,
                    volume_profile_data=volume_profiles_data, # Teruskan data Volume Profile
                    filename=f"{chart_symbol}_{chart_timeframe}_Chart_{datetime.now().strftime('%Y%m%d%H%M%S')}"
                )

                if chart_filepath:
                    try:
                        # Mengirim grafik ke Telegram
                        notification_service.send_telegram_photo(chart_filepath, caption=caption)
                        logger.debug(f"Grafik {chart_filepath} telah ditambahkan ke antrean Telegram untuk dikirim.")
                    except Exception as e_telegram:
                        logger.error(f"Gagal menambahkan grafik ke antrean Telegram: {e_telegram}", exc_info=True)
                    finally:
                        # Opsional: Hapus file grafik setelah dikirim untuk menghemat ruang disk
                        # os.remove(chart_filepath)
                        # logger.info(f"File grafik {chart_filepath} dihapus setelah dikirim.")
                        pass # Biarkan file tetap ada untuk debug/analisis manual
                else:
                    logger.error("Gagal membuat grafik karena chart_generator.py tidak mengembalikan filepath.")

            # Membuat dan memulai background thread untuk pembuatan grafik
            thread = threading.Thread(target=run_chart_generation_in_background,
                                      args=(symbol, timeframe, num_candles, include_signals,
                                            include_ma, include_sr, include_sd_ob, include_fvg_liq, include_ms_fib,
                                            include_volume_profile, # Pastikan parameter ini diteruskan
                                            request_start_date_str, request_end_date_str))
            thread.daemon = True
            thread.start()

            # Mengembalikan respons sukses ke klien API
            return jsonify({"status": "success", "message": f"Pembuatan grafik untuk {symbol} {timeframe} sedang dipicu."}), 200

        except Exception as e:
            # Menangani error yang terjadi saat memproses permintaan API awal
            logger.error(f"API ERROR: Gagal memicu pembuatan grafik: {e}", exc_info=True)
            return jsonify({"status": "error", "message": str(e)}), 500

    # --- Rute API untuk WebSocket Events (SocketIO) ---
    @socketio.on('connect')
    def handle_connect():
        """Menangani event koneksi klien ke WebSocket."""
        logger.info("Client connected to WebSocket.")
        emit('response', {'data': 'Connected'})

    @socketio.on('disconnect')
    def handle_disconnect():
        """Menangani event diskoneksi klien dari WebSocket."""
        logger.info("Client disconnected from WebSocket.")
    
    # Mengembalikan instance aplikasi Flask dan SocketIO
    return app, socketio

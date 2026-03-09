import logging
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import threading 

from config import config
import notification_service 
import utils
import database_manager # Pastikan ini diimpor untuk digunakan di check_detector_health

logger = logging.getLogger(__name__)

# --- Instance global dari DetectorMonitor ---
_detector_monitoring_instance = None
_instance_lock = threading.Lock() 

def _get_detector_monitoring_instance(): 
    global _detector_monitoring_instance
    with _instance_lock:
        if _detector_monitoring_instance is None:
            _detector_monitoring_instance = DetectorMonitor() 
            logger.info("DetectorMonitor instance created.") 
        return _detector_monitoring_instance

class DetectorMonitor: 
    def __init__(self):
        # --- PERBAIKAN PENTING DI SINI: Hapus baris yang menimpa defaultdict ---
        self._detector_health_status = defaultdict(lambda: {
            "last_run_time_utc": None,
            "last_successful_run_time_utc": None,
            "last_error_time_utc": None,
            "total_runs": 0,
            "successful_runs": 0,
            "failed_runs": 0,
            "detected_count_last_run": 0,
            "status": "Not Run", 
            "error_message_last_run": None,
            "warning_message_last_run": None,
            "zero_detections_count": 0,
            "consecutive_zero_detections": 0,
            "last_success_time_utc": None,
            "last_failure_time_utc": None
        })
        self._detector_health_lock = threading.Lock() # Pastikan ini hanya sekali
        self._last_health_report_time = {} 


    # --- PERBAIKAN PENTING DI SINI: Ubah signature fungsi ini ---
    # detector_name_for_logging sekarang menjadi argumen terpisah
    def wrap_detector_function(self, func, detector_name_for_logging: str, *args, **kwargs):
        """
        Membungkus fungsi detektor untuk memantau status kesehatannya.
        'func' adalah callable yang akan dipantau.
        'detector_name_for_logging' adalah nama yang akan digunakan dalam status kesehatan.
        '*args' dan '**kwargs' adalah argumen yang akan diteruskan ke 'func'.
        """
        detector_name = detector_name_for_logging # Gunakan nama yang disediakan untuk logging
        
        with self._detector_health_lock:
            # defaultdict akan otomatis membuat entri jika detector_name belum ada
            self._detector_health_status[detector_name]["total_runs"] += 1
            self._detector_health_status[detector_name]["status"] = "Running"
            self._detector_health_status[detector_name]["last_run_time_utc"] = datetime.now(timezone.utc)

        result = None
        try:
            # --- PERBAIKAN PENTING DI SINI: Panggil func() dengan argumen yang benar ---
            # Jika func adalah lambda atau callable tanpa argumen yang akan dipanggil, panggil func()
            # Jika func adalah callable yang membutuhkan *args dan **kwargs, panggil func(*args, **kwargs)
            # Karena di check_detector_health kita menggunakan `lambda: len(count)`, maka panggil `func()`
            result = func() # Asumsi func adalah callable tanpa argumen yang akan dieksekusi di sini
            
            with self._detector_health_lock:
                self._detector_health_status[detector_name]["status"] = "OK"
                self._detector_health_status[detector_name]["successful_runs"] += 1 # Menggunakan 'successful_runs' sesuai defaultdict
                self._detector_health_status[detector_name]["last_successful_run_time_utc"] = datetime.now(timezone.utc) # Sesuai defaultdict
                self._detector_health_status[detector_name]["error_message_last_run"] = None
                
                if isinstance(result, int):
                    self._detector_health_status[detector_name]["detected_count_last_run"] = result # Menggunakan 'detected_count_last_run'
                    if result == 0:
                        self._detector_health_status[detector_name]["consecutive_zero_detections"] += 1
                        self._detector_health_status[detector_name]["warning_message_last_run"] = "0 deteksi."
                        if self._detector_health_status[detector_name]["consecutive_zero_detections"] >= getattr(config.Monitoring, 'DETECTOR_ZERO_DETECTIONS_THRESHOLD', 3):
                            self._detector_health_status[detector_name]["status"] = "Warning"
                    else:
                        self._detector_health_status[detector_name]["consecutive_zero_detections"] = 0
                        self._detector_health_status[detector_name]["warning_message_last_run"] = None
                else: 
                    self._detector_health_status[detector_name]["consecutive_zero_detections"] = 0
                    self._detector_health_status[detector_name]["warning_message_last_run"] = None

        except Exception as e:
            with self._detector_health_lock:
                self._detector_health_status[detector_name]["status"] = "Failed"
                self._detector_health_status[detector_name]["failed_runs"] += 1 # Menggunakan 'failed_runs'
                self._detector_health_status[detector_name]["last_error_time_utc"] = datetime.now(timezone.utc) # Sesuai defaultdict
                self._detector_health_status[detector_name]["error_message_last_run"] = str(e)
                logger.error(f"Detector '{detector_name}' FAILED: {e}", exc_info=True)
        return result
    
    def get_health_status(self):
        with self._detector_health_lock:
            return self._detector_health_status.copy()

    def send_health_report(self):
        anomalies_found = []
        current_time = datetime.now(timezone.utc)
        
        with self._detector_health_lock:
            for detector_name, status_data in self._detector_health_status.items():
                last_notified = self._last_health_report_time.get(detector_name, datetime.min.replace(tzinfo=timezone.utc))
                
                telegram_cooldown = getattr(config.Telegram, 'NOTIFICATION_COOLDOWN_SECONDS', 3600)
                if (current_time - last_notified).total_seconds() < telegram_cooldown:
                    continue

                if status_data["status"] == "Failed":
                    anomalies_found.append(f"🔴 Detektor *{detector_name}* GAGAL! Error: `{utils._escape_markdown(status_data.get('error_message_last_run', 'Tidak ada pesan'))}`. Cek log lebih lanjut.")
                    self._last_health_report_time[detector_name] = current_time

                elif status_data["status"] == "Warning" and status_data["consecutive_zero_detections"] >= getattr(config.Monitoring, 'DETECTOR_ZERO_DETECTIONS_THRESHOLD', 3):
                    anomalies_found.append(f"🟡 Detektor *{detector_name}* Peringatan: {status_data['consecutive_zero_detections']} run berturut-turut dengan 0 deteksi. Pesan: `{utils._escape_markdown(status_data.get('warning_message_last_run', 'Tidak ada pesan'))}`.")
                    self._last_health_report_time[detector_name] = current_time

                elif status_data["status"] == "Not Run" and \
                    (status_data["last_run_time_utc"] is None or \
                     (current_time - status_data["last_run_time_utc"]).total_seconds() > config.Monitoring.DETECTOR_HEALTH_CHECK_INTERVAL_SECONDS * 1.5):
                    anomalies_found.append(f"⚪ Detektor *{detector_name}* belum pernah berjalan atau terlalu lama tidak aktif ({status_data['status']}).")
                    self._last_health_report_time[detector_name] = current_time

        if anomalies_found:
            message = "⚠️ *Anomali Detektor Terdeteksi:*\n\n" + "\n".join(anomalies_found)
            notification_service.send_telegram_message(message)
            logger.warning(f"Mengirim notifikasi anomali detektor: {len(anomalies_found)} anomali.")
        else:
            logger.info("Tidak ada anomali detektor yang terdeteksi.")

    # --- METODE INI HARUS ADA DI DALAM KELAS DetectorMonitor ---
    def check_detector_health(self, symbol: str):
        """
        Memeriksa kesehatan detektor dengan memverifikasi apakah detektor masih mendeteksi entitas.
        Ini adalah metode utama yang dipanggil oleh loop scheduler.
        """
        logger.info(f"Memulai pemeriksaan kesehatan detektor untuk {symbol}...")
        
        # PENTING: Pastikan database_manager diimpor di bagian atas file
        # import database_manager # Tidak perlu import di sini jika sudah di atas
        
        try:
            # --- CONTOH PEMANTAUAN DETEKTOR ---
            # Pastikan Anda memanggil wrap_detector_function dengan:
            # 1. Sebuah callable (fungsi atau lambda) yang mengembalikan jumlah deteksi (int)
            # 2. Nama detektor sebagai string
            
            # Contoh detektor: Support/Resistance Levels
            # Dapatkan jumlah deteksi atau error jika ada
            sr_levels = database_manager.get_support_resistance_levels(symbol=symbol, timeframe="H1", limit=10)
            self.wrap_detector_function(lambda: len(sr_levels), "SR_Levels_H1")
            
            # Contoh detektor: Order Blocks
            ob_levels = database_manager.get_order_blocks(symbol=symbol, timeframe="H1", limit=10)
            self.wrap_detector_function(lambda: len(ob_levels), "Order_Blocks_H1")

            # Contoh detektor: Fair Value Gaps
            fvg_gaps = database_manager.get_fair_value_gaps(symbol=symbol, timeframe="H1", limit=10)
            self.wrap_detector_function(lambda: len(fvg_gaps), "FVG_H1")

            # Tambahkan panggilan untuk detektor lain yang Anda miliki
            # Misalnya:
            liq_zones = database_manager.get_liquidity_zones(symbol=symbol, timeframe="H1", limit=10)
            self.wrap_detector_function(lambda: len(liq_zones), "Liquidity_H1")

            fib_levels = database_manager.get_fibonacci_levels(symbol=symbol, timeframe="H1", limit=10)
            self.wrap_detector_function(lambda: len(fib_levels), "Fibonacci_H1")

            ms_events = database_manager.get_market_structure_events(symbol=symbol, timeframe="H1", limit=10)
            self.wrap_detector_function(lambda: len(ms_events), "Market_Structure_H1")

            sd_zones = database_manager.get_supply_demand_zones(symbol=symbol, timeframe="H1", limit=10)
            self.wrap_detector_function(lambda: len(sd_zones), "SupplyDemand_H1")
            
            # Anda juga bisa memantau apakah MACD dan RSI values ada
            macd_val = database_manager.get_macd_values(symbol=symbol, timeframe="H1", limit=1)
            self.wrap_detector_function(lambda: 1 if macd_val else 0, "MACD_Values_H1")

            rsi_val = database_manager.get_rsi_values(symbol=symbol, timeframe="H1", limit=1)
            self.wrap_detector_function(lambda: 1 if rsi_val else 0, "RSI_Values_H1")


            logger.info(f"Pemeriksaan kesehatan detektor untuk {symbol} selesai.")
            self.send_health_report() # Kirim laporan setelah semua detektor diperiksa

        except Exception as e:
            logger.error(f"DetectorMonitor: Error utama saat menjalankan check_detector_health: {e}", exc_info=True)
            # Kegagalan di sini berarti masalah sistemik, notifikasi sudah ditangani oleh scheduler.
            # Jadi, tidak perlu notifikasi ulang di sini.

# --- FUNGSI TINGKAT MODUL YANG DIPANGGIL DARI SCHEDULER ---
def detector_health_monitoring_loop(symbol_param: str, stop_event: threading.Event, _feature_backfill_completed: bool):
    """
    Loop periodik untuk memicu pemeriksaan kesehatan detektor.
    Ini adalah fungsi yang dipanggil oleh scheduler.
    """
    _service = _get_detector_monitoring_instance() 
    interval = config.Scheduler.UPDATE_INTERVALS.get('detector_health_monitoring_loop', 3600)
    
    while not stop_event.is_set():
        if _feature_backfill_completed: 
            try:
                logger.info(f"DetectorMonitoring: Memicu pemeriksaan kesehatan detektor untuk {symbol_param}...")
                _service.check_detector_health(symbol_param) # Panggil metode di instance
                
            except Exception as e:
                logger.error(f"DetectorMonitoring: ERROR (Detector Health Loop): {e}", exc_info=True)
        else:
            logger.info("DetectorMonitoring loop menunggu backfill fitur selesai...")
        stop_event.wait(interval)

# Instance global dari DetectorMonitor
# Ini harus di bagian paling bawah file, setelah semua definisi
# agar _get_detector_monitoring_instance bisa diakses.
# detector_monitor = DetectorMonitor() # Baris ini dikomentari karena _get_detector_monitoring_instance yang memanage
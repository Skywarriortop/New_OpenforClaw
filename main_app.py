# main_app.py

import logging
import os
import sys
import time
from datetime import datetime, timezone, timedelta
import json

# Impor konfigurasi aplikasi dari config.py
from config import config

# Impor modul database
import database_manager

# Impor modul untuk akses database yang disiapkan untuk Qwen
from qwen_database_access import QwenDatabaseAccess

# Impor modul untuk koneksi ke Qwen API (menggunakan fungsi yang dipilih di qwen_connector.py)
from qwen_connector import call_qwen3_8b_api

# --- Pastikan direktori log ada sebelum konfigurasi logging ---
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Konfigurasi logging untuk aplikasi utama
logging.basicConfig(
    level=config.System.LOG_LEVEL, #
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, config.System.LOG_FILE)), #
        logging.StreamHandler() # Menampilkan log di konsol
    ]
)

logger = logging.getLogger(__name__)

class TradingAssistantApp:
    def __init__(self):
        # Inisialisasi QwenDatabaseAccess
        self.qwen_db_access = QwenDatabaseAccess(trading_symbol=config.TRADING_SYMBOL)
        self.current_trading_symbol = config.TRADING_SYMBOL
        logger.info(f"Aplikasi Asisten Trading diinisialisasi untuk simbol: {self.current_trading_symbol}")

    def initialize_system(self) -> bool:
        """
        Menginisialisasi koneksi database dan thread penulis.
        """
        logger.info("Memulai inisialisasi sistem...")
        if not database_manager.init_db_connection(config.Database.URL):
            logger.critical("Gagal menginisialisasi koneksi database.")
            return False
        logger.info("Koneksi database berhasil.")

        if not database_manager.create_all_tables():
            logger.critical("Gagal membuat/memeriksa tabel database.")
            return False
        logger.info("Tabel database berhasil diperiksa.")

        if not database_manager.init_db_writer():
            logger.critical("Gagal memulai thread penulis database.")
            return False
        logger.info("Thread penulis database dimulai.")
        
        logger.info("Menggunakan LM Studio untuk hosting model AI lokal.")

        return True

    def shutdown_system(self):
        """
        Menghentikan thread penulis database saat aplikasi ditutup.
        """
        logger.info("Menghentikan sistem...")
        database_manager.stop_db_writer()
        logger.info("Sistem berhasil dihentikan.")

    def build_qwen_prompt(self, user_query: str) -> str:
        """
        Membangun prompt komprehensif untuk Qwen,
        menggabungkan skema DB, data pasar, event, dan pertanyaan pengguna.
        """
        # 1. Deskripsi Skema Database
        # Untuk uji coba ini, deskripsi skema akan diambil dari qwen_database_access.py
        # dan disarankan untuk dibuat sangat minimal di sana untuk mengurangi token.
        db_schema = self.qwen_db_access.get_database_schema_description()

        # 2. Data Pasar Terbaru (SANGAT DIKURANGI UNTUK MENGURANGI UKURAN PROMPT)
        target_symbol = self.current_trading_symbol
        target_timeframe = config.Trading.DEFAULT_TIMEFRAME

        market_data = self.qwen_db_access.get_market_data_for_qwen(
            symbol=target_symbol,
            timeframe=target_timeframe,
            limit=1 # <<< SANGAT DIKURANGI: HANYA 1 CANDLE TERAKHIR
        )
        
        # 3. Event Ekonomi dan Berita Terbaru (SANGAT DIKURANGI UNTUK MENGURANGI UKURAN PROMPT)
        economic_news_data = self.qwen_db_access.get_economic_events_for_qwen(
            days_past=0, # <<< HANYA EVENT HARI INI
            days_future=0, # <<< HANYA EVENT HARI INI
            min_impact='High', # <<< HANYA HIGH IMPACT
            target_currency='USD', # Contoh: fokus pada USD, bisa disesuaikan
            limit=1 # <<< SANGAT DIKURANGI: HANYA 1 EVENT/ARTIKEL
        )

        # 4. Informasi Akun MT5 (DIKOMENTARI SEMENTARA UNTUK MENGURANGI TOKEN)
        # mt5_account_info = database_manager.get_mt5_account_info_from_db()
        # mt5_account_info_serialized = self.qwen_db_access._serialize_for_qwen(mt5_account_info) if mt5_account_info else {}
        mt5_account_info_serialized = {} # Default kosong saat dikomentari

        # Bangun prompt utama
        prompt = f"""
        Anda adalah asisten trading AI yang cerdas dan berpengetahuan luas, terintegrasi dengan database pasar real-time dan data trading MT5.
        Tugas Anda adalah menganalisis data yang diberikan dan menjawab pertanyaan pengguna, memberikan wawasan, atau bahkan menghasilkan rekomendasi trading (jika diminta dan Anda yakin).

        **Pedoman Respons:**
        - Jawab dengan jelas dan ringkas.
        - Jika Anda menghasilkan analisis trading, sertakan potensi arah (Bullish/Bearish/Netral), alasan, dan jika memungkinkan, rekomendasi tindakan (Buy/Sell/Hold) dengan harga Entry, Stop Loss, dan Take Profit.
        - Jika pertanyaan menyangkut data historis atau statistik, berikan ringkasan data tersebut.
        - Jika Anda diminta untuk menghasilkan query SQL, berikan hanya query SQL yang valid untuk PostgreSQL.

        ---
        **KONTEKS DATABASE:**
        Berikut adalah deskripsi skema database PostgreSQL yang Anda akses. Gunakan ini untuk memahami struktur data yang Anda terima dan untuk merumuskan respons yang akurat.
        {db_schema}

        ---
        **DATA PASAR TERKINI ({target_symbol}, {target_timeframe}):**
        Berikut adalah data pasar real-time dan historis terbaru dari database Anda:
        ```json
        {json.dumps(market_data, indent=2)}
        ```

        ---
        **EVENT EKONOMI & BERITA RELEVAN:**
        Berikut adalah event ekonomi dan artikel berita terbaru yang relevan:
        ```json
        {json.dumps(economic_news_data, indent=2)}
        ```

        ---
        **INFORMASI AKUN MT5:**
        Berikut adalah informasi akun MT5 Anda:
        ```json
        {json.dumps(mt5_account_info_serialized, indent=2)}
        ```

        ---
        **PERTANYAAN PENGGUNA:**
        {user_query}

        ---
        **TANGGAPAN ANDA:**
        """
        return prompt

    def run(self):
        """
        Fungsi utama untuk menjalankan aplikasi.
        """
        if not self.initialize_system():
            logger.critical("Aplikasi gagal diinisialisasi. Keluar.")
            sys.exit(1)

        logger.info("Sistem siap. Ketik 'exit' untuk keluar.")

        while True:
            user_input = input(f"\n[{self.current_trading_symbol}] Anda: ")
            if user_input.lower() == 'exit':
                break

            if not user_input.strip():
                print("Mohon masukkan pertanyaan.")
                continue

            try:
                # Bangun prompt untuk Qwen
                full_prompt = self.build_qwen_prompt(user_input)
                logger.debug(f"Prompt lengkap yang dikirim ke Qwen:\n{full_prompt}")

                # Panggil Qwen API
                qwen_response = call_qwen3_8b_api(full_prompt)
                
                print(f"\n[Qwen AI]: {qwen_response}")

                # --- Opsional: Proses dan simpan hasil analisis AI ---
                # Logika ini dikomentari di sini, tapi bisa diaktifkan kembali
                # jika Anda ingin menyimpan respons AI yang terstruktur.

            except Exception as e:
                logger.error(f"Terjadi kesalahan saat memproses permintaan: {e}", exc_info=True)
                print("Maaf, terjadi kesalahan saat memproses permintaan Anda. Silakan coba lagi.")
            
            # Beri jeda singkat agar tidak terlalu cepat
            time.sleep(0.5)

        self.shutdown_system()

if __name__ == "__main__":
    app = TradingAssistantApp()
    app.run()
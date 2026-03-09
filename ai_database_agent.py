# ai_database_agent.py

import logging
import openai
import requests
import json
from decimal import Decimal
from datetime import datetime, timezone, timedelta
import pandas as pd
from openai import OpenAI
import re
import uuid # Pastikan ini diimpor untuk penanganan UUID
import numpy as np # Pastikan ini diimpor untuk np.bool_

# Asumsi ada di utils.py, jika tidak, pastikan fungsi-fungsi ini didefinisikan atau diimpor dengan benar.
from utils import to_float_or_none, to_iso_format_or_none, to_int_or_none 

# Import semua model database Anda.
from database_models import ( 
    DailyOpen, PriceTick, HistoricalCandle, SessionMetadata, SessionCandleData, SessionSwingData,
    MT5Position, MT5Order, MT5DealHistory, MT5AccountInfo, EconomicEvent, NewsArticle,
    SupportResistanceLevel, SupplyDemandZone, FibonacciLevel, OrderBlock, FairValueGap,
    MarketStructureEvent, LiquidityZone, MovingAverage, MACDValue, RSIValue, Divergence,
    VolumeProfile, AIAnalysisResult, FeatureBackfillStatus 
)

from config import config
from sqlalchemy.orm import Session
from sqlalchemy import text

logger = logging.getLogger(__name__)

_openai_client = None

# Peta nama tabel ke kelas model SQLAlchemy dan kolom unik/penting
DATABASE_SCHEMA_PROMPT_MAPPING = {
    "daily_opens": {
        "model": DailyOpen,
        "description": "Menyimpan harga pembukaan harian.",
        "columns": {
            "symbol": "TEXT (e.g., XAUUSD)",
            "date_referenced": "TEXT (YYYY-MM-DD)",
            "reference_price": "DECIMAL",
            "source_candle_time_utc": "DATETIME UTC"
        }
    },
    "price_ticks": {
        "model": PriceTick,
        "description": "Menyimpan data tick harga real-time.",
        "columns": {
            "symbol": "TEXT",
            "time": "BIGINT (Epoch ms)",
            "time_utc_datetime": "DATETIME UTC",
            "last_price": "DECIMAL",
            "bid_price": "DECIMAL",
            "daily_open_price": "DECIMAL",
            "delta_point": "DECIMAL",
            "change": "DECIMAL",
            "change_percent": "DECIMAL"
        }
    },
    "historical_candles": {
        "model": HistoricalCandle,
        "description": "Menyimpan data candle historis untuk berbagai timeframe.",
        "columns": {
            "symbol": "TEXT",
            "timeframe": "TEXT (e.g., M5, H1, D1)",
            "open_time_utc": "DATETIME UTC",
            "open_price": "DECIMAL",
            "high_price": "DECIMAL",
            "low_price": "DECIMAL",
            "close_price": "DECIMAL",
            "tick_volume": "BIGINT",
            "spread": "INTEGER",
            "real_volume": "BIGINT"
        }
    },
    "mt5_account_info": {
        "model": MT5AccountInfo,
        "description": "Informasi akun MetaTrader 5.",
        "columns": {
            "login": "BIGINT",
            "name": "TEXT",
            "balance": "DECIMAL",
            "equity": "DECIMAL",
            "profit": "DECIMAL",
            "free_margin": "DECIMAL",
            "currency": "TEXT"
        }
    },
    "mt5_positions": {
        "model": MT5Position,
        "description": "Detail posisi trading MT5 yang aktif.",
        "columns": {
            "ticket": "BIGINT",
            "symbol": "TEXT",
            "type": "TEXT (buy/sell)",
            "volume": "DECIMAL",
            "price_open": "DECIMAL",
            "time_open": "DATETIME UTC",
            "current_price": "DECIMAL",
            "profit": "DECIMAL",
            "sl_price": "DECIMAL",
            "tp_price": "DECIMAL",
            "partial_tp_hit_flags_json": "TEXT (JSON string)",
            "tp_levels_config_json": "TEXT (JSON string)"
        }
    },
    "mt5_deal_history": {
        "model": MT5DealHistory,
        "description": "Riwayat deal (eksekusi trading) MT5.",
        "columns": {
            "ticket": "BIGINT (deal ticket)",
            "order_ticket": "BIGINT (order ticket)",
            "symbol": "TEXT",
            "type": "TEXT (e.g., DEAL_TYPE_BUY, DEAL_TYPE_SELL)",
            "entry_type": "TEXT (e.g., DEAL_ENTRY_IN, DEAL_ENTRY_OUT)",
            "price": "DECIMAL",
            "volume": "DECIMAL",
            "profit": "DECIMAL",
            "time": "DATETIME UTC"
        }
    },
    "economic_events": {
        "model": EconomicEvent,
        "description": "Event kalender ekonomi penting.",
        "columns": {
            "event_id": "TEXT",
            "name": "TEXT",
            "country": "TEXT",
            "currency": "TEXT",
            "impact": "TEXT (Low, Medium, High)",
            "event_time_utc": "DATETIME UTC",
            "actual_value": "TEXT", # Bisa juga DECIMAL jika selalu numerik
            "forecast_value": "TEXT",
            "previous_value": "TEXT"
        }
    },
    "news_articles": {
        "model": NewsArticle,
        "description": "Artikel berita pasar keuangan.",
        "columns": {
            "id": "TEXT",
            "source": "TEXT",
            "title": "TEXT",
            "summary": "TEXT",
            "url": "TEXT",
            "published_time_utc": "DATETIME UTC",
            "relevance_score": "INTEGER"
        }
    },
    "rsi_values": {
        "model": RSIValue,
        "description": "Nilai historis Relative Strength Index (RSI).",
        "columns": {
            "symbol": "TEXT",
            "timeframe": "TEXT",
            "timestamp_utc": "DATETIME UTC",
            "value": "DECIMAL"
        }
    },
    "macd_values": {
        "model": MACDValue,
        "description": "Nilai historis Moving Average Convergence Divergence (MACD).",
        "columns": {
            "symbol": "TEXT",
            "timeframe": "TEXT",
            "timestamp_utc": "DATETIME UTC",
            "macd_line": "DECIMAL",
            "signal_line": "DECIMAL",
            "histogram": "DECIMAL",
            "macd_pcent": "DECIMAL"
        }
    },
    "support_resistance_levels": {
        "model": SupportResistanceLevel,
        "description": "Level Support dan Resistance yang terdeteksi.",
        "columns": {
            "symbol": "TEXT",
            "timeframe": "TEXT",
            "level_type": "TEXT (Support/Resistance)",
            "price_level": "DECIMAL",
            "is_active": "BOOLEAN",
            "formation_time_utc": "DATETIME UTC",
            "strength_score": "INTEGER"
        }
    },
    "order_blocks": {
        "model": OrderBlock,
        "description": "Zona Order Block (OB) yang terdeteksi.",
        "columns": {
            "symbol": "TEXT",
            "timeframe": "TEXT",
            "type": "TEXT (Bullish/Bearish)",
            "ob_top_price": "DECIMAL",
            "ob_bottom_price": "DECIMAL",
            "formation_time_utc": "DATETIME UTC",
            "is_mitigated": "BOOLEAN",
            "strength_score": "INTEGER"
        }
    },
    "fair_value_gaps": {
        "model": FairValueGap,
        "description": "Zona Fair Value Gap (FVG) yang terdeteksi.",
        "columns": {
            "symbol": "TEXT",
            "timeframe": "TEXT",
            "type": "TEXT (Bullish/Bearish)",
            "fvg_top_price": "DECIMAL",
            "fvg_bottom_price": "DECIMAL",
            "formation_time_utc": "DATETIME UTC",
            "is_filled": "BOOLEAN"
        }
    },
    "market_structure_events": {
        "model": MarketStructureEvent,
        "description": "Peristiwa perubahan struktur pasar (BOS/ChoCH).",
        "columns": {
            "symbol": "TEXT",
            "timeframe": "TEXT",
            "event_type": "TEXT (BOS/ChoCH/Overall Trend)",
            "direction": "TEXT (Bullish/Bearish)",
            "price_level": "DECIMAL",
            "event_time_utc": "DATETIME UTC"
        }
    },
    "liquidity_zones": {
        "model": LiquidityZone,
        "description": "Zona likuiditas yang terdeteksi.",
        "columns": {
            "symbol": "TEXT",
            "timeframe": "TEXT",
            "zone_type": "TEXT (Buy Side/Sell Side)",
            "price_level": "DECIMAL",
            "formation_time_utc": "DATETIME UTC",
            "is_tapped": "BOOLEAN"
        }
    },
    "moving_averages": {
        "model": MovingAverage,
        "description": "Nilai historis Moving Average.",
        "columns": {
            "symbol": "TEXT",
            "timeframe": "TEXT",
            "ma_type": "TEXT (SMA/EMA)",
            "period": "INTEGER",
            "timestamp_utc": "DATETIME UTC",
            "value": "DECIMAL"
        }
    },
    "volume_profiles": {
        "model": VolumeProfile,
        "description": "Data Volume Profile (POC, VAH, VAL).",
        "columns": {
            "symbol": "TEXT",
            "timeframe": "TEXT",
            "period_start_utc": "DATETIME UTC",
            "period_end_utc": "DATETIME UTC",
            "poc_price": "DECIMAL",
            "vah_price": "DECIMAL",
            "val_price": "DECIMAL",
            "total_volume": "DECIMAL",
            "row_height_value": "DECIMAL",
            "profile_data_json": "TEXT (JSON string)"
        }
    },
    "divergences": {
        "model": Divergence,
        "description": "Divergensi harga-indikator (RSI, MACD).",
        "columns": {
            "symbol": "TEXT",
            "timeframe": "TEXT",
            "indicator_type": "TEXT (RSI/MACD)",
            "divergence_type": "TEXT (Regular Bullish/Bearish, Hidden Bullish/Bearish)",
            "price_point_time_utc": "DATETIME UTC",
            "indicator_point_time_utc": "DATETIME UTC",
            "price_level_1": "DECIMAL",
            "price_level_2": "DECIMAL",
            "indicator_value_1": "DECIMAL",
            "indicator_value_2": "DECIMAL",
            "is_active": "BOOLEAN"
        }
    }
}


def init_openai_client(api_key: str, api_endpoint: str = None):
    """
    Menginisialisasi klien OpenAI atau koneksi ke server LLM lokal (LM Studio).
    Fungsi ini harus dipanggil sekali saat startup.
    """
    global _openai_client
    if _openai_client is not None:
        logger.info("Klien OpenAI/LM Studio sudah diinisialisasi.")
        return

    # Ambil endpoint dari config jika tidak disediakan
    if api_endpoint is None:
        api_endpoint = config.APIKeys.LM_STUDIO_API_ENDPOINT

    try:
        _openai_client = OpenAI(base_url=api_endpoint, api_key=api_key) # api_key bisa dummy jika LM Studio tidak memerlukannya
        logger.info(f"Klien OpenAI/LM Studio berhasil diinisialisasi dengan endpoint: {api_endpoint}")
    except ImportError:
        logger.critical("Modul 'openai' tidak ditemukan. Pastikan Anda telah menginstal 'openai' (pip install openai).")
        _openai_client = None
        raise
    except Exception as e:
        logger.critical(f"Gagal menginisialisasi klien OpenAI/LM Studio: {e}", exc_info=True)
        _openai_client = None
        raise

def generate_schema_prompt():
    """Menghasilkan representasi skema database dalam format teks yang optimal untuk LLM yang sadar kode."""
    schema_text = (
        "Anda adalah modul AI yang sangat spesifik dan efisien, dirancang untuk berfungsi sebagai jembatan antara permintaan bahasa alami pengguna dan database PostgreSQL. "
        "PERHATIAN: Respons Anda AKAN DIPROSES SECARA OTOMATIS OLEH PROGRAM LAIN. "
        "Program tersebut mengharapkan HANYA query SQL sebagai output agar bisa mengambil data dari database. "
        "Jika Anda memberikan teks lain selain SQL, program akan GAGAL dan pengguna TIDAK AKAN mendapatkan jawaban. "
        "Tugas Anda adalah mengubah permintaan pengguna menjadi query SQL yang akurat dan siap eksekusi. "
        "Ini adalah cara SATU-SATUNYA Anda dapat 'berbicara' dengan database dan 'membantu' pengguna. \n\n"
        "Berikut adalah daftar tabel dan kolomnya, beserta deskripsi dan tipe data Python yang sesuai untuk nilai yang diambil. "
        "Semua kolom waktu disimpan dalam UTC. Nilai moneter, harga, volume, dan persentase harus diperlakukan sebagai DECIMAL untuk akurasi data keuangan.\n\n"
    )

    for table_name, details in DATABASE_SCHEMA_PROMPT_MAPPING.items():
        schema_text += f"Tabel: `{table_name}`\n"
        schema_text += f"  Deskripsi: {details['description']}\n"
        schema_text += "  Kolom:\n"
        for col_name, col_type in details['columns'].items():
            schema_text += f"    - `{col_name}`: {col_type}\n"
        
        # Tambahan instruksi spesifik yang lebih detail untuk setiap tabel
        # Pastikan tidak ada duplikasi jika sudah di update sebelumnya
        if table_name == "mt5_account_info":
            schema_text += "  Catatan Penting: Ini adalah tabel historis. Untuk mendapatkan informasi akun TERBARU (saldo, ekuitas, profit, margin bebas), Anda harus selalu mengurutkan berdasarkan `timestamp_recorded` DESC dan mengambil `LIMIT 1`. Kolom utama yang sering dicari: `balance`, `equity`, `profit`, `free_margin`.\n"
        elif table_name == "mt5_positions":
            schema_text += "  Catatan Penting: Tabel ini berisi posisi trading MT5 yang AKTIF. Kolom `type` hanya bisa 'buy' atau 'sell'. Untuk query posisi aktif, umumnya tidak perlu filter `time_open` kecuali diminta rentang waktu spesifik. Kolom penting: `ticket`, `symbol`, `type`, `volume`, `price_open`, `profit`, `time_open`. Kolom `partial_tp_hit_flags_json` dan `tp_levels_config_json` adalah string JSON.\n"
        elif table_name == "historical_candles":
            schema_text += "  Catatan Penting: `timeframe` adalah string (misalnya 'M1', 'M5', 'H1', 'D1'). `open_time_utc` adalah waktu MULAI candle (termasuk timezone UTC). Anda harus menggunakan `open_time_utc` untuk semua filter dan pengurutan waktu. Kolom penting: `symbol`, `timeframe`, `open_time_utc`, `open_price`, `high_price`, `low_price`, `close_price`, `tick_volume`.\n"
        elif table_name == "mt5_deal_history":
            schema_text += "  Catatan Penting: Tabel ini berisi riwayat transaksi yang sudah dieksekusi. `type` menunjukkan jenis deal (misal: 'DEAL_TYPE_BUY', 'DEAL_TYPE_SELL'). `entry_type` menunjukkan apakah deal itu pembukaan ('DEAL_ENTRY_IN') atau penutupan ('DEAL_ENTRY_OUT'). Kolom penting: `ticket`, `order_ticket`, `symbol`, `type`, `entry_type`, `price`, `volume`, `profit`, `time`.\n"
        elif table_name == "economic_events":
            schema_text += "  Catatan Penting: `impact` bisa 'Low', 'Medium', atau 'High'. `actual_value`, `forecast_value`, `previous_value` adalah TEXT tapi sering berisi nilai numerik yang bisa di-CAST jika dibutuhkan untuk perhitungan (misal `CAST(actual_value AS DECIMAL)`). Gunakan `event_time_utc` untuk filter waktu. Kolom penting: `name`, `country`, `currency`, `impact`, `event_time_utc`.\n"
        elif table_name == "news_articles":
            schema_text += "  Catatan Penting: `published_time_utc` adalah waktu publikasi berita. `title` dan `summary` untuk pencarian teks. Gunakan `ILIKE` untuk pencarian teks *case-insensitive*. Kolom penting: `title`, `source`, `url`, `published_time_utc`.\n"
        elif table_name == "rsi_values":
            schema_text += "  Catatan Penting: Kolom penting: `symbol`, `timeframe`, `timestamp_utc`, `value`. RSI Value (`value`) adalah DECIMAL.\n"
        elif table_name == "macd_values":
            schema_text += "  Catatan Penting: Kolom penting: `symbol`, `timeframe`, `timestamp_utc`, `macd_line`, `signal_line`, `histogram`, `macd_pcent`. Semua nilai MACD (`macd_line`, `signal_line`, `histogram`, `macd_pcent`) adalah DECIMAL.\n"
        elif table_name == "support_resistance_levels":
            schema_text += "  Catatan Penting: `level_type` adalah 'Support' atau 'Resistance'. `is_active` adalah BOOLEAN (TRUE/FALSE). `strength_score` adalah INTEGER (semakin tinggi semakin kuat). Kolom penting: `symbol`, `timeframe`, `level_type`, `price_level`, `is_active`, `strength_score`.\n"
        elif table_name == "order_blocks":
            schema_text += "  Catatan Penting: `type` adalah 'Bullish' atau 'Bearish'. `is_mitigated` adalah BOOLEAN (TRUE/FALSE). Kolom penting: `symbol`, `timeframe`, `type`, `ob_top_price`, `ob_bottom_price`, `formation_time_utc`, `is_mitigated`.\n"
        elif table_name == "fair_value_gaps":
            schema_text += "  Catatan Penting: `type` adalah 'Bullish Imbalance' atau 'Bearish Imbalance'. `is_filled` adalah BOOLEAN (TRUE/FALSE). Kolom penting: `symbol`, `timeframe`, `type`, `fvg_top_price`, `fvg_bottom_price`, `formation_time_utc`, `is_filled`.\n"
        elif table_name == "market_structure_events":
            schema_text += "  Catatan Penting: `event_type` bisa 'Break of Structure', 'Change of Character', 'Overall Trend', 'Yearly High', 'Yearly Low'. `direction` bisa 'Bullish' atau 'Bearish'. Kolom penting: `symbol`, `timeframe`, `event_type`, `direction`, `price_level`, `event_time_utc`. Kolom `swing_high_ref_time` dan `swing_low_ref_time` bisa menjadi referensi waktu untuk swing points yang relevan dengan event ini.\n"
        elif table_name == "liquidity_zones":
            schema_text += "  Catatan Penting: `zone_type` bisa 'Buy Side' atau 'Sell Side' (atau 'Equal Highs', 'Equal Lows'). `is_tapped` adalah BOOLEAN. Kolom penting: `symbol`, `timeframe`, `zone_type`, `price_level`, `formation_time_utc`, `is_tapped`.\n"
        elif table_name == "fibonacci_levels":
            schema_text += "  Catatan Penting: `type` adalah 'Retracement' atau 'Extension'. `is_active` adalah BOOLEAN. `ratio` adalah DECIMAL. `price_level` adalah level harga Fib. Kolom `current_retracement_percent` dan `deepest_retracement_percent` adalah DECIMAL persentase retracement saat ini/terdalam. `retracement_direction` adalah INTEGER (1=Bullish, -1=Bearish). Kolom penting: `symbol`, `timeframe`, `type`, `ratio`, `price_level`, `is_active`, `start_time_ref_utc`, `end_time_ref_utc`, `current_retracement_percent`, `deepest_retracement_percent`, `retracement_direction`.\n"
        elif table_name == "moving_averages":
            schema_text += "  Catatan Penting: `ma_type` adalah 'SMA' atau 'EMA'. `period` adalah INTEGER. Kolom penting: `symbol`, `timeframe`, `ma_type`, `period`, `timestamp_utc`, `value`. `value` adalah DECIMAL.\n"
        elif table_name == "volume_profiles":
            schema_text += "  Catatan Penting: `poc_price` (Point of Control), `vah_price` (Value Area High), `val_price` (Value Area Low). `total_volume` adalah total volume untuk periode VP tersebut. `profile_data_json` adalah string JSON dari data profil detail. `row_height_value` adalah ukuran setiap 'bin' harga VP. Kolom penting: `symbol`, `timeframe`, `period_start_utc`, `poc_price`, `vah_price`, `val_price`, `total_volume`, `profile_data_json`, `row_height_value`.\n"
        elif table_name == "divergences":
            schema_text += "  Catatan Penting: `indicator_type` adalah 'RSI' atau 'MACD'. `divergence_type` bisa 'Regular Bullish/Bearish', 'Hidden Bullish/Bearish'. `price_level_1`, `price_level_2`, `indicator_value_1`, `indicator_value_2` adalah DECIMAL. `is_active` adalah BOOLEAN. Kolom penting: `symbol`, `timeframe`, `indicator_type`, `divergence_type`, `price_point_time_utc`, `indicator_point_time_utc`, `price_level_1`, `price_level_2`, `indicator_value_1`, `indicator_value_2`, `is_active`.\n"
        elif table_name == "ai_analysis_results":
            schema_text += "  Catatan Penting: Tabel ini menyimpan hasil analisis dari berbagai modul AI. `potential_direction` dan `recommendation_action` adalah string. `entry_price`, `stop_loss`, `take_profit` adalah DECIMAL. `ai_confidence` adalah INTEGER (0-100). `raw_response_json` adalah respons mentah dari AI. Kolom penting: `symbol`, `timestamp`, `analyst_id`, `summary`, `potential_direction`, `recommendation_action`, `ai_confidence`.\n"
        
        schema_text += "\n"
    
    schema_text += (
        "INSTRUKSI PENTING DAN KETAT UNTUK OUTPUT ANDA:\n"
        "- INGAT: Respons Anda AKAN DIPROSES OTOMATIS OLEH PROGRAM. Program ini mengharapkan HANYA BLOK KODE SQL sebagai output. Jika ada teks lain (seperti penjelasan, salam, atau instruksi manual), program akan GAGAL memproses permintaan. Tugas Anda adalah membantu program ini dengan memberikan output yang tepat.\n"
        "- FORMAT OUTPUT HARUS HANYA BLOK KODE SQL BERIKUT:\n"
        "```sql\n"
        "SELECT column1, column2 FROM table_name WHERE condition ORDER BY relevant_time_column DESC LIMIT 10;\n"
        "```\n"
        "- TIDAK ADA TEKS LAIN, TIDAK ADA PENJELASAN, TIDAK ADA SAMBUTAN. TIDAK ADA KARAKTER TAMBAHAN SEBELUM ATAU SESUDAH BLOK KODE.\n"
        "- **Batasan Kritis:** Untuk semua query SELECT, tambahkan `LIMIT 10` di akhir untuk membatasi jumlah hasil, **KECUALI** jika pertanyaan pengguna secara eksplisit meminta jumlah yang berbeda (misal `LIMIT 5`), atau query tersebut adalah agregasi sederhana yang mengembalikan satu baris (misal `COUNT()`, `SUM()`, `AVG()`). Untuk query agregasi sederhana (yang hanya mengembalikan satu nilai), JANGAN tambahkan LIMIT.\n"
        "- Gunakan klausa `WHERE` untuk filter data berdasarkan kriteria yang disebutkan oleh pengguna. Selalu filter `symbol` dan `timeframe` jika relevan dan tersedia. Jika pengguna tidak menyebutkan `symbol` atau `timeframe`, dan itu relevan untuk tabel, berikan `symbol='XAUUSD'` dan `timeframe='H1'` sebagai default. **JANGAN membuat kueri untuk simbol atau timeframe yang Anda tidak yakin ada datanya, kecuali secara eksplisit diminta. Fokus pada XAUUSD.**\n" # **Perubahan Penting di Sini**
        "- Untuk filter tanggal dan waktu, gunakan format `'YYYY-MM-DD HH:MM:SS'` dan pastikan kolom adalah `DATETIME UTC`. Anda dapat menggunakan fungsi PostgreSQL seperti `NOW() AT TIME ZONE 'UTC'` untuk waktu saat ini, `DATE_TRUNC('month', NOW() AT TIME ZONE 'UTC')` untuk awal bulan ini, `DATE_TRUNC('day', NOW() AT TIME ZONE 'UTC')` untuk awal hari ini. Gunakan `INTERVAL` untuk rentang waktu (misal `NOW() AT TIME ZONE 'UTC' - INTERVAL '7 days'`). Contoh: `open_time_utc >= '2024-07-01 00:00:00'` atau `event_time_utc BETWEEN '2024-07-01 00:00:00' AND '2024-07-31 23:59:59'`.\n"
        "- Urutkan hasil dengan `ORDER BY` berdasarkan kolom waktu yang paling relevan secara descending (`DESC`) untuk mendapatkan data terbaru, kecuali jika pengguna secara eksplisit meminta urutan lain (misal `ASC` atau urutan berdasarkan kolom lain). Jika hanya ingin data terbaru, selalu gunakan `ORDER BY timestamp_recorded DESC LIMIT 1` atau `ORDER BY event_time_utc DESC LIMIT 1` tergantung tabelnya.\n"
        "- Saat mengambil data TERBARU dari tabel historis (misal `mt5_account_info`, `historical_candles`), selalu gunakan `ORDER BY [kolom_waktu_relevan] DESC LIMIT 1`.\n"
        "- Untuk pertanyaan yang melibatkan agregasi (misalnya 'rata-rata', 'jumlah total', 'berapa banyak'), gunakan fungsi SQL seperti `AVG()`, `SUM()`, `COUNT()`, `MIN()`, `MAX()`. Untuk agregasi, `GROUP BY` mungkin diperlukan. Pastikan fungsi agregasi diterapkan pada kolom numerik (`DECIMAL`, `INTEGER`, `BIGINT`).\n"
        "- Perhatikan penggunaan `IS NULL` atau `IS NOT NULL` di kolom yang bisa bernilai NULL.\n"
        "- Jika query melibatkan lebih dari satu tabel, gunakan `JOIN` yang sesuai (INNER JOIN, LEFT JOIN) dan alias tabel untuk kejelasan (misal `SELECT t1.columnA, t2.columnB FROM table1 AS t1 JOIN table2 AS t2 ON t1.id = t2.t1_id;`).\n"
        "- Pilih kolom yang relevan secara eksplisit. Jangan gunakan `SELECT *` kecuali benar-benar diperlukan oleh pertanyaan pengguna (misal: 'tampilkan semua kolom').\n"
        "- Jika pengguna bertanya tentang 'profit' atau 'balance' untuk posisi, ingat bahwa `mt5_positions` memiliki kolom `profit` dan `mt5_account_info` memiliki `balance`.\n"
        "- Gunakan `ILIKE` untuk pencarian teks *case-insensitive* pada kolom `TEXT` (misal `title ILIKE '%keyword%'`).\n"
        "\nContoh Interaksi (INGAT: Output Anda HANYA blok kode SQL):\n"
        "User: 'Berapa saldo akun MT5 saya saat ini?'\n"
        "```sql\n"
        "SELECT balance, equity, profit, timestamp_recorded FROM mt5_account_info ORDER BY timestamp_recorded DESC LIMIT 1;\n"
        "```\n"
        "User: 'Tampilkan 3 posisi buy aktif XAUUSD yang paling baru.'\n"
        "```sql\n"
        "SELECT ticket, symbol, volume, price_open, time_open, profit FROM mt5_positions WHERE symbol = 'XAUUSD' AND type = 'buy' ORDER BY time_open DESC LIMIT 3;\n"
        "```\n"
        "User: 'Apa 2 berita ekonomi terbaru tentang USD dengan dampak High?'\n"
        "```sql\n"
        "SELECT name, currency, impact, event_time_utc FROM economic_events WHERE currency = 'USD' AND impact = 'High' ORDER BY event_time_utc DESC LIMIT 2;\n"
        "```\n"
        "User: 'Berapa harga penutupan candle D1 terbaru untuk XAUUSD?'\n"
        "```sql\n"
        "SELECT close_price FROM historical_candles WHERE symbol = 'XAUUSD' AND timeframe = 'D1' ORDER BY open_time_utc DESC LIMIT 1;\n"
        "```\n"
        "User: 'Berapa rata-rata harga pembukaan harian untuk XAUUSD di bulan ini?'\n"
        "```sql\n"
        "SELECT AVG(reference_price) FROM daily_opens WHERE symbol = 'XAUUSD' AND TO_CHAR(source_candle_time_utc, 'YYYY-MM') = TO_CHAR(NOW() AT TIME ZONE 'UTC', 'YYYY-MM');\n"
        "```\n"
        "User: 'Berikan saya 5 FVG yang belum terisi di timeframe M15 untuk XAUUSD.'\n"
        "```sql\n"
        "SELECT fvg_top_price, fvg_bottom_price, formation_time_utc, type, is_filled FROM fair_value_gaps WHERE symbol = 'XAUUSD' AND timeframe = 'M15' AND is_filled = FALSE ORDER BY formation_time_utc DESC LIMIT 5;\n"
        "```\n"
        "User: 'Daftar semua Order Block Bullish di timeframe H1 untuk XAUUSD yang belum dimitigasi.'\n"
        "```sql\n"
        "SELECT ob_top_price, ob_bottom_price, formation_time_utc, type, is_mitigated FROM order_blocks WHERE symbol = 'XAUUSD' AND timeframe = 'H1' AND type = 'Bullish' AND is_mitigated = FALSE ORDER BY formation_time_utc DESC LIMIT 10;\n" # Diperbarui (simbolnya kembali ke XAUUSD)
        "```\n"
        "User: 'Tunjukkan 5 level Support Resistance terkuat di H4 untuk XAUUSD.'\n"
        "```sql\n"
        "SELECT level_type, price_level, strength_score, is_active FROM support_resistance_levels WHERE symbol = 'XAUUSD' AND timeframe = 'H4' ORDER BY strength_score DESC LIMIT 5;\n"
        "```\n"
        "User: 'Berapa nilai RSI terbaru di timeframe H1 untuk XAUUSD?'\n" # Diperbarui (simbolnya kembali ke XAUUSD)
        "```sql\n"
        "SELECT value, timestamp_utc FROM rsi_values WHERE symbol = 'XAUUSD' AND timeframe = 'H1' ORDER BY timestamp_utc DESC LIMIT 1;\n"
        "```\n"
        "User: 'Tampilkan semua event struktur pasar 'Break of Structure' bullish di D1 untuk XAUUSD dari minggu lalu.'\n" # Diperbarui (simbolnya kembali ke XAUUSD)
        "```sql\n"
        "SELECT event_type, direction, price_level, event_time_utc FROM market_structure_events WHERE symbol = 'XAUUSD' AND timeframe = 'D1' AND event_type = 'Break of Structure' AND direction = 'Bullish' AND event_time_utc >= NOW() AT TIME ZONE 'UTC' - INTERVAL '7 days' ORDER BY event_time_utc DESC LIMIT 10;\n"
        "```\n"
        "User: 'Berapa total profit dari semua deal BUY XAUUSD di bulan ini?'\n"
        "```sql\n"
        "SELECT SUM(profit) FROM mt5_deal_history WHERE symbol = 'XAUUSD' AND type = 'DEAL_TYPE_BUY' AND time >= DATE_TRUNC('month', NOW() AT TIME ZONE 'UTC');\n"
        "```\n"
        "User: 'Berapa moving average EMA 50 terakhir untuk XAUUSD di H1?'\n"
        "```sql\n"
        "SELECT value FROM moving_averages WHERE symbol = 'XAUUSD' AND timeframe = 'H1' AND ma_type = 'EMA' AND period = 50 ORDER BY timestamp_utc DESC LIMIT 1;\n"
        "```\n"
        "User: 'Tampilkan 5 Order Block bearish yang paling dekat dengan harga saat ini di timeframe H4 untuk XAUUSD.'\n" # Diperbarui (fokus ke "harga saat ini" daripada angka spesifik, dan simbolnya kembali ke XAUUSD)
        "```sql\n"
        "SELECT ob_top_price, ob_bottom_price, type, formation_time_utc FROM order_blocks WHERE symbol = 'XAUUSD' AND timeframe = 'H4' AND type = 'Bearish' ORDER BY ABS(ob_top_price - (SELECT close_price FROM historical_candles WHERE symbol = 'XAUUSD' AND timeframe = 'H1' ORDER BY open_time_utc DESC LIMIT 1)) ASC LIMIT 5;\n" # **Contoh Kueri JOIN untuk harga dinamis**
        "```\n"
        "User: 'Apakah ada Divergensi bullish aktif untuk MACD di timeframe M30 untuk XAUUSD?'\n" # Diperbarui (simbolnya kembali ke XAUUSD)
        "```sql\n"
        "SELECT divergence_type, price_level_1, price_level_2, indicator_value_1, indicator_value_2, price_point_time_utc FROM divergences WHERE symbol = 'XAUUSD' AND timeframe = 'M30' AND indicator_type = 'MACD' AND divergence_type ILIKE '%Bullish%' AND is_active = TRUE ORDER BY price_point_time_utc DESC LIMIT 10;\n"
        "```\n"
    )
    return schema_text

# ----------------------------------------------------
# Fungsi sanitize_sql_query harus didefinisikan di sini
# (karena dipanggil oleh call_llm_for_sql)
# ----------------------------------------------------
def sanitize_sql_query(raw_llm_response: str) -> str:
    """
    Sanitasi query SQL yang dihasilkan oleh LLM.
    Mampu mengekstrak SQL dari code block Markdown dan memperbaiki beberapa kesalahan umum.
    Penting: Fungsi ini memastikan hanya query SELECT yang dieksekusi dan membatasi potensi injeksi.
    """
    raw_llm_response = raw_llm_response.strip()
    extracted_sql = ""

    # DEBUG: Log raw response dari LLM
    logger.debug(f"SAN_DEBUG: Raw LLM Response (full):\n---\n{raw_llm_response}\n---")

    # 1. Prioritas utama: Coba ekstrak dari code block Markdown
    code_block_match = re.search(r'```(?:sql)?\s*(.*?)\s*```', raw_llm_response, re.IGNORECASE | re.DOTALL)
    if code_block_match:
        extracted_sql = code_block_match.group(1).strip()
        logger.debug(f"SAN_DEBUG: SQL diekstrak dari code block: {extracted_sql}")
    else:
        # 2. Cadangan: Jika tidak ada code block, coba temukan pola SELECT...FROM yang ketat
        # Mencari pola SELECT di awal baris atau setelah spasi/newline
        sql_pattern = r'^\s*SELECT\s+(?:[^;]|;(?=\s*SELECT))*?\s+FROM\s+\w+(?:\s+AS\s+\w+)?(?:\s+(?:INNER|LEFT|RIGHT|FULL)\s+JOIN\s+\w+(?:\s+AS\s+\w+)?\s+ON\s+.*?)?(?:\s+WHERE\s+.*?)?(?:\s+GROUP\s+BY\s+.*?)?(?:\s+ORDER\s+BY\s+.*?)?(?:\s+LIMIT\s+\d+)?(?:\s*;?\s*)?$'
        direct_sql_match = re.search(sql_pattern, raw_llm_response, re.IGNORECASE | re.DOTALL | re.MULTILINE)
        if direct_sql_match:
            extracted_sql = direct_sql_match.group(0).strip()
            logger.debug(f"SAN_DEBUG: SQL diekstrak dari pola langsung: {extracted_sql}")
        else:
            # 3. STRATEGI LEBIH AGRESID: Coba potong dari 'SELECT' pertama yang ditemukan hingga akhir respons.
            # Ini bertujuan untuk menangkap SQL meskipun ada teks pengantar atau penjelasan di awal.
            select_start_match = re.search(r'SELECT\s+', raw_llm_response, re.IGNORECASE)
            if select_start_match:
                extracted_sql = raw_llm_response[select_start_match.start():].strip()
                logger.debug(f"SAN_DEBUG: SQL diekstrak agresif dari 'SELECT' pertama: {extracted_sql}")
            else:
                logger.warning(f"SAN_WARNING: Tidak dapat mengekstrak query SELECT yang valid dari respons LLM. Respon mentah (awal): '{raw_llm_response[:500]}...'.")
                return "Error: Query yang dihasilkan tidak valid (tidak dapat mengekstrak SELECT)."

    # Hapus semicolon di akhir jika ada, dan pastikan hanya ada satu query
    if ';' in extracted_sql:
        parts = [p.strip() for p in extracted_sql.split(';') if p.strip()]
        if len(parts) > 1:
            logger.warning(f"SAN_WARNING: Terdeteksi multiple statements dalam query. Hanya statement pertama yang akan digunakan: '{parts[0]}'.")
            extracted_sql = parts[0]
        else:
            extracted_sql = extracted_sql.replace(';', '').strip()


    # Hapus komentar SQL (sangat penting untuk keamanan!)
    extracted_sql = re.sub(r'--.*$', '', extracted_sql, flags=re.MULTILINE) # Hapus komentar baris
    extracted_sql = re.sub(r'/\*.*?\*/', '', extracted_sql, flags=re.DOTALL) # Hapus komentar blok

    # PERBAIKAN SINTAKS UMUM
    # Ganti 'is' dengan '=' untuk perbandingan (LLM sering salah di sini)
    extracted_sql = re.sub(r'\b(is)\s+([\'"].*?[\'"])', r'= \2', extracted_sql, flags=re.IGNORECASE) 
    extracted_sql = re.sub(r'\b(is)\s+([a-zA-Z0-9_.]+)', r'= \2', extracted_sql, flags=re.IGNORECASE) # Tambah support untuk kolom dengan alias (e.g., t1.column IS value)

    # Hapus kata "the" di SELECT (contoh: "select the balance")
    extracted_sql = re.sub(r'SELECT\s+the\s+', 'SELECT ', extracted_sql, flags=re.IGNORECASE)

    # Hapus koma sebelum ORDER BY atau LIMIT (jika LLM salah format)
    extracted_sql = re.sub(r',\s*ORDER\s+BY', ' ORDER BY', extracted_sql, flags=re.IGNORECASE)
    extracted_sql = re.sub(r',\s*LIMIT', ' LIMIT', extracted_sql, flags=re.IGNORECASE)

    # **PENTING:** Hanya izinkan SELECT. Ini adalah baris keamanan terpenting.
    if not extracted_sql.lower().startswith("select"):
        logger.warning(f"SAN_WARNING: Setelah ekstraksi/perbaikan, query tidak dimulai dengan SELECT. Query mentah (truncated): '{raw_llm_response[:100]}...'. Query hasil sanitasi: '{extracted_sql}'. Menolak.")
        return "Error: Hanya query SELECT yang diizinkan untuk eksekusi."

    # Pastikan ada klausa FROM
    if "from" not in extracted_sql.lower():
        logger.warning(f"SAN_WARNING: Query yang diekstrak tidak memiliki klausa FROM: '{extracted_sql}'. Menolak.")
        return "Error: Query yang dihasilkan tidak valid (tidak ada klausa FROM)."

    # Terakhir, pastikan LIMIT 10, kecuali jika query adalah agregasi atau sudah memiliki LIMIT yang valid
    # Gunakan lookahead untuk memastikan LIMIT tidak di dalam string atau komentar.
    if not re.search(r'\b(COUNT|SUM|AVG|MIN|MAX)\s*\(', extracted_sql, re.IGNORECASE): 
        if not re.search(r'\bLIMIT\s+\d+\b', extracted_sql, re.IGNORECASE): 
            extracted_sql += " LIMIT 10"
        else: # Jika sudah ada LIMIT, pastikan tidak ada format aneh
            # Ganti LIMIT dengan angka non-digit (misal LIMIT ALL atau LIMIT 'X') menjadi LIMIT 10
            extracted_sql = re.sub(r'LIMIT\s+\D+', 'LIMIT 10', extracted_sql, flags=re.IGNORECASE)
            # Opsional: Jika Anda ingin membatasi semua LIMIT ke maksimal 100 baris, misalnya:
            # extracted_sql = re.sub(r'(LIMIT\s+)(\d+)', lambda m: m.group(1) + str(min(int(m.group(2)), 100)), extracted_sql, flags=re.IGNORECASE)

    # Final cleanup (optional, sangat agresif)
    # Hapus baris yang tidak dimulai dengan SELECT/WITH/atau spasi jika terlihat seperti noise
    lines = extracted_sql.split('\n')
    cleaned_lines = []
    found_sql_start = False
    for line in lines:
        stripped_line = line.strip()
        if stripped_line.lower().startswith("select") or stripped_line.lower().startswith("with"):
            found_sql_start = True
            cleaned_lines.append(stripped_line)
        elif found_sql_start and stripped_line: # Jika sudah mulai SQL dan baris tidak kosong
            cleaned_lines.append(stripped_line)
        elif not found_sql_start and not stripped_line: # Abaikan baris kosong sebelum SQL
            continue
        elif not found_sql_start and stripped_line: # Jika ada teks sebelum SQL dimulai
            logger.warning(f"SAN_WARNING: Mengabaikan baris non-SQL awal: '{stripped_line}'")
            continue
    extracted_sql = "\n".join(cleaned_lines)


    return extracted_sql.strip()

# ----------------------------------------------------
# Pastikan fungsi call_llm_for_sql didefinisikan di sini
# (sebelum process_user_data_query memanggilnya)
# ----------------------------------------------------
def call_llm_for_sql(user_question: str) -> str:
    """Mengirim pertanyaan pengguna ke LLM lokal/remote untuk menghasilkan query SQL."""
    if _openai_client is None:
        logger.critical("Klien AI belum diinisialisasi. Pastikan Anda memanggil init_openai_client.")
        return "Error: Klien AI belum diinisialisasi. Pastikan Anda memanggil init_openai_client."

    llm_endpoint = config.APIKeys.LM_STUDIO_API_ENDPOINT # Mengambil endpoint dari config

    system_prompt_content = generate_schema_prompt() # Ambil prompt yang diperbarui

    payload_messages = [
        {"role": "user", "content": system_prompt_content + "\n\n" + user_question}
    ]

    try:
        logger.info(f"Mengirim permintaan ke LLM di: {llm_endpoint} dengan model {config.AIAnalysts.OPENAI_MODEL_NAME}.")
        logger.debug(f"Prompt System (digabung ke User) yang dikirim ke LLM:\n{system_prompt_content}")
        logger.debug(f"Pertanyaan Pengguna yang dikirim ke LLM: '{user_question}'")

        response = _openai_client.chat.completions.create(
            model=config.AIAnalysts.OPENAI_MODEL_NAME,
            messages=payload_messages,
            temperature=config.AIAnalysts.OPENAI_TEMPERATURE,
            max_tokens=250, # <-- MODIFIKASI: Sedikit dinaikkan dari 200 untuk lebih banyak ruang SQL
            timeout=120 # <-- MODIFIKASI: Sedikit diturunkan dari 120 (jika model Anda cukup cepat)
        )
        
        sql_query_raw = response.choices[0].message.content.strip()
        logger.debug(f"LLM menghasilkan SQL (mentah): {sql_query_raw}")
        
        sql_query_sanitized = sanitize_sql_query(sql_query_raw)
        
        return sql_query_sanitized

    except requests.exceptions.Timeout:
        logger.error("Permintaan ke LLM timeout (requests.exceptions.Timeout).")
        return "Error: Permintaan ke AI timeout. Coba lagi nanti."
    except requests.exceptions.RequestException as e:
        logger.error(f"Error saat memanggil LLM (requests.exceptions.RequestException): {e}", exc_info=True)
        return f"Error: Gagal terhubung ke AI lokal atau layanan. Pastikan layanan AI berjalan. Detail: {e}"
    except Exception as e:
        logger.error(f"Error tak terduga saat memanggil LLM: {e}", exc_info=True)
        return f"Error: Terjadi kesalahan saat memproses permintaan AI. Detail: {e}"


# ----------------------------------------------------
# Pastikan fungsi execute_sql_query_from_llm didefinisikan di sini
# (karena dipanggil oleh process_user_data_query)
# ----------------------------------------------------
def execute_sql_query_from_llm(sql_query: str) -> list:
    """Mengeksekusi query SQL yang sudah disanitasi."""
    # Import database_manager di sini untuk menghindari circular import jika ada
    import database_manager 
    
    if database_manager.SessionLocal is None: 
        logger.error("SessionLocal belum diinisialisasi. Tidak dapat mengeksekusi query.")
        return [{"error": "Database connection not initialized."}]

    if not sql_query.lower().startswith("select"):
        logger.warning(f"Percobaan eksekusi non-SELECT query: {sql_query}. Menolak.")
        return [{"error": "Hanya query SELECT yang diizinkan."}]

    db = database_manager.SessionLocal()
    results = []
    try:
        logger.info(f"Mengeksekusi query SQL yang disanitasi: {sql_query}")
        result_proxy = db.execute(text(sql_query))
        
        column_names = result_proxy.keys()
        
        for row in result_proxy.fetchall():
            row_dict = {}
            for i, col_name in enumerate(column_names):
                value = row[i]
                # Menggunakan fungsi utilitas yang ada untuk konversi yang robust.
                # Ini mengubah Decimal ke float dan datetime ke ISO string untuk output JSON.
                if isinstance(value, Decimal):
                    row_dict[col_name] = to_float_or_none(value)
                elif isinstance(value, datetime):
                    row_dict[col_name] = to_iso_format_or_none(value)
                elif isinstance(value, uuid.UUID): # Menangani tipe UUID
                    row_dict[col_name] = str(value)
                elif isinstance(value, (bool, np.bool_)): # Menangani tipe boolean dari database driver
                    row_dict[col_name] = bool(value)
                elif isinstance(value, (np.integer, np.floating)): # Menangani tipe numerik NumPy yang mungkin muncul
                    row_dict[col_name] = value.item() # Konversi ke tipe Python standar (int/float)
                else:
                    row_dict[col_name] = value
            results.append(row_dict)
            
        logger.info(f"Berhasil mengeksekusi query. Ditemukan {len(results)} baris.")
        return results
    except Exception as e:
        logger.error(f"Gagal mengeksekusi query SQL: '{sql_query}'. Error: {e}", exc_info=True)
        # Di sini, Anda bisa memanggil LLM kedua untuk menginterpretasi pesan error ini
        # Misalnya: interpreted_error_message = call_llm_for_error_interpretation(str(e))
        return [{"error": f"Gagal mengeksekusi query. Pastikan format SQL benar dan data tersedia. Detail: {e}"}]
    finally:
        if db:
            db.close()

def process_user_data_query(user_question: str) -> dict:
    """
    Fungsi utama untuk memproses pertanyaan data pengguna.
    Mengirim pertanyaan ke LLM untuk menghasilkan query SQL, mengeksekusinya,
    dan kemudian menggunakan LLM lain untuk menginterpretasikan hasilnya.
    """
    logger.info(f"Memproses pertanyaan pengguna: '{user_question}'")
    
    # 1. Hasilkan Query SQL dari LLM
    sql_query = call_llm_for_sql(user_question)
    if "Error:" in sql_query:
        logger.error(f"Gagal menghasilkan query SQL: {sql_query}")
        return {"status": "error", "message": sql_query, "sql_query": None, "raw_results": [], "interpreted_results": "Tidak dapat menghasilkan query SQL. Coba formulasi ulang pertanyaan Anda atau berikan detail lebih lanjut."}
    
    logger.info(f"Query SQL yang dihasilkan (setelah sanitasi): {sql_query}")

    # 2. Eksekusi Query SQL di Database
    query_results = execute_sql_query_from_llm(sql_query)
    
    if query_results and isinstance(query_results[0], dict) and "error" in query_results[0]:
        error_message_from_db = query_results[0]["error"]
        logger.error(f"Gagal mengeksekusi query SQL: {error_message_from_db}")
        return {"status": "error", "message": f"Terjadi kesalahan saat mengambil data: {error_message_from_db}", "sql_query": sql_query, "raw_results": [], "interpreted_results": "Terjadi kesalahan saat mengambil data. Silakan periksa detailnya atau coba lagi."}


    # 3. Interpretasikan Hasil Menggunakan LLM (PERBAIKAN UTAMA DI SINI)
    interpretation_prompt = (
            f"Anda adalah seorang analis data yang sangat baik dan ringkas. "
            f"Tugas Anda adalah memberikan jawaban LANGSUNG atas pertanyaan pengguna, menggunakan data dari query SQL yang telah dieksekusi. "
            f"Berikut adalah pertanyaan pengguna asli:\n'{user_question}'\n\n"
            f"Dan hasil query SQL (dalam format JSON):\n{json.dumps(query_results, indent=2, default=str)}\n\n"
            f"INSTRUKSI KETAT UNTUK FORMAT OUTPUT INTERPRETASI ANDA:\n"
            f"- **JIKA HASIL QUERY KOSONG (tidak ada baris data ditemukan), RESPON ANDA HARUS SECARA LANGSUNG MENYATAKAN BAHWA TIDAK ADA DATA YANG DITEMUKAN UNTUK PERTANYAAN TERSEBUT, TANPA MENAMBAHKAN TEKS LAIN.**\n" # <-- MODIFIKASI UTAMA DI SINI
            f"  Contoh respons jika query kosong: 'Tidak ada [tipe_data_yang_diminta] yang ditemukan untuk [parameter_filter_relevan, misal: XAUUSD di timeframe H1].'\n" # <-- Tambahan contoh
            f"- JANGAN PERNAH MEMULAI RESPON DENGAN SAPAAN, PENGANTAR, ATAU KALIMAT SEPERTI 'Berdasarkan pertanyaan pengguna...', 'Berikut adalah jawaban singkat:', 'Hasilnya adalah:', 'Tersedia data berikut:', 'The following are', atau frasa pengantar sejenis lainnya. MULAI LANGSUNG DENGAN JAWABAN.\n"
            f"- BERIKAN JAWABAN YANG SANGAT SINGKAT, PADAT, DAN RELEVAN. Fokus hanya pada data yang secara langsung menjawab pertanyaan.\n"
            f"- JANGAN MENAMBAHKAN ANALISIS, REKOMENDASI, KESIMPULAN, ATAU INFORMASI TAMBAHAN APAPUN YANG TIDAK DIMINTA SECARA EKSPILSIT OLEH PERTANYANAAN. INI MUTLAK.\n"
            f"- Jika pertanyaan adalah tentang 'saldo akun', tampilkan 'Total Saldo Akun' (dari 'balance'), 'Equity Akun' (dari 'equity'), dan 'Profit/Loss Akun' (dari 'profit', sebutkan 'kerugian' jika negatif).\n"
            f"- **JIKA menampilkan daftar item:**\n" # Baris ini dimodifikasi
            f"  - Gunakan format daftar bernomor atau poin-poin yang jelas, misalnya:\n"
            f"    1. [Nama Item]: Atribut1: [nilai], Atribut2: [nilai]\n"
            f"    2. [Nama Item]: Atribut1: [nilai], Atribut2: [nilai]\n"
            f"  - Pastikan semua atribut kunci (seperti harga, waktu, tipe) disajikan dengan rapi untuk SETIAP ITEM dalam format daftar ini. Tampilkan SEMUA item yang ditemukan hingga batas LIMIT SQL (jika jumlahnya kurang dari LIMIT, tampilkan yang ditemukan saja; jika tidak ada, ikuti instruksi 'kosong').\n"
            f"  - **Untuk Order Blocks (dari order_blocks):** Sebutkan secara eksplisit apakah itu 'Bullish Order Block' atau 'Bearish Order Block', beserta harga Top dan Bottom, serta waktu formasi. JANGAN sebut sebagai Support atau Resistance. Contoh: 'Bullish Order Block: Top: [harga], Bottom: [harga], Formasi: [waktu]'.\n" # Baris ini baru ditambahkan
            f"  - **Untuk Support/Resistance Levels (dari support_resistance_levels):** Sebutkan secara eksplisit apakah itu 'Support Level' atau 'Resistance Level' di samping harga masing-masing level dalam daftar. Contoh: 'Support Level: [harga]', 'Resistance Level: [harga]'.\n" # Baris ini dimodifikasi
            f"- Sajikan data penting secara langsung dan gunakan istilah yang sama dengan pertanyaan pengguna jika memungkinkan."
        )

    
    # Panggil fungsi baru untuk menghasilkan teks bebas (interpretasi)
    interpreted_result = call_llm_for_text_generation(
        prompt_content=interpretation_prompt,
        max_tokens=1000 # Maks token untuk interpretasi penuh
    )

    if "Error:" in interpreted_result:
        logger.warning(f"Gagal mendapatkan interpretasi LLM untuk hasil query: {interpreted_result}. Mengembalikan hasil mentah.")
        interpreted_result = "Tidak dapat memberikan interpretasi yang cerdas saat ini karena masalah AI. Berikut adalah data mentahnya:\n" + json.dumps(query_results, indent=2, default=str)


    return {
        "status": "success",
        "question": user_question,
        "sql_query": sql_query,
        "raw_results": query_results,
        "interpreted_results": interpreted_result
    }

def call_llm_for_text_generation(prompt_content: str, max_tokens: int = 1000) -> str:
    if _openai_client is None:
        logger.error("AI_DB_AGENT: OpenAI client not initialized. Cannot call LLM.")
        return "Error: OpenAI client not initialized."

    if not isinstance(prompt_content, str):
        logger.error(f"AI_DB_AGENT: prompt_content must be a string, got {type(prompt_content)}. Converting to string.")
        prompt_content = str(prompt_content)

    model_name = config.AIAnalysts.OPENAI_MODEL_NAME
    temperature = float(config.AIAnalysts.OPENAI_TEMPERATURE)

    logger.debug(f"AI_DB_AGENT: Calling LLM with model '{model_name}' and temperature {temperature}. Prompt length: {len(prompt_content)} chars.")
    # --- BARIS DEBUG SEMENTARA YANG PERLU ANDA TAMBAHKAN ---
    logger.debug(f"AI_DB_AGENT: FULL PROMPT CONTENT (first 2000 chars):\n{prompt_content[:2000]}")
    if len(prompt_content) > 2000:
        logger.debug(f"AI_DB_AGENT: PROMPT TRUNCATED. Total length: {len(prompt_content)} chars.")
    # --- AKHIR BARIS DEBUG ---

    try:
        response = _openai_client.chat.completions.create(
            model=model_name,
            messages=[
                {"role": "user", "content": prompt_content}
            ],
            temperature=temperature,
            max_tokens=max_tokens,
            top_p=1,
            frequency_penalty=0,
            presence_penalty=0
        )
        if response.choices and response.choices[0].message.content:
            llm_response_content = response.choices[0].message.content.strip()
            logger.debug(f"AI_DB_AGENT: LLM raw response received (first 200 chars): {llm_response_content[:200]}...")
            return llm_response_content
        else:
            logger.warning("AI_DB_AGENT: LLM returned an empty response.")
            return "Error: Empty response from LLM."

    except openai.APIConnectionError as e: # <--- INI SEKARANG AKAN DIKENALI KARENA 'import openai'
        logger.error(f"AI_DB_AGENT: OpenAI API connection error: {e}", exc_info=True)
        return f"Error: OpenAI API connection error: {e}"
    except openai.RateLimitError as e: # <--- INI SEKARANG AKAN DIKENALI
        logger.error(f"AI_DB_AGENT: OpenAI API rate limit exceeded: {e}", exc_info=True)
        return f"Error: OpenAI API rate limit exceeded: {e}"
    except openai.APIStatusError as e: # <--- INI SEKARANG AKAN DIKENALI
        logger.error(f"AI_DB_AGENT: OpenAI API status error {e.status_code}: {e.response}", exc_info=True)
        return f"Error: OpenAI API status error {e.status_code}: {e.response}"
    except Exception as e:
        logger.error(f"AI_DB_AGENT: An unexpected error occurred during LLM call: {e}", exc_info=True)
        return f"Error: An unexpected error occurred: {e}"



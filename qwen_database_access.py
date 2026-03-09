# qwen_database_access.py

import logging
from datetime import datetime, timezone, timedelta
import json
from decimal import Decimal

# Impor database_manager untuk berinteraksi dengan DB
import database_manager
# Impor model untuk mendapatkan nama tabel dan kolom secara dinamis (opsional, tapi lebih robust)
from database_models import (
    DailyOpen, PriceTick, HistoricalCandle, SessionMetadata, SessionCandleData,
    SessionSwingData, SupportResistanceLevel, SupplyDemandZone, FibonacciLevel,
    OrderBlock, FairValueGap, MarketStructureEvent, LiquidityZone,
    MovingAverage, MACDValue, RSIValue, Divergence, VolumeProfile,
    AIAnalysisResult, MT5AccountInfo, MT5Position, MT5Order, MT5DealHistory,
    EconomicEvent, NewsArticle, FeatureBackfillStatus
)
# Impor config untuk pengaturan seperti TRADING_SYMBOL
from config import config

logger = logging.getLogger(__name__)

class QwenDatabaseAccess:
    def __init__(self, trading_symbol: str = None):
        # Menggunakan TRADING_SYMBOL dari config jika tidak disediakan
        self.trading_symbol = trading_symbol if trading_symbol else config.TRADING_SYMBOL
        logger.info(f"QwenDatabaseAccess diinisialisasi untuk simbol: {self.trading_symbol}")

    def _serialize_for_qwen(self, data):
        """
        Mengonversi data dari format Python (Decimal, datetime) ke format yang lebih mudah
        dipahami oleh LLM (float, ISO string).
        """
        if isinstance(data, list):
            return [self._serialize_for_qwen(item) for item in data]
        if isinstance(data, dict):
            return {k: self._serialize_for_qwen(v) for k, v in data.items()}
        if isinstance(data, Decimal):
            return float(data)
        if isinstance(data, datetime):
            return data.isoformat() # ISO format sudah bagus
        return data

    def get_database_schema_description(self) -> str:
        """
        Mengembalikan deskripsi skema database yang mudah dipahami oleh Qwen.
        SANGAT DISEDERHANAKAN UNTUK MENGURANGI TOKEN UNTUK UJI COBA INI.
        """
        # >>> GANTI ISI FUNGSI INI DENGAN YANG SANGAT MINIMAL INI UNTUK UJI COBA <<<
        return """
        Berikut adalah skema database PostgreSQL yang relevan:
        - price_ticks: symbol (TEXT), last_price (DECIMAL), time_utc_datetime (DATETIME)
        - historical_candles: symbol (TEXT), timeframe (TEXT), close_price (DECIMAL), open_time_utc (DATETIME)
        - economic_events: name (TEXT), event_time_utc (DATETIME), impact (TEXT), currency (TEXT), actual_value (TEXT)
        - news_articles: title (TEXT), url (TEXT), published_time_utc (DATETIME)
        - mt5_account_info: login (BIGINT), balance (DECIMAL), equity (DECIMAL), profit (DECIMAL)
        - mt5_positions: ticket (BIGINT), symbol (TEXT), type (TEXT), volume (DECIMAL), profit (DECIMAL)
        """
        # >>> AKHIR PERUBAHAN SANGAT MINIMAL <<<

    def get_market_data_for_qwen(self, symbol: str = None, timeframe: str = None, limit: int = None, start_time_utc: datetime = None, end_time_utc: datetime = None) -> dict:
        """
        Mengambil berbagai jenis data pasar yang relevan untuk analisis AI.
        Mengembalikan data dalam format dictionary yang mudah diproses LLM.
        PEMBATASAN AGRESIV UNTUK MENGURANGI TOKEN.
        """
        data = {}
        target_symbol = symbol if symbol else self.trading_symbol
        
        # Contoh: Mengambil candle historis terbaru
        if target_symbol and timeframe:
            candles = database_manager.get_historical_candles_from_db(
                symbol=target_symbol,
                timeframe=timeframe,
                limit=limit if limit else 1, # <<< SANGAT DIKURANGI: HANYA 1 CANDLE TERAKHIR
                start_time_utc=start_time_utc,
                end_time_utc=end_time_utc,
                order_asc=False
            )
            data['historical_candles'] = self._serialize_for_qwen(candles)
            logger.debug(f"Mengambil {len(candles)} candle {timeframe} untuk {target_symbol}.")
        
        # Contoh: Mengambil level S&R aktif (DIKOMENTARI SEMENTARA UNTUK MENGURANGI TOKEN)
        # if target_symbol and timeframe:
        #     sr_levels = database_manager.get_support_resistance_levels(
        #         symbol=target_symbol,
        #         timeframe=timeframe,
        #         is_active=True,
        #         limit=1 # Sangat kecil
        #     )
        #     data['active_support_resistance_levels'] = self._serialize_for_qwen(sr_levels)
        #     logger.debug(f"Mengambil {len(sr_levels)} S&R level aktif untuk {target_symbol} {timeframe}.")

        # Contoh: Mengambil FVG yang belum terisi (DIKOMENTARI SEMENTARA UNTUK MENGURANGI TOKEN)
        # if target_symbol and timeframe:
        #     fvg_gaps = database_manager.get_fair_value_gaps(
        #         symbol=target_symbol,
        #         timeframe=timeframe,
        #         is_filled=False,
        #         limit=1 # Sangat kecil
        #     )
        #     data['unfilled_fair_value_gaps'] = self._serialize_for_qwen(fvg_gaps)
        #     logger.debug(f"Mengambil {len(fvg_gaps)} FVG yang belum terisi untuk {target_symbol} {timeframe}.")
            
        # Contoh: Mengambil posisi MT5 yang sedang terbuka (DIKOMENTARI SEMENTARA UNTUK MENGURANGI TOKEN)
        # mt5_positions = database_manager.get_mt5_positions_from_db(symbol=target_symbol) # Ini harus ada di database_manager
        # if mt5_positions:
        #     data['mt5_open_positions'] = self._serialize_for_qwen(mt5_positions)
        #     logger.debug(f"Mengambil {len(mt5_positions)} posisi MT5 terbuka.")
        # else:
        #     data['mt5_open_positions'] = []
        #     logger.debug("Tidak ada posisi MT5 terbuka.")

        # Anda bisa menambahkan lebih banyak jenis data sesuai kebutuhan AI Anda:
        # - MACD terbaru: database_manager.get_macd_values(...)
        # - RSI terbaru: database_manager.get_rsi_values(...)
        # - Order Blocks: database_manager.get_order_blocks(...)

        return data

    def get_economic_events_for_qwen(self, days_past: int = 1, days_future: int = 2, min_impact: str = 'Medium', target_currency: str = None, limit: int = None) -> list:
        """
        Mengambil event ekonomi dan berita yang relevan untuk analisis AI.
        PEMBATASAN AGRESIV UNTUK MENGURANGI TOKEN.
        """
        events = database_manager.get_upcoming_events_from_db(
            days_past=days_past,
            days_future=days_future,
            min_impact=min_impact,
            target_currency=target_currency,
            limit=limit if limit else 1 # <<< SANGAT DIKURANGI: HANYA 1 EVENT/ARTIKEL
        )
        logger.debug(f"Mengambil {len(events)} event ekonomi dan artikel berita.")
        return self._serialize_for_qwen(events)
    
    # Fungsi lain untuk mengambil data spesifik (misal: MACD, RSI, OrderBlocks, Liquidity Zones)
    # def get_latest_macd_for_qwen(self, symbol, timeframe, limit=1) -> list:
    #     macd_data = database_manager.get_macd_values(symbol=symbol, timeframe=timeframe, limit=limit)
    #     return self._serialize_for_qwen(macd_data)

    # def get_latest_rsi_for_qwen(self, symbol, timeframe, limit=1) -> list:
    #     rsi_data = database_manager.get_rsi_values(symbol=symbol, timeframe=timeframe, limit=limit)
    #     return self._serialize_for_qwen(rsi_data)

    # def get_latest_order_blocks_for_qwen(self, symbol, timeframe, limit=5, is_mitigated=False, type=None) -> list:
    #     ob_data = database_manager.get_order_blocks(symbol=symbol, timeframe=timeframe, limit=limit, is_mitigated=is_mitigated, type=type)
    #     return self._serialize_for_qwen(ob_data)

    # def get_mt5_account_summary_for_qwen(self) -> dict:
    #     account_info = database_manager.get_mt5_account_info_from_db()
    #     return self._serialize_for_qwen(account_info)

    def save_ai_analysis_result_for_qwen(self, result_data: dict):
        """
        Menyimpan hasil analisis AI ke database.
        Menerima dictionary yang sudah disiapkan untuk model AIAnalysisResult.
        """
        try:
            # Pastikan data yang masuk sudah dalam format yang benar untuk database_manager
            # (misal: Decimal untuk harga, datetime untuk waktu)
            processed_data = {
                "symbol": result_data.get("symbol", self.trading_symbol),
                "timestamp": datetime.now(timezone.utc), # Gunakan waktu saat ini sebagai waktu analisis AI
                "analyst_id": result_data.get("analyst_id", "Qwen_Trading_Assistant"),
                "summary": result_data.get("summary"),
                "potential_direction": result_data.get("potential_direction"),
                "recommendation_action": result_data.get("recommendation_action"),
                "entry_price": Decimal(str(result_data["entry_price"])) if "entry_price" in result_data and result_data["entry_price"] is not None else None,
                "stop_loss": Decimal(str(result_data["stop_loss"])) if "stop_loss" in result_data and result_data["stop_loss"] is not None else None,
                "take_profit": Decimal(str(result_data["take_profit"])) if "take_profit" in result_data and result_data["take_profit"] is not None else None,
                "reasoning": result_data.get("reasoning"),
                "ai_confidence": result_data.get("ai_confidence"),
                "raw_response_json": json.dumps(result_data.get("raw_response", {}))
            }
            database_manager.save_ai_analysis_result(**processed_data)
            logger.info(f"Hasil analisis AI untuk {processed_data['symbol']} ({processed_data['potential_direction']}) berhasil disimpan.")
        except Exception as e:
            logger.error(f"Gagal menyimpan hasil analisis AI: {e}", exc_info=True)
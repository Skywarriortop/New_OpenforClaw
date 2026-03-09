# database_models.py

from sqlalchemy import Column, String, DateTime, Integer, ForeignKey, UniqueConstraint, Index, Boolean, DECIMAL, Text, BigInteger # PASTIKAN BigInteger DIIMPOR
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime, timezone
import uuid
from decimal import Decimal
# Base declarative untuk model SQLAlchemy Anda
Base = declarative_base()

# --- Model untuk Data Pasar ---

class DailyOpen(Base):
    __tablename__ = 'daily_opens'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False)
    date_referenced = Column(String(10), nullable=False)
    reference_price = Column(DECIMAL(10, 5), nullable=False)
    source_candle_time_utc = Column(DateTime(timezone=True), nullable=False)
    timestamp_recorded = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint('symbol', 'date_referenced', name='_symbol_date_uc'),
        Index('idx_daily_open_symbol_date', 'symbol', 'date_referenced'),
    )

class PriceTick(Base):
    __tablename__ = 'price_ticks'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False)
    time = Column(BigInteger, nullable=False) # Pastikan ini BigInteger jika timestampnya sangat besar
    time_utc_datetime = Column(DateTime(timezone=True), nullable=False) 
    last_price = Column(DECIMAL(10, 5), nullable=False)
    bid_price = Column(DECIMAL(10, 5), nullable=False)
    daily_open_price = Column(DECIMAL(10, 5), nullable=True) 
    delta_point = Column(DECIMAL(10, 5), nullable=True)
    change = Column(DECIMAL(10, 5), nullable=True)
    change_percent = Column(DECIMAL(10, 5), nullable=True)
    timestamp_recorded = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint('symbol', 'time', name='_symbol_time_tick_uc'),
        Index('idx_price_tick_symbol_time_utc', 'symbol', 'time_utc_datetime'),
    )

class HistoricalCandle(Base):
    __tablename__ = 'historical_candles'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False)
    timeframe = Column(String(5), nullable=False)
    open_time_utc = Column(DateTime(timezone=True), nullable=False)
    open_price = Column(DECIMAL(10, 5), nullable=False)
    high_price = Column(DECIMAL(10, 5), nullable=False)
    low_price = Column(DECIMAL(10, 5), nullable=False)
    close_price = Column(DECIMAL(10, 5), nullable=False)
    tick_volume = Column(BigInteger, nullable=False) # Bisa jadi BigInteger
    spread = Column(Integer, nullable=False)
    real_volume = Column(BigInteger, nullable=True) # Bisa jadi BigInteger
    timestamp_recorded = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint('symbol', 'timeframe', 'open_time_utc', name='_symbol_tf_time_uc'),
        Index('idx_historical_candle_symbol_tf_time', 'symbol', 'timeframe', 'open_time_utc'),
    )

class SessionMetadata(Base):
    __tablename__ = 'session_metadata'
    id = Column(Integer, primary_key=True, autoincrement=True)
    session_name = Column(String(50), unique=True, nullable=False)
    utc_start_hour = Column(Integer, nullable=False)
    utc_end_hour = Column(Integer, nullable=False)
    timestamp_recorded = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))

class SessionCandleData(Base):
    __tablename__ = 'session_candle_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(Integer, ForeignKey('session_metadata.id'), nullable=False)
    symbol = Column(String(10), nullable=False)
    trade_date = Column(String(10), nullable=False)
    candle_time_utc = Column(DateTime(timezone=True), nullable=False)
    open_price = Column(DECIMAL(10, 5), nullable=False)
    high_price = Column(DECIMAL(10, 5), nullable=False)
    low_price = Column(DECIMAL(10, 5), nullable=False)
    close_price = Column(DECIMAL(10, 5), nullable=False)
    tick_volume = Column(BigInteger, nullable=False) # Bisa jadi BigInteger
    spread = Column(Integer, nullable=False)
    real_volume = Column(BigInteger, nullable=True) # Bisa jadi BigInteger
    base_candle_name = Column(String(50), nullable=True)
    timestamp_recorded = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint('session_id', 'trade_date', 'candle_time_utc', 'base_candle_name', 'symbol', name='_session_candle_uc'), 
        Index('idx_session_candle_data', 'session_id', 'trade_date', 'symbol'), 
    )

class SessionSwingData(Base):
    __tablename__ = 'session_swing_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(Integer, ForeignKey('session_metadata.id'), nullable=False)
    symbol = Column(String(10), nullable=False)
    trade_date = Column(String(10), nullable=False)
    swing_high = Column(DECIMAL(10, 5), nullable=True)
    swing_high_timestamp_utc = Column(DateTime(timezone=True), nullable=True)
    swing_low = Column(DECIMAL(10, 5), nullable=True)
    swing_low_timestamp_utc = Column(DateTime(timezone=True), nullable=True)
    timestamp_recorded = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint('session_id', 'trade_date', 'symbol', name='_session_swing_uc'), 
        Index('idx_session_swing_data', 'session_id', 'trade_date', 'symbol'), 
    )

# --- Model untuk Data Indikator & Level Harga ---

class SupportResistanceLevel(Base):
    __tablename__ = 'support_resistance_levels'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False)
    timeframe = Column(String(5), nullable=False)
    level_type = Column(String(20), nullable=False)
    price_level = Column(DECIMAL(10, 5), nullable=False)
    zone_start_price = Column(DECIMAL(10, 5), nullable=True)
    zone_end_price = Column(DECIMAL(10, 5), nullable=True)
    strength_score = Column(Integer, nullable=True)
    is_active = Column(Boolean, default=True)
    formation_time_utc = Column(DateTime(timezone=True), nullable=False)
    last_test_time_utc = Column(DateTime(timezone=True), nullable=True)
    is_key_level = Column(Boolean, default=False)
    confluence_score = Column(Integer, default=0)
    timestamp_recorded = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint('symbol', 'timeframe', 'price_level', 'level_type', name='_sr_uc'),
        Index('idx_sr_symbol_tf', 'symbol', 'timeframe'),
        Index('idx_sr_active', 'is_active'),
    )

class SupplyDemandZone(Base):
    __tablename__ = 'supply_demand_zones'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False)
    timeframe = Column(String(5), nullable=False)
    zone_type = Column(String(20), nullable=False)
    base_type = Column(String(50), nullable=True)
    zone_top_price = Column(DECIMAL(10, 5), nullable=True)
    zone_bottom_price = Column(DECIMAL(10, 5), nullable=True)
    formation_time_utc = Column(DateTime(timezone=True), nullable=False)
    last_retest_time_utc = Column(DateTime(timezone=True), nullable=True)
    is_mitigated = Column(Boolean, default=False)
    strength_score = Column(Integer, nullable=True)
    is_key_level = Column(Boolean, default=False)
    confluence_score = Column(Integer, default=0)
    timestamp_recorded = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint('symbol', 'timeframe', 'zone_type', 'formation_time_utc', name='_sd_uc'),
        Index('idx_sd_symbol_tf', 'symbol', 'timeframe'),
        Index('idx_sd_mitigated', 'is_mitigated'),
    )

class FibonacciLevel(Base):
    __tablename__ = 'fibonacci_levels'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False)
    timeframe = Column(String(5), nullable=False)
    type = Column(String(50), nullable=False) # e.g., "Retracement", "Extension"
    high_price_ref = Column(DECIMAL(10, 5), nullable=False) 
    low_price_ref = Column(DECIMAL(10, 5), nullable=False)  
    start_time_ref_utc = Column(DateTime(timezone=True), nullable=False)
    end_time_ref_utc = Column(DateTime(timezone=True), nullable=False)
    ratio = Column(DECIMAL(10, 5), nullable=False)          
    price_level = Column(DECIMAL(10, 5), nullable=False)    
    is_active = Column(Boolean, default=True)
    timestamp_recorded = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))

    confluence_score = Column(Integer, default=0)

    current_retracement_percent = Column(DECIMAL(10, 5), nullable=True) 
    deepest_retracement_percent = Column(DECIMAL(10, 5), nullable=True) 
    retracement_direction = Column(Integer, nullable=True)
    last_test_time_utc = Column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        # Ini adalah UniqueConstraint yang penting untuk deduplikasi
        # Sebuah level Fibonacci dianggap unik berdasarkan:
        # simbol, timeframe, tipe (Retracement/Extension), harga referensi high, harga referensi low, dan rasio Fib.
        UniqueConstraint('symbol', 'timeframe', 'type', 'high_price_ref', 'low_price_ref', 'ratio', name='_fib_uc'),
        Index('idx_fibonacci_symbol_timeframe_ratio', 'symbol', 'timeframe', 'ratio'),
    )

    
class OrderBlock(Base):
    __tablename__ = 'order_blocks'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False)
    timeframe = Column(String(5), nullable=False)
    type = Column(String(20), nullable=False)
    ob_top_price = Column(DECIMAL(10, 5), nullable=False)
    ob_bottom_price = Column(DECIMAL(10, 5), nullable=False)
    formation_time_utc = Column(DateTime(timezone=True), nullable=False)
    is_mitigated = Column(Boolean, default=False)
    last_mitigation_time_utc = Column(DateTime(timezone=True), nullable=True)
    strength_score = Column(Integer, nullable=True)
    confluence_score = Column(Integer, default=0) # PASTIKAN KOLOM INI ADA
    timestamp_recorded = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint('symbol', 'timeframe', 'formation_time_utc', 'type', name='_ob_uc'),
        Index('idx_ob_symbol_tf', 'symbol', 'timeframe'),
    )


class FairValueGap(Base):
    __tablename__ = 'fair_value_gaps'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False)
    timeframe = Column(String(5), nullable=False)
    type = Column(String(50), nullable=False)
    fvg_top_price = Column(DECIMAL(10, 5), nullable=False)
    fvg_bottom_price = Column(DECIMAL(10, 5), nullable=False)
    formation_time_utc = Column(DateTime(timezone=True), nullable=False)
    is_filled = Column(Boolean, default=False)
    last_fill_time_utc = Column(DateTime(timezone=True), nullable=True)
    retest_count = Column(Integer, default=0)
    last_retest_time_utc = Column(DateTime(timezone=True), nullable=True)
    # Tambahkan baris ini untuk strength_score
    strength_score = Column(Integer, nullable=True) # Diasumsikan INTEGER, sesuaikan jika desimal/float
    timestamp_recorded = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint('symbol', 'timeframe', 'formation_time_utc', 'type', name='_fvg_uc'),
        Index('idx_fvg_symbol_tf', 'symbol', 'timeframe'),
    )

class MarketStructureEvent(Base):
    __tablename__ = 'market_structure_events'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False)
    timeframe = Column(String(5), nullable=False)
    event_type = Column(String(50), nullable=False)
    direction = Column(String(10), nullable=False)
    price_level = Column(DECIMAL(10, 5), nullable=False)
    event_time_utc = Column(DateTime(timezone=True), nullable=False)
    swing_high_ref_time = Column(DateTime(timezone=True), nullable=True)
    swing_low_ref_time = Column(DateTime(timezone=True), nullable=True)
    # TAMBAHKAN BARIS INI:
    confluence_score = Column(Integer, default=0) # <-- Tambahkan kolom ini
    timestamp_recorded = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint('symbol', 'timeframe', 'event_time_utc', 'event_type', 'direction', name='_ms_uc'),
        Index('idx_ms_symbol_tf', 'symbol', 'timeframe'),
    )

class LiquidityZone(Base):
    __tablename__ = 'liquidity_zones'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False)
    timeframe = Column(String(5), nullable=False)
    zone_type = Column(String(50), nullable=False)
    price_level = Column(DECIMAL(10, 5), nullable=False)
    formation_time_utc = Column(DateTime(timezone=True), nullable=False)
    is_tapped = Column(Boolean, default=False)
    tap_time_utc = Column(DateTime(timezone=True), nullable=True)
    timestamp_recorded = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint('symbol', 'timeframe', 'zone_type', 'price_level', 'formation_time_utc', name='_liq_uc'),
        Index('idx_liq_symbol_tf', 'symbol', 'timeframe'),
    )

class MovingAverage(Base):
    __tablename__ = 'moving_averages'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False)
    timeframe = Column(String(5), nullable=False)
    ma_type = Column(String(10), nullable=False)
    period = Column(Integer, nullable=False)
    timestamp_utc = Column(DateTime(timezone=True), nullable=False)
    value = Column(DECIMAL(10, 5), nullable=False)
    timestamp_recorded = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint('symbol', 'timeframe', 'ma_type', 'period', 'timestamp_utc', name='_ma_uc'),
        Index('idx_ma_symbol_tf_type_period', 'symbol', 'timeframe', 'ma_type', 'period'),
    )

class MACDValue(Base):
    __tablename__ = 'macd_values'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False)
    timeframe = Column(String(5), nullable=False)
    timestamp_utc = Column(DateTime(timezone=True), nullable=False)
    macd_line = Column(DECIMAL(15, 8), nullable=False) # Presisi lebih tinggi untuk MACD
    signal_line = Column(DECIMAL(15, 8), nullable=False)
    histogram = Column(DECIMAL(15, 8), nullable=False)
    # Tambahkan kolom lain jika _calculate_macd Anda mengembalikan `macd_pcent`
    macd_pcent = Column(DECIMAL(15, 8), nullable=True) # MACD sebagai persentase, bisa nullable
    timestamp_recorded = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint('symbol', 'timeframe', 'timestamp_utc', name='_macd_uc'),
        Index('idx_macd_symbol_tf_time', 'symbol', 'timeframe', 'timestamp_utc'),
    )
# --- AKHIR MODEL BARU UNTUK MACD ---
class RSIValue(Base):
    __tablename__ = 'rsi_values'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False)
    timeframe = Column(String(5), nullable=False)
    timestamp_utc = Column(DateTime(timezone=True), nullable=False)
    value = Column(DECIMAL(15, 8), nullable=False) # Nilai RSI
    timestamp_recorded = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint('symbol', 'timeframe', 'timestamp_utc', name='_rsi_uc'),
        Index('idx_rsi_symbol_tf_time', 'symbol', 'timeframe', 'timestamp_utc'),
    )


class Divergence(Base):
    __tablename__ = 'divergences'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False)
    timeframe = Column(String(5), nullable=False)
    indicator_type = Column(String(50), nullable=False)
    divergence_type = Column(String(50), nullable=False)
    price_point_time_utc = Column(DateTime(timezone=True), nullable=False)
    indicator_point_time_utc = Column(DateTime(timezone=True), nullable=False)
    price_level_1 = Column(DECIMAL(10, 5), nullable=True)
    price_level_2 = Column(DECIMAL(10, 5), nullable=True)
    indicator_value_1 = Column(DECIMAL(10, 5), nullable=True)
    indicator_value_2 = Column(DECIMAL(10, 5), nullable=True)
    is_active = Column(Boolean, default=True)
    timestamp_recorded = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint('symbol', 'timeframe', 'indicator_type', 'divergence_type', 'price_point_time_utc', 'indicator_point_time_utc', name='_div_uc'),
        Index('idx_div_symbol_tf_type', 'symbol', 'timeframe', 'divergence_type'),
    )

class VolumeProfile(Base):
    __tablename__ = 'volume_profiles'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False)
    timeframe = Column(String(5), nullable=False)
    period_start_utc = Column(DateTime(timezone=True), nullable=False)
    period_end_utc = Column(DateTime(timezone=True), nullable=False)
    poc_price = Column(DECIMAL(10, 5), nullable=True)
    vah_price = Column(DECIMAL(10, 5), nullable=True)
    val_price = Column(DECIMAL(10, 5), nullable=True)
    total_volume = Column(DECIMAL(15, 2), nullable=True)
    profile_data_json = Column(Text, nullable=True)
    row_height_value = Column(DECIMAL(10, 5), nullable=True) # <-- TAMBAHAN BARU INI
    timestamp_recorded = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint('symbol', 'timeframe', 'period_start_utc', name='_vp_uc'),
        Index('idx_vp_symbol_tf_start', 'symbol', 'timeframe', 'period_start_utc'),
    )


# --- Model untuk Data AI dan Trading ---

class AIAnalysisResult(Base):
    __tablename__ = 'ai_analysis_results'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False)
    timestamp = Column(DateTime(timezone=True), nullable=False)
    # MODIFIKASI INI: Ubah String(10) menjadi String(100)
    analyst_id = Column(String(100), nullable=False) # Contoh: Perpanjang menjadi 100 karakter
    summary = Column(Text, nullable=True)
    potential_direction = Column(String(10), nullable=True)
    recommendation_action = Column(String(10), nullable=True)
    entry_price = Column(DECIMAL(10, 5), nullable=True)
    stop_loss = Column(DECIMAL(10, 5), nullable=True)
    take_profit = Column(DECIMAL(10, 5), nullable=True)
    reasoning = Column(Text, nullable=True)
    # MODIFIKASI INI: Ubah String(10) menjadi String(50)
    ai_confidence = Column(String(50), nullable=True) # Contoh: Perpanjang menjadi 50 karakter
    raw_response_json = Column(Text, nullable=True)
    timestamp_recorded = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint('symbol', 'timestamp', 'analyst_id', name='_ai_analysis_uc'),
        Index('idx_ai_analysis_symbol_time', 'symbol', 'timestamp'),
    )

class LLMPerformanceReview(Base):
    __tablename__ = 'llm_performance_reviews'
    id = Column(Integer, primary_key=True, autoincrement=True)
    review_time_utc = Column(DateTime(timezone=True), nullable=False, default=datetime.now(timezone.utc))
    review_period_start_utc = Column(DateTime(timezone=True), nullable=False)
    review_period_end_utc = Column(DateTime(timezone=True), nullable=False)
    symbol = Column(String(10), nullable=False)
    total_proposals_reviewed = Column(Integer, nullable=False)
    profitable_proposals = Column(Integer, nullable=False)
    losing_proposals = Column(Integer, nullable=False)
    drawdown_proposals = Column(Integer, nullable=False) # Untuk SL hits atau kerugian signifikan
    accuracy_score_proposals = Column(DECIMAL(5,2), nullable=True) # Akurasi prediksi arah jika ada
    
    # Diagnosis LLM
    llm_diagnosis_summary = Column(Text, nullable=True) # Rangkuman diagnosis diri oleh LLM
    llm_identified_strengths = Column(Text, nullable=True) # Kekuatan yang diidentifikasi LLM
    llm_identified_weaknesses = Column(Text, nullable=True) # Kelemahan yang diidentifikasi LLM
    llm_improvement_suggestions = Column(Text, nullable=True) # Saran perbaikan dari LLM (contoh prompt, data)
    raw_llm_response_json = Column(Text, nullable=True) # Respons mentah LLM untuk diagnosis
    
    timestamp_recorded = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint('symbol', 'review_period_start_utc', 'review_period_end_utc', name='_llm_review_uc'),
        Index('idx_llm_review_symbol_time', 'symbol', 'review_period_start_utc'),
    )
    
        
class MT5AccountInfo(Base):
    __tablename__ = 'mt5_account_info'
    login = Column(BigInteger, primary_key=True) # Pastikan BigInteger
    name = Column(String(100), nullable=False)
    server = Column(String(100), nullable=False)
    balance = Column(DECIMAL(15, 2), nullable=False)
    equity = Column(DECIMAL(15, 2), nullable=False)
    profit = Column(DECIMAL(15, 2), nullable=False)
    free_margin = Column(DECIMAL(15, 2), nullable=False)
    currency = Column(String(10), nullable=False)
    leverage = Column(Integer, nullable=False)
    company = Column(String(100), nullable=False)
    timestamp_recorded = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))

class MT5Position(Base):
    __tablename__ = 'mt5_positions'

    id = Column(Integer, primary_key=True)
    ticket = Column(BigInteger, unique=True, nullable=False, index=True)
    symbol = Column(String(50), nullable=False)
    type = Column(String(10), nullable=False) # 'buy' or 'sell'
    volume = Column(DECIMAL(20, 8), nullable=False)
    price_open = Column(DECIMAL(20, 8), nullable=False)
    time_open = Column(DateTime(timezone=True), nullable=False)
    current_price = Column(DECIMAL(20, 8), nullable=False)
    profit = Column(DECIMAL(20, 8), nullable=False)
    swap = Column(DECIMAL(20, 8), nullable=False, default=Decimal('0.0'))
    comment = Column(String(255))
    magic = Column(Integer)
    sl_price = Column(DECIMAL(20, 8)) # Bisa NULL
    tp_price = Column(DECIMAL(20, 8)) # Bisa NULL
    
    # --- START MODIFIKASI BARU ---
    # Kolom baru untuk menyimpan status hit TP parsial dalam format JSON string (misal: "[true, false, false]")
    partial_tp_hit_flags_json = Column(Text, nullable=False, default="[]") 

    # --- END MODIFIKASI BARU ---

    timestamp_recorded = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    tp_levels_config_json = Column(Text, nullable=False, default="[]") 

    __table_args__ = (
        UniqueConstraint('ticket', name='_mt5_position_ticket_uc'),
        Index('idx_mt5_positions_symbol_type', 'symbol', 'type'),
    )

    def __repr__(self):
        return f"<MT5Position(ticket={self.ticket}, symbol='{self.symbol}', type='{self.type}', volume={float(self.volume):.2f})>"


class MT5Order(Base):
    __tablename__ = 'mt5_orders'
    ticket = Column(BigInteger, primary_key=True) # PASTIKAN BIGINTEGER
    symbol = Column(String(10), nullable=False)
    type = Column(String(50), nullable=False)
    price_open = Column(DECIMAL(10, 5), nullable=False)
    volume_initial = Column(DECIMAL(10, 2), nullable=False)
    volume_current = Column(DECIMAL(10, 2), nullable=False)
    time_setup = Column(DateTime(timezone=True), nullable=False)
    expiration = Column(DateTime(timezone=True), nullable=True)
    comment = Column(String(255), nullable=True)
    magic = Column(BigInteger, nullable=True) # PASTIKAN BIGINTEGER
    state = Column(String(50), nullable=True)
    timestamp_recorded = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))

class MT5DealHistory(Base):
    __tablename__ = 'mt5_deal_history'
    ticket = Column(BigInteger, primary_key=True) # PASTIKAN BIGINTEGER
    order_ticket = Column(BigInteger, nullable=False) # PASTIKAN BIGINTEGER
    symbol = Column(String(10), nullable=False)
    type = Column(String(10), nullable=False)
    entry_type = Column(String(10), nullable=True)
    price = Column(DECIMAL(10, 5), nullable=False)
    volume = Column(DECIMAL(10, 2), nullable=False)
    profit = Column(DECIMAL(15, 2), nullable=True)
    time = Column(DateTime(timezone=True), nullable=False)
    comment = Column(String(255), nullable=True)
    magic = Column(BigInteger, nullable=True) # PASTIKAN BIGINTEGER
    timestamp_recorded = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))

# --- Model untuk Data Fundamental ---

class EconomicEvent(Base):
    __tablename__ = 'economic_events'
    event_id = Column(String(255), primary_key=True, default=lambda: str(uuid.uuid4())) # <-- MODIFIKASI INI
    name = Column(String(255), nullable=False)
    country = Column(String(10), nullable=True)
    currency = Column(String(10), nullable=False)
    impact = Column(String(10), nullable=False)
    event_time_utc = Column(DateTime(timezone=True), nullable=False)
    actual_value = Column(String(50), nullable=True)
    forecast_value = Column(String(50), nullable=True)
    previous_value = Column(String(50), nullable=True)
    smart_analysis = Column(Text, nullable=True)
    timestamp_recorded = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint('event_id', name='_economic_event_uc'),
        Index('idx_economic_event_time', 'event_time_utc'),
        Index('idx_economic_event_currency_impact', 'currency', 'impact'),
    )

class NewsArticle(Base):
    __tablename__ = 'news_articles'
    id = Column(String(255), primary_key=True, default=lambda: str(uuid.uuid4())) # <-- MODIFIKASI INI
    source = Column(String(50), nullable=False)
    title = Column(String(500), nullable=False)
    summary = Column(Text, nullable=True)
    url = Column(String(1000), unique=True, nullable=False)
    published_time_utc = Column(DateTime(timezone=True), nullable=False)
    relevance_score = Column(Integer, nullable=True)
    thumbnail_url = Column(String(1000), nullable=True)
    smart_analysis = Column(Text, nullable=True)
    content_hash = Column(String(64), unique=True, nullable=True)
    timestamp_recorded = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint('url', name='_news_article_url_uc'),
        Index('idx_news_article_published_time', 'published_time_utc'),
        Index('idx_news_article_source', 'source'),
    )

class TradeJournal(Base):
    __tablename__ = 'trade_journal'

    id = Column(Integer, primary_key=True, index=True)

    symbol = Column(String, index=True)
    timeframe = Column(String)

    entry_price = Column(Float)
    stop_loss = Column(Float)
    take_profit = Column(Float)

    indicator_snapshot = Column(JSON)
    news_sentiment = Column(String)

    ai_reasoning = Column(String)
    ai_confidence = Column(Float)

    profit_loss = Column(Float)
    rr = Column(Float)
    drawdown = Column(Float)

    trade_result = Column(String)

    created_at = Column(DateTime, default=datetime.utcnow)

class FeatureBackfillStatus(Base):
    __tablename__ = 'feature_backfill_status'
    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False)
    timeframe = Column(String(5), nullable=False)
    last_processed_time_utc = Column(DateTime(timezone=True), nullable=False, default=datetime(1970, 1, 1, tzinfo=timezone.utc))
    last_update_run_time_utc = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))

    __table_args__ = (
        UniqueConstraint('symbol', 'timeframe', name='_symbol_timeframe_backfill_uc'),
        Index('idx_fbs_symbol_tf', 'symbol', 'timeframe'),
    )
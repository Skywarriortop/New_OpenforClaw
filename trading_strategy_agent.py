import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal
import pandas as pd
import numpy as np
import talib
import json
from collections import defaultdict
import math
import re
import utils
from config import config

# Import modul lokal
import database_manager
import ai_database_agent
import ml_predictor


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


_ml_model = None

def _load_ml_model_once():
    """
    Memuat model ML hanya sekali saat modul atau fungsi pertama kali membutuhkannya.
    """
    global _ml_model
    if _ml_model is None:
        _ml_model = ml_predictor.load_ml_model()
        if _ml_model:
            logger.info("TRADE_AGENT: Model ML berhasil dimuat.")
        else:
            logger.warning("TRADE_AGENT: Gagal memuat model ML. Strategi akan mengandalkan aturan dasar saja.")

# --- FUNGSI _construct_market_analysis_prompt YANG TELAH DIMODIFIKASI ---
def _construct_market_analysis_prompt(
    symbol: str,
    current_price: Decimal,
    current_price_timestamp: datetime,
    historical_candles_df: pd.DataFrame,
    relevant_levels: list,
    ml_prediction_data: dict,
    indicator_analysis_points: list,
    account_info: dict,
    open_positions: list,
    economic_events: list,
    news_articles: list,
    llm_performance_review: dict = None
) -> str:
    logger.debug(f"Membangun prompt LLM untuk {symbol} pada {current_price_timestamp}...")

    llm_self_diagnosis_context = ""
    fundamental_context_str = ""

    analysis_data_for_llm = {
        "current_price": utils.to_float_or_none(current_price),
        "current_price_timestamp": current_price_timestamp.isoformat(),
        "ml_prediction": ml_prediction_data,
        "basic_indicator_analysis": indicator_analysis_points, # Data ini sudah mengandung nilai spesifik
        "relevant_technical_levels_and_zones": relevant_levels,
        "account_info_summary": {
            "balance": utils.to_float_or_none(account_info.get('balance')),
            "equity": utils.to_float_or_none(account_info.get('equity')),
            "profit": utils.to_float_or_none(account_info.get('profit'))
        } if account_info else {},
    }

    prompt_template = """
    Anda adalah analis pasar keuangan yang ahli, logis, dan sangat berhati-hati.
    Tugas Anda adalah analisis data trading berikut dan berikan rekomendasi BUY/SELL/HOLD dengan penalaran yang sangat jelas dan KONSISTEN.

    **Langkah-langkah Analisis WAJIB:**
    1.  **Analisis Indikator & Tren:** Interpretasikan RSI, MACD (divergensi/histogram), dan tren. **PASTIKAN Anda menyertakan NILAI NUMERIK SPESIFIK dari RSI (misal: 'RSI saat ini di 35.2') dan MACD Histogram (misal: 'MACD Histogram di -0.0012') dalam penalaran Anda.**
    2.  **Analisis Level Kunci (S/R, OB, FVG, S/D, Liquidity):**
        * Identifikasi level-level kunci di sekitar harga saat ini (`current_price`).
        * **SANGAT KRUSIAL untuk sinyal BUY:** Level Support (termasuk Bullish OB, Bullish FVG, Demand Zone, Equal Lows Liquidity) yang relevan **HARUS berada DI BAWAH `current_price`** dan/atau `entry_price` yang diusulkan. Ini adalah tempat harga diharapkan memantul NAIK. Jika Anda merekomendasikan BUY, JANGAN sebutkan level support yang berada di atas harga saat ini sebagai "entry point" atau "support".
        * **SANGAT KRUSIAL untuk sinyal SELL:** Level Resistance (termasuk Bearish OB, Bearish FVG, Supply Zone, Equal Highs Liquidity) yang relevan **HARUS berada DI ATAS `current_price`** dan/atau `entry_price` yang diusulkan. Ini adalah tempat harga diharapkan memantul TURUN. Jika Anda merekomendasikan SELL, JANGAN sebutkan level resistance yang berada di bawah harga saat ini sebagai "entry point" atau "resistance".
        * Jika tidak ada level kunci yang relevan sesuai arah sinyal dan posisi harga saat ini, nyatakan demikian dengan jelas.
    3.  **Evaluasi Prediksi ML:** Integrasikan hasil prediksi model Machine Learning dan tingkat kepercayaannya.
    4.  **Rumuskan Rekomendasi Final:** Berikan rekomendasi BUY/SELL/HOLD beserta `entry_price`, `stop_loss`, dan `take_profit` yang spesifik.

    **PENTING: Aturan Harga, Risiko & KONSISTENSI Output:**
    -   Jika `recommendation_action` adalah "BUY": `stop_loss` HARUS lebih kecil dari `entry_price`, dan `take_profit` HARUS lebih besar dari `entry_price`.
    -   Jika `recommendation_action` adalah "SELL": `stop_loss` HARUS lebih besar dari `entry_price`, dan `take_profit` HARUS lebih kecil dari `entry_price`.
    -   `stop_loss` dan `take_profit` HARUS berbeda secara signifikan (lebih dari 0.001) dari `entry_price`.
    -   **KONSISTENSI NUMERIK:** `entry_price`, `stop_loss`, dan `take_profit` dalam JSON output Anda **HARUS SAMA PERSIS** dengan yang Anda jelaskan dalam `reasoning`. Jangan ada perbedaan angka.
    -   **PENENTUAN SL:** Jelaskan bagaimana Stop Loss ditentukan. Idealnya, Stop Loss ditempatkan **di luar** (di bawah untuk BUY, di atas untuk SELL) level kunci yang signifikan (misalnya, di bawah support terdekat atau swing low untuk BUY; di atas resistance terdekat atau swing high untuk SELL).
    -   **PENENTUAN TP:** Jelaskan bagaimana Take Profit ditentukan. Idealnya, Take Profit ditargetkan pada level kunci signifikan berikutnya atau berdasarkan Risk-Reward Ratio yang masuk akal.

    **Data Analisis Input:**
    ```json
    {analysis_data}
    ```

    **Instruksi Output (FORMAT SANGAT KRITIS):**
    Hasilkan respons Anda **HANYA** dalam format JSON. **WAJIB DIAWALI DENGAN ````json` DAN DIAKHIRI DENGAN ````. TIDAK ADA TEKS LAIN DI LUAR BLOK JSON INI.** Jika Anda gagal mengikuti format ini, sistem tidak dapat memprosesnya.

    ```json
    {{
      "summary": "[Rangkuman singkat rekomendasi dan alasan utama, maks 50 kata. Pastikan konsisten dengan rekomendasi dan penalaran, DAN SERTAKAN NILAI RSI/MACD jika relevan.]",
      "recommendation_action": "[BUY/SELL/HOLD]",
      "entry_price": [harga_float_or_null],
      "stop_loss": [harga_float_or_null],
      "take_profit": [harga_float_or_null],
      "reasoning": "[Penjelasan detail mengapa rekomendasi diberikan (maksimal 200 kata, perhatikan batas token). Ikuti poin-poin analisis wajib di atas. **SANGAT JELASKAN bagaimana Entry, SL, dan TP ditentukan secara spesifik relatif terhadap level kunci dan `current_price`. Contoh: 'Entry di [harga] karena ... SL di [harga] (di bawah support [level_harga]) karena ... TP di [harga] (di level resistance [level_harga]) karena ...'. SERTAKAN NILAI RSI DAN MACD NUMERIK DI SINI.** Pastikan semua angka di sini cocok dengan nilai di field JSON di atas.]"
    }}
    ```
    """

    return prompt_template.format(
        analysis_data=json.dumps(analysis_data_for_llm, indent=2, default=str),
        symbol=symbol,
        llm_self_diagnosis_context=llm_self_diagnosis_context,
        fundamental_context_str=fundamental_context_str
    )

# --- FUNGSI analyze_and_propose_trade YANG TELAH DIMODIFIKASI SECARA KONSISTEN DAN TERMASUK PERBAIKAN TYPO ---
def analyze_and_propose_trade(symbol: str, timeframe: str = config.Trading.DEFAULT_TIMEFRAME) -> dict:
    logger.info(f"Memulai analisis trading dan proposal untuk {symbol} {timeframe}...")

    _load_ml_model_once()
    if _ml_model is None:
        logger.warning("TRADE_AGENT: Model ML tidak tersedia. Hanya menggunakan aturan dasar saja.")

    num_candles_for_current_tf_analysis = 500
    try:
        df_candles = ml_predictor.load_historical_data_for_ml(symbol, timeframe, num_candles_for_current_tf_analysis + 50)

        if df_candles.empty or len(df_candles) < 50:
            return {"status": "error", "message": f"Tidak ada data candle {timeframe} untuk {symbol} yang cukup ({len(df_candles)}). Tidak dapat menganalisis."}

    except Exception as e:
        logger.error(f"Gagal mengambil data candle untuk analisis: {e}", exc_info=True)
        return {"status": "error", "message": f"Gagal mengambil data candle historis dari database: {e}"}

    latest_close_price = df_candles['close'].iloc[-1]
    latest_close_price_dec = utils.to_decimal_or_none(latest_close_price)
    if latest_close_price_dec is None:
        logger.warning("Harga penutupan terbaru tidak valid. Melewatkan analisis.")
        return {"status": "error", "message": "Harga penutupan terbaru tidak valid untuk analisis."}

    ma_short_period = int(config.MarketData.MA_PERIODS_TO_CALCULATE[0]) if config.MarketData.MA_PERIODS_TO_CALCULATE else 20
    ma_long_period = int(config.MarketData.MA_PERIODS_TO_CALCULATE[1]) if len(config.MarketData.MA_PERIODS_TO_CALCULATE) > 1 else 50

    latest_ma_short, latest_ma_long, latest_rsi, latest_macd_hist = None, None, None, None
    try:
        latest_ma_short_obj = database_manager.get_moving_averages(symbol, timeframe, "EMA", ma_short_period, limit=1)
        latest_ma_long_obj = database_manager.get_moving_averages(symbol, timeframe, "EMA", ma_long_period, limit=1)
        latest_rsi_obj = database_manager.get_rsi_values(symbol, timeframe, limit=1)
        latest_macd_obj = database_manager.get_macd_values(symbol, timeframe, limit=1)

        latest_ma_short = utils.to_decimal_or_none(latest_ma_short_obj[0]['value']) if latest_ma_short_obj else None
        latest_ma_long = utils.to_decimal_or_none(latest_ma_long_obj[0]['value']) if latest_ma_long_obj else None
        latest_rsi = utils.to_decimal_or_none(latest_rsi_obj[0]['value']) if latest_rsi_obj else None
        latest_macd_hist = utils.to_decimal_or_none(latest_macd_obj[0]['histogram']) if latest_macd_obj else None

    except Exception as e:
        logger.warning(f"Gagal mengambil data indikator terbaru dari DB: {e}. Menggunakan kalkulasi on-the-fly jika memungkinkan.", exc_info=True)
        if 'close' in df_candles.columns and len(df_candles) >= max(ma_short_period, ma_long_period, 14):
            latest_ma_short = utils.to_decimal_or_none(df_candles['close'].ewm(span=ma_short_period, adjust=False).mean().iloc[-1])
            latest_ma_long = utils.to_decimal_or_none(df_candles['close'].ewm(span=ma_long_period, adjust=False).mean().iloc[-1])
            latest_rsi = utils.to_decimal_or_none(pd.Series(talib.RSI(df_candles['close'].values.astype(float), timeperiod=14), index=df_candles.index).iloc[-1])
            macd, _, macd_hist = talib.MACD(df_candles['close'].values.astype(float), fastperiod=12, slowperiod=26, signalperiod=9)
            latest_macd_hist = utils.to_decimal_or_none(pd.Series(macd_hist, index=df_candles.index).iloc[-1])
            logger.info("TRADE_AGENT: Indikator dihitung on-the-fly karena tidak ada di DB.")
        else:
            logger.warning("TRADE_AGENT: Data tidak cukup atau kolom indikator hilang untuk kalkulasi on-the-fly.")
            latest_ma_short, latest_ma_long, latest_rsi, latest_macd_hist = None, None, None, None

    # --- BAGIAN PERHITUNGAN ATR UNTUK SL/TP ---
    atr_period = config.MarketData.ATR_PERIOD
    latest_atr = None
    if 'high' in df_candles.columns and 'low' in df_candles.columns and 'close' in df_candles.columns and len(df_candles) >= atr_period:
        atr_values = talib.ATR(
            df_candles['high'].values.astype(float),
            df_candles['low'].values.astype(float),
            df_candles['close'].values.astype(float),
            timeperiod=atr_period
        )
        latest_atr = utils.to_decimal_or_none(atr_values[-1])
        logger.info(f"TRADE_AGENT: ATR terbaru dihitung: {latest_atr:.5f} (periode: {atr_period})")
    else:
        logger.warning(f"TRADE_AGENT: Data tidak cukup atau kolom hilang untuk kalkulasi ATR on-the-fly. Diperlukan minimal {atr_period} lilin untuk ATR.")
    # --- AKHIR BAGIAN PERHITUNGAN ATR ---

    # --- Pengambilan Data Level Penting dari Database (Lintas Timeframe) ---
    relevant_levels_raw = [] # Mengganti nama agar lebih jelas ini adalah data mentah

    fixed_timeframes_for_llm_levels = ["M15", "H1", "H4", "D1"]

    if timeframe not in fixed_timeframes_for_llm_levels:
        timeframes_for_level_collection = [timeframe] + fixed_timeframes_for_llm_levels
    else:
        timeframes_for_level_collection = fixed_timeframes_for_llm_levels

    timeframe_order = ["M5","M15","M30","H1","H4","D1"]
    timeframes_for_level_collection = sorted(list(set(timeframes_for_level_collection)),
                                             key=lambda tf: timeframe_order.index(tf) if tf in timeframe_order else len(timeframe_order))

    price_range_buffer = latest_close_price_dec * Decimal('0.01')
    min_price_filter = latest_close_price_dec - price_range_buffer
    max_price_filter = latest_close_price_dec + price_range_buffer

    logger.debug(f"Mengumpulkan level penting di rentang: {float(min_price_filter):.5f} - {float(max_price_filter):.5f}")

    for tf_level in timeframes_for_level_collection:
        logger.debug(f"Mengambil level untuk TF: {tf_level}")

        relevant_levels_raw.extend([
            {"type": "Resistance Level", "timeframe": tf_level,
             "price": utils.to_float_or_none(res['price_level']),
             "confluence_score": res.get('confluence_score', 0)}
            for res in database_manager.get_support_resistance_levels(
                symbol=symbol, timeframe=tf_level, level_type="Resistance", is_active=True,
                min_price_level=min_price_filter, max_price_level=max_price_filter, # <--- DIPERBAIKI
                order_by_price_asc=True, limit=2
            )
        ])
        relevant_levels_raw.extend([
            {"type": "Support Level", "timeframe": tf_level,
             "price": utils.to_float_or_none(sup['price_level']),
             "confluence_score": sup.get('confluence_score', 0)}
            for sup in database_manager.get_support_resistance_levels(
                symbol=symbol, timeframe=tf_level, level_type="Support", is_active=True,
                min_price_level=min_price_filter, max_price_level=max_price_filter, # <--- DIPERBAIKI
                order_by_price_desc=True, limit=2
            )
        ])
        relevant_levels_raw.extend([
            {"type": "Bullish Order Block", "timeframe": tf_level,
             "top_price": utils.to_float_or_none(ob['ob_top_price']),
             "bottom_price": utils.to_float_or_none(ob['ob_bottom_price']),
             "confluence_score": ob.get('confluence_score', 0)}
            for ob in database_manager.get_order_blocks(
                symbol=symbol, timeframe=tf_level, type="Bullish", is_mitigated=False,
                min_price_level=min_price_filter, max_price_level=max_price_filter, # <--- DIPERBAIKI
                order_by_price_desc=True, limit=2
            )
        ])
        relevant_levels_raw.extend([
            {"type": "Bearish Order Block", "timeframe": tf_level,
             "top_price": utils.to_float_or_none(ob['ob_top_price']),
             "bottom_price": utils.to_float_or_none(ob['ob_bottom_price']),
             "confluence_score": ob.get('confluence_score', 0)}
            for ob in database_manager.get_order_blocks(
                symbol=symbol, timeframe=tf_level, type="Bearish", is_mitigated=False,
                min_price_level=min_price_filter, max_price_level=max_price_filter, # <--- DIPERBAIKI
                order_by_price_asc=True, limit=2
            )
        ])
        relevant_levels_raw.extend([
            {"type": "Bullish FVG", "timeframe": tf_level,
             "top_price": utils.to_float_or_none(fvg['fvg_top_price']),
             "bottom_price": utils.to_float_or_none(fvg['fvg_bottom_price']),
             "confluence_score": fvg.get('strength_score', 0)}
            for fvg in database_manager.get_fair_value_gaps(
                symbol=symbol, timeframe=tf_level, type="Bullish Imbalance", is_filled=False,
                min_price_level=min_price_filter, max_price_level=max_price_filter, # <--- DIPERBAIKI
                order_by_price_desc=True, limit=2
            )
        ])
        relevant_levels_raw.extend([
            {"type": "Bearish FVG", "timeframe": tf_level,
             "top_price": utils.to_float_or_none(fvg['fvg_top_price']),
             "bottom_price": utils.to_float_or_none(fvg['fvg_bottom_price']),
             "confluence_score": fvg.get('strength_score', 0)}
            for fvg in database_manager.get_fair_value_gaps(
                symbol=symbol, timeframe=tf_level, type="Bearish Imbalance", is_filled=False,
                min_price_level=min_price_filter, max_price_level=max_price_filter, # <--- DIPERBAIKI
                order_by_price_asc=True, limit=2
            )
        ])
        relevant_levels_raw.extend([
            {"type": "Equal Highs Liquidity", "timeframe": tf_level,
             "price": utils.to_float_or_none(liq['price_level']),
             "confluence_score": liq.get('confluence_score', 0)}
            for liq in database_manager.get_liquidity_zones(
                symbol=symbol, timeframe=tf_level, zone_type="Equal Highs", is_tapped=False,
                min_price_level=min_price_filter, max_price_level=max_price_filter, # <--- DIPERBAIKI
                limit=1
            )
        ])
        relevant_levels_raw.extend([
            {"type": "Equal Lows Liquidity", "timeframe": tf_level,
             "price": utils.to_float_or_none(liq['price_level']),
             "confluence_score": liq.get('confluence_score', 0)}
            for liq in database_manager.get_liquidity_zones(
                symbol=symbol, timeframe=tf_level, zone_type="Equal Lows", is_tapped=False,
                min_price_level=min_price_filter, max_price_level=max_price_filter, # <--- DIPERBAIKI
                limit=1
            )
        ])
        relevant_levels_raw.extend([
            {"type": "Demand Zone", "timeframe": tf_level,
             "top_price": utils.to_float_or_none(sd['zone_top_price']),
             "bottom_price": utils.to_float_or_none(sd['zone_bottom_price']),
             "confluence_score": sd.get('confluence_score', 0)}
            for sd in database_manager.get_supply_demand_zones(
                symbol=symbol, timeframe=tf_level, zone_type="Demand", is_mitigated=False,
                min_price_level=min_price_filter, max_price_level=max_price_filter, # <--- DIPERBAIKI
                limit=1
            )
        ])
        relevant_levels_raw.extend([
            {"type": "Supply Zone", "timeframe": tf_level,
             "top_price": utils.to_float_or_none(sd['zone_top_price']),
             "bottom_price": utils.to_float_or_none(sd['zone_bottom_price']),
             "confluence_score": sd.get('confluence_score', 0)}
            for sd in database_manager.get_supply_demand_zones(
                symbol=symbol, timeframe=tf_level, zone_type="Supply", is_mitigated=False,
                min_price_level=min_price_filter, max_price_level=max_price_filter, # <--- DIPERBAIKI
                limit=1
            )
        ])
        relevant_levels_raw.extend([
            {"type": "Fibonacci Level", "timeframe": tf_level,
             "price": utils.to_float_or_none(fib['price_level']),
             "ratio": utils.to_float_or_none(fib['ratio']),
             "confluence_score": fib.get('confluence_score', 0)}
            for fib in database_manager.get_fibonacci_levels(
                symbol=symbol, timeframe=tf_level, is_active=True,
                min_price_level=min_price_filter, max_price_level=max_price_filter, # <--- DIPERBAIKI
                limit=1
            )
        ])
        relevant_levels_raw.extend([
            {"type": "Market Structure Event", "timeframe": tf_level,
             "price": utils.to_float_or_none(ms_event['price_level']),
             "confluence_score": ms_event.get('confluence_score', 0)}
            for ms_event in database_manager.get_market_structure_events(
                symbol=symbol, timeframe=tf_level,
                event_type=['Yearly High', 'Yearly Low'],
                min_price_level=min_price_filter, max_price_level=max_price_filter, # <--- DIPERBAIKI
                limit=1
            )
        ])

    divergences_obj = database_manager.get_divergences(symbol=symbol, timeframe=timeframe, is_active=True, limit=1)
    divergence_info_for_llm = []
    for div in divergences_obj:
        price_level_1 = utils.to_float_or_none(div.get('price_level_1'))
        price_level_2 = utils.to_float_or_none(div.get('price_level_2'))
        if price_level_1 is not None and price_level_2 is not None:
             divergence_info_for_llm.append(
                {"type": div.get('divergence_type'), "indicator": div.get('indicator_type'),
                 "timeframe": div.get('timeframe'), "price_range_min": min(price_level_1, price_level_2),
                 "price_range_max": max(price_level_1, price_level_2)}
             )
        else:
             divergence_info_for_llm.append(
                {"type": div.get('divergence_type'), "indicator": div.get('indicator_type'),
                 "timeframe": div.get('timeframe'), "description": "Price range not available"}
             )

    # --- LOGIKA KEPUTUSAN (ML Predictor & Aturan Dasar) ---
    trade_recommendation_ml = "HOLD"
    ml_confidence = 0
    indicator_analysis_points_list = []

    if _ml_model:
        required_candles_for_ml_features = max(
            int(config.MarketData.MA_PERIODS_TO_CALCULATE[1]),
            config.AIAnalysts.RSI_PERIOD,
            config.AIAnalysts.MACD_SLOW_PERIOD + config.AIAnalysts.MACD_SIGNAL_PERIOD,
            config.MarketData.ATR_PERIOD
        ) + 5

        if len(df_candles) >= required_candles_for_ml_features:
            df_for_ml_features = df_candles.iloc[-required_candles_for_ml_features:].copy()
            features_for_prediction, _ = ml_predictor.generate_features_and_labels(df_for_ml_features, prediction_horizon=1)

            if not features_for_prediction.empty:
                latest_features = features_for_prediction.iloc[-1].to_frame().T
                ml_prediction_raw = ml_predictor.predict_with_model(_ml_model, latest_features)

                if ml_prediction_raw == 1:
                    trade_recommendation_ml = "BUY"
                    indicator_analysis_points_list.append("Model Machine Learning memprediksi harga akan NAIK.")
                elif ml_prediction_raw == -1:
                    trade_recommendation_ml = "SELL"
                    indicator_analysis_points_list.append("Model Machine Learning memprediksi harga akan TURUN.")
                else:
                    trade_recommendation_ml = "HOLD"
                    indicator_analysis_points_list.append("Model Machine Learning memprediksi harga akan TETAP (Sideways).")

                if hasattr(_ml_model, 'predict_proba'):
                    prediction_proba = _ml_model.predict_proba(latest_features)
                    predicted_class_index = np.where(_ml_model.classes_ == ml_prediction_raw)[0][0]
                    ml_confidence = int(prediction_proba[0, predicted_class_index] * 100)
                    indicator_analysis_points_list.append(f"Kepercayaan model ML: {ml_confidence}%.")
                else:
                    ml_confidence = 50
                    indicator_analysis_points_list.append("Kepercayaan model ML: Tidak tersedia (menggunakan default).")

            else:
                logger.warning("TRADE_AGENT: Gagal membuat fitur untuk prediksi ML. Menggunakan aturan dasar.")
        else:
            logger.warning(f"TRADE_AGENT: Data candle ({len(df_candles)}) tidak cukup untuk fitur ML ({required_candles_for_ml_features}). Menggunakan aturan dasar.")

    final_trade_recommendation = trade_recommendation_ml
    final_confidence = ml_confidence

    if _ml_model is None or final_confidence < 50:
        logger.warning("TRADE_AGENT: Menggunakan aturan dasar karena model ML tidak tersedia atau kepercayaan rendah.")
        trade_recommendation_rule = "HOLD"
        confidence_rule = 0

        if latest_ma_short is not None and latest_ma_long is not None:
            if latest_ma_short > latest_ma_long:
                indicator_analysis_points_list.append(f"ATURAN DASAR: Tren jangka pendek (MA {ma_short_period}) berada di atas tren jangka panjang (MA {ma_long_period}), menunjukkan bias bullish.")
                confidence_rule += 30
                if latest_close_price_dec is not None and latest_close_price_dec > latest_ma_short:
                    indicator_analysis_points_list.append(f"ATURAN DASAR: Harga ({latest_close_price_dec:.5f}) di atas MA jangka pendek, mengkonfirmasi kekuatan bullish.")
                    confidence_rule += 15
                    trade_recommendation_rule = "BUY"
            elif latest_ma_short < latest_ma_long:
                indicator_analysis_points_list.append(f"ATURAN DASAR: Tren jangka pendek (MA {ma_short_period}) berada di bawah tren jangka panjang (MA {ma_long_period}), menunjukkan bias bearish.")
                confidence_rule += 30
                if latest_close_price_dec is not None and latest_close_price_dec < latest_ma_short:
                    indicator_analysis_points_list.append(f"ATURAN DASAR: Harga ({latest_close_price_dec:.5f}) di bawah MA jangka pendek, mengkonfirmasi kekuatan bearish.")
                    confidence_rule += 15
                    trade_recommendation_rule = "SELL"
            else:
                indicator_analysis_points_list.append("ATURAN DASAR: MA jangka pendek dan panjang saling berdekatan, mengindikasikan pasar ranging atau tren tidak jelas.")
                confidence_rule += 5
        # Pindahkan bagian ini agar selalu disertakan di indicator_analysis_points_list
        if latest_rsi is not None:
            indicator_analysis_points_list.append(f"RSI terbaru: {latest_rsi:.2f}.")
            if latest_rsi > Decimal('70'):
                indicator_analysis_points_list.append(f"RSI ({latest_rsi:.2f}) di area overbought, mengindikasikan potensi pembalikan turun.")
                if trade_recommendation_rule == "BUY": # Hanya dalam mode rule-based, ini bisa mengubah rekomendasi
                    trade_recommendation_rule = "HOLD"
            elif latest_rsi < Decimal('30'):
                indicator_analysis_points_list.append(f"RSI ({latest_rsi:.2f}) di area oversold, mengindikasikan potensi pembalikan naik.")
                if trade_recommendation_rule == "SELL": # Hanya dalam mode rule-based, ini bisa mengubah rekomendasi
                    trade_recommendation_rule = "HOLD"
            else:
                indicator_analysis_points_list.append(f"RSI ({latest_rsi:.2f}) berada di area netral (30-70).")
        if latest_macd_hist is not None:
            indicator_analysis_points_list.append(f"MACD Histogram terbaru: {latest_macd_hist:.5f}.")
            if latest_macd_hist > Decimal('0'):
                indicator_analysis_points_list.append(f"MACD Histogram ({latest_macd_hist:.5f}) positif, menunjukkan momentum bullish.")
            elif latest_macd_hist < Decimal('0'):
                indicator_analysis_points_list.append(f"MACD Histogram ({latest_macd_hist:.5f}) negatif, menunjukkan momentum bearish.")
            else:
                indicator_analysis_points_list.append("MACD Histogram mendekati nol, menunjukkan momentum yang tidak jelas.")

        final_trade_recommendation = trade_recommendation_rule
        final_confidence = min(confidence_rule, 100)

    # --- BAGIAN PRA-FILTER RELEVANT LEVELS UNTUK LLM ---
    filtered_relevant_levels = []
    if final_trade_recommendation == "BUY":
        # Untuk BUY, hanya sertakan support-type levels yang di bawah harga saat ini
        for level in relevant_levels_raw:
            level_price = level.get('price') or ((level.get('top_price') + level.get('bottom_price')) / 2 if 'top_price' in level and 'bottom_price' in level else None)
            if level_price is None: continue

            is_support_type = level['type'] in ["Support Level", "Bullish Order Block", "Bullish FVG", "Equal Lows Liquidity", "Demand Zone", "Fibonacci Level"] # Fib juga bisa jadi support
            if is_support_type and level_price < float(latest_close_price_dec):
                filtered_relevant_levels.append(level)
            elif "Market Structure Event" in level['type'] and level_price < float(latest_close_price_dec):
                 filtered_relevant_levels.append(level)
    elif final_trade_recommendation == "SELL":
        # Untuk SELL, hanya sertakan resistance-type levels yang di atas harga saat ini
        for level in relevant_levels_raw:
            level_price = level.get('price') or ((level.get('top_price') + level.get('bottom_price')) / 2 if 'top_price' in level and 'bottom_price' in level else None)
            if level_price is None: continue

            is_resistance_type = level['type'] in ["Resistance Level", "Bearish Order Block", "Bearish FVG", "Equal Highs Liquidity", "Supply Zone", "Fibonacci Level"] # Fib juga bisa jadi resistance
            if is_resistance_type and level_price > float(latest_close_price_dec):
                filtered_relevant_levels.append(level)
            elif "Market Structure Event" in level['type'] and level_price > float(latest_close_price_dec):
                 filtered_relevant_levels.append(level)
    else: # HOLD atau tidak ada rekomendasi kuat, kirim semua level relevan
        filtered_relevant_levels = relevant_levels_raw

    logger.debug(f"TRADE_AGENT: Jumlah level relevan setelah pra-filter: {len(filtered_relevant_levels)}")
    # --- AKHIR BAGIAN PRA-FILTER RELEVANT LEVELS ---


    # --- LOGIKA PERHITUNGAN ENTRY, STOP LOSS, DAN TAKE PROFIT ---
    proposed_entry = latest_close_price_dec
    proposed_sl = None
    proposed_tp = None

    if config.TRADING_SYMBOL_POINT_VALUE is None or config.TRADING_SYMBOL_POINT_VALUE == Decimal('0.0'):
        logger.error("Nilai TRADING_SYMBOL_POINT_VALUE tidak valid di config.py. Tidak dapat menghitung SL/TP.")
        pip_value_in_dollars = Decimal('0.001') # Fallback
    else:
        pip_value_in_dollars = config.PIP_UNIT_IN_DOLLAR

    # --- BAGIAN MENENTUKAN JARAK SL BERDASARKAN ATR ATAU FALLBACK ---
    calculated_sl_distance_dec = Decimal('0')
    ATR_SL_MULTIPLIER = Decimal('1.5')

    if latest_atr is not None:
        calculated_sl_distance_dec = latest_atr * ATR_SL_MULTIPLIER
        logger.info(f"TRADE_AGENT: Jarak SL dihitung berdasarkan ATR ({latest_atr:.5f}) x {ATR_SL_MULTIPLIER} = {calculated_sl_distance_dec:.5f}")
    else:
        logger.warning("TRADE_AGENT: ATR tidak tersedia, menggunakan fallback SL distance dari DEFAULT_SL_PIPS.")
        if config.TRADING_SYMBOL_POINT_VALUE is not None and config.TRADING_SYMBOL_POINT_VALUE > Decimal('0') and \
           config.PIP_UNIT_IN_DOLLAR is not None and config.PIP_UNIT_IN_DOLLAR > Decimal('0'):
             calculated_sl_distance_dec = config.RuleBasedStrategy.DEFAULT_SL_PIPS * config.PIP_UNIT_IN_DOLLAR
        else:
             calculated_sl_distance_dec = Decimal('0.003')
             logger.error("TRADE_AGENT: TRADING_SYMBOL_POINT_VALUE atau PIP_UNIT_IN_DOLLAR tidak valid, menggunakan hardcoded SL distance.")

    target_risk_reward_ratio = config.RuleBasedStrategy.DEFAULT_SL_TP_RATIO
    # --- AKHIR BAGIAN MENENTUKAN JARAK SL BERDASARKAN ATR ---

    proposed_entry_float = float(proposed_entry) if proposed_entry is not None else None

    if proposed_entry_float is None:
        logger.warning("Proposed entry price tidak valid, tidak dapat menghitung SL/TP.")
    else:
        # Menggunakan filtered_relevant_levels di sini untuk perhitungan awal SL/TP
        potential_sl_tp_levels = []
        for level in filtered_relevant_levels:
            if 'price' in level and level['price'] is not None:
                level_float = level['price']
            elif 'top_price' in level and 'bottom_price' in level and level['top_price'] is not None and level['bottom_price'] is not None:
                level_float = (float(level['top_price']) + float(level['bottom_price'])) / 2.0
            else:
                continue

            potential_sl_tp_levels.append({
                'level_info': level,
                'distance': abs(level_float - proposed_entry_float),
                'price_float': level_float
            })

        potential_sl_tp_levels.sort(key=lambda x: (x['distance'], -x['level_info']['confluence_score']))

        if final_trade_recommendation == "BUY":
            initial_sl_calc = proposed_entry_float - float(calculated_sl_distance_dec)

            chosen_sl_level_price = None
            for level_data in potential_sl_tp_levels:
                level_info = level_data['level_info']
                level_price_float = level_data['price_float']
                is_support_type = level_info['type'] in ["Support Level", "Bullish Order Block", "Bullish FVG", "Equal Lows Liquidity", "Demand Zone"]

                if is_support_type and level_price_float < proposed_entry_float:
                    min_sl_distance_from_entry = calculated_sl_distance_dec * Decimal('0.2')
                    if (proposed_entry_float - level_price_float) < float(min_sl_distance_from_entry):
                        continue
                    margin_below_level = Decimal('2') * pip_value_in_dollars
                    potential_sl_candidate = Decimal(str(level_price_float)) - margin_below_level
                    min_risk_for_level_sl = calculated_sl_distance_dec * Decimal('0.5')
                    if (proposed_entry - potential_sl_candidate >= min_risk_for_level_sl) and level_info['confluence_score'] > 0:
                        chosen_sl_level_price = potential_sl_candidate
                        break
            if chosen_sl_level_price is not None:
                proposed_sl = chosen_sl_level_price.quantize(Decimal('0.00001'))
            else:
                proposed_sl = Decimal(str(initial_sl_calc)).quantize(Decimal('0.00001'))

            risk_in_dollars = proposed_entry - proposed_sl
            if risk_in_dollars > Decimal('0'):
                proposed_tp = proposed_entry + (risk_in_dollars * target_risk_reward_ratio)
                proposed_tp = proposed_tp.quantize(Decimal('0.00001'))
                chosen_tp_level_price = None
                for level_data in potential_sl_tp_levels:
                    level_info = level_data['level_info']
                    level_price_float = level_data['price_float']
                    is_resistance_type = level_info['type'] in ["Resistance Level", "Bearish Order Block", "Bearish FVG", "Equal Highs Liquidity", "Supply Zone"]
                    if is_resistance_type and level_price_float > proposed_entry_float:
                        min_reward_for_level_tp = risk_in_dollars * target_risk_reward_ratio * Decimal('0.5')
                        if (level_price_float - proposed_entry_float) < float(min_reward_for_level_tp):
                            continue
                        margin_below_level = Decimal('2') * pip_value_in_dollars
                        potential_tp_candidate = Decimal(str(level_price_float)) - margin_below_level
                        if (potential_tp_candidate - proposed_entry >= (risk_in_dollars * Decimal('0.5'))) and level_info['confluence_score'] > 0:
                            chosen_tp_level_price = potential_tp_candidate
                            break
                if chosen_tp_level_price is not None:
                    proposed_tp = chosen_tp_level_price.quantize(Decimal('0.00001'))
            else:
                proposed_tp = None

        elif final_trade_recommendation == "SELL":
            initial_sl_calc = proposed_entry_float + float(calculated_sl_distance_dec)

            chosen_sl_level_price = None
            for level_data in potential_sl_tp_levels:
                level_info = level_data['level_info']
                level_price_float = level_data['price_float']
                is_resistance_type = level_info['type'] in ["Resistance Level", "Bearish Order Block", "Bearish FVG", "Equal Highs Liquidity", "Supply Zone"]
                if is_resistance_type and level_price_float > proposed_entry_float:
                    min_sl_distance_from_entry = calculated_sl_distance_dec * Decimal('0.2')
                    if (level_price_float - proposed_entry_float) < float(min_sl_distance_from_entry):
                        continue
                    margin_above_level = Decimal('2') * pip_value_in_dollars
                    potential_sl_candidate = Decimal(str(level_price_float)) + margin_above_level
                    min_risk_for_level_sl = calculated_sl_distance_dec * Decimal('0.5')
                    if (potential_sl_candidate - proposed_entry >= min_risk_for_level_sl) and level_info['confluence_score'] > 0:
                        chosen_sl_level_price = potential_sl_candidate
                        break
            if chosen_sl_level_price is not None:
                proposed_sl = chosen_sl_level_price.quantize(Decimal('0.00001'))
            else:
                proposed_sl = Decimal(str(initial_sl_calc)).quantize(Decimal('0.00001'))

            risk_in_dollars = proposed_sl - proposed_entry
            if risk_in_dollars > Decimal('0'):
                proposed_tp = proposed_entry - (risk_in_dollars * target_risk_reward_ratio)
                proposed_tp = proposed_tp.quantize(Decimal('0.00001'))
                chosen_tp_level_price = None
                for level_data in potential_sl_tp_levels:
                    level_info = level_data['level_info']
                    level_price_float = level_data['price_float']
                    is_support_type = level_info['type'] in ["Support Level", "Bullish Order Block", "Bullish FVG", "Equal Lows Liquidity", "Demand Zone"]
                    if is_support_type and level_price_float < proposed_entry_float:
                        min_reward_for_level_tp = risk_in_dollars * target_risk_reward_ratio * Decimal('0.5')
                        if (proposed_entry_float - level_price_float) < float(min_reward_for_level_tp):
                            continue
                        margin_above_level = Decimal('2') * pip_value_in_dollars
                        potential_tp_candidate = Decimal(str(level_price_float)) + margin_above_level
                        if (proposed_entry - potential_tp_candidate <= -(risk_in_dollars * Decimal('0.5'))) and level_info['confluence_score'] > 0:
                            chosen_tp_level_price = potential_tp_candidate
                            break
                if chosen_tp_level_price is not None:
                    proposed_tp = chosen_tp_level_price.quantize(Decimal('0.00001'))
            else:
                proposed_tp = None

    # --- Ambil data info akun dan posisi terbuka terbaru ---
    account_info_data = database_manager.get_mt5_account_info_from_db()
    open_positions_data = database_manager.get_mt5_positions_from_db(symbol=symbol)

    # --- Ambil Hasil Diagnosa Mandiri LLM Terbaru ---
    latest_llm_review = database_manager.get_llm_performance_reviews(
        symbol=symbol,
        limit=1,
        end_time_utc=datetime.now(timezone.utc)
    )
    llm_review_data = latest_llm_review[0] if latest_llm_review else None

    # --- Ambil Data Fundamental/Berita Terbaru ---
    recent_economic_events = database_manager.get_economic_events_for_llm_prompt(
        days_past=config.AIAnalysts.MAX_FUNDAMENTAL_EVENTS_FOR_LLM_DAYS_LOOKBACK,
        limit=config.AIAnalysts.MAX_FUNDAMENTAL_EVENTS_FOR_LLM,
        target_currency=symbol[:3]
    )
    recent_news_articles = database_manager.get_news_articles_for_llm_prompt(
        days_past=config.AIAnalysts.MAX_FUNDAMENTAL_ARTICLES_FOR_LLM_DAYS_LOOKBACK,
        limit=config.AIAnalysts.MAX_FUNDAMENTAL_ARTICLES_FOR_LLM
    )


    # --- PANGGIL FUNGSI PEMBENTUK PROMPT BARU ---
    # KIRIM filtered_relevant_levels BUKAN relevant_levels_raw
    prompt_content = _construct_market_analysis_prompt(
        symbol=symbol,
        current_price=latest_close_price_dec,
        current_price_timestamp=datetime.now(timezone.utc),
        historical_candles_df=df_candles,
        relevant_levels=filtered_relevant_levels,
        ml_prediction_data={"prediction": trade_recommendation_ml, "confidence": ml_confidence},
        indicator_analysis_points=indicator_analysis_points_list,
        account_info=account_info_data,
        open_positions=open_positions_data,
        economic_events=recent_economic_events,
        news_articles=recent_news_articles,
        llm_performance_review=llm_review_data
    )

    # --- PANGGILAN LLM DAN VALIDASI OUTPUT ---
    llm_raw_response = ai_database_agent.call_llm_for_text_generation(
        prompt_content=prompt_content,
        max_tokens=2500
    )

    parsed_llm_response = None
    validation_errors = []

    try:
        json_match = re.search(r"```json\s*([\s\S]*?)\s*```", llm_raw_response)
        if json_match:
            pure_json_string = json_match.group(1).strip()
            logger.debug(f"TRADE_AGENT: Extracted pure JSON string: {pure_json_string[:200]}...")
        else:
            pure_json_string = llm_raw_response.strip()
            logger.warning("TRADE_AGENT: No ```json block found, attempting to parse raw response as JSON.")

        parsed_llm_response = json.loads(pure_json_string)
        logger.debug(f"TRADE_AGENT: LLM raw response successfully parsed: {parsed_llm_response}")

        # --- Pemeriksaan Skema dan Konsistensi Logis ---
        required_keys = ["summary", "recommendation_action", "entry_price", "stop_loss", "take_profit", "reasoning"]
        for key in required_keys:
            if key not in parsed_llm_response:
                validation_errors.append(f"Missing key: {key}")

        if parsed_llm_response.get("recommendation_action") not in ["BUY", "SELL", "HOLD"]:
            validation_errors.append("Invalid recommendation_action. Must be BUY, SELL, or HOLD.")

        rec_action = parsed_llm_response.get("recommendation_action")
        entry_p = utils.to_decimal_or_none(parsed_llm_response.get("entry_price"))
        sl_p = utils.to_decimal_or_none(parsed_llm_response.get("stop_loss"))
        tp_p = utils.to_decimal_or_none(parsed_llm_response.get("take_profit"))

        if rec_action in ["BUY", "SELL"]:
            if entry_p is None or entry_p <= 0:
                 validation_errors.append("Entry price cannot be null or zero for BUY/SELL signal.")

            if sl_p is None or sl_p <= 0:
                validation_errors.append(f"{rec_action} signal: Stop Loss cannot be null or zero.")
            if tp_p is None or tp_p <= 0:
                validation_errors.append(f"{rec_action} signal: Take Profit cannot be null or zero.")

            if sl_p is not None and entry_p is not None and rec_action == "BUY" and sl_p >= entry_p:
                validation_errors.append(f"BUY signal: SL ({sl_p}) must be less than Entry ({entry_p}).")
            if tp_p is not None and entry_p is not None and rec_action == "BUY" and tp_p <= entry_p:
                validation_errors.append(f"BUY signal: TP ({tp_p}) must be greater than Entry ({entry_p}).")
            elif sl_p is not None and entry_p is not None and rec_action == "SELL" and sl_p <= entry_p:
                validation_errors.append(f"SELL signal: SL ({sl_p}) must be greater than Entry ({entry_p}).")
            elif tp_p is not None and entry_p is not None and rec_action == "SELL" and tp_p >= entry_p:
                validation_errors.append(f"SELL signal: TP ({tp_p}) must be less than Entry ({entry_p}).")

        if validation_errors:
            logger.error(f"TRADE_AGENT: LLM response validation failed for {symbol} {timeframe}. Errors: {'; '.join(validation_errors)}. Raw response: {llm_raw_response}")
            parsed_llm_response = None

    except json.JSONDecodeError as e:
        logger.error(f"TRADE_AGENT: LLM response is not valid JSON for {symbol} {timeframe}: {e}. Raw response: {llm_raw_response[:min(len(llm_raw_response), 500)]}...")
        validation_errors.append(f"JSON Decode Error: {e}")
        parsed_llm_response = None
    except Exception as e:
        logger.error(f"TRADE_AGENT: Unexpected error during LLM response parsing/validation: {e}", exc_info=True)
        validation_errors.append(f"Unexpected parsing/validation error: {e}")
        parsed_llm_response = None

    # --- Menentukan Output Akhir Berdasarkan Validasi ---
    if parsed_llm_response and not validation_errors:
        final_output = {
            "status": "success",
            "symbol": symbol,
            "timeframe": timeframe,
            "recommendation": parsed_llm_response["recommendation_action"],
            "confidence": final_confidence,
            "analysis_points": indicator_analysis_points_list,
            "ai_summary": parsed_llm_response["summary"],
            "ai_reasoning": parsed_llm_response["reasoning"],
            "proposed_entry": utils.to_float_or_none(parsed_llm_response.get("entry_price")),
            "proposed_sl": utils.to_float_or_none(parsed_llm_response.get("stop_loss")),
            "proposed_tp": utils.to_float_or_none(parsed_llm_response.get("take_profit"))
        }
        logger.info(f"TRADE_AGENT: LLM-based proposal successful for {symbol} {timeframe}. Rec: {final_output['recommendation']}, Entry: {final_output['proposed_entry']:.5f}")

        # --- BAGIAN BARU: SIMPAN PROPOSAL LLM KE DATABASE (UNTUK PERSISTENSI DAN SELF-DIAGNOSIS) ---
        try:
            database_manager.save_llm_proposal(
                symbol=symbol,
                timeframe=timeframe,
                recommendation_action=final_output['recommendation'],
                entry_price=Decimal(str(final_output['proposed_entry'])) if final_output['proposed_entry'] is not None else None,
                stop_loss=Decimal(str(final_output['proposed_sl'])) if final_output['proposed_sl'] is not None else None,
                take_profit=Decimal(str(final_output['proposed_tp'])) if final_output['proposed_tp'] is not None else None,
                confidence_score=final_output['confidence'],
                summary=final_output['ai_summary'],
                reasoning=final_output['ai_reasoning'],
                proposal_timestamp=datetime.now(timezone.utc)
            )
            logger.info(f"TRADE_AGENT: Proposal LLM untuk {symbol} {timeframe} berhasil disimpan ke database.")
        except Exception as e:
            logger.error(f"TRADE_AGENT: Gagal menyimpan proposal LLM ke database untuk {symbol} {timeframe}: {e}", exc_info=True)
        # --- AKHIR BAGIAN BARU ---

    else:
        fallback_summary = "Analisis LLM tidak dapat divalidasi. Menggunakan rekomendasi dasar."
        fallback_reasoning = f"LLM respons tidak valid atau tidak dapat diproses. Detail error: {'; '.join(validation_errors)}. Rekomendasi awal: {final_trade_recommendation} dengan kepercayaan {final_confidence}%."

        final_output = {
            "status": "error",
            "symbol": symbol,
            "timeframe": timeframe,
            "recommendation": final_trade_recommendation,
            "confidence": final_confidence,
            "analysis_points": indicator_analysis_points_list,
            "ai_summary": fallback_summary,
            "ai_reasoning": fallback_reasoning,
            "proposed_entry": utils.to_float_or_none(proposed_entry),
            "proposed_sl": utils.to_float_or_none(proposed_sl),
            "proposed_tp": utils.to_float_or_none(proposed_tp)
        }
        logger.error(f"TRADE_AGENT: LLM proposal failed for {symbol} {timeframe}. Falling back to ML/Rule-based. Rec: {final_output['recommendation']}")

    return final_output
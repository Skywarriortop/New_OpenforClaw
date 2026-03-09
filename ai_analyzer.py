from openai import OpenAI
import json
import sys
# ...
import logging
import re
from decimal import Decimal
import time

# Import modul lain yang dibutuhkan
import database_manager
from config import config
import market_data_processor
import notification_service
import utils # Pastikan ini ada
from utils import to_float_or_none, to_iso_format_or_none, datetime, timezone

client = None
logger = logging.getLogger(__name__)

# Variabel global untuk cooldown notifikasi konsensus TIDAK PERLU DI SINI lagi
# _last_notified_signal_consensus = None
# _last_notified_signal_time = None
# _NOTIFICATION_COOLDOWN_SECONDS = 3600 * 4 # 4 jam cooldown

def init_openai_client(api_key):
    global client
    if api_key:
        try:
            client = OpenAI(api_key=api_key)
            logger.info("Klien OpenAI berhasil diinisialisasi dalam ai_analyzer.")
        except Exception as e:
            logger.error(f"Gagal menginisialisasi klien OpenAI: {e}")
            client = None
    else:
        logger.warning("OpenAI API key tidak diberikan. Klien OpenAI tidak diinisialisasi.")

def json_default(o):
    if isinstance(o, datetime):
        return o.isoformat()
    if isinstance(o, Decimal):
        return float(o)
    raise TypeError(f"{repr(o)} is not JSON serializable")

def get_market_context_for_ai(symbol, timeframes_to_collect=["D1", "H4", "H1", "M30", "M15"], num_candles_per_tf=50):
    """
    Mengumpulkan data pasar dan fitur yang terdeteksi sebagai konteks untuk analisis AI.
    """
    context_data = {
        "current_time_utc": datetime.now(timezone.utc).isoformat(),
        "daily_open_price": database_manager.get_daily_open_price(datetime.now(timezone.utc).date().isoformat()),
        "market_status": config.MarketData.market_status_data,
        "historical_candles": {},
        "sr_levels": [], "sd_zones": [], "order_blocks": [], "fair_value_gaps": [],
        "market_structure_events": [], "liquidity_zones": [], "fibonacci_levels": [],
        "higher_timeframe_trend": {},
        "divergences": [],
        "volume_profiles": [],
        "fundamental_data": {
            "economic_events": [],
            "news_articles": []
        }
    }

    for tf in timeframes_to_collect:
        candles = database_manager.get_historical_candles_from_db(symbol, tf, limit=num_candles_per_tf)
        context_data["historical_candles"][tf] = candles

    context_data["sr_levels"] = database_manager.get_support_resistance_levels(symbol, is_active=True)
    context_data["sd_zones"] = database_manager.get_supply_demand_zones(symbol, is_mitigated=False)
    context_data["order_blocks"] = database_manager.get_order_blocks(symbol, is_mitigated=False)
    context_data["fair_value_gaps"] = database_manager.get_fair_value_gaps(symbol, is_filled=False)
    context_data["market_structure_events"] = database_manager.get_market_structure_events(symbol)
    context_data["liquidity_zones"] = database_manager.get_liquidity_zones(symbol, is_tapped=False)
    context_data["fibonacci_levels"] = database_manager.get_fibonacci_levels(symbol, is_active=True)
    
    context_data["higher_timeframe_trend"] = market_data_processor.get_trend_context_from_ma(symbol)
    
    context_data["divergences"] = database_manager.get_divergences(symbol, is_active=True)

    context_data["volume_profiles"] = database_manager.get_volume_profiles(symbol, limit=5)

    days_past_for_fundamental = 2
    days_future_for_fundamental = 1
    min_impact_for_ai = "Medium"
    news_topics_for_ai = ["gold", "usd", "suku bunga", "fed", "inflation", "war", "geopolitics", "market", "economy"]

    fundamental_data_from_db = database_manager.get_upcoming_events_from_db(
        days_past=days_past_for_fundamental,
        days_future=days_future_for_fundamental,
        min_impact=min_impact_for_ai,
        target_currency="USD"
    )

    context_data["fundamental_data"]["economic_events"] = [
        item for item in fundamental_data_from_db if item['type'] == 'economic_calendar'
    ]
    if news_topics_for_ai:
        context_data["fundamental_data"]["news_articles"] = [
            item for item in fundamental_data_from_db if item['type'] == 'news_article' and
            any(topic.lower() in (item.get('name', '') + ' ' + item.get('summary', '')).lower() for topic in news_topics_for_ai)
        ]
    else:
        context_data["fundamental_data"]["news_articles"] = [
            item for item in fundamental_data_from_db if item['type'] == 'news_article'
        ]

    logger.info(f"Mengambil {len(context_data['fundamental_data']['economic_events'])} event ekonomi dan {len(context_data['fundamental_data']['news_articles'])} artikel berita untuk konteks AI.")

    return context_data

@utils.retry_on_failure(retries=3, delay=10)
def run_single_analyst_ai(analyst_id: str, prompt_persona: str, market_context: dict, analyst_conf: dict) -> dict:
    """
    Mengirim konteks pasar ke model AI dan mendapatkan analisis dari satu analis.
    AI akan memberikan reasoning lengkap, yang kemudian akan disederhanakan untuk notifikasi.
    """
    if client is None:
        logger.error(f"Klien OpenAI belum diinisialisasi. Analis '{analyst_id}' tidak dapat melakukan analisis.")
        return {"error": "OpenAI client not initialized.", "analyst_id": analyst_id}

    system_prompt_content = prompt_persona
    
    # --- KODE FILTERING AGGRESIF ---
    filtered_context = market_context.copy()
    
    # Ambil timeframe relevan dari analyst_conf yang diteruskan
    relevant_timeframes = analyst_conf.get("relevant_timeframes", []) 

    if analyst_id == "Technical_Trend":
        temp_candles = {}
        for tf in relevant_timeframes:
            if tf in market_context["historical_candles"]:
                temp_candles[tf] = market_context["historical_candles"][tf][-5:]
        
        filtered_context = {
            "current_time_utc": market_context["current_time_utc"],
            "daily_open_price": market_context["daily_open_price"],
            "market_status": market_context["market_status"],
            "historical_candles": temp_candles,
            "higher_timeframe_trend": market_context["higher_timeframe_trend"]
        }
    
    elif analyst_id == "Technical_Levels":
        temp_candles = {}
        for tf in list(set(relevant_timeframes + ["H4", "D1"])): 
            if tf in market_context["historical_candles"]:
                temp_candles[tf] = market_context["historical_candles"][tf][-10:]
        
        filtered_context = {
            "current_time_utc": market_context["current_time_utc"],
            "daily_open_price": market_context["daily_open_price"],
            "market_status": market_context["market_status"],
            "historical_candles": temp_candles,
            "sr_levels": market_context["sr_levels"],
            "sd_zones": market_context["sd_zones"],
            "order_blocks": market_context["order_blocks"],
            "fair_value_gaps": market_context["fair_value_gaps"],
            "liquidity_zones": market_context["liquidity_zones"],
            "fibonacci_levels": market_context["fibonacci_levels"],
            "volume_profiles": market_context["volume_profiles"]
        }
    
    elif analyst_id == "Technical_Momentum":
        temp_candles = {}
        for tf in relevant_timeframes:
            if tf in market_context["historical_candles"]:
                temp_candles[tf] = market_context["historical_candles"][tf][-15:]
        
        filtered_context = {
            "current_time_utc": market_context["current_time_utc"],
            "historical_candles": temp_candles,
            "divergences": market_context["divergences"]
        }
    
    elif analyst_id == "Fundamental_Analyst":
        filtered_context = {
            "current_time_utc": market_context["current_time_utc"],
            "market_status": market_context["market_status"],
            "fundamental_data": market_context["fundamental_data"]
        }
    
    else:
        logger.warning(f"Analis '{analyst_id}' tidak memiliki filter konteks spesifik. Menggunakan konteks penuh.")
        filtered_context = market_context.copy() 

    final_prompt = f"""
    {system_prompt_content}


    Analisis data pasar XAUUSD yang sangat detail yang diberikan di bawah ini dan berikan ringkasan, identifikasi pola penting,
    dan potensi arah pergerakan harga.

    Berikan output Anda dalam format JSON dengan kunci-kunci berikut (sesuai dengan format AIAnalysisResult):
    {{
      "summary": "Ringkasan analisis pasar XAUUSD saat ini, sesuai dengan fokus persona Anda.",
      "key_observations": [
        "Pengamatan penting 1 dari data yang relevan dengan fokus Anda.",
        "Pengamatan penting 2 dari konfluensi atau sinyal kuat.",
        "Pengamatan penting 3 (jika ada)."
      ],
      "potential_direction": "Bullish" | "Bearish" | "Sideways" | "Undefined",
      "trading_recommendation": {{
        "action": "BUY" | "SELL" | "HOLD",
        "timeframe_for_trade": "H1",
        "entry_price_suggestion": <harga_float_atau_null>,
        "stop_loss_suggestion": <harga_float_atau_null>,
        "take_profit_suggestion": <harga_float_atau_null>,
        "reasoning": "Alasan di balik rekomendasi, dengan merujuk pada level dan sinyal spesifik dari data konteks, sesuai fokus Anda. Sertakan satu emoji yang paling mewakili sentimen/arah sinyal di awal alasan ini."
      }},
      "ai_confidence": "High" | "Medium" | "Low",
      "disclaimer": "Informasi ini hanya untuk tujuan analisis dan bukan saran keuangan. Lakukan riset Anda sendiri."
    }}

    Data Konteks Pasar (JSON):
    {json.dumps(filtered_context, default=json_default, indent=2)}
    """
    
    try:
        logger.info(f"Mengirim permintaan analisis ke OpenAI untuk Analis: '{analyst_id}'...")
        response = client.chat.completions.create(
            model=config.AIAnalysts.OPENAI_MODEL_NAME,
            messages=[
                {"role": "system", "content": system_prompt_content},
                {"role": "user", "content": final_prompt}
            ],
            response_format={"type": "json_object"},
            temperature=config.AIAnalysts.OPENAI_TEMPERATURE
        )
        
        ai_response_content = response.choices[0].message.content
        logger.info(f"Menerima respons dari OpenAI untuk Analis: '{analyst_id}'.")
        
        parsed_analysis = json.loads(ai_response_content)
        parsed_analysis['analyst_id'] = analyst_id

        if 'trading_recommendation' in parsed_analysis and 'reasoning' in parsed_analysis['trading_recommendation']:
            reasoning = parsed_analysis['trading_recommendation']['reasoning']
            if not re.match(r'^\S+\s', reasoning): 
                logger.warning(f"Analis '{analyst_id}' lupa menambahkan emoji ke alasan. Menambahkan fallback.")
                parsed_analysis['trading_recommendation']['reasoning'] = "ℹ️ " + reasoning

        if parsed_analysis.get('trading_recommendation'):
            rec = parsed_analysis['trading_recommendation']
            rec['entry_price_suggestion'] = Decimal(str(rec['entry_price_suggestion'])) if rec.get('entry_price_suggestion') is not None else None
            rec['stop_loss_suggestion'] = Decimal(str(rec['stop_loss_suggestion'])) if rec.get('stop_loss_suggestion') is not None else None
            rec['take_profit_suggestion'] = Decimal(str(rec['take_profit_suggestion'])) if rec.get('take_profit_suggestion') is not None else None        
                    
        return parsed_analysis

    except json.JSONDecodeError as jde:
        logger.error(f"Gagal parse JSON dari respons AI untuk Analis '{analyst_id}': {jde}")
        logger.error(f"Raw AI response: {ai_response_content}")
        return {"error": "Failed to parse AI response.", "raw_response": ai_response_content, "analyst_id": analyst_id}
    except ValueError as ve:
        logger.error(f"Gagal konversi harga rekomendasi ke float untuk Analis '{analyst_id}': {ve}")
        logger.error(f"Raw AI response: {ai_response_content}")
        return {"error": "Failed to convert recommendation prices.", "raw_response": ai_response_content, "analyst_id": analyst_id}
    except Exception as e:
        logger.error(f"Gagal menghubungi OpenAI API untuk Analis '{analyst_id}': {e}", exc_info=True)
        return {"error": f"OpenAI API call failed: {e}", "analyst_id": analyst_id}
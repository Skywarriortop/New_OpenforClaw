# llm_self_diagnoser.py

import logging
from datetime import datetime, timezone, timedelta
import json
from decimal import Decimal

import database_manager
import ai_database_agent
import utils
from config import config

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def run_llm_self_diagnosis(symbol: str, review_period_minutes: int = 1440): # Default 24 jam
    """
    Memicu LLM untuk menganalisis kinerja proposalnya sendiri
    selama periode tertentu dan menyarankan perbaikan.
    """
    logger.info(f"Memulai diagnosa mandiri LLM untuk {symbol} selama {review_period_minutes} menit terakhir.")

    review_period_end_utc = datetime.now(timezone.utc)
    review_period_start_utc = review_period_end_utc - timedelta(minutes=review_period_minutes)

    # 1. Ambil data proposal LLM yang relevan
    llm_proposals_data = database_manager.get_ai_analysis_results(
        symbol=symbol,
        start_time_utc=review_period_start_utc,
        end_time_utc=review_period_end_utc,
        analyst_id="trading_strategy_agent" # Filter hanya proposal dari agen strategi trading utama
    )

    if not llm_proposals_data:
        logger.info("Tidak ada proposal LLM ditemukan dalam periode review. Melewatkan diagnosa mandiri.")
        return

    # 2. Ambil data deal MetaTrader 5 yang relevan (untuk melihat hasil proposal)
    # Ini adalah bagian paling kompleks karena tidak ada link langsung proposal ke deal.
    # Kita akan mencoba mencocokkan berdasarkan waktu buka proposal dan deal yang terjadi setelahnya.
    # Atau, jika Anda melacak `ticket` di `AIAnalysisResult` (yang belum ada), itu akan lebih mudah.
    
    # Untuk implementasi ini, kita akan membuat asumsi sederhana:
    # Sebuah proposal dianggap "profitable" jika sebuah deal dengan profit > 0 terjadi dalam jangka waktu tertentu
    # setelah proposal dan jika tipe dealnya sesuai (buy/sell). Ini adalah penyederhanaan!
    # Implementasi yang lebih kuat membutuhkan linking ticket dari proposal ke posisi dan deal.
    
    # Ambil semua deal dalam periode review (bisa juga ambil deal dari awal proposal paling lama)
    all_deals_in_period = database_manager.get_mt5_deal_history(
        symbol=symbol,
        start_time_utc=review_period_start_utc - timedelta(hours=1), # Beri buffer
        end_time_utc=review_period_end_utc + timedelta(hours=1) # Beri buffer
    )

    reviewed_proposals_for_llm_prompt = []
    profitable_count = 0
    losing_count = 0
    drawdown_count = 0 # Misal: proposal yang menyebabkan SL hit

    for proposal in llm_proposals_data:
        proposal_outcome = "UNKNOWN"
        proposal_pnl = Decimal('0.0')
        
        # Cari deal yang mungkin terkait dengan proposal ini.
        # Ini adalah penyederhanaan! Dalam sistem riil, Anda perlu MAGIC NUMBER
        # atau COMMENT yang menghubungkan proposal ke deal yang dieksekusi.
        related_deals = []
        for deal in all_deals_in_period:
            # Asumsi: deal terjadi tak lama setelah proposal, dan komentarnya mungkin cocok
            # Ini sangat rapuh. Idealnya: link via magic number atau ticket.
            if deal['time'] > proposal['timestamp'] and \
               deal['time'] < proposal['timestamp'] + timedelta(minutes=60): # Dalam 1 jam setelah proposal
                related_deals.append(deal)

        if related_deals:
            # Ambil PnL dari deal-deal terkait (misal, PnL dari penutupan posisi)
            total_deal_profit = sum(d['profit'] for d in related_deals if d['profit'] is not None)
            proposal_pnl = total_deal_profit

            if total_deal_profit > Decimal('0'):
                proposal_outcome = "PROFITABLE"
                profitable_count += 1
            elif total_deal_profit < Decimal('0'):
                proposal_outcome = "LOSING"
                losing_count += 1
                # Anda bisa menambahkan logika untuk mendeteksi SL hit di sini
                # Misalnya, jika ada deal penutup dengan komentar "sl"
                if any("sl" in d['comment'].lower() for d in related_deals if d['comment']):
                    drawdown_count += 1
            else:
                proposal_outcome = "BREAKEVEN"
        else:
            proposal_outcome = "NO_TRADE_EXECUTED_OR_OUTCOME_UNKNOWN"

        reviewed_proposals_for_llm_prompt.append({
            "timestamp": proposal['timestamp'].isoformat(),
            "recommendation": proposal['recommendation_action'],
            "entry_price": utils.to_float_or_none(proposal['entry_price']),
            "stop_loss": utils.to_float_or_none(proposal['stop_loss']),
            "take_profit": utils.to_float_or_none(proposal['take_profit']),
            "reasoning": proposal['reasoning'],
            "ml_confidence": proposal['ai_confidence'],
            "actual_outcome": proposal_outcome,
            "actual_pnl_in_currency": utils.to_float_or_none(proposal_pnl)
        })

    # 3. Bentuk Prompt untuk LLM (Self-Diagnosis)
    diagnosis_prompt_template = """
    Anda adalah seorang analis kualitas AI yang sangat berpengalaman dan kritis.
    Tugas Anda adalah meninjau kinerja masa lalu dari model AI trading Anda sendiri.
    Anda akan menganalisis proposal trading Anda (sebagai "trading_strategy_agent") dan membandingkannya
    dengan hasil aktual pasar untuk mengidentifikasi kekuatan, kelemahan, dan area untuk perbaikan.

    **Periode Review:** {start_time_str} hingga {end_time_str}
    **Simbol:** {symbol}

    **Riwayat Proposal dan Hasil Aktual:**
    ```json
    {proposals_json}
    ```

    **Lakukan analisis langkah demi langkah:**
    1.  **Identifikasi Pola Keberhasilan:** Kapan proposal Anda cenderung berhasil? Apa ciri-ciri proposal yang menguntungkan?
    2.  **Identifikasi Pola Kegagalan:** Kapan proposal Anda cenderung merugi atau tidak dieksekusi? Apa saja kesalahan umum (misalnya, SL terlalu ketat/longgar, TP tidak tercapai, salah membaca tren)?
    3.  **Diagnosis Potensi Masalah:** Apakah ada masalah dengan pemahaman Anda tentang data input (misalnya, level kunci, indikator, prediksi ML)? Apakah ada instruksi prompt yang mungkin Anda salah interpretasi?
    4.  **Saran Perbaikan:** Berdasarkan diagnosis Anda, berikan saran spesifik tentang bagaimana prompt Anda (sebagai "trading_strategy_agent") atau data input ke Anda bisa ditingkatkan. Contoh: "Prompt harus lebih menekankan konfluensi dari X," atau "Data input harus mencakup Y untuk konteks yang lebih baik."

    **Instruksi Output:**
    Hasilkan respons Anda dalam format JSON yang ketat. Pastikan semua kunci yang diminta ada.

    ```json
    {{
      "diagnosis_summary": "[Rangkuman umum diagnosis Anda]",
      "identified_strengths": "[Pola keberhasilan yang Anda identifikasi]",
      "identified_weaknesses": "[Pola kegagalan yang Anda identifikasi, termasuk detail seperti SL/TP yang buruk, salah tren, dll.]",
      "improvement_suggestions": "[Saran praktis untuk perbaikan prompt atau data input]"
    }}
    ```
    """

    diagnosis_prompt = diagnosis_prompt_template.format(
        start_time_str=review_period_start_utc.isoformat(),
        end_time_str=review_period_end_utc.isoformat(),
        symbol=symbol,
        proposals_json=json.dumps(reviewed_proposals_for_llm_prompt, indent=2, default=str)
    )

    # 4. Panggil LLM
    llm_raw_diagnosis_response = ai_database_agent.call_llm_for_text_generation(
        prompt_content=diagnosis_prompt,
        max_tokens=1000 # Cukup untuk diagnosis yang komprehensif
    )

    parsed_diagnosis_response = None
    try:
        # Ekstrak JSON dari dalam blok kode markdown jika ada
        json_match = re.search(r"```json\s*([\s\S]*?)\s*```", llm_raw_diagnosis_response)
        if json_match:
            pure_json_string = json_match.group(1).strip()
        else:
            pure_json_string = llm_raw_diagnosis_response.strip()

        parsed_diagnosis_response = json.loads(pure_json_string)
        logger.info("LLM Self-Diagnosis: Successfully parsed LLM response.")

        # Validasi dasar skema output LLM
        required_keys = ["diagnosis_summary", "identified_strengths", "identified_weaknesses", "improvement_suggestions"]
        if not all(k in parsed_diagnosis_response for k in required_keys):
            raise ValueError("LLM diagnosis response missing required keys.")

    except (json.JSONDecodeError, ValueError) as e:
        logger.error(f"LLM Self-Diagnosis: Failed to parse or validate LLM diagnosis response: {e}. Raw: {llm_raw_diagnosis_response[:500]}...")
        parsed_diagnosis_response = None
    except Exception as e:
        logger.error(f"LLM Self-Diagnosis: Unexpected error during LLM diagnosis parsing: {e}", exc_info=True)
        parsed_diagnosis_response = None

    # 5. Simpan Hasil Diagnosa ke Database
    if parsed_diagnosis_response:
        review_data_to_save = {
            "review_time_utc": review_period_end_utc,
            "review_period_start_utc": review_period_start_utc,
            "review_period_end_utc": review_period_end_utc,
            "symbol": symbol,
            "total_proposals_reviewed": len(llm_proposals_data),
            "profitable_proposals": profitable_count,
            "losing_proposals": losing_count,
            "drawdown_proposals": drawdown_count,
            "accuracy_score_proposals": (Decimal(profitable_count) / len(llm_proposals_data) * 100).quantize(Decimal('0.01')) if llm_proposals_data else Decimal('0.00'),
            "llm_diagnosis_summary": parsed_diagnosis_response.get("diagnosis_summary"),
            "llm_identified_strengths": parsed_diagnosis_response.get("identified_strengths"),
            "llm_identified_weaknesses": parsed_diagnosis_response.get("identified_weaknesses"),
            "llm_improvement_suggestions": parsed_diagnosis_response.get("improvement_suggestions"),
            "raw_llm_response_json": parsed_diagnosis_response # Simpan objek dict yang sudah diparse
        }
        database_manager.save_llm_performance_review(review_data_to_save)

        # 6. Laporkan Hasil via Notifikasi
        notification_message = (
            f"**LLM Self-Diagnosis Report ({symbol})**\n"
            f"Periode: {review_period_start_utc.strftime('%Y-%m-%d %H:%M')} - {review_period_end_utc.strftime('%Y-%m-%d %H:%M')}\n"
            f"Total Proposals: {review_data_to_save['total_proposals_reviewed']}\n"
            f"Profitable: {review_data_to_save['profitable_proposals']}, Losing: {review_data_to_save['losing_proposals']}, Drawdown: {review_data_to_save['drawdown_proposals']}\n"
            f"Akurasi (%): {review_data_to_save['accuracy_score_proposals']}\n\n"
            f"**Diagnosis Summary:**\n{parsed_diagnosis_response.get('diagnosis_summary', 'N/A')}\n\n"
            f"**Identified Strengths:**\n{parsed_diagnosis_response.get('identified_strengths', 'N/A')}\n\n"
            f"**Identified Weaknesses:**\n{parsed_diagnosis_response.get('identified_weaknesses', 'N/A')}\n\n"
            f"**Improvement Suggestions:**\n{parsed_diagnosis_response.get('improvement_suggestions', 'N/A')}"
        )
        # Assuming you have a notification service configured
        notification_service.send_telegram_message(notification_message) # uncomment if notification_service is available
        logger.info(notification_message)
    else:
        logger.error("LLM Self-Diagnosis: Tidak dapat menyimpan hasil diagnosa karena parsing/validasi gagal.")
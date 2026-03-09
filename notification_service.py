# notification_service.py

import logging
import requests
import json
import os
import re
import queue
import threading
import asyncio
import telegram
from telegram.error import TelegramError
from concurrent.futures import ThreadPoolExecutor

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from config import config
import utils

logger = logging.getLogger(__name__)

_notification_queue = asyncio.Queue()
_worker_thread = None
_stop_worker = threading.Event()
_loop_initialized_event = threading.Event()

PIP_UNIT_XAUUSD = Decimal('1.0')

MAX_TELEGRAM_MESSAGE_LENGTH = 4096
MAX_TELEGRAM_LINE_LENGTH = 1000

def send_telegram_message(message: str, disable_notification: bool = False):
    if not message:
        logger.warning("Mencoba mengirim pesan Telegram kosong. Dilewati.")
        return

    escaped_message = utils._escape_markdown(message)

    message_chunks = []
    lines = escaped_message.split('\n')
    current_chunk = ""

    for i, line in enumerate(lines):
        if len(line) > MAX_TELEGRAM_LINE_LENGTH:
            logger.warning(f"Baris ke-{i+1} terlalu panjang ({len(line)} karakter). Memotong baris ini.")
            line = line[:MAX_TELEGRAM_LINE_LENGTH].rsplit(' ', 1)[0] + '...'

        if len(current_chunk) + len(line) + 1 > MAX_TELEGRAM_MESSAGE_LENGTH:
            if current_chunk:
                message_chunks.append(current_chunk.strip())
            current_chunk = line + '\n'
        else:
            current_chunk += line + '\n'
    
    if current_chunk.strip():
        message_chunks.append(current_chunk.strip())

    if not message_chunks:
        message_chunks.append(escaped_message[:MAX_TELEGRAM_MESSAGE_LENGTH])
        logger.warning("Pemecahan pesan menghasilkan bagian kosong, menggunakan pemotongan kasar sebagai fallback.")

    logger.debug(f"Pesan asli dipecah menjadi {len(message_chunks)} bagian.")
    for chunk in message_chunks:
        try:
            # Menggunakan put_nowait di sini karena ini adalah fungsi sinkron
            _notification_queue.put_nowait({
                'type': 'text',
                'content': chunk,
                'disable_notification': disable_notification
            })
            logger.debug(f"Pesan teks (bagian) ditambahkan ke antrean notifikasi. Ukuran: {len(chunk)}.")
        except asyncio.QueueFull: # Gunakan asyncio.QueueFull untuk asyncio.Queue
            logger.warning("Antrean notifikasi penuh, tidak dapat menambahkan pesan teks baru.")
        except Exception as e:
            logger.error(f"Gagal menambahkan pesan teks ke antrean notifikasi: {e}", exc_info=True)

def send_telegram_photo(photo_path: str, caption: str = "", disable_notification: bool = False):
    if not photo_path or not os.path.exists(photo_path):
        logger.error(f"Mencoba mengirim foto yang tidak ada: {photo_path}. Dilewati.")
        return

    escaped_caption = utils._escape_markdown(caption)

    try:
        logger.debug(f"Menambahkan foto '{photo_path}' dengan caption '{escaped_caption[:50]}...' ke antrean notifikasi.")
        # Menggunakan put_nowait di sini karena ini adalah fungsi sinkron
        _notification_queue.put_nowait({
            'type': 'photo',
            'content': photo_path,
            'caption': escaped_caption,
            'disable_notification': disable_notif
        })
        logger.debug(f"Foto '{photo_path}' berhasil ditambahkan ke antrean notifikasi.")
    except asyncio.QueueFull: # Gunakan asyncio.QueueFull untuk asyncio.Queue
        logger.warning("Antrean notifikasi penuh, tidak dapat menambahkan foto baru.")
    except Exception as e:
        logger.error(f"Gagal menambahkan foto ke antrean notifikasi: {e}", exc_info=True)

def notify_signal(
    symbol: str,
    action: str,
    entry_price: Decimal | float | None,
    stop_loss: Decimal | float | None,
    take_profit_suggestion: Decimal | float | None = None,
    tp2_suggestion: Decimal | float | None = None,
    tp3_suggestion: Decimal | float | None = None,
    take_profit_levels_suggestion: list = None,
    reasoning: str = "",
    confidence: str = "",
    analyst_id: str = None,
    ai_confidence: str = None,
    summary: str = None,
    potential_direction: str = None,
    is_individual_analyst_signal: bool = False
):
    # --- START MODIFIKASI: Filter sinyal HOLD ---
    if action == "HOLD":
        logger.debug(f"Melewatkan notifikasi sinyal HOLD untuk {symbol}.")
        return # Langsung keluar dari fungsi jika aksinya HOLD
    # --- AKHIR MODIFIKASI ---

    if is_individual_analyst_signal:
        if not config.Telegram.SEND_INDIVIDUAL_ANALYST_SIGNALS:
            logger.info(f"Notifikasi sinyal individual dari {analyst_id or 'N/A'} dinonaktifkan.")
            return
    else:
        if not config.Telegram.SEND_SIGNAL_NOTIFICATIONS:
            logger.info("Notifikasi sinyal konsensus/final Telegram dinonaktifkan.")
            return

    message_lines = []
    
    header_text = "FINAL"
    if analyst_id and analyst_id != "Consensus_Executor":
        header_text = f"Analis: {analyst_id}"
    elif is_individual_analyst_signal:
        header_text = "INDIVIDU"
    elif analyst_id == "Rule_Based_Strategy":
        header_text = "BERBASIS ATURAN"
    elif analyst_id == "Aggressive_Strategy":
        header_text = "AGRESIF"

    message_lines.append(f"📊 *Sinyal Trading {header_text} untuk {symbol}*")

    direction_emoji = "📈" if action == "BUY" else ("📉" if action == "SELL" else "↔️")
    message_lines.append(f"{direction_emoji} *Aksi: {action}*")

    if entry_price is not None:
        message_lines.append(f"Entri: `${float(entry_price):.5f}`")
    if stop_loss is not None:
        message_lines.append(f"SL: `${float(stop_loss):.5f}`")

    if take_profit_levels_suggestion and isinstance(take_profit_levels_suggestion, list) and len(take_profit_levels_suggestion) > 0:
        message_lines.append("*Target Profit (Parsial):*")
        for i, tp_level in enumerate(take_profit_levels_suggestion):
            tp_price = tp_level.get('price')
            vol_perc = tp_level.get('volume_percentage')
            if tp_price is not None:
                message_lines.append(f"  • TP{i+1}: `${float(tp_price):.5f}` ({float(vol_perc)*100:.0f}% vol)")
    elif take_profit_suggestion is not None:
        message_lines.append("*Target Profit (TP):*")
        if take_profit_suggestion is not None:
            message_lines.append(f"  • TP1: `${float(take_profit_suggestion):.5f}`")
        if tp2_suggestion is not None:
            message_lines.append(f"  • TP2: `${float(tp2_suggestion):.5f}`")
        if tp3_suggestion is not None:
            message_lines.append(f"  • TP3: `${float(tp3_suggestion):.5f}`")
    else:
        message_lines.append("TP: N/A")

    if reasoning:
        message_lines.append(f"Alasan: {reasoning}")
    if confidence:
        message_lines.append(f"Kepercayaan: {confidence}")
    if ai_confidence and ai_confidence != confidence:
        message_lines.append(f"AI Conf: {ai_confidence}")
    if summary:
        message_lines.append(f"Ringkasan: {summary}")

    send_telegram_message("\n".join(message_lines))
    logger.info(f"Notifikasi sinyal {action} untuk {symbol} dikirim.")


def notify_trade_status(
    symbol: str, action: str, volume: Decimal | float, entry_price: Decimal | float,
    stop_loss: Decimal | float | None, take_profit: Decimal | float | None,
    comment: str, status: str, deal_id: int = None, exit_price: Decimal | float | None = None,
    exit_time: datetime = None
):
    if not config.Telegram.SEND_TRADE_NOTIFICATIONS:
        logger.info("Notifikasi status trade Telegram dinonaktifkan.")
        return

    status_emoji = ""
    if status == "Executed": status_emoji = "✅"
    elif status == "Partial TP Hit": status_emoji = "🎯"
    elif status == "SL Modified": status_emoji = "⛓️"
    elif status == "Closed Fully": status_emoji = "🔴"
    elif status == "SL Hit": status_emoji = "🛑"
    elif status == "BE Hit": status_emoji = "🔒"
    elif status == "TSL Hit": status_emoji = "➡️"
    elif status == "Time Limit": status_emoji = "⏳"
    elif status == "Opposite Signal": status_emoji = "↔️"
    elif status == "Failed": status_emoji = "❌"
    elif status == "Partial TP Failed": status_emoji = "⚠️"
    elif status == "SL Modify Failed": status_emoji = "🚨"
    else: status_emoji = "ℹ️"

    message_lines = [f"{status_emoji} *Status Trade: {status}*"]
    message_lines.append(f"  Aksi: `{action}` | Volume: `{float(volume):.2f}` lots")
    message_lines.append(f"  Simbol: `{symbol}`")

    if entry_price is not None:
        message_lines.append(f"  Entry Price: `${float(entry_price):.5f}`")
    if stop_loss is not None:
        message_lines.append(f"  SL: `${float(stop_loss):.5f}`")
    if take_profit is not None:
        message_lines.append(f"  TP: `${float(take_profit):.5f}`")

    if exit_price is not None:
        message_lines.append(f"  Harga Tutup: `${float(exit_price):.5f}`")
    if exit_time is not None:
        message_lines.append(f"  Waktu Tutup: `{exit_time.strftime('%Y-%m-%d %H:%M UTC')}`")

    message_lines.append(f"  Catatan: `{comment}`")
    if deal_id is not None:
        message_lines.append(f"  Deal ID: `{deal_id}`")

    send_telegram_message("\n".join(message_lines))
    logger.info(f"Notifikasi trade status '{status}' untuk {symbol} dikirim.")


def notify_position_update(position_data: dict):
    if not config.Telegram.SEND_TRADE_NOTIFICATIONS:
        logger.info("Notifikasi pembaruan posisi Telegram dinonaktifkan.")
        return

    message = (
        f"📝 *Position Update for {position_data['symbol']}*\n"
        f"  Ticket: `{position_data['ticket']}` (`{position_data['type']}` {float(position_data['volume']):.2f} lots)\n"
        f"  Open Price: `${float(position_data['price_open']):.5f}`\n"
        f"  Current Price: `${float(position_data['current_price']):.5f}`\n"
        f"  Profit: `${float(position_data['profit']):.2f}`\n"
        f"  SL: `${float(position_data['sl_price']):.5f}` | TP: `${float(position_data['tp_price']):.5f}`"
    )
    send_telegram_message(message, disable_notification=True)
    logger.info(f"Notifikasi update posisi {position_data['symbol']} dikirim.")

def notify_account_info(account_info_data: dict):
    if not config.Telegram.SEND_ACCOUNT_NOTIFICATIONS:
        logger.info("Notifikasi info akun Telegram dinonaktifkan.")
        return

    message = (
        f"💰 *MT5 Account Info ({account_info_data.get('login')})*\n"
        f"  Balance: `${float(account_info_data.get('balance', 0.0)):.2f}`\n"
        f"  Equity: `${float(account_info_data.get('equity', 0.0)):.2f}`\n"
        f"  Profit: `${float(account_info_data.get('profit', 0.0)):.2f}`\n"
        f"  Free Margin: `${float(account_info_data.get('free_margin', 0.0)):.2f}`\n"
        f"  Currency: `{account_info_data.get('currency')}`"
    )
    send_telegram_message(message, disable_notification=True)
    logger.info("Notifikasi info akun MT5 dikirim.")

def notify_daily_summary(symbol: str):
    if not config.Telegram.SEND_DAILY_SUMMARY:
        logger.info("Notifikasi ringkasan harian Telegram dinonaktifkan.")
        return

    try:
        # Import database_manager secara lokal untuk menghindari circular dependency
        from database_manager import get_ai_analysis_results
        latest_consensus = get_ai_analysis_results(symbol=symbol, analyst_id="Consensus_Executor", limit=1)
        
        # Anda perlu mengisi logic untuk summary_data di sini.
        # Ini hanya placeholder. Contoh:
        summary_data = {
            "date": datetime.now(timezone.utc).strftime('%Y-%m-%d'),
            "net_profit": 0.0, # Anda perlu mengambil ini dari DB/Trade history
            "total_trades": 0, # Anda perlu mengambil ini
            "winning_trades": 0, # Anda perlu mengambil ini
            "losing_trades": 0, # Anda perlu mengambil ini
            "win_rate_percent": "0.00%", # Hitung dari winning_trades/total_trades
            "max_drawdown": 0.0 # Anda perlu mengambil ini
        }
        # Contoh sederhana untuk mengisi sebagian data (Anda perlu logika yang lebih kompleks)
        # Asumsi ada fungsi di data_updater atau database_manager untuk mendapatkan PnL harian
        from data_updater import get_daily_realized_pnl
        daily_pnl = get_daily_realized_pnl(symbol)
        if daily_pnl is not None:
            summary_data["net_profit"] = float(daily_pnl)

    except Exception as e:
        logger.error(f"Gagal mendapatkan data untuk ringkasan harian: {e}", exc_info=True)
        summary_data = {
            "date": datetime.now(timezone.utc).strftime('%Y-%m-%d'),
            "net_profit": 0.0,
            "total_trades": 0,
            "win_rate_percent": "N/A",
            "max_drawdown": 0.0,
            "message": "Error getting summary data."
        }

    message = (
        f"📈 *Daily Performance Summary ({summary_data.get('date', datetime.now(timezone.utc).strftime('%Y-%m-%d'))})*\n"
        f"  Net Profit: `${float(summary_data.get('net_profit', 0.0)):.2f}`\n"
        f"  Total Trades: `{summary_data.get('total_trades', 0)}`\n"
        f"  Win Rate: `{summary_data.get('win_rate_percent', '0.00%')}`\n"
        f"  Max Drawdown: `${float(summary_data.get('max_drawdown', 0.0)):.2f}`"
    )
    send_telegram_message(message)
    logger.info("Notifikasi ringkasan harian dikirim.")

def notify_error(error_message: str, error_type: str = "Application Error"):
    if not config.Telegram.SEND_ERROR_NOTIFICATIONS:
        logger.info("Notifikasi error Telegram dinonaktifkan.")
        return

    message = f"❗ *ERROR: {error_type}*\n\n`{utils._escape_markdown(error_message)}`"
    send_telegram_message(message)
    logger.error(f"Notifikasi error '{error_type}' dikirim.")

def notify_app_start():
    if not config.Telegram.SEND_APP_STATUS_NOTIFICATIONS:
        logger.info("Notifikasi status aplikasi Telegram dinonaktifkan.")
        return

    part1 = "🚀 "
    part2_raw = "Trading Bot Berhasil Dimulai!"
    part3_raw = " Aplikasi telah berhasil diinisialisasi dan siap beroperasi."

    escaped_part1 = utils._escape_markdown(part1)
    escaped_part3 = utils._escape_markdown(part3_raw)
    escaped_part2_content = utils._escape_markdown(part2_raw)
    formatted_part2 = f"*{escaped_part2_content}*"

    final_message_content = escaped_part1 + formatted_part2 + escaped_part3

    send_telegram_message(final_message_content)
    logger.info("Notifikasi aplikasi dimulai dikirim.")

def notify_app_stop():
    if not config.Telegram.SEND_APP_STATUS_NOTIFICATIONS:
        logger.info("Notifikasi status aplikasi Telegram dinonaktifkan.")
        return

    part1 = "🛑 "
    part2_raw = "Trading Bot Dihentikan!"
    part3_raw = " Aplikasi telah dihentikan dengan aman."

    escaped_part1 = utils._escape_markdown(part1)
    escaped_part3 = utils._escape_markdown(part3_raw)
    escaped_part2_content = utils._escape_markdown(part2_raw)
    formatted_part2 = f"*{escaped_part2_content}*"

    final_message_content = escaped_part1 + formatted_part2 + escaped_part3

    send_telegram_message(final_message_content)
    logger.info("Notifikasi aplikasi dihentikan dikirim.")

impact_emoji = {
    "Low": "⚪",
    "Medium": "🟡",
    "High": "🔴"
}

def notify_fundamental_data_summary(
    economic_events_list: list, 
    news_articles_list: list, 
    total_scraped_events: int, 
    total_scraped_articles: int,
    min_impact_filter_level: str = None, # UBAH DEFAULT KE NONE, AGAR MENGGUNAKAN GLOBAL CONFIG
    news_topics_filter: list = None,
    include_ai_analysis_status: bool = False
):
    if not config.Telegram.SEND_FUNDAMENTAL_NOTIFICATIONS:
        logger.info("Notifikasi fundamental Telegram dinonaktifkan di konfigurasi.")
        return

    message_parts = []
    
    # --- MODIFIKASI INI ---
    # Jika min_impact_filter_level tidak disetel secara eksplisit, ambil dari config global
    if min_impact_filter_level is None:
        min_impact_filter_level = config.Telegram.MIN_IMPACT_NOTIF_LEVEL
    # --- AKHIR MODIFIKASI ---

    impact_levels = {"Low": 1, "Medium": 2, "High": 3}
    min_impact_value_num = impact_levels.get(min_impact_filter_level, 0)

    filtered_events = [
        event for event in economic_events_list 
        if impact_levels.get(event.get("impact"), 0) >= min_impact_value_num
    ]
    events_to_show = sorted(filtered_events, key=lambda x: x.get('event_time_utc', datetime.min))
    
    message_parts.append("📅 *Event Kalender Ekonomi Terbaru:*")
    if events_to_show:
        display_count_events = 0
        for event in events_to_show:
            if config.Telegram.MAX_EVENTS_TO_NOTIFY != 0 and display_count_events >= config.Telegram.MAX_EVENTS_TO_NOTIFY:
                break
            
            time_utc = event.get('event_time_utc')
            time_str = time_utc.strftime('%Y-%m-%d %H:%M UTC') if isinstance(time_utc, datetime) else 'N/A'
            
            current_event_impact = event.get('impact', 'N/A')
            current_event_currency = event.get('currency', 'N/A')
            current_event_name = event.get('name', 'N/A')

            impact_emoji_char = impact_emoji.get(current_event_impact, '⚪')
            
            event_detail_lines = []
            event_detail_lines.append(f"{impact_emoji_char} *{utils._escape_markdown(current_event_name)}* (`{utils._escape_markdown(current_event_currency)}`)")
            if time_str != 'N/A':
                event_detail_lines.append(f"   Waktu: `{utils._escape_markdown(time_str)}`")
            event_detail_lines.append(f"   Dampak: `{utils._escape_markdown(current_event_impact)}`")
            
            actual_val = event.get('actual_value') if event.get('actual_value') is not None else 'N/A'
            forecast_val = event.get('forecast_value') if event.get('forecast_value') is not None else 'N/A'
            previous_val = event.get('previous_value') if event.get('previous_value') is not None else 'N/A'

            if str(actual_val) != 'N/A' or str(forecast_val) != 'N/A' or str(previous_val) != 'N/A':
                message_parts.append(f"   Actual: `{utils._escape_markdown(str(actual_val))}` | Forecast: `{utils._escape_markdown(str(forecast_val))}` | Previous: `{utils._escape_markdown(str(previous_val))}`")

            message_parts.append("\n".join(event_detail_lines))

            display_count_events += 1
            if (config.Telegram.MAX_EVENTS_TO_NOTIFY == 0 and display_count_events < len(events_to_show)) or \
               (config.Telegram.MAX_EVENTS_TO_NOTIFY != 0 and display_count_events < config.Telegram.MAX_EVENTS_TO_NOTIFY and display_count_events < len(events_to_show)):
                 message_parts.append("")
    else:
        message_parts.append("_Tidak ada event kalender yang memenuhi kriteria._")

    message_parts.append("")

    message_parts.append(f"✅ *Pengumpulan Kalender Selesai:*")
    logger.info(f"Notifikasi {display_count_events} event kalender dikirim (dari {len(economic_events_list)} total yang diterima).")


def notify_only_news_articles(news_articles_list: list, include_topics: list = None):
    if not config.Telegram.SEND_FUNDAMENTAL_NOTIFICATIONS:
        logger.info("Notifikasi fundamental (berita) Telegram dinonaktifkan.")
        return
        
    if not news_articles_list:
        logger.info("Tidak ada artikel berita untuk dinotifikasi.")
        return

    message_lines = ["📰 *Berita Fundamental Terbaru (Hanya Berita):*"]
    
    filtered_articles = []
    if include_topics and len(include_topics) > 0:
        for article in news_articles_list:
            is_relevant = False
            title = article.get('title', '')
            summary = article.get('summary', '')
            for topic in include_topics:
                if topic.lower() in title.lower() or (summary and topic.lower() in summary.lower()):
                    is_relevant = True
                    break
            if is_relevant:
                filtered_articles.append(article)
            else:
                logger.debug(f"Melewatkan artikel '{article.get('title')}' karena tidak relevan dengan topik: {include_topics}.")
    else:
        filtered_articles = news_articles_list[:]

    articles_to_show = sorted(filtered_articles, key=lambda x: x.get('published_time_utc', datetime.min), reverse=True)
    
    max_articles_to_notify = config.Telegram.MAX_ARTICLES_TO_NOTIFY

    display_count = 0
    if articles_to_show:
        for i, article in enumerate(articles_to_show):
            if max_articles_to_notify != 0 and display_count >= max_articles_to_notify:
                break

            title = article.get('title', 'N/A')
            source = article.get('source', 'N/A')
            url = article.get('url', '#')
            published_time_utc = article.get('published_time_utc')
            
            time_str = published_time_utc.strftime('%Y-%m-%d %H:%M UTC') if isinstance(published_time_utc, datetime) else 'N/A'

            article_detail_lines = []
            article_detail_lines.append(f"*{i+1}\\. {utils._escape_markdown(title)}*")
            article_detail_lines.append(f"   Sumber: `{utils._escape_markdown(source)}`")
            article_detail_lines.append(f"   Waktu: `{utils._escape_markdown(time_str)}`")
            article_detail_lines.append(f"   \\[Baca Selengkapnya\\]({utils._escape_markdown(url)})")
            
            message_lines.append("\n".join(article_detail_lines))

            display_count += 1
            if (max_articles_to_notify == 0 and display_count < len(articles_to_show)) or \
               (max_articles_to_notify != 0 and display_count < max_articles_to_notify and display_count < len(articles_to_show)):
                message_lines.append("")
    else:
        message_lines.append("_Tidak ada artikel berita yang memenuhi kriteria._")

    message_lines.append("")
    
    # Bagian ini seharusnya ada di notify_fundamental_data_summary, bukan di sini
    # message_parts.append(
    #     f"✅ *Pengumpulan Berita Selesai:*\n"
    #     f"Data terbaru \\({total_scraped_articles} berita, {total_scraped_events} event\\) telah dikumpulkan dan disimpan ke database."
    # )
    # if include_ai_analysis_status:
    #     message_parts.append("Analisis AI fundamental berhasil dijalankan.")
    # else:
    #     message_parts.append("Analisis AI fundamental dilewati sesuai permintaan\\.")
    
    send_telegram_message("\n".join(message_lines)) # Mengirim hanya berita
    logger.info(f"Notifikasi berita fundamental dikirim (Total Berita: {display_count}).") # Perbaikan log

def notify_only_economic_calendar(economic_events_list: list, min_impact: str = None): # UBAH DEFAULT KE NONE
    """
    Mengumpulkan hanya data kalender ekonomi dari sumber yang relevan.
    """
    if not config.Telegram.SEND_FUNDAMENTAL_NOTIFICATIONS:
        logger.info("Notifikasi fundamental (kalender ekonomi) Telegram dinonaktifkan.")
        return

    if not economic_events_list:
        logger.info("Tidak ada event kalender ekonomi untuk dinotifikasi.")
        return

    message_lines = ["📅 *Event Kalender Ekonomi Terbaru (Hanya Event):*"]

    # --- MODIFIKASI INI ---
    # Jika min_impact tidak disetel secara eksplisit, ambil dari config global
    if min_impact is None:
        min_impact = config.Telegram.MIN_IMPACT_NOTIF_LEVEL
    # --- AKHIR MODIFIKASI ---

    impact_levels = {"Low": 1, "Medium": 2, "High": 3}
    min_impact_value_num = impact_levels.get(min_impact, 0)

    filtered_events = [
        event for event in economic_events_list 
        if impact_levels.get(event.get("impact"), 0) >= min_impact_value_num
    ]
    events_to_show = sorted(filtered_events, key=lambda x: x.get('event_time_utc', datetime.min))

    display_count_events = 0
    if events_to_show:
        for event in events_to_show:
            if config.Telegram.MAX_EVENTS_TO_NOTIFY != 0 and display_count_events >= config.Telegram.MAX_EVENTS_TO_NOTIFY:
                break
            
            time_utc = event.get('event_time_utc')
            time_str = time_utc.strftime('%Y-%m-%d %H:%M UTC') if isinstance(time_utc, datetime) else 'N/A'
            
            current_event_impact = event.get('impact', 'N/A')
            current_event_currency = event.get('currency', 'N/A')
            current_event_name = event.get('name', 'N/A')

            impact_emoji_char = impact_emoji.get(current_event_impact, '⚪')
            
            event_detail_lines = []
            event_detail_lines.append(f"{impact_emoji_char} *{utils._escape_markdown(current_event_name)}* (`{utils._escape_markdown(current_event_currency)}`)")
            if time_str != 'N/A':
                event_detail_lines.append(f"   Waktu: `{utils._escape_markdown(time_str)}`")
            event_detail_lines.append(f"   Dampak: `{utils._escape_markdown(current_event_impact)}`")
            
            actual_val = event.get('actual_value') if event.get('actual_value') is not None else 'N/A'
            forecast_val = event.get('forecast_value') if event.get('forecast_value') is not None else 'N/A'
            previous_val = event.get('previous_value') if event.get('previous_value') is not None else 'N/A'

            if str(actual_val) != 'N/A' or str(forecast_val) != 'N/A' or str(previous_val) != 'N/A':
                event_detail_lines.append(f"   Actual: `{utils._escape_markdown(str(actual_val))}` | Forecast: `{utils._escape_markdown(str(forecast_val))}` | Previous: `{utils._escape_markdown(str(previous_val))}`")

            message_lines.append("\n".join(event_detail_lines))

            display_count_events += 1
            if (config.Telegram.MAX_EVENTS_TO_NOTIFY == 0 and display_count_events < len(events_to_show)) or \
               (config.Telegram.MAX_EVENTS_TO_NOTIFY != 0 and display_count_events < config.Telegram.MAX_EVENTS_TO_NOTIFY and display_count_events < len(events_to_show)):
                 message_lines.append("")
    else:
        message_lines.append("_Tidak ada event kalender yang memenuhi kriteria._")
    
    send_telegram_message("\n".join(message_lines))
    logger.info(f"Notifikasi event kalender dikirim (Total Event: {display_count_events}).")



async def _async_telegram_sender_worker():
    custom_request = telegram.request.HTTPXRequest(
        connect_timeout=20.0,
        read_timeout=20.0,
        write_timeout=20.0,
    )
    local_telegram_bot = telegram.Bot(token=config.APIKeys.TELEGRAM_BOT_TOKEN, request=custom_request)

    _loop_initialized_event.set()

    logger.info("Telegram sender worker loop dimulai.")

    # Waktu tunggu maksimum saat antrean kosong
    QUEUE_GET_TIMEOUT_SECONDS = 5 

    while not _stop_worker.is_set() or not _notification_queue.empty():
        notification_item = None
        try:
            # Menggunakan await _notification_queue.get() dengan timeout
            # Ini akan menunggu item selama X detik sebelum memunculkan asyncio.TimeoutError
            notification_item = await asyncio.wait_for(
                _notification_queue.get(),
                timeout=QUEUE_GET_TIMEOUT_SECONDS
            )
            # Jika berhasil mendapatkan item, task_done akan dipanggil setelah pengiriman
            
        except asyncio.TimeoutError:
            # Ini adalah kondisi normal: antrean kosong setelah menunggu
            logger.debug(f"Antrean notifikasi Telegram kosong setelah {QUEUE_GET_TIMEOUT_SECONDS} detik. Menunggu item baru.")
            continue # Lanjutkan ke iterasi berikutnya di while loop

        except Exception as e:
            # Ini adalah error yang tidak terduga dan harus dicatat sebagai ERROR
            logger.error(f"Error tak terduga saat mencoba mengambil item dari antrean: {e}", exc_info=True)
            await asyncio.sleep(1) # Jeda singkat sebelum retry
            continue

        try:
            notif_type = notification_item['type']
            content = notification_item['content']
            disable_notif = notification_item.get('disable_notification', False)

            if notif_type == 'text':
                print("\n" + "="*50)
                print(">>> DEBUG: PESAN TELEGRAM FINAL YANG DIKIRIM <<<")
                print("="*50)
                print(content)
                print("="*50 + "\n")
                await local_telegram_bot.send_message(
                    chat_id=config.APIKeys.TELEGRAM_CHAT_ID,
                    text=content,
                    parse_mode='MarkdownV2',
                    disable_notification=disable_notif
                )
                logger.info("Notifikasi Telegram teks berhasil terkirim.")
            elif notif_type == 'photo':
                caption = notification_item.get('caption', '')
                
                print("\n" + "="*50)
                print(">>> DEBUG: CAPTION TELEGRAM FINAL YANG DIKIRIM <<<")
                print("="*50)
                print(caption)
                print("="*50 + "\n")

                with open(content, 'rb') as photo_file:
                    await local_telegram_bot.send_photo(
                        chat_id=config.APIKeys.TELEGRAM_CHAT_ID,
                        photo=photo_file,
                        caption=utils._escape_markdown(caption),
                        parse_mode='MarkdownV2',
                        disable_notification=disable_notif
                    )
                logger.info(f"Notifikasi Telegram foto '{content}' berhasil terkirim.")
            
            # Penting: task_done dipanggil HANYA jika item berhasil diproses/dikirim
            _notification_queue.task_done()

        except TelegramError as e:
            logger.error(f"Gagal mengirim notifikasi Telegram (TelegramError): {e}", exc_info=True)
            # Meskipun gagal, item ini sudah "dicoba" dan harus ditandai selesai
            # agar tidak menghalangi antrean, kecuali ada retry logic khusus.
            _notification_queue.task_done() 
        except Exception as e:
            logger.error(f"Error tak terduga di Telegram sender worker setelah mengambil item: {e}", exc_info=True)
            # Sama seperti di atas, tandai item selesai
            _notification_queue.task_done() 

    logger.info("Telegram sender worker loop dihentikan.")
    while not _notification_queue.empty():
        try:
            _notification_queue.get_nowait()
            _notification_queue.task_done()
        except asyncio.QueueEmpty: # Gunakan asyncio.QueueEmpty di sini
            pass
    logger.info("Telegram notification queue dibersihkan saat shutdown.")

def start_notification_service():
    """
    Memulai worker thread untuk mengirim notifikasi Telegram secara asinkron.
    """
    global _worker_thread
    if _worker_thread is None or not _worker_thread.is_alive():
        _stop_worker.clear()
        _loop_initialized_event.clear()
        
        # Penting: `asyncio.run` harus dipanggil di thread yang sama tempat event loop dijalankan.
        # Jadi, kita membungkusnya dalam lambda.
        _worker_thread = threading.Thread(target=lambda: asyncio.run(_async_telegram_sender_worker()), name="TelegramSenderThread", daemon=True)
        _worker_thread.start()
        
        # Tunggu sampai event loop di thread worker benar-benar dimulai
        _loop_initialized_event.wait(timeout=10)
        if not _loop_initialized_event.is_set():
            logger.error("Gagal memulai Telegram sender worker event loop dalam batas waktu.")
            return False
        
        logger.info("Telegram notification service dimulai.")
        return True
    logger.info("Telegram notification service sudah berjalan.")
    return False

def stop_notification_service():
    """
    Menghentikan worker thread pengiriman notifikasi Telegram.
    """
    global _worker_thread
    if _worker_thread and _worker_thread.is_alive():
        logger.info("Menghentikan Telegram notification service...")
        _stop_worker.set()
        
        # Memberi kesempatan antrean untuk kosong sebelum join, tapi tidak wajib
        # try:
        #     _notification_queue.put_nowait(None) # Sinyal berhenti (opsional, jika worker memproses None)
        # except asyncio.QueueFull:
        #     logger.warning("Notification queue penuh saat shutdown, tidak dapat mengirim sinyal berhenti.")
        # except Exception as e:
        #     logger.error(f"Error mengirim sinyal berhenti ke notif queue: {e}", exc_info=True)

        _worker_thread.join(timeout=5) # Beri waktu thread untuk selesai
        if _worker_thread.is_alive():
            logger.warning("Telegram notification worker thread tidak berhenti dalam batas waktu.")
        else:
            logger.info("Telegram notification worker thread telah dihentikan.")
        _worker_thread = None
    else:
        logger.info("Telegram notification service tidak berjalan atau sudah berhenti.")

if not config.APIKeys.TELEGRAM_BOT_TOKEN or not config.APIKeys.TELEGRAM_CHAT_ID:
    logger.critical("Token Bot Telegram atau Chat ID tidak dikonfigurasi! Notifikasi Telegram akan dinonaktifkan.")
    config.Telegram.SEND_SIGNAL_NOTIFICATIONS = False
    config.Telegram.SEND_TRADE_NOTIFICATIONS = False
    config.Telegram.SEND_ACCOUNT_NOTIFICATIONS = False
    config.Telegram.SEND_DAILY_SUMMARY = False
    config.Telegram.SEND_ERROR_NOTIFICATIONS = False
    config.Telegram.SEND_APP_STATUS_NOTIFICATIONS = False
    config.Telegram.SEND_FUNDAMENTAL_NOTIFICATIONS = False
    config.Telegram.SEND_INDIVIDUAL_ANALYST_SIGNALS = False 
else:
    logger.info("Konfigurasi Telegram API ditemukan.")
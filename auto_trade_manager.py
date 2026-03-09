# auto_trade_manager.py
import logging
import mt5_connector
from config import config
import re
from decimal import Decimal, getcontext
from datetime import datetime, timezone, timedelta
import decimal
import data_updater
import notification_service
import database_manager # Pastikan ini diimpor
from utils import to_float_or_none, to_iso_format_or_none , to_decimal_or_none, to_utc_datetime_or_none, _get_scalar_from_possibly_ndarray
import utils
form trade_journal_service import TradeJournalService

logger = logging.getLogger(__name__)

journal_service = TradeJournalService()

# Set presisi Decimal untuk perhitungan keuangan
getcontext().prec = 10

# Variabel global untuk melacak status harian
_daily_limit_hit_today = False
_last_daily_limit_reset_date = None

# --- MODIFIKASI: Variabel global untuk melacak status Take Partials per posisi ---
# Format: {ticket_id: {'hit_flags': {idx: True/False}, 'tp_levels_config': [...], 'open_price': Decimal, 'sl_initial': Decimal, 'tp1_initial': Decimal, 'type': str}}
_position_partial_tp_status = {}
# --- AKHIR MODIFIKASI ---


def _reset_daily_limit_status():
    """Meriset status limit harian jika hari sudah berganti."""
    global _daily_limit_hit_today, _last_daily_limit_reset_date, _position_partial_tp_status
    current_date_utc = datetime.now(timezone.utc).date()

    if _last_daily_limit_reset_date is None or _last_daily_limit_reset_date < current_date_utc:
        _daily_limit_hit_today = False
        _last_daily_limit_reset_date = current_date_utc
        
        logger.info(f"Status limit harian dan partial TP direset untuk hari baru: {current_date_utc}.")

        # --- START MODIFIKASI: Memuat status TP saat reset harian/startup ---
        _position_partial_tp_status = {} # Reset sebelum memuat dari DB, untuk menghindari duplikasi
        logger.info("Mengecek posisi terbuka untuk memuat status TP parsial dari DB...")
        
        # Ambil semua posisi terbuka saat ini dari MT5
        all_open_mt5_positions = mt5_connector.get_mt5_positions_raw()
        
        if all_open_mt5_positions:
            for pos in all_open_mt5_positions:
                ticket = pos.get('ticket')
                symbol_pos = pos.get('symbol')
                pos_type_mt5 = pos.get('type')
                pos_price_open = utils.to_decimal_or_none(pos.get('price_open'))
                pos_current_sl = utils.to_decimal_or_none(pos.get('sl'))
                pos_current_tp = utils.to_decimal_or_none(pos.get('tp'))
                
                # Coba muat hit_flags dan tp_levels_config dari database
                stored_position_status = database_manager.get_mt5_position_partial_tp_flags(ticket)
                
                if stored_position_status is not None:
                    stored_hit_flags = stored_position_status.get('hit_flags', [])
                    stored_tp_levels_config = stored_position_status.get('tp_levels_config', [])

                    # Pastikan panjang hit_flags sesuai dengan PARTIAL_TP_LEVELS yang dikonfigurasi
                    expected_num_levels = len(config.Trading.PARTIAL_TP_LEVELS)
                    if len(stored_hit_flags) != expected_num_levels:
                        logger.warning(f"Panjang stored_hit_flags ({len(stored_hit_flags)}) tidak cocok dengan config ({expected_num_levels}) untuk posisi {ticket}. Menginisialisasi ulang flags.")
                        stored_hit_flags = [False] * expected_num_levels
                    
                    if not stored_tp_levels_config: # Jika tp_levels_config tidak ada di DB (mungkin dari trade lama)
                        logger.warning(f"tp_levels_config tidak ditemukan di DB untuk posisi {ticket}. Menggunakan default PARTIAL_TP_LEVELS dari config. Ini mungkin tidak akurat.")
                        # Ini adalah batasan jika data lama tidak punya tp_levels_config_json
                        tp_levels_config_for_pos = config.Trading.PARTIAL_TP_LEVELS
                    else:
                        # Konversi semua Decimal string kembali ke Decimal object di tp_levels_config
                        tp_levels_config_for_pos = []
                        for level in stored_tp_levels_config:
                            processed_level = level.copy()
                            for key, value in processed_level.items():
                                if 'price' in key or 'multiplier' in key or 'percentage' in key: # Angka yang perlu Decimal
                                    processed_level[key] = utils.to_decimal_or_none(value)
                            tp_levels_config_for_pos.append(processed_level)


                    logger.info(f"Memuat status TP parsial dari DB untuk posisi {ticket}: {stored_hit_flags}")
                else:
                    stored_hit_flags = [False] * len(config.Trading.PARTIAL_TP_LEVELS)
                    tp_levels_config_for_pos = config.Trading.PARTIAL_TP_LEVELS # Gunakan default config jika tidak ada di DB
                    logger.info(f"Tidak ada status TP parsial tersimpan di DB untuk posisi {ticket}. Menginisialisasi flags baru: {stored_hit_flags} dan config default.")
                
                _position_partial_tp_status[ticket] = {
                    'hit_flags': stored_hit_flags,
                    'tp_levels_config': tp_levels_config_for_pos, # <-- Gunakan yang dimuat/default
                    'open_price': pos_price_open,
                    'initial_sl': pos_current_sl, # Gunakan SL saat ini dari MT5
                    'initial_tp1': pos_current_tp, # Gunakan TP saat ini dari MT5
                    'type': pos_type_mt5
                }
                logger.debug(f"Posisi {ticket} ditambahkan ke _position_partial_tp_status saat startup.")
        # --- AKHIR MODIFIKASI ---
        
        if not config.Trading.auto_trade_enabled:
            config.Trading.auto_trade_enabled = True
            notification_service.notify_app_status("Auto-trade diaktifkan kembali secara otomatis di awal hari baru.")
            logger.info("Auto-trade diaktifkan kembali karena hari baru.")


def close_all_open_positions(symbol_param: str):
    """
    Menutup semua posisi terbuka untuk simbol tertentu.
    Akan mengirim notifikasi untuk setiap posisi yang ditutup.
    """
    logger.info(f"Menerima permintaan untuk menutup semua posisi terbuka untuk {symbol_param}.")

    positions = mt5_connector.get_mt5_positions_raw()
    if not positions:
        logger.info(f"Tidak ada posisi terbuka untuk {symbol_param} yang perlu ditutup.")
        return True

    closed_count = 0

    for pos in positions:

        if pos.get('symbol') == symbol_param:

            ticket = pos.get('ticket')
            volume_to_close = pos.get('volume')
            pos_type = pos.get('type')

            price_open = utils.to_decimal_or_none(pos.get('price_open'))
            current_sl = utils.to_decimal_or_none(pos.get('sl'))
            current_tp = utils.to_decimal_or_none(pos.get('tp'))

            if volume_to_close is None or volume_to_close <= Decimal('0.0'):
                logger.warning(f"Posisi {ticket} untuk {symbol_param} memiliki volume nol atau tidak valid. Melewatkan penutupan.")
                continue

            logger.info(f"Mencoba menutup posisi {pos.get('type')} {volume_to_close} lot untuk {symbol_param} (Ticket: {ticket}).")

            result = mt5_connector.close_position(ticket, symbol_param, volume_to_close)

            if result and result.get('retcode') == mt5_connector.mt5.TRADE_RETCODE_DONE:

                logger.info(f"Posisi {ticket} berhasil ditutup: {result.get('comment')}.")
                closed_count += 1

                # -------------------------------
                # UPDATE TRADE JOURNAL
                # -------------------------------

                journal_id = _position_partial_tp_status.get(ticket, {}).get("journal_id")

                if journal_id:

                    profit_value = pos.get("profit", 0)

                    journal_service.update_trade_result(
                        trade_id=journal_id,
                        profit_loss=float(profit_value),
                        rr=0,
                        drawdown=0,
                        trade_result="win" if profit_value > 0 else "loss",
                    )

                # -------------------------------
                # Hapus status partial TP
                # -------------------------------

                if ticket in _position_partial_tp_status:
                    del _position_partial_tp_status[ticket]

                database_manager.update_mt5_position_partial_tp_flags(ticket, [], [])

                # -------------------------------
                # Notifikasi
                # -------------------------------

                notification_service.notify_trade_status(
                    symbol_param,
                    "BUY" if pos_type == mt5_connector.mt5.POSITION_TYPE_BUY else "SELL",
                    volume_to_close,
                    price_open,
                    current_sl,
                    current_tp,
                    result.get('comment'),
                    "Closed Fully (All Positions Closed)",
                    result.get('deal'),
                    result.get('price')
                )

            else:

                logger.error(f"Gagal menutup posisi {ticket}: {result.get('comment') if result else 'Unknown error'}.")

                notification_service.notify_trade_status(
                    symbol_param,
                    "BUY" if pos_type == mt5_connector.mt5.POSITION_TYPE_BUY else "SELL",
                    volume_to_close,
                    price_open,
                    current_sl,
                    current_tp,
                    result.get('comment') if result else 'Unknown error',
                    "Close Failed (All Positions Closed)",
                    result.get('deal') if result else None,
                    result.get('price') if result else None
                )

    if closed_count > 0:
        logger.info(f"Berhasil menutup {closed_count} posisi untuk {symbol_param}.")
        return True

    else:
        logger.warning(f"Tidak ada posisi yang berhasil ditutup untuk {symbol_param}.")
        return False

def check_daily_pnl_and_act(symbol_param: str):
    """
    Memeriksa profit/loss harian yang terealisasi terhadap target/limit.
    Jika target/limit tercapai, akan menutup posisi dan menonaktifkan auto-trade.
    """
    _reset_daily_limit_status()

    global _daily_limit_hit_today
    if _daily_limit_hit_today:
        logger.info("Limit profit/loss harian sudah tercapai hari ini. Tidak ada tindakan trading baru.")
        return

    daily_pnl = data_updater.get_daily_realized_pnl(symbol_param) # `get_daily_realized_pnl` hanya mengembalikan satu nilai, bukan tuple.
    account_info = mt5_connector.get_mt5_account_info_raw()

    if account_info is None:
        logger.warning("Tidak dapat mengambil info akun MT5 untuk memeriksa P/L harian (account_info is None).")
        return

    current_balance = utils.to_decimal_or_none(account_info.get('balance'))
    current_equity = utils.to_decimal_or_none(account_info.get('equity'))

    if current_balance is None or current_equity is None:
        logger.warning("Saldo atau ekuitas akun MT5 tidak valid. Tidak dapat memeriksa P/L harian.")
        return

    initial_balance_today = current_balance - daily_pnl

    if initial_balance_today == Decimal('0.0'):
        logger.warning("Saldo awal harian nol, tidak dapat menghitung persentase P/L. Melewatkan.")
        return

    pnl_percent = (daily_pnl / initial_balance_today) * Decimal('100.0')

    daily_profit_target = config.Trading.DAILY_PROFIT_TARGET_PERCENT
    daily_loss_limit = config.Trading.DAILY_LOSS_LIMIT_PERCENT

    action_taken = False

    if daily_profit_target is not None and daily_profit_target > Decimal('0.0') and pnl_percent >= daily_profit_target:
        logger.info(f"TARGET PROFIT HARIAN TERCAPAI! P/L Harian: {float(daily_pnl):.2f} ({float(pnl_percent):.2f}%). Target: {float(daily_profit_target):.2f}%.")
        notification_service.send_telegram_message(
            f"TARGET PROFIT HARIAN TERCAPAI! Profit: {float(daily_pnl):.2f} ({float(pnl_percent):.2f}%). Auto-trade dinonaktifkan.",
            disable_notification=False
        )
        action_taken = True
    elif daily_loss_limit is not None and daily_loss_limit > Decimal('0.0') and pnl_percent <= -daily_loss_limit:
        logger.warning(f"BATAS KERUGIAN HARIAN TERCAPAI! P/L Harian: {float(daily_pnl):.2f} ({float(pnl_percent):.2f}%). Limit: {float(daily_loss_limit):.2f}%.")
        notification_service.send_telegram_message(
            f"BATAS KERUGIAN HARIAN TERCAPAI! Loss: {float(daily_pnl):.2f} ({float(pnl_percent):.2f}%). Auto-trade dinonaktifkan.",
            disable_notification=False
        )
        action_taken = True

    if action_taken:
        logger.info("Menutup semua posisi terbuka karena target/limit harian tercapai.")
        close_all_open_positions(symbol_param) # Fungsi ini sudah diupdate untuk notifikasi penutupan
        config.Trading.auto_trade_enabled = False
        _daily_limit_hit_today = True
        logger.info("Auto-trade dinonaktifkan untuk sisa hari ini.")
    else:
        logger.debug(f"P/L Harian saat ini: {float(daily_pnl):.2f} ({float(pnl_percent):.2f}%). Belum mencapai target/limit.")


def check_and_execute_take_partials(symbol_param: str, current_price: Decimal):
    """
    Memeriksa semua posisi terbuka dan mengeksekusi take partials jika level TP tercapai.
    Args:
        symbol_param (str): Simbol trading.
        current_price (Decimal): Harga bid/ask saat ini dari tick real-time.
    """
    if not config.Trading.auto_trade_enabled:
        logger.debug("Auto-trade dinonaktifkan, melewatkan pemeriksaan take partials.")
        return

    positions = mt5_connector.get_mt5_positions_raw()
    if not positions:
        logger.debug(f"Tidak ada posisi terbuka untuk {symbol_param}. Melewatkan pemeriksaan take partials.")
        return

    for pos in positions:
        if pos.get('symbol') != symbol_param:
            continue

        ticket = pos.get('ticket')
        pos_type = pos.get('type')
        price_open = utils.to_decimal_or_none(pos.get('price_open'))
        current_volume = utils.to_decimal_or_none(pos.get('volume'))
        current_sl = utils.to_decimal_or_none(pos.get('sl'))
        current_tp = utils.to_decimal_or_none(pos.get('tp')) # TP utama posisi di MT5

        if price_open is None or current_volume is None or current_volume <= Decimal('0.0'):
            logger.warning(f"Posisi {ticket} memiliki data tidak valid (price_open/volume). Melewatkan take partials.")
            continue

        # --- MODIFIKASI: Ambil konfigurasi TP parsial dan status dari _position_partial_tp_status ---
        pos_details = _position_partial_tp_status.get(ticket)
        if not pos_details:
            logger.warning(f"Posisi {ticket} tidak ditemukan dalam _position_partial_tp_status (mungkin bot restart atau dibuka manual). Mencoba memuat dari DB...")
            # Coba muat dari DB jika tidak ada di memory
            stored_position_status = database_manager.get_mt5_position_partial_tp_flags(ticket)
            if stored_position_status:
                pos_details = {
                    'hit_flags': stored_position_status.get('hit_flags', []),
                    'tp_levels_config': stored_position_status.get('tp_levels_config', []),
                    'open_price': price_open, # Gunakan harga buka MT5
                    'initial_sl': current_sl, # Gunakan SL MT5
                    'initial_tp1': current_tp, # Gunakan TP MT5
                    'type': pos_type # Gunakan tipe MT5
                }
                _position_partial_tp_status[ticket] = pos_details # Tambahkan ke memory
                logger.info(f"Posisi {ticket} berhasil dimuat dari DB untuk pemrosesan TP parsial.")
            else:
                logger.warning(f"Posisi {ticket} tidak ditemukan di DB. Melewatkan pemeriksaan take partials.")
                continue

        tp_levels_config_for_this_pos = pos_details.get('tp_levels_config', [])
        hit_flags = pos_details.get('hit_flags', []) # Daftar boolean [False, False, ...]
        # --- AKHIR MODIFIKASI ---

        if not tp_levels_config_for_this_pos:
            logger.debug(f"Tidak ada level take partials yang dikonfigurasi untuk posisi {ticket}. Melewatkan pemeriksaan.")
            continue

        logger.debug(f"Memeriksa take partials untuk posisi {ticket} ({pos_type}) @ {float(price_open):.5f}, Volume: {float(current_volume):.2f}.")

        for idx, tp_level_cfg in enumerate(tp_levels_config_for_this_pos):
            if hit_flags[idx]: # Cek apakah TP level ini sudah tercapai sebelumnya
                logger.debug(f"TP level {idx+1} untuk posisi {ticket} sudah tercapai. Melewatkan.")
                continue

            # --- MODIFIKASI: Gunakan harga TP absolut dari sinyal ---
            target_tp_price = utils.to_decimal_or_none(tp_level_cfg.get('price')) # <- Ambil langsung harga TP absolut
            if target_tp_price is None:
                logger.warning(f"Harga TP level {idx+1} untuk posisi {ticket} tidak valid. Melewatkan.")
                continue
            # --- AKHIR MODIFIKASI ---

            volume_percentage_to_close = utils.to_decimal_or_none(tp_level_cfg.get('volume_percentage', Decimal('1.00')))
            move_sl_to_breakeven = tp_level_cfg.get('move_sl_to_breakeven_after_partial', False)
            move_sl_to_price = utils.to_decimal_or_none(tp_level_cfg.get('move_sl_to_price_after_partial'))

            if volume_percentage_to_close is None or volume_percentage_to_close <= Decimal('0.0'):
                logger.warning(f"Konfigurasi TP level {idx+1} untuk take partials tidak valid (volume_percentage). Melewatkan.")
                continue

            # Cek apakah TP level ini sudah tercapai
            tp_hit = False
            # Perhatikan toleransi hit: harga saat ini harus mencapai target_tp_price
            # Jika harga TP adalah 3376.05 dan harga saat ini 3376.05 atau lebih baik
            if pos_type == mt5_connector.mt5.POSITION_TYPE_BUY:
                if current_price >= target_tp_price:
                    tp_hit = True
            elif pos_type == mt5_connector.mt5.POSITION_TYPE_SELL:
                if current_price <= target_tp_price:
                    tp_hit = True

            if tp_hit:
                logger.info(f"TP level {idx+1} TERCAPAI untuk {'BUY' if pos_type == mt5_connector.mt5.POSITION_TYPE_BUY else 'SELL'} posisi {ticket} di harga {float(current_price):.5f}.")

                _execute_partial_close_and_sl_move(
                    ticket=ticket,
                    symbol_param=symbol_param,
                    pos_type=pos_type,
                    price_open=price_open,
                    current_volume=current_volume, # Volume saat ini dari MT5
                    current_sl=current_sl, # SL saat ini dari MT5
                    current_tp=current_tp, # TP utama saat ini dari MT5
                    partial_tp_price=target_tp_price, # Harga TP yang hit
                    close_percentage=volume_percentage_to_close,
                    move_sl_to_breakeven=move_sl_to_breakeven,
                    move_sl_to_price=move_sl_to_price,
                    # --- MODIFIKASI: Teruskan detail posisi awal untuk notifikasi ---
                    original_entry_price=pos_details.get('open_price'),
                    initial_sl_price_for_notification=pos_details.get('initial_sl'),
                    initial_tp1_price_for_notification=pos_details.get('initial_tp1')
                    # --- AKHIR MODIFIKASI ---
                )
                hit_flags[idx] = True # Update status hit flag
                _position_partial_tp_status[ticket]['hit_flags'] = hit_flags # Simpan kembali hit_flags yang diperbarui
                # --- START MODIFIKASI: Simpan status hit_flags yang diperbarui ke DB setelah TP hit ---
                database_manager.update_mt5_position_partial_tp_flags(
                    ticket=ticket,
                    partial_tp_hit_flags=hit_flags,
                    tp_levels_config=tp_levels_config_for_this_pos # Simpan juga config terbaru
                )
                # --- AKHIR MODIFIKASI ---

                # Jika volume posisi menjadi nol setelah parsial, kita asumsikan posisi ditutup penuh
                # Ini akan dipantau oleh pengecekan posisi terbuka di loop utama atau oleh MT5 sendiri
                # dan akan memicu notifikasi penutupan penuh jika sudah ada mekanisme untuk itu.
                break # Setelah satu TP parsial dieksekusi, keluar dari loop TP untuk posisi ini

# --- MODIFIKASI: Tambahkan parameter baru untuk notifikasi ---
def _execute_partial_close_and_sl_move(ticket: int, symbol_param: str, pos_type: int, price_open: Decimal, current_volume: Decimal, current_sl: Decimal, current_tp: Decimal, partial_tp_price: Decimal, close_percentage: Decimal, move_sl_to_breakeven: bool, move_sl_to_price: Decimal = None, original_entry_price: Decimal = None, initial_sl_price_for_notification: Decimal = None, initial_tp1_price_for_notification: Decimal = None):
    """
    Mengeksekusi penutupan sebagian posisi dan memindahkan SL.
    Termasuk notifikasi Telegram.
    """
    volume_to_close = (current_volume * close_percentage).quantize(Decimal('0.01'), rounding=decimal.ROUND_UP)

    if volume_to_close <= Decimal('0.0'):
        logger.warning(f"Volume untuk partial close posisi {ticket} adalah nol atau tidak valid ({float(volume_to_close):.2f}). Melewatkan.")
        return

    logger.info(f"Mencoba partial close {float(volume_to_close):.2f} lot dari posisi {ticket} ({pos_type}) di harga {float(partial_tp_price):.5f}.")
    result = mt5_connector.close_position(ticket, symbol_param, volume_to_close)

    if result and result.retcode == mt5_connector.mt5.TRADE_RETCODE_DONE:
        logger.info(f"Partial close posisi {ticket} berhasil. Volume tersisa: {float(current_volume - volume_to_close):.2f}.")
        # --- MODIFIKASI: Notifikasi untuk Partial TP Hit ---
        notification_service.notify_trade_status(
            symbol=symbol_param,
            action="BUY" if pos_type == mt5_connector.mt5.POSITION_TYPE_BUY else "SELL",
            volume=volume_to_close,
            entry_price=original_entry_price, # Harga entry posisi asli
            stop_loss=initial_sl_price_for_notification, # SL awal untuk notifikasi
            take_profit=initial_tp1_price_for_notification, # TP1 awal untuk notifikasi
            comment=f"Partial TP Hit ({float(close_percentage*100):.0f}% volume closed)",
            status="Partial TP Hit",
            deal_id=result.deal,
            exit_price=result.price, # Harga penutupan parsial
            exit_time=datetime.now(timezone.utc) # Waktu saat ini
        )
        # --- AKHIR MODIFIKASI ---

        # Hitung SL baru jika perlu
        new_sl_price = current_sl # Default tetap SL saat ini
        sl_moved = False

        if move_sl_to_price is not None:
            new_sl_price = move_sl_to_price
            sl_moved = True
            logger.info(f"Memindahkan SL posisi {ticket} ke harga spesifik ({float(new_sl_price):.5f}).")
        elif move_sl_to_breakeven:
            new_sl_price = price_open # Pindahkan SL ke harga open
            sl_moved = True
            logger.info(f"Memindahkan SL posisi {ticket} ke Breakeven ({float(new_sl_price):.5f}).")

        if sl_moved:
            # Pastikan new_sl_price tidak terlalu dekat dengan harga saat ini (min_sl_pips)
            min_sl_distance_dec = config.Trading.MIN_SL_PIPS * config.TRADING_SYMBOL_POINT_VALUE

            is_sl_too_close = False
            # Asumsi `result.price` adalah harga eksekusi close parsial.
            # Kita perlu cek SL baru terhadap harga market saat ini (bid/ask) atau harga terakhir yang diketahui.
            # Untuk simplifikasi, kita bisa pakai `result.price` sebagai referensi.
            if pos_type == mt5_connector.mt5.POSITION_TYPE_BUY and (new_sl_price > result.price - min_sl_distance_dec):
                is_sl_too_close = True
            elif pos_type == mt5_connector.mt5.POSITION_TYPE_SELL and (new_sl_price < result.price + min_sl_distance_dec):
                is_sl_too_close = True

            if is_sl_too_close:
                logger.warning(f"SL baru ({float(new_sl_price):.5f}) terlalu dekat dengan harga eksekusi ({float(result.price):.5f}) untuk posisi {ticket}. Tidak memodifikasi SL.")
                # --- MODIFIKASI: Notifikasi kegagalan modifikasi SL ---
                notification_service.notify_trade_status(
                    symbol_param,
                    "BUY" if pos_type == mt5_connector.mt5.POSITION_TYPE_BUY else "SELL",
                    volume_to_close,
                    original_entry_price, initial_sl_price_for_notification, initial_tp1_price_for_notification,
                    f"SL Modified Failed: SL too close ({float(new_sl_price):.5f})", "SL Modify Failed"
                )
                # --- AKHIR MODIFIKASI ---
            else:
                modify_result = mt5_connector.modify_position_sl_tp(ticket, symbol_param, new_sl_price, current_tp)
                if modify_result and modify_result.get('retcode') == mt5_connector.mt5.TRADE_RETCODE_DONE:
                    logger.info(f"SL posisi {ticket} berhasil dimodifikasi ke {float(new_sl_price):.5f}.")
                    # --- MODIFIKASI: Notifikasi untuk SL Modified ---
                    notification_service.notify_trade_status(
                        symbol_param,
                        "BUY" if pos_type == mt5_connector.mt5.POSITION_TYPE_BUY else "SELL",
                        current_volume - volume_to_close, # Volume posisi setelah parsial
                        original_entry_price, new_sl_price, initial_tp1_price_for_notification,
                        f"SL Modified to {'BE' if move_sl_to_breakeven else float(new_sl_price):.5f}", "SL Modified"
                    )
                    # --- AKHIR MODIFIKASI ---
                else:
                    logger.error(f"Gagal memodifikasi SL posisi {ticket}: {modify_result.get('comment', 'Unknown error')}.")
                    # Notifikasi kegagalan
                    notification_service.notify_trade_status(
                        symbol_param,
                        "BUY" if pos_type == mt5_connector.mt5.POSITION_TYPE_BUY else "SELL",
                        current_volume - volume_to_close,
                        original_entry_price, initial_sl_price_for_notification, initial_tp1_price_for_notification,
                        f"SL Modified Failed: {modify_result.get('comment', 'Unknown')}", "SL Modify Failed"
                    )
        else:
            logger.debug(f"Tidak ada perubahan SL yang diperlukan atau SL baru tidak valid untuk posisi {ticket}.")

    else:
        logger.error(f"Partial close posisi {ticket} GAGAL: {result.comment if result else 'Unknown error'}.")
        notification_service.notify_trade_status(
            symbol=symbol_param,
            action="BUY" if pos_type == mt5_connector.mt5.POSITION_TYPE_BUY else "SELL",
            volume=volume_to_close,
            entry_price=original_entry_price,
            stop_loss=initial_sl_price_for_notification,
            take_profit=initial_tp1_price_for_notification,
            comment=f"Partial TP Failed: {result.comment if result else 'Unknown error'}",
            status="Partial TP Failed",
            deal_id=result.deal,
            exit_price=result.price,
            exit_time=datetime.now(timezone.utc)
        )

def execute_ai_trade(symbol: str, action: str, volume: Decimal, entry_price: Decimal, stop_loss: Decimal, take_profit: Decimal, slippage: int, magic_number: int, tp_levels_config: list):
    logger.info(f"Menerima permintaan trade: {action} {float(volume):.2f} {symbol} @ Entry:{float(entry_price):.5f}, SL:{float(stop_loss):.5f}, TP:{float(take_profit):.5f}")

    global _daily_limit_hit_today
    if not config.Trading.auto_trade_enabled:
        logger.warning("Auto-trade dinonaktifkan. Tidak ada trade yang dieksekusi.")
        notification_service.notify_trade_status(
            symbol, action, volume, entry_price, stop_loss, take_profit,
            "Trade Rejected: Auto-trade Disabled", "Rejected"
        )
        class MockDisabledResult:
            retcode = -1
            comment = "Auto-trade disabled."
            deal = 0
            volume = Decimal('0.0')
            price = entry_price
        return MockDisabledResult()

    if _daily_limit_hit_today:
        logger.warning("Limit profit/loss harian sudah tercapai. Tidak ada trade baru yang dieksekusi.")
        notification_service.notify_trade_status(
            symbol, action, volume, entry_price, stop_loss, take_profit,
            "Trade Rejected: Daily Limit Hit", "Rejected"
        )
        class MockLimitHitResult:
            retcode = -1
            comment = "Daily limit hit."
            deal = 0
            volume = Decimal('0.0')
            price = entry_price
        return MockLimitHitResult()

    # --- START MODIFIKASI: Cek Batas Total Lot dan Jumlah Posisi ---
    open_positions = mt5_connector.get_mt5_positions_raw(symbol) # Ambil posisi hanya untuk simbol ini
    current_total_lots = Decimal('0.0')
    current_open_position_count = 0

    if open_positions:
        for pos in open_positions:
            current_total_lots += utils.to_decimal_or_none(pos.get('volume', '0.0'))
            current_open_position_count += 1

    # Cek MAX_TOTAL_LOTS
    if config.Trading.MAX_TOTAL_LOTS is not None and config.Trading.MAX_TOTAL_LOTS > Decimal('0.0'):
        if (current_total_lots + volume) > config.Trading.MAX_TOTAL_LOTS:
            logger.warning(f"Trade Rejected: Pembukaan posisi baru akan melebihi MAX_TOTAL_LOTS. Total lot saat ini: {float(current_total_lots):.2f}, Lot baru: {float(volume):.2f}, Batas: {float(config.Trading.MAX_TOTAL_LOTS):.2f}.")
            notification_service.notify_trade_status(
                symbol, action, volume, entry_price, stop_loss, take_profit,
                f"Trade Rejected: Max Total Lots Exceeded ({float(current_total_lots):.2f} + {float(volume):.2f} > {float(config.Trading.MAX_TOTAL_LOTS):.2f})", "Rejected"
            )
            return type('obj', (object,), {'retcode': -1, 'comment': "Max Total Lots Exceeded", 'deal': 0, 'volume': Decimal('0.0'), 'price': entry_price})()

    # Cek MAX_OPEN_POSITIONS
    if config.Trading.MAX_OPEN_POSITIONS is not None and config.Trading.MAX_OPEN_POSITIONS > 0:
        if current_open_position_count >= config.Trading.MAX_OPEN_POSITIONS:
            logger.warning(f"Trade Rejected: Pembukaan posisi baru akan melebihi MAX_OPEN_POSITIONS. Jumlah posisi saat ini: {current_open_position_count}, Batas: {config.Trading.MAX_OPEN_POSITIONS}.")
            notification_service.notify_trade_status(
                symbol, action, volume, entry_price, stop_loss, take_profit,
                f"Trade Rejected: Max Open Positions Exceeded ({current_open_position_count} >= {config.Trading.MAX_OPEN_POSITIONS})", "Rejected"
            )
            return type('obj', (object,), {'retcode': -1, 'comment': "Max Open Positions Exceeded", 'deal': 0, 'volume': Decimal('0.0'), 'price': entry_price})()
    # --- END MODIFIKASI LAMA ---

    # --- START MODIFIKASI BARU: Logika Layering/Konsolidasi ---
    # Periksa jumlah posisi sejenis yang sudah ada
    current_same_type_position_count = 0
    current_same_type_profit_pips = Decimal('0.0')
    has_same_type_position = False

    if open_positions: # Gunakan open_positions yang sudah diambil di atas
        for pos in open_positions:
            pos_type_mt5 = pos.get('type')
            
            # Tentukan tipe order MT5 dari 'action' string
            if action == "BUY":
                expected_pos_type = mt5_connector.mt5.POSITION_TYPE_BUY
            elif action == "SELL":
                expected_pos_type = mt5_connector.mt5.POSITION_TYPE_SELL
            else: # Should not happen if action is valid (BUY/SELL)
                expected_pos_type = -1

            if pos_type_mt5 == expected_pos_type:
                has_same_type_position = True
                current_same_type_position_count += 1
                
                # Hitung floating profit/loss dalam pips untuk posisi ini
                pos_price_open = utils.to_decimal_or_none(pos.get('price_open'))
                
                if pos_price_open is not None and pos_price_open != Decimal('0.0') and config.TRADING_SYMBOL_POINT_VALUE != Decimal('0.0') and config.PIP_SIZE != Decimal('0.0'):
                    # Dapatkan harga tick terkini untuk perhitungan pips yang akurat
                    current_tick_info_for_layering = mt5_connector.get_current_tick_info(symbol) # Variabel baru untuk menghindari konflik
                    current_bid_for_layering = utils.to_decimal_or_none(current_tick_info_for_layering.get('bid')) if current_tick_info_for_layering else Decimal('0.0')
                    current_ask_for_layering = utils.to_decimal_or_none(current_tick_info_for_layering.get('ask')) if current_tick_info_for_layering else Decimal('0.0')
                    
                    current_pos_pips = Decimal('0.0')
                    if expected_pos_type == mt5_connector.mt5.POSITION_TYPE_BUY and current_bid_for_layering != Decimal('0.0'):
                        current_pos_pips = (current_bid_for_layering - pos_price_open) / config.PIP_SIZE
                    elif expected_pos_type == mt5_connector.mt5.POSITION_TYPE_SELL and current_ask_for_layering != Decimal('0.0'):
                        current_pos_pips = (pos_price_open - current_ask_for_layering) / config.PIP_SIZE
                    
                    current_same_type_profit_pips += current_pos_pips
                    logger.debug(f"DEBUG Layering: Posisi {pos.get('ticket')} ({'BUY' if pos_type_mt5 == 0 else 'SELL'}) P/L Pips: {float(current_pos_pips):.2f}")
                else:
                    logger.warning(f"DEBUG Layering: Gagal menghitung pips untuk posisi {pos.get('ticket')} (data tidak valid atau PIP_SIZE/POINT_VALUE nol).")

    # Logika penolakan atau layering
    if has_same_type_position and config.Trading.MAX_LAYERS_PER_DIRECTION is not None and config.Trading.MAX_LAYERS_PER_DIRECTION > 0:
        if current_same_type_position_count >= (1 + config.Trading.MAX_LAYERS_PER_DIRECTION): # 1 posisi awal + MAX_LAYERS_PER_DIRECTION
            logger.warning(f"Trade Rejected: Pembukaan posisi baru akan melebihi MAX_LAYERS_PER_DIRECTION. Posisi sejenis saat ini: {current_same_type_position_count}, Batas: {1 + config.Trading.MAX_LAYERS_PER_DIRECTION}.")
            notification_service.notify_trade_status(
                symbol, action, volume, entry_price, stop_loss, take_profit,
                f"Trade Rejected: Max Layers Exceeded ({current_same_type_position_count} >= {1 + config.Trading.MAX_LAYERS_PER_DIRECTION})", "Rejected"
            )
            return type('obj', (object,), {'retcode': -1, 'comment': "Max Layers Exceeded", 'deal': 0, 'volume': Decimal('0.0'), 'price': entry_price})()

        # Jika belum melebihi MAX_LAYERS_PER_DIRECTION, cek MIN_PROFIT_FOR_LAYERING_PIPS
        # Asumsi: Jika ada beberapa posisi sejenis, cek total pips profit atau rata-rata pips profit.
        # Untuk simplisitas awal, kita akan cek rata-rata profit pips per posisi sejenis
        average_profit_pips_per_pos = Decimal('0.0')
        if current_same_type_position_count > 0:
            average_profit_pips_per_pos = current_same_type_profit_pips / Decimal(str(current_same_type_position_count))

        if config.Trading.MIN_PROFIT_FOR_LAYERING_PIPS is not None and average_profit_pips_per_pos < config.Trading.MIN_PROFIT_FOR_LAYERING_PIPS:
            logger.warning(f"Trade Rejected: Layering tidak diizinkan karena profit rata-rata posisi sejenis ({float(average_profit_pips_per_pos):.2f} pips) di bawah batas ({float(config.Trading.MIN_PROFIT_FOR_LAYERING_PIPS):.2f} pips).")
            notification_service.notify_trade_status(
                symbol, action, volume, entry_price, stop_loss, take_profit,
                f"Trade Rejected: Layering Min Profit Not Met ({float(average_profit_pips_per_pos):.2f} < {float(config.Trading.MIN_PROFIT_FOR_LAYERING_PIPS):.2f} pips)", "Rejected"
            )
            return type('obj', (object,), {'retcode': -1, 'comment': "Layering Min Profit Not Met", 'deal': 0, 'volume': Decimal('0.0'), 'price': entry_price})()

        # Jika lolos kedua cek di atas, sesuaikan volume jika Layering
        volume = volume * config.Trading.LAYER_VOLUME_MULTIPLIER
        logger.info(f"Layering diizinkan. Volume disesuaikan menjadi {float(volume):.2f} (multiplier: {float(config.Trading.LAYER_VOLUME_MULTIPLIER):.2f}).")
        
    elif has_same_type_position and (config.Trading.MAX_LAYERS_PER_DIRECTION == 0 or config.Trading.MAX_LAYERS_PER_DIRECTION is None):
        # Jika ada posisi sejenis dan MAX_LAYERS_PER_DIRECTION disetel 0 atau None (tidak mengizinkan layering)
        logger.warning(f"Trade Rejected: Layering tidak diizinkan (MAX_LAYERS_PER_DIRECTION disetel 0/None). Posisi sejenis sudah ada.")
        notification_service.notify_trade_status(
            symbol, action, volume, entry_price, stop_loss, take_profit,
            f"Trade Rejected: Layering Disabled (Max Layers = 0)", "Rejected"
        )
        return type('obj', (object,), {'retcode': -1, 'comment': "Layering Disabled", 'deal': 0, 'volume': Decimal('0.0'), 'price': entry_price})()
    # --- END MODIFIKASI BARU: Logika Layering/Konsolidasi ---


    trade_result = None

    # --- MODIFIKASI: Gunakan fungsi open_position generik dari mt5_connector ---
    # Dapatkan harga Bid/Ask terkini untuk menentukan harga eksekusi yang valid
    current_tick_info = mt5_connector.get_current_tick_info(symbol)
    if current_tick_info is None or current_tick_info.get('bid') is None or current_tick_info.get('ask') is None:
        logger.error(f"Tidak dapat mendapatkan tick harga terkini untuk {symbol}. Tidak dapat membuka posisi.")
        return {"retcode": -1, "comment": "No current tick price"}

    execution_price = Decimal('0.0')
    trade_type_mt5 = None
    comment_str = ""

    if action == "BUY":
        execution_price = utils.to_decimal_or_none(current_tick_info['ask']) # Gunakan harga Ask untuk BUY
        trade_type_mt5 = mt5_connector.mt5.ORDER_TYPE_BUY
        comment_str = "AI_BUY_CONSENSUS"
    elif action == "SELL":
        execution_price = utils.to_decimal_or_none(current_tick_info['bid']) # Gunakan harga Bid untuk SELL
        trade_type_mt5 = mt5_connector.mt5.ORDER_TYPE_SELL
        comment_str = "AI_SELL_CONSENSU"
    else: # Action adalah HOLD atau tidak didukung
        logger.info(f"Rekomendasi {action}. Tidak ada trade yang dieksekusi.")
        return {"retcode": 10009, "comment": f"{action} recommendation - no trade executed.", "deal": 0, "volume": Decimal('0.0'), "price": entry_price} # Mengembalikan mock result

    if execution_price is None or execution_price == Decimal('0.0'):
        logger.error(f"Harga eksekusi tidak valid ({execution_price}) untuk {action} {symbol}.")
        return {"retcode": -1, "comment": "Invalid execution price"}

    # Memanggil fungsi open_position generik
    trade_result_raw = mt5_connector.open_position(
        symbol=symbol,
        type=trade_type_mt5,
        volume=volume,
        price=execution_price, # Gunakan harga eksekusi dari tick
        slippage=slippage,
        magic=magic_number,
        comment=comment_str,
        stoploss=stop_loss,
        takeprofit=take_profit
    )
    # mt5_connector.open_position mengembalikan dictionary, jadi akses elemennya
    trade_result = {
        "retcode": trade_result_raw.get('retcode', -1),
        "comment": trade_result_raw.get('comment', 'Unknown error'),
        "deal": trade_result_raw.get('deal', 0),
        "order": trade_result_raw.get('order', 0), # Penting: order adalah ticket posisi baru
        "volume": utils.to_decimal_or_none(trade_result_raw.get('volume', Decimal('0.0'))),
        "price": utils.to_decimal_or_none(trade_result_raw.get('price', Decimal('0.0'))),
    }
    # --- AKHIR MODIFIKASI ---
    
    # === START MODIFIKASI LENGKAP UNTUK FALLBACK SL/TP ===
    if trade_result and trade_result.get('retcode') == mt5_connector.mt5.TRADE_RETCODE_DONE:
        logger.info(f"Trade {action} {float(volume):.2f} {symbol} berhasil dieksekusi. Deal: {trade_result.get('deal')}, Price: {float(trade_result.get('price')):.5f}.")

        new_position_ticket = trade_result.get('order')
        if new_position_ticket is None or new_position_ticket == 0:
            logger.error(f"Gagal mendapatkan order ID untuk posisi baru {symbol}. Partial TP management tidak akan berfungsi.")
            positions_after_open = mt5_connector.get_mt5_positions_raw(symbol) # Ambil posisi untuk simbol spesifik
            for pos in positions_after_open:
                # Coba temukan posisi yang baru dibuka berdasarkan magic number, simbol, tipe, dan waktu (dalam 30 detik terakhir)
                if pos.get('magic') == magic_number and \
                   pos.get('symbol') == symbol and \
                   pos.get('type') == trade_type_mt5 and \
                   (datetime.now(timezone.utc) - utils.to_utc_datetime_or_none(pos.get('time'))).total_seconds() < 30:
                    new_position_ticket = pos.get('ticket')
                    logger.warning(f"Berhasil fallback menemukan ticket posisi: {new_position_ticket}")
                    break
            
            if new_position_ticket is None or new_position_ticket == 0:
                logger.error("Gagal total mendapatkan ticket posisi baru. Partial TP management dinonaktifkan untuk trade ini.")


        if new_position_ticket is not None and new_position_ticket != 0:
            # --- START Logika Pemasangan SL/TP Fallback ---
            # Periksa apakah SL/TP benar-benar terpasang di request yang dikembalikan oleh broker
            # result.request.sl dan result.request.tp ini adalah yang MT5 konfirmasi sebagai terpasang
            request_obj_from_raw = trade_result_raw.get('request') #
            mt5_confirmed_sl = request_obj_from_raw.sl if request_obj_from_raw and hasattr(request_obj_from_raw, 'sl') else 0.0 #
            mt5_confirmed_tp = request_obj_from_raw.tp if request_obj_from_raw and hasattr(request_obj_from_raw, 'tp') else 0.0 #

            # Ubah ke Decimal untuk perbandingan yang akurat
            mt5_confirmed_sl_dec = Decimal(str(mt5_confirmed_sl))
            mt5_confirmed_tp_dec = Decimal(str(mt5_confirmed_tp))

            # Cek apakah SL atau TP awal yang dikirimkan bot tidak terpasang (nilai 0.0 dari MT5)
            if mt5_confirmed_sl_dec == Decimal('0.0') or mt5_confirmed_tp_dec == Decimal('0.0'):
                logger.warning(f"SL/TP tidak terpasang saat open posisi untuk ticket {new_position_ticket}. Mencoba memodifikasi posisi...")
                
                # Panggil fungsi modifikasi SL/TP
                # Gunakan nilai stop_loss dan take_profit yang DIINGINKAN (dari argumen fungsi execute_ai_trade)
                modify_result = mt5_connector.modify_position_sl_tp(
                    ticket=new_position_ticket,
                    symbol=symbol,
                    new_sl_price=stop_loss,    # Gunakan SL yang diinginkan bot
                    new_tp_price=take_profit   # Gunakan TP yang diinginkan bot
                )

                if modify_result and modify_result.get('retcode') == mt5_connector.mt5.TRADE_RETCODE_DONE:
                    logger.info(f"SL/TP berhasil dimodifikasi untuk posisi {new_position_ticket}. SL: {float(stop_loss):.5f}, TP: {float(take_profit):.5f}.")
                    # Notifikasi SL/TP berhasil dimodifikasi
                    notification_service.notify_trade_status(
                        symbol=symbol,
                        action=action,
                        volume=volume,
                        entry_price=trade_result.get('price'),
                        stop_loss=stop_loss,
                        take_profit=take_profit,
                        comment=f"SL/TP Added Post-Open",
                        status="SL/TP Modified",
                        deal_id=trade_result.get('deal'),
                        exit_price=None,
                        exit_time=None
                    )
                else:
                    logger.error(f"Gagal memodifikasi SL/TP untuk posisi {new_position_ticket}: {modify_result.get('comment', 'Unknown error')}. SL/TP posisi TIDAK TERPASANG!")
                    # Notifikasi SL/TP gagal dimodifikasi
                    notification_service.notify_trade_status(
                        symbol=symbol,
                        action=action,
                        volume=volume,
                        entry_price=trade_result.get('price'),
                        stop_loss=stop_loss,
                        take_profit=take_profit,
                        comment=f"SL/TP Modify Failed Post-Open: {modify_result.get('comment', 'Unknown')}",
                        status="SL/TP Failed",
                        deal_id=trade_result.get('deal'),
                        exit_price=None,
                        exit_time=None
                    )
            else:
                logger.info(f"SL/TP berhasil terpasang saat open posisi (dikonfirmasi MT5). SL: {float(mt5_confirmed_sl_dec):.5f}, TP: {float(mt5_confirmed_tp_dec):.5f}.")
                # Notifikasi tradeExecuted sudah mencakup ini, jadi tidak perlu notifikasi duplikat di sini.
            # --- END Logika Pemasangan SL/TP Fallback ---

            # --- START MODIFIKASI BARU: Logika Konsolidasi SL ---
            if config.Trading.CONSOLIDATE_SL_ON_LAYER:
                logger.info(f"Konsolidasi SL diaktifkan. Mengecek posisi {symbol} sejenis untuk mengkonsolidasikan SL...")
                
                all_same_type_positions = []
                total_volume_same_type = Decimal('0.0')
                
                # Mengumpulkan semua posisi sejenis (termasuk yang baru saja dibuka)
                # Dapatkan daftar posisi terbaru setelah order terakhir
                current_mt5_positions = mt5_connector.get_mt5_positions_raw(symbol)

                if current_mt5_positions:
                    for pos in current_mt5_positions:
                        pos_type_mt5 = pos.get('type')
                        # Tentukan tipe order MT5 dari 'action' string (seperti di atas)
                        if action == "BUY":
                            expected_pos_type_for_consolidation = mt5_connector.mt5.POSITION_TYPE_BUY
                        elif action == "SELL":
                            expected_pos_type_for_consolidation = mt5_connector.mt5.POSITION_TYPE_SELL
                        else:
                            expected_pos_type_for_consolidation = -1 # Should not happen

                        if pos_type_mt5 == expected_pos_type_for_consolidation:
                            all_same_type_positions.append(pos)
                            total_volume_same_type += utils.to_decimal_or_none(pos.get('volume', '0.0'))
                
                # Hanya konsolidasi jika ada lebih dari 1 posisi sejenis dan total volume > 0
                if len(all_same_type_positions) > 1 and total_volume_same_type > Decimal('0.0'):
                    # Gunakan SL dari posisi TERBARU sebagai SL konsolidasi
                    # Atau Anda bisa menghitung SL rata-rata tertimbang di sini
                    # Contoh: Weighted Average SL (lebih kompleks)
                    # weighted_sl_sum = Decimal('0.0')
                    # for pos in all_same_type_positions:
                    #     sl_val = utils.to_decimal_or_none(pos.get('sl'))
                    #     vol_val = utils.to_decimal_or_none(pos.get('volume'))
                    #     if sl_val and vol_val:
                    #         weighted_sl_sum += sl_val * vol_val
                    # new_consolidated_sl = weighted_sl_sum / total_volume_same_type if total_volume_same_type > 0 else Decimal('0.0')

                    # Untuk simplisitas dan agresivitas, kita akan menggunakan SL dari posisi terbaru
                    # Atau bisa juga menggunakan SL paling konservatif (paling jauh dari harga saat ini untuk proteksi)
                    
                    # Kita asumsikan SL dari posisi BARU (yang baru saja dibuka) adalah target SL konsolidasi
                    # Karena posisi baru ini seharusnya memiliki SL yang paling relevan dengan kondisi entri terakhir
                    target_consolidated_sl = stop_loss # stop_loss ini adalah SL yang diinginkan untuk posisi baru

                    if target_consolidated_sl is not None and target_consolidated_sl != Decimal('0.0'):
                        for pos_to_modify in all_same_type_positions:
                            # Hindari memodifikasi SL jika sudah sama atau jika posisi baru saja dibuka dengan SL yang sama
                            if utils.to_decimal_or_none(pos_to_modify.get('sl')) != target_consolidated_sl:
                                logger.debug(f"Memodifikasi SL posisi {pos_to_modify.get('ticket')} dari {float(utils.to_decimal_or_none(pos_to_modify.get('sl'))):.5f} ke {float(target_consolidated_sl):.5f}")
                                
                                modify_sl_tp_result = mt5_connector.modify_position_sl_tp(
                                    ticket=pos_to_modify.get('ticket'),
                                    symbol=symbol,
                                    new_sl_price=target_consolidated_sl,
                                    new_tp_price=utils.to_decimal_or_none(pos_to_modify.get('tp')) # Pertahankan TP posisi lama
                                )
                                if modify_sl_tp_result and modify_sl_tp_result.get('retcode') == mt5_connector.mt5.TRADE_RETCODE_DONE:
                                    logger.info(f"SL posisi {pos_to_modify.get('ticket')} berhasil dikonsolidasikan ke {float(target_consolidated_sl):.5f}.")
                                else:
                                    logger.error(f"Gagal mengkonsolidasikan SL posisi {pos_to_modify.get('ticket')}: {modify_sl_tp_result.get('comment', 'Unknown error')}.")
                    else:
                        logger.warning("Target SL konsolidasi tidak valid atau nol. Melewatkan konsolidasi SL.")
                else:
                    logger.debug("Tidak ada cukup posisi sejenis untuk mengkonsolidasikan SL atau total volume nol.")
            # --- END MODIFIKASI BARU: Logika Konsolidasi SL ---

            initial_hit_flags = [False for _ in tp_levels_config]
            
            _position_partial_tp_status[new_position_ticket] = {
                'hit_flags': initial_hit_flags,
                'tp_levels_config': tp_levels_config, # <--- Ini harus disimpan ke DB
                'open_price': entry_price,
                'initial_sl': stop_loss,
                'initial_tp1': take_profit,
                'type': trade_type_mt5
            }
            logger.debug(f"Detail TP parsial disimpan untuk posisi {new_position_ticket}.")

            # --- START MODIFIKASI: Menyimpan initial_hit_flags & tp_levels_config ke DB ---
            # Simpan status awal ke database segera setelah posisi dibuka
            database_manager.update_mt5_position_partial_tp_flags(
                ticket=new_position_ticket,
                partial_tp_hit_flags=initial_hit_flags,
                tp_levels_config=tp_levels_config # <--- Argumen BARU
            )
            # --- AKHIR MODIFIKASI ---

        else:
            logger.error("Tidak ada ticket posisi yang valid untuk menyimpan detail TP parsial.")

        # Notifikasi sukses eksekusi trade utama (ini sudah ada sebelumnya, saya tambahkan di sini untuk konteks)
        notification_service.notify_trade_status(
            symbol=symbol,
            action=action,
            volume=volume,
            entry_price=trade_result.get('price'),
            stop_loss=stop_loss, # Tetap pakai stop_loss awal, karena ini yang diinginkan bot
            take_profit=take_profit, # Tetap pakai take_profit awal
            comment=trade_result.get('comment'),
            status="Executed",
            deal_id=trade_result.get('deal'),
            exit_price=None,
            exit_time=None
        )
    else:
        logger.error(f"Trade {action} {float(volume):.2f} {symbol} GAGAL. Retcode: {trade_result.get('retcode')}, Comment: {trade_result.get('comment')}.")
        notification_service.notify_trade_status(
            symbol=symbol,
            action=action,
            volume=volume,
            entry_price=entry_price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            comment=trade_result.get('comment', 'Unknown Error'),
            status="Failed"
        )

    return trade_result
# mt5_connector.py

import MetaTrader5 as mt5
import logging
from datetime import datetime, timezone
from decimal import Decimal
import threading
import time
import sys
import pandas as pd

import utils 
from config import config

logger = logging.getLogger(__name__)

MT5_TIMEFRAMES = {
    "M1": mt5.TIMEFRAME_M1,
    "M5": mt5.TIMEFRAME_M5,
    "M15": mt5.TIMEFRAME_M15,
    "M30": mt5.TIMEFRAME_M30,
    "H1": mt5.TIMEFRAME_H1,
    "H4": mt5.TIMEFRAME_H4,
    "D1": mt5.TIMEFRAME_D1,
}

_mt5_initialized_lock = threading.Lock()
_mt5_is_initialized = False
_mt5_account_info = None
_mt5_path_configured = config.Paths.MQL5_DATA_FILE if hasattr(config.Paths, 'MQL5_DATA_FILE') else None
_mt5_login_configured = None
_mt5_password_configured = None
_mt5_server_configured = None

def initialize_mt5(path: str = None, account: int = None, password: str = None, server: str = None) -> bool:
    """
    Menginisialisasi koneksi MetaTrader 5 API.
    Memastikan ia terinisialisasi dan login jika kredensial diberikan.
    Fungsi ini TIDAK akan menggunakan mt5.is_connected() karena masalah kompatibilitas.
    """
    global _mt5_is_initialized, _mt5_account_info
    global _mt5_path_configured, _mt5_login_configured, _mt5_password_configured, _mt5_server_configured

    with _mt5_initialized_lock:
        # Jika status internal menunjukkan sudah diinisialisasi, asumsikan terkoneksi.
        if _mt5_is_initialized:
            logger.debug("MT5 sudah diinisialisasi berdasarkan status internal. Melewatkan inisialisasi ulang.")
            return True 
        
        logger.info("Mencoba menginisialisasi MT5...")

        actual_path = path or _mt5_path_configured
        
        if sys.version_info < (3, 9):
            logger.warning(f"Versi Python Anda {sys.version.split(' ')[0]}. Untuk MT5 API, disarankan Python 3.9 atau yang lebih baru.")
        
        if not actual_path:
            logger.critical("Path ke terminal MT5 belum dikonfigurasi. Tidak dapat menginisialisasi.")
            _mt5_is_initialized = False
            return False
        
        logger.info(f"Mencoba menginisialisasi MT5 dengan path: {actual_path}")
        if not mt5.initialize(path=actual_path):
            logger.error(f"Gagal menginisialisasi MT5. Error: {mt5.last_error()}", exc_info=True)
            _mt5_is_initialized = False
            return False
        
        _mt5_path_configured = actual_path

        actual_account = account or _mt5_login_configured
        actual_password = password or _mt5_password_configured
        actual_server = server or _mt5_server_configured

        if actual_account and actual_password and actual_server:
            if not mt5.login(actual_account, actual_password, actual_server):
                logger.error(f"Gagal login ke akun MT5 {actual_account}. Error: {mt5.last_error()}", exc_info=True)
                mt5.shutdown()
                _mt5_is_initialized = False
                return False
            
            _mt5_login_configured = actual_account
            _mt5_password_configured = actual_password
            _mt5_server_configured = actual_server
            
            _mt5_account_info = mt5.account_info()
            if _mt5_account_info:
                logger.info(f"MT5 Terminal initialized. Account: {_mt5_account_info.login} ({_mt5_account_info.server})")
            else:
                logger.warning(f"Gagal mendapatkan info akun setelah login. Error: {mt5.last_error()}")
        else:
            # Jika tidak ada kredensial yang diberikan, dan mt5.is_connected() tidak tersedia
            # Kita asumsikan inisialisasi berhasil jika mt5.initialize() sukses.
            # Namun, kita akan mencoba mendapatkan info akun untuk konfirmasi fungsionalitas dasar.
            _mt5_account_info = mt5.account_info()
            if _mt5_account_info:
                logger.info(f"MT5 terkoneksi (akun yang sudah ada): {_mt5_account_info.login} ({_mt5_account_info.server})")
            else:
                logger.warning(f"MT5 berhasil diinisialisasi tanpa kredensial, tetapi gagal mendapatkan info akun. Ini mungkin berarti MT5 tidak terkoneksi. Error: {mt5.last_error()}")
                # Jika tidak dapat mendapatkan info akun, kita anggap inisialisasi gagal.
                mt5.shutdown()
                _mt5_is_initialized = False
                return False


        _mt5_is_initialized = True
        logger.info("Koneksi MT5 berhasil diinisialisasi.")
        
        _initialize_symbol_point_value()
        
        return True

def print_mt5_status():
    """Mencetak status koneksi MT5 dan info akun ke log."""
    global _mt5_is_initialized, _mt5_account_info
    
    logger.info("--- STATUS KONEKSI MT5 ---")
    if _mt5_is_initialized:
        logger.info("Status Inisialisasi: Berhasil")
        if _mt5_account_info:
            logger.info(f"Akun Terkoneksi: {_mt5_account_info.login}")
            logger.info(f"Server: {_mt5_account_info.server}")
            logger.info(f"Balance: {float(_mt5_account_info.balance):.2f}")
            logger.info(f"Equity: {float(_mt5_account_info.equity):.2f}")
            logger.info(f"Profit: {float(_mt5_account_info.profit):.2f}")
        else:
            logger.warning("MT5 diinisialisasi, tapi info akun tidak tersedia (mungkin belum login atau gagal ambil info).")
    else:
        logger.warning("Status Inisialisasi: Gagal atau belum diinisialisasi.")
    logger.info("-------------------------")

def shutdown_mt5():
    """
    Mematikan koneksi MT5 API.
    Fungsi ini TIDAK akan menggunakan mt5.is_connected() untuk menghindari AttributeError.
    """
    global _mt5_is_initialized
    with _mt5_initialized_lock:
        if _mt5_is_initialized: # Jika status internal menunjukkan sudah diinisialisasi
            mt5.shutdown()
            _mt5_is_initialized = False
            logger.info("Koneksi MT5 dimatikan.")
        else:
            logger.debug("MT5 tidak diinisialisasi berdasarkan status internal, tidak perlu dimatikan.")


def _robust_mt5_call(func):
    """
    Dekorator untuk membuat panggilan MT5 API lebih tangguh terhadap error koneksi.
    Akan mencoba re-initialize MT5 jika ada error IPC.
    Dekorator ini akan mengandalkan status _mt5_is_initialized global dan error MT5.
    """
    def wrapper(*args, **kwargs):
        symbol = kwargs.get('symbol') or (args[0] if args and isinstance(args[0], str) else 'N/A_Symbol')
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                # Pastikan MT5 terinisialisasi sebelum panggilan API.
                # initialize_mt5() akan mengelola status _mt5_is_initialized secara internal.
                if not initialize_mt5(): 
                    if attempt < max_retries - 1:
                        logger.warning(f"MT5 tidak terinisialisasi (Percobaan {attempt + 1}/{max_retries}). Coba inisialisasi ulang.")
                        time.sleep(1) 
                        continue
                    else:
                        logger.error(f"Gagal menginisialisasi MT5 setelah {max_retries} percobaan. Tidak dapat melakukan panggilan API untuk {symbol}.")
                        return None 
                
                result = func(*args, **kwargs)
                
                mt5_error = mt5.last_error()
                # Jika ada error, DAN itu adalah No IPC connection
                if mt5_error[0] == -10004: 
                    logger.warning(f"MT5 API call GAGAL dengan No IPC connection untuk {symbol} (Percobaan {attempt + 1}/{max_retries}). Error: {mt5_error}. Re-inisialisasi MT5...")
                    shutdown_mt5() # Matikan koneksi yang putus
                    time.sleep(1) 
                    continue 

                return result # Jika berhasil dan tidak ada error IPC

            except Exception as e:
                logger.error(f"Error tak terduga dalam panggilan MT5 API untuk {symbol} (Percobaan {attempt + 1}/{max_retries}): {e}", exc_info=True)
                if attempt < max_retries - 1:
                    shutdown_mt5() 
                    time.sleep(1) 
                    continue 
                else:
                    logger.error(f"Panggilan MT5 API GAGAL TOTAL setelah {max_retries} percobaan untuk {symbol}.")
                    return None 

    return wrapper

# --- FUNGSI UTILITY INTERNAL ---
_SYMBOL_POINT_VALUE = None 

def _initialize_symbol_point_value():
    """
    Menginisialisasi atau memperbarui nilai point untuk simbol trading.
    Menggunakan nilai dari config.py sebagai fallback jika MT5 tidak bisa memberikan.
    Dipanggil setelah MT5 berhasil diinisialisasi.
    """
    global _SYMBOL_POINT_VALUE
    logger.debug(f"DEBUG: _initialize_symbol_point_value() dipanggil. Current _SYMBOL_POINT_VALUE: {_SYMBOL_POINT_VALUE}")

    # --- KOREKSI KRITIKAL DI SINI: Hapus mt5.is_connected() ---
    # Asumsikan MT5 sudah (atau akan) terinisialisasi dengan benar oleh initialize_mt5()
    # Panggilan ke mt5.symbol_info() akan gagal jika koneksi tidak aktif, dan ini akan ditangani oleh _robust_mt5_call
    symbol_to_check = config.TRADING_SYMBOL
    if not symbol_to_check:
        logger.error("config.TRADING_SYMBOL belum terdefinisi. Tidak dapat mengambil nilai POINT dari MT5.")
        retrieved_point_value = None
    else:
        logger.debug(f"Mengambil nilai POINT untuk simbol: {symbol_to_check}")
        symbol_info = mt5.symbol_info(symbol_to_check) # Panggilan ini akan di-robust oleh _robust_mt5_call jika diakses dari luar
        if symbol_info:
            retrieved_point_value = symbol_info.point
            logger.info(f"Nilai point untuk {symbol_to_check} diinisialisasi dari MT5: {retrieved_point_value}")
        else:
            logger.warning(f"Gagal mendapatkan info simbol untuk {symbol_to_check}. MT5 Error: {mt5.last_error()}.")
            retrieved_point_value = None
    # --- AKHIR KOREKSI KRITIKAL ---

    if _SYMBOL_POINT_VALUE is None or _SYMBOL_POINT_VALUE == Decimal('0.0'):
        if retrieved_point_value is None or retrieved_point_value == 0.0:
            _SYMBOL_POINT_VALUE = utils.to_decimal_or_none(config.TRADING_SYMBOL_POINT_VALUE)
            if _SYMBOL_POINT_VALUE is None:
                _SYMBOL_POINT_VALUE = Decimal('0.001')
                logger.critical(f"Gagal mendapatkan nilai POINT untuk {config.TRADING_SYMBOL}. Menggunakan fallback keras: {_SYMBOL_POINT_VALUE}.")
            else:
                logger.error(f"Gagal mendapatkan nilai POINT untuk {config.TRADING_SYMBOL}. Menggunakan nilai default dari config: {_SYMBOL_POINT_VALUE}.")
        else:
            _SYMBOL_POINT_VALUE = utils.to_decimal_or_none(retrieved_point_value)
            logger.info(f"Nilai POINT untuk {config.TRADING_SYMBOL} berhasil diinisialisasi: {_SYMBOL_POINT_VALUE}")

    elif _SYMBOL_POINT_VALUE == utils.to_decimal_or_none(config.TRADING_SYMBOL_POINT_VALUE) and retrieved_point_value is not None and retrieved_point_value != 0.0:
        _SYMBOL_POINT_VALUE = utils.to_decimal_or_none(retrieved_point_value)
        logger.info(f"Nilai POINT untuk {config.TRADING_SYMBOL} berhasil diperbarui: {_SYMBOL_POINT_VALUE}")

    logger.debug(f"DEBUG: _initialize_symbol_point_value() selesai. Final _SYMBOL_POINT_VALUE: {_SYMBOL_POINT_VALUE}")


# --- FUNGSI PENGAMBILAN DATA MT5 ---

@_robust_mt5_call
def get_symbol_point_value(symbol: str) -> Decimal:
    """Mengembalikan nilai point untuk simbol tertentu."""
    symbol_info = mt5.symbol_info(symbol)
    if symbol_info:
        return utils.to_decimal_or_none(symbol_info.point)
    return Decimal('0.0')


@_robust_mt5_call
def get_current_tick_info(symbol: str) -> dict:
    """
    Mengambil informasi tick terbaru untuk simbol.
    Mengembalikan dict dengan bid, ask, last, volume, dan time.
    """
    # Tidak menggunakan mt5.is_connected() di sini karena _robust_mt5_call sudah memastikan inisialisasi
    result = mt5.symbol_info_tick(symbol)
    if result:
        logger.debug(f"Raw tick from MT5: {result}")
        
        last_price_val = utils.to_decimal_or_none(result.last)
        bid_price_val = utils.to_decimal_or_none(result.bid)
        ask_price_val = utils.to_decimal_or_none(result.ask)

        if last_price_val is None or last_price_val == Decimal('0.0'):
            if bid_price_val is not None and bid_price_val != Decimal('0.0'):
                last_price_val = bid_price_val
                logger.debug(f"Tick.last adalah 0.0, menggunakan tick.bid ({float(bid_price_val):.5f}) sebagai last_price.")
            elif ask_price_val is not None and ask_price_val != Decimal('0.0'):
                last_price_val = ask_price_val
                logger.debug(f"Tick.bid dan Tick.last adalah 0.0. Menggunakan Tick.ask ({float(ask_price_val):.5f}) sebagai last_price.")
            else:
                logger.warning(f"Bid, Ask, dan Last price adalah 0.0 untuk {symbol}. Tick tidak valid.")
                return None
        
        return {
            'symbol': symbol,
            'time': utils.to_utc_datetime_or_none(result.time),
            'bid': bid_price_val,
            'ask': ask_price_val,
            'last': last_price_val, 
            'volume': utils.to_decimal_or_none(result.volume), 
            'time_msc': result.time_msc,
            'flags': result.flags,
            'volume_real': utils.to_decimal_or_none(result.volume_real) if hasattr(result, 'volume_real') else Decimal('0.0') 
        }
    logger.warning(f"Gagal mengambil tick untuk {symbol}. mt5.symbol_info_tick() mengembalikan None. MT5 last error: {mt5.last_error()}")
    return None


@_robust_mt5_call
def get_historical_candles(symbol: str, timeframe: str, num_candles: int) -> list:
    """
    Mengambil candle historis terbaru dari MT5.
    """
    # Tidak menggunakan mt5.is_connected() di sini karena _robust_mt5_call sudah memastikan inisialisasi
    result = mt5.copy_rates_from_pos(symbol, MT5_TIMEFRAMES.get(timeframe), 0, num_candles)
    if result is None or len(result) == 0:
        logger.warning(f"Gagal mendapatkan data candle historis untuk {symbol} {timeframe}. mt5.copy_rates_from_pos() mengembalikan None atau kosong. MT5 Error: {mt5.last_error()}")
        return []

    result_list = []
    for rate in result:
        result_list.append({
            'symbol': symbol, 'timeframe': timeframe,
            'open_time_utc': utils.to_utc_datetime_or_none(rate['time']),
            'open_price': utils.to_decimal_or_none(rate['open']),
            'high_price': utils.to_decimal_or_none(rate['high']),
            'low_price': utils.to_decimal_or_none(rate['low']),
            'close_price': utils.to_decimal_or_none(rate['close']),
            'tick_volume': utils.to_decimal_or_none(rate['tick_volume']),
            'spread': utils.to_decimal_or_none(rate['spread']),
            'real_volume': utils.to_decimal_or_none(rate['real_volume']) if 'real_volume' in rate else Decimal('0.0')
        })
    logger.debug(f"Berhasil mendapatkan {len(result_list)} candle {timeframe} untuk {symbol}.")
    return result_list


@_robust_mt5_call
def get_historical_candles_in_range(symbol: str, timeframe: str, start_time: datetime, end_time: datetime) -> list:
    """
    Mengambil candle historis dalam rentang tanggal tertentu dari MT5.
    """
    # Tidak menggunakan mt5.is_connected() di sini karena _robust_mt5_call sudah memastikan inisialisasi
    
    # Pastikan start_time dan end_time adalah naive datetime (MT5 prefer ini)
    if start_time.tzinfo is not None:
        start_time = start_time.astimezone(timezone.utc).replace(tzinfo=None)
    if end_time.tzinfo is not None:
        end_time = end_time.astimezone(timezone.utc).replace(tzinfo=None)

    result = mt5.copy_rates_range(symbol, MT5_TIMEFRAMES.get(timeframe), start_time, end_time)
    if result is None or len(result) == 0:
        logger.warning(f"Gagal mendapatkan data candle historis untuk {symbol} {timeframe} dari {start_time} hingga {end_time}. mt5.copy_rates_range() mengembalikan None atau kosong. MT5 Error: {mt5.last_error()}")
        return []

    result_list = []
    for rate in result:
        result_list.append({
            'symbol': symbol, 'timeframe': timeframe,
            'open_time_utc': utils.to_utc_datetime_or_none(rate['time']),
            'open_price': utils.to_decimal_or_none(rate['open']),
            'high_price': utils.to_decimal_or_none(rate['high']),
            'low_price': utils.to_decimal_or_none(rate['low']),
            'close_price': utils.to_decimal_or_none(rate['close']),
            'tick_volume': utils.to_decimal_or_none(rate['tick_volume']),
            'spread': utils.to_decimal_or_none(rate['spread']),
            'real_volume': utils.to_decimal_or_none(rate['real_volume']) if 'real_volume' in rate else Decimal('0.0')
        })
    logger.debug(f"Berhasil mendapatkan {len(result_list)} candle {timeframe} dalam rentang untuk {symbol}.")
    return result_list


@_robust_mt5_call
def get_mt5_account_info_raw() -> dict:
    """Mengambil informasi akun MT5 mentah."""
    # Tidak menggunakan mt5.is_connected() di sini karena _robust_mt5_call sudah memastikan inisialisasi
    account_info = mt5.account_info()
    if account_info is None:
        logger.error(f"(get_mt5_account_info_raw): mt5.account_info() returned None. MT5 Error: {mt5.last_error()}")
        return None

    return account_info._asdict() 


@_robust_mt5_call
def get_mt5_positions_raw(symbol: str = None) -> list:
    """Mengambil posisi terbuka MT5 mentah."""
    # Tidak menggunakan mt5.is_connected() di sini karena _robust_mt5_call sudah memastikan inisialisasi
    if symbol:
        positions = mt5.positions_get(symbol=symbol)
    else:
        positions = mt5.positions_get()

    if positions is None:
        logger.warning(f"Gagal mendapatkan posisi. Error: {mt5.last_error()}")
        return []

    return [pos._asdict() for pos in positions]


@_robust_mt5_call
def get_mt5_orders_raw(symbol: str = None) -> list:
    """Mengambil order pending MT5 mentah."""
    # Tidak menggunakan mt5.is_connected() di sini karena _robust_mt5_call sudah memastikan inisialisasi
    if symbol:
        orders = mt5.orders_get(symbol=symbol)
    else:
        orders = mt5.orders_get()

    if orders is None:
        logger.warning(f"Gagal mendapatkan order. Error: {mt5.last_error()}")
        return []

    return [order._asdict() for order in orders]


@_robust_mt5_call
def get_mt5_deals_history_raw(from_time: datetime, to_time: datetime, group: str = None) -> list:
    """Mengambil riwayat deal MT5 mentah dalam rentang waktu tertentu."""
    # Tidak menggunakan mt5.is_connected() di sini karena _robust_mt5_call sudah memastikan inisialisasi
    
    # Pastikan from_time dan to_time adalah naive datetime (MT5 prefer ini)
    if from_time.tzinfo is not None:
        from_time = from_time.astimezone(timezone.utc).replace(tzinfo=None)
    if to_time.tzinfo is not None:
        to_time = to_time.astimezone(timezone.utc).replace(tzinfo=None)

    deals = mt5.history_deals_get(from_time, to_time, group=group)
    if deals is None:
        logger.warning(f"Gagal mendapatkan riwayat deal. Error: {mt5.last_error()}")
        return []

    return [deal._asdict() for deal in deals]


@_robust_mt5_call
def open_position(symbol: str, type: int, volume: Decimal, price: Decimal, slippage: int, magic: int, comment: str = "", stoploss: Decimal = 0.0, takeprofit: Decimal = 0.0) -> dict:
    """
    Mengirim permintaan untuk membuka posisi (BUY atau SELL) ke MT5.
    Args:
        symbol (str): Simbol trading.
        type (int): Tipe order (mt5.ORDER_TYPE_BUY atau mt5.ORDER_TYPE_SELL).
        volume (Decimal): Volume lot.
        price (Decimal): Harga di mana order akan dibuka (biasanya harga Bid/Ask terkini).
        slippage (int): Toleransi slippage dalam poin.
        magic (int): Nomor Magic order.
        comment (str): Komentar order.
        stoploss (Decimal): Harga Stop Loss.
        takeprofit (Decimal): Harga Take Profit.
    Returns:
        dict: Hasil order dari MT5 (send_request).
    """
    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": float(volume), # MetaTrader5 mengharapkan float untuk volume
        "type": type,
        "price": float(price), # MetaTrader5 mengharapkan float untuk harga
        "slippage": slippage,
        "deviation": slippage, # Menggunakan slippage sebagai deviation juga
        "magic": magic,
        "comment": comment,
        "type_time": mt5.ORDER_TIME_GTC, # Good Till Cancel
        "type_filling": mt5.ORDER_FILLING_IOC, # Immediate Or Cancel,
    }

    # Tambahkan SL/TP hanya jika valid dan tidak nol
    if stoploss is not None and stoploss > 0.0 and (
        (type == mt5.ORDER_TYPE_BUY and stoploss < price) or
        (type == mt5.ORDER_TYPE_SELL and stoploss > price)
    ):
        request["stoploss"] = float(stoploss)
    elif stoploss is not None and stoploss == 0.0:
         logger.debug(f"Stoploss 0.0 dilewati untuk order {symbol} {type}.")
    else:
         logger.debug(f"Stoploss None atau invalid untuk order {symbol} {type}. Tidak ditambahkan.")

    if takeprofit is not None and takeprofit > 0.0 and (
        (type == mt5.ORDER_TYPE_BUY and takeprofit > price) or
        (type == mt5.ORDER_TYPE_SELL and takeprofit < price)
    ):
        request["takeprofit"] = float(takeprofit)
    elif takeprofit is not None and takeprofit == 0.0:
         logger.debug(f"Takeprofit 0.0 dilewati untuk order {symbol} {type}.")
    else:
         logger.debug(f"Takeprofit None atau invalid untuk order {symbol} {type}. Tidak ditambahkan.")

    logger.info(f"Mengirim order: {request}")
    result = mt5.order_send(request)

    if result is None:
        logger.error(f"Order send gagal, result is None. MT5 Error: {mt5.last_error()}")
        return {"retcode": -1, "comment": "Order send returned None"}

    logger.info(f"Order sent: {result}")
    return result._asdict() # Kembalikan sebagai dictionary

@_robust_mt5_call
def close_position(ticket: int, symbol: str, volume: Decimal) -> dict:
    """
    Mengirim permintaan untuk menutup posisi yang ada ke MT5.
    Args:
        ticket (int): Ticket posisi yang akan ditutup.
        symbol (str): Simbol posisi.
        volume (Decimal): Volume lot yang akan ditutup.
    Returns:
        dict: Hasil order penutupan dari MT5.
    """
    position = mt5.positions_get(ticket=ticket)
    if not position:
        logger.warning(f"Posisi dengan ticket {ticket} tidak ditemukan untuk ditutup.")
        return {"retcode": -1, "comment": "Position not found"}

    position_type = position[0].type
    price_current = mt5.symbol_info_tick(symbol).bid if position_type == mt5.POSITION_TYPE_BUY else mt5.symbol_info_tick(symbol).ask

    # Tentukan tipe order penutupan berdasarkan tipe posisi
    order_type = mt5.ORDER_TYPE_SELL if position_type == mt5.POSITION_TYPE_BUY else mt5.ORDER_TYPE_BUY

    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": float(volume),
        "type": order_type,
        "position": ticket, # Ticket posisi yang ditutup
        "price": float(price_current),
        "deviation": 10, # Deviation dalam poin, bisa dari config
        "magic": position[0].magic,
        "comment": "Close by Bot",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_RETURN, # Return agar partial close bisa dilakukan
    }
    logger.info(f"Mengirim permintaan penutupan posisi: {request}")
    result = mt5.order_send(request)
    if result is None:
        logger.error(f"Permintaan penutupan posisi gagal, result is None. MT5 Error: {mt5.last_error()}")
        return {"retcode": -1, "comment": "Close request returned None"}
    logger.info(f"Permintaan penutupan posisi berhasil: {result}")
    return result._asdict()

@_robust_mt5_call
def modify_position_sl_tp(ticket: int, symbol: str, new_sl_price: Decimal, new_tp_price: Decimal) -> dict:
    """
    Mengubah Stop Loss dan Take Profit untuk posisi yang ada.
    Args:
        ticket (int): Ticket posisi.
        symbol (str): Simbol posisi.
        new_sl_price (Decimal): Harga Stop Loss baru (0.0 untuk menghapus).
        new_tp_price (Decimal): Harga Take Profit baru (0.0 untuk menghapus).
    Returns:
        dict: Hasil modifikasi order dari MT5.
    """
    position = mt5.positions_get(ticket=ticket)
    if not position:
        logger.warning(f"Posisi dengan ticket {ticket} tidak ditemukan untuk dimodifikasi.")
        return {"retcode": -1, "comment": "Position not found for modification"}

    position_type = position[0].type
    current_price = mt5.symbol_info_tick(symbol).bid if position_type == mt5.POSITION_TYPE_BUY else mt5.symbol_info_tick(symbol).ask

    request = {
        "action": mt5.TRADE_ACTION_SLTP,
        "symbol": symbol,
        "position": ticket,
        "type_filling": mt5.ORDER_FILLING_RETURN,
        "sl": float(new_sl_price) if new_sl_price is not None and new_sl_price > 0 else 0.0,
        "tp": float(new_tp_price) if new_tp_price is not None and new_tp_price > 0 else 0.0,
        "magic": position[0].magic,
        "deviation": 10, # Deviation dalam poin, bisa dari config
        "comment": "SL/TP Modified by Bot"
    }
    logger.info(f"Mengirim permintaan modifikasi SL/TP: {request}")
    result = mt5.order_send(request)
    if result is None:
        logger.error(f"Permintaan modifikasi SL/TP gagal, result is None. MT5 Error: {mt5.last_error()}")
        return {"retcode": -1, "comment": "SL/TP modify request returned None"}
    logger.info(f"Permintaan modifikasi SL/TP berhasil: {result}")
    return result._asdict()
import logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal, getcontext
import pandas as pd
import json
import matplotlib.pyplot as plt
import math
import os
import decimal
import random

# Import modul yang sudah ada
import database_manager
from config import config as app_config
import mt5_connector
import ai_analyzer
import ai_consensus_manager
import rule_based_signal_generator
import aggressive_signal_generator
import utils
import numpy as np
from utils import to_float_or_none , to_iso_format_or_none, _json_default
import chart_generator

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Set presisi Decimal untuk perhitungan keuangan
getcontext().prec = 10


class SimulatedTrade:
    def __init__(self, ticket: str, symbol: str, trade_type: str, price_open: Decimal,
                 time_open: datetime, sl_price: Decimal, initial_volume: Decimal,
                 commission_per_lot: Decimal, spread_points: Decimal, slippage_points: Decimal, point_value: Decimal,
                 tp_levels_config: list,
                 pip_unit_in_dollar: Decimal,
                 hit_tolerance_points: Decimal,
                 max_hold_duration: timedelta = None
                ):
        print(f"DEBUG_PRINT_BM: SimulatedTrade Init: Ticket={ticket}, Type={trade_type}, Price_Open={float(price_open):.5f}, SL={float(sl_price) if sl_price else 'None'}, Vol={float(initial_volume):.5f}")
        logger.debug(f"DEBUG SimulatedTrade.__init__: Point_Value={float(point_value):.5f}, Pip_Unit_in_Dollar={float(pip_unit_in_dollar):.5f}, Hit_Tolerance_Points={float(hit_tolerance_points):.2f}")
        logger.debug(f"DEBUG SimulatedTrade.__init__: TP Levels Config: {tp_levels_config}")

        self.ticket = ticket
        self.symbol = symbol
        self.type = trade_type
        self.price_open = price_open
        self.time_open = time_open
        self.sl_price = sl_price
        self.original_sl_price = sl_price
        self.initial_volume = initial_volume
        self.current_volume = initial_volume

        self.commission_per_lot = commission_per_lot

        self.spread_points = spread_points
        self.slippage_points = slippage_points
        self.point_value = point_value
        self.pip_unit_in_dollar = pip_unit_in_dollar

        self.hit_tolerance_value = hit_tolerance_points * self.point_value
        logger.debug(f"DEBUG SimulatedTrade.__init__: Calculated hit_tolerance_value={float(self.hit_tolerance_value):.5f}")

        self.price_change_per_pip = self.point_value
        logger.debug(f"DEBUG SimulatedTrade.__init__: Calculated price_change_per_pip={float(self.price_change_per_pip):.5f}")

        self.entry_commission_cost = self.initial_volume * self.commission_per_lot

        self.tp_levels = {}
        risk_distance_points = Decimal('0')
        if self.sl_price is not None and self.point_value != 0:
            risk_distance_points = abs(self.price_open - self.sl_price) / self.point_value
        logger.debug(f"DEBUG SimulatedTrade.__init__: Risk Distance (points/pips based on point_value) = {float(risk_distance_points):.2f}")

        for i, tp_cfg in enumerate(tp_levels_config):
            tp_key = f'tp{i+1}'
            tp_price_calculated = None

            if 'price' in tp_cfg and tp_cfg['price'] is not None:
                tp_price_calculated = utils.to_decimal_or_none(tp_cfg['price'])
                logger.debug(f"DEBUG SimulatedTrade.__init__: TP{i+1} Price from config: {float(tp_price_calculated) if tp_price_calculated else 'N/A'}")
            else:
                if risk_distance_points > 0 and tp_cfg.get('price_multiplier') is not None:
                    target_profit_points = risk_distance_points * tp_cfg['price_multiplier']
                    logger.debug(f"DEBUG SimulatedTrade.__init__: TP{i+1} Target Profit (points) = {float(target_profit_points):.2f} (Calculated from RR)")

                    if self.type == 'buy':
                        tp_price_calculated = self.price_open + (target_profit_points * self.price_change_per_pip)
                    elif self.type == 'sell':
                        tp_price_calculated = self.price_open - (target_profit_points * self.price_change_per_pip)

            tp_price_calculated = utils.to_decimal_or_none(tp_price_calculated)

            self.tp_levels[tp_key] = {
                'price': tp_price_calculated,
                'volume_ratio': utils.to_decimal_or_none(tp_cfg.get('volume_percentage', Decimal('1.00'))),
                'hit': False,
                'move_sl_to_breakeven_after_partial': tp_cfg.get('move_sl_to_breakeven_after_partial', False),
                'move_sl_to_price_after_partial': utils.to_decimal_or_none(tp_cfg.get('move_sl_to_price_after_partial'))
            }
            print(f"DEBUG_PRINT_BM: ST Init: TP{i+1} Price: {float(tp_price_calculated) if tp_price_calculated else 'N/A'} (Vol Ratio: {float(self.tp_levels[tp_key]['volume_ratio']):.2f})")

        self.breakeven_active = False
        self.profit_from_partials = Decimal('0.0')
        self.high_water_mark = price_open
        self.max_hold_duration = max_hold_duration


    def _calculate_profit_loss_for_volume(self, close_price: Decimal, volume_to_calc: Decimal, PIP_UNIT_IN_DOLLAR: Decimal) -> Decimal:
        if close_price is None or self.price_open is None:
            logger.debug(f"DEBUG P/L Calc for {self.ticket}: Close price or Open price is None. Returning 0.0.")
            return Decimal('0.0')

        if self.type == 'buy':
            price_difference_units = (close_price - self.price_open)
        elif self.type == 'sell':
            price_difference_units = (self.price_open - close_price)

        if app_config.PIP_SIZE == Decimal('0.0'):
            logger.error("PIP_SIZE in config is zero. Cannot calculate P/L correctly.")
            return Decimal('0.0')

        pip_difference = price_difference_units / app_config.PIP_SIZE

        # Mengembalikan ke formula asli dari kode Anda, dengan asumsi:
        # self.point_value adalah unit pergerakan terkecil (misal 0.001 untuk XAUUSD jika point = $0.001 per ons)
        # self.pip_unit_in_dollar adalah nilai 1 pip dalam dolar (misal 1.00 untuk XAUUSD)
        # Dan ada asumsi 1 pip = 10 "points" (unit terkecil dari self.point_value)
        # Dan volume_to_calc harus diskalakan terhadap 0.01 lot.
        profit_loss_currency = (price_difference_units / self.point_value) * (PIP_UNIT_IN_DOLLAR / 10) * (volume_to_calc / Decimal('0.01'))

        profit_loss_currency = profit_loss_currency.quantize(Decimal('0.0000001'), rounding=decimal.ROUND_HALF_UP)

        logger.debug(f"DEBUG P/L Calc for {self.ticket}: Close {float(close_price):.5f}, Open {float(self.price_open):.5f}, Volume {float(volume_to_calc):.5f}, Price Diff Units {float(price_difference_units):.5f}, Currency PL {float(profit_loss_currency):.7f}")
        return profit_loss_currency

    def update_high_water_mark(self, candle_high: Decimal, candle_low: Decimal):
        if self.type == 'buy':
            self.high_water_mark = max(self.high_water_mark, candle_high)
        elif self.type == 'sell':
            self.high_water_mark = min(self.high_water_mark, candle_low)

    def attempt_trail_stop_loss(self, trailing_stop_pips: Decimal, trailing_stop_step_pips: Decimal, pip_value_per_point: Decimal, current_price: Decimal):
        # trailing_stop_pips dan trailing_stop_step_pips sudah dalam pips.
        # Kita perlu mengkonversinya ke unit harga menggunakan app_config.PIP_SIZE.
        trailing_stop_price_units = trailing_stop_pips * app_config.PIP_SIZE
        trailing_stop_step_price_units = trailing_stop_step_pips * app_config.PIP_SIZE

        if trailing_stop_price_units <= 0 or trailing_stop_step_price_units <= 0 or self.sl_price is None:
            return False

        current_sl = self.sl_price
        new_sl_candidate = None

        if self.type == 'buy':
            if current_price > self.price_open + trailing_stop_price_units:
                new_sl_candidate = self.high_water_mark - trailing_stop_price_units
                if new_sl_candidate > current_sl:
                    if (new_sl_candidate - current_sl) >= trailing_stop_step_price_units:
                        self.sl_price = new_sl_candidate.quantize(Decimal('0.00001'), rounding=decimal.ROUND_HALF_UP)
                        logger.debug(f"TSL updated for BUY {self.ticket} to {float(self.sl_price):.5f}")
                        return True
        elif self.type == 'sell':
            if current_price < self.price_open - trailing_stop_price_units:
                new_sl_candidate = self.high_water_mark + trailing_stop_price_units
                if new_sl_candidate < current_sl:
                    if (current_sl - new_sl_candidate) >= trailing_stop_step_price_units:
                        self.sl_price = new_sl_candidate.quantize(Decimal('0.00001'), rounding=decimal.ROUND_HALF_UP)
                        logger.debug(f"TSL updated for SELL {self.ticket} to {float(self.sl_price):.5f}")
                        return True
        return False

    def attempt_breakeven(self):
        if self.breakeven_active or self.sl_price is None: return False

        move_to_be = False
        if self.type == 'buy':
            if self.high_water_mark >= self.price_open:
                 move_to_be = True
        elif self.type == 'sell':
            if self.high_water_mark <= self.price_open:
                move_to_be = True

        if move_to_be:
            if (self.type == 'buy' and self.sl_price < self.price_open) or \
               (self.type == 'sell' and self.sl_price > self.price_open):
                self.sl_price = self.price_open.quantize(Decimal('0.00001'), rounding=decimal.ROUND_HALF_UP)
                self.breakeven_active = True
                logger.debug(f"SL for {self.ticket} moved to BE ({float(self.sl_price):.5f}).")
                return True
        return False

    def check_and_execute_tp_levels(self, candle_high: Decimal, candle_low: Decimal, pip_unit_in_dollar: Decimal) -> tuple[str | None, Decimal | None]:
        # Iterate through TP levels in order, typically tp1, then tp2, etc.
        for tp_key_order in ['tp1', 'tp2', 'tp3']:
            tp_info = self.tp_levels.get(tp_key_order)

            if tp_info and tp_info['price'] is not None and not tp_info['hit'] and self.current_volume > Decimal('0.0000001'):
                logger.debug(f"DEBUG ST_TP: Checking {self.ticket} {tp_key_order}. TP Price: {float(tp_info['price']):.5f}, Current Vol: {float(self.current_volume):.5f}")
                logger.debug(f"DEBUG ST_TP: Candle H: {float(candle_high):.5f}, L: {float(candle_low):.5f}, SL: {float(self.sl_price) if self.sl_price else 'N/A'}. Hit Tol: {float(self.hit_tolerance_value):.5f}")

                tp_hit_condition = False
                if self.type == 'buy':
                    if candle_high >= (tp_info['price'] - self.hit_tolerance_value):
                        tp_hit_condition = True
                elif self.type == 'sell':
                    if candle_low <= (tp_info['price'] + self.hit_tolerance_value):
                        tp_hit_condition = True

                if tp_hit_condition:
                    print(f"DEBUG_PRINT_BM: ST_TP: {self.ticket} {tp_key_order} HIT CONDITION TRUE. Price: {float(tp_info['price']):.5f}")

                    volume_to_close = self.initial_volume * tp_info['volume_ratio']

                    if volume_to_close > self.current_volume:
                        volume_to_close = self.current_volume

                    if volume_to_close > Decimal('0.0000001'):
                        profit_partial_gross = self._calculate_profit_loss_for_volume(tp_info['price'], volume_to_close, pip_unit_in_dollar)
                        commission_partial_exit = volume_to_close * self.commission_per_lot
                        self.profit_from_partials += (profit_partial_gross - commission_partial_exit)
                        self.current_volume -= volume_to_close
                        self.profit_from_partials = self.profit_from_partials.quantize(Decimal('0.0000001'), rounding=decimal.ROUND_HALF_UP)
                        self.current_volume = self.current_volume.quantize(Decimal('0.0000001'), rounding=decimal.ROUND_HALF_UP)
                        print(f"DEBUG_PRINT_BM: ST_TP: {self.ticket} {tp_key_order} PARTIAL CLOSURE. P/L:{float(profit_partial_gross):.7f}, Comm:{float(commission_partial_exit):.2f}, New Vol:{float(self.current_volume):.7f}")
                    else:
                        print(f"DEBUG_PRINT_BM: ST_TP: {self.ticket} {tp_key_order} HIT, NO VOLUME CLOSED (likely SL adjustment trigger).")

                    tp_info['hit'] = True

                    if tp_info['move_sl_to_breakeven_after_partial'] and not self.breakeven_active:
                        print(f"DEBUG_PRINT_BM: ST_TP: Attempting breakeven for {self.ticket} after {tp_key_order}.")
                        self.attempt_breakeven()
                    elif tp_info['move_sl_to_price_after_partial'] is not None:
                        target_sl_price = tp_info['move_sl_to_price_after_partial']
                        if (self.type == 'buy' and target_sl_price > self.sl_price) or \
                           (self.type == 'sell' and target_sl_price < self.sl_price):
                            self.sl_price = target_sl_price.quantize(Decimal('0.00001'), rounding=decimal.ROUND_HALF_UP)
                            print(f"DEBUG_PRINT_BM: ST_TP: SL for {self.ticket} moved to specific price {float(target_sl_price):.5f} after {tp_key_order}.")

                    if self.current_volume <= Decimal('0.0000001'):
                        print(f"DEBUG_PRINT_BM: ST_TP: {self.ticket} CLOSED_FULLY by {tp_key_order}. Final Vol: {float(self.current_volume):.7f}")
                        return 'CLOSED_FULLY', tp_info['price']

                    print(f"DEBUG_PRINT_BM: ST_TP: {self.ticket} {tp_key_order} hit, position still open. Remaining Vol: {float(self.current_volume):.7f}")
                    return tp_key_order, tp_info['price']
        return None, None

    def check_sl_hit(self, candle_high: Decimal, candle_low: Decimal):
        if self.sl_price is None:
            return None

        logger.debug(f"DEBUG SL Check for {self.ticket}: "
                     f"Type: {self.type}, SL Price: {float(self.sl_price):.5f}, "
                     f"Hit Tolerance Value: {float(self.hit_tolerance_value):.5f}, "
                     f"Candle H: {float(candle_high):.5f}, L: {float(candle_low):.5f}.")

        sl_hit_condition = False
        if self.type == 'buy':
            if candle_low <= (self.sl_price + self.hit_tolerance_value):
                sl_hit_condition = True
        elif self.type == 'sell':
            if candle_high >= (self.sl_price - self.hit_tolerance_value):
                sl_hit_condition = True

        if sl_hit_condition:
            print(f"DEBUG_PRINT_BM: ST_SL: {self.ticket} SL HIT CONDITION TRUE. Price: {float(self.sl_price):.5f}")
            return self.sl_price
        return None


class BacktestManager:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.initial_balance = Decimal('10000.00')

        self.commission_per_lot = app_config.Trading.COMMISSION_PER_LOT
        self.swap_rate_buy_per_day = app_config.Trading.SWAP_RATE_PER_LOT_BUY
        self.swap_rate_sell_per_day = app_config.Trading.SWAP_RATE_PER_LOT_SELL

        # --- MODIFIKASI: Pastikan PIP_SIZE juga diinisialisasi di __init__ ---
        self.pip_size = app_config.PIP_SIZE # Mengambil PIP_SIZE dari root Config
        # --- AKHIR MODIFIKASI ---

        self.point_value = app_config.TRADING_SYMBOL_POINT_VALUE
        self.PIP_UNIT_IN_DOLLAR = app_config.PIP_UNIT_IN_DOLLAR
        logger.debug(f"DEBUG BM: __init__ point_value={self.point_value}, PIP_UNIT_IN_DOLLAR={self.PIP_UNIT_IN_DOLLAR}, PIP_SIZE={self.pip_size}")

        self.trailing_stop_pips = Decimal(str(app_config.Trading.TRAILING_STOP_PIPS))
        self.trailing_stop_step_pips = Decimal(str(app_config.Trading.TRAILING_STOP_STEP_PIPS))
        self.backtest_hit_tolerance_points = app_config.Trading.BACKTEST_HIT_TOLERANCE_POINTS

        self.min_spread_points = Decimal(str(app_config.Trading.MIN_SPREAD_POINTS))
        self.max_spread_points = Decimal(str(app_config.Trading.MAX_SPREAD_POINTS))
        self.min_slippage_points = Decimal(str(app_config.Trading.MIN_SLIPPAGE_POINTS))
        self.max_slippage_points = Decimal(str(app_config.Trading.MAX_SLIPPAGE_POINTS))

        self.risk_percent_per_trade = Decimal(str(app_config.Trading.RISK_PERCENT_PER_TRADE))
        self.max_position_hold_time = timedelta(minutes=app_config.Trading.MAX_POSITION_HOLD_MINUTES)

        self.balance_history = []
        self.closed_trade_profits_losses = []
        self.simulated_trade_events = []
        self.open_positions: dict[str, SimulatedTrade] = {}

    def _calculate_total_pnl_for_trade(self, trade: SimulatedTrade, close_price: Decimal) -> Decimal:
        profit_from_remaining_volume = trade._calculate_profit_loss_for_volume(
            close_price, trade.current_volume, self.PIP_UNIT_IN_DOLLAR
        )

        exit_commission_cost = trade.current_volume * trade.commission_per_lot

        final_pnl_total = profit_from_remaining_volume + trade.profit_from_partials - trade.entry_commission_cost - exit_commission_cost
        return final_pnl_total.quantize(Decimal('0.0000001'), rounding=decimal.ROUND_HALF_UP)

    def _simulate_trade_execution(self, current_candle: dict, signal_action: str,
                                  entry_price_sugg: Decimal, sl_sugg: Decimal,
                                  tp_levels_list_from_signal: list,
                                  current_balance: Decimal):
        print(f"DEBUG_PRINT_BM: _simulate_trade_execution: Sig Action='{signal_action}', Sugg Entry={float(entry_price_sugg) if entry_price_sugg else 'None'}.")

        simulated_entry_price = utils.to_decimal_or_none(entry_price_sugg if entry_price_sugg is not None else current_candle['open_price'])
        simulated_entry_time = current_candle['open_time_utc']

        if simulated_entry_price is None:
            print(f"DEBUG_PRINT_BM: _simulate_trade_execution: simulated_entry_price is None. Cannot proceed.")
            return None

        sl_sugg_dec = utils.to_decimal_or_none(sl_sugg)

        actual_spread_points = Decimal(str(random.uniform(float(self.min_spread_points), float(self.max_spread_points))))
        actual_slippage_points = Decimal(str(random.uniform(float(self.min_slippage_points), float(self.max_slippage_points))))

        spread_price_impact = actual_spread_points * self.point_value
        slippage_price_impact = actual_slippage_points * self.point_value

        actual_entry_price = simulated_entry_price
        position_type = None

        if signal_action == "BUY":
            actual_entry_price = simulated_entry_price + spread_price_impact + slippage_price_impact
            position_type = "buy"
        elif signal_action == "SELL":
            actual_entry_price = simulated_entry_price - spread_price_impact - slippage_price_impact
            position_type = "sell"
        else:
            print(f"DEBUG_PRINT_BM: _simulate_trade_execution: signal_action '{signal_action}' is not BUY/SELL. Returning None.")
            return None

        print(f"DEBUG_PRINT_BM: _simulate_trade_execution: Actual Entry Price: {float(actual_entry_price):.5f} (Spread: {float(actual_spread_points):.2f} pts, Slippage: {float(actual_slippage_points):.2f} pts)")

        adjusted_sl_price = sl_sugg_dec

        if sl_sugg_dec is not None and simulated_entry_price is not None:
            if signal_action == "BUY":
                sl_distance_from_signal_entry = simulated_entry_price - sl_sugg_dec
                adjusted_sl_price = actual_entry_price - sl_distance_from_signal_entry
            elif signal_action == "SELL":
                sl_distance_from_signal_entry = sl_sugg_dec - simulated_entry_price
                adjusted_sl_price = actual_entry_price + sl_distance_from_signal_entry
            logger.debug(f"DEBUG BM:_simulate_trade_execution: adjusted_sl_price={float(adjusted_sl_price) if adjusted_sl_price else 'None'}")

        tp_levels_for_sim_trade = tp_levels_list_from_signal

        total_volume = Decimal('0.0')
        if adjusted_sl_price is not None and self.point_value > Decimal('0.0'):
            risk_per_trade_amount = current_balance * self.risk_percent_per_trade

            if app_config.PIP_SIZE == Decimal('0.0') or app_config.PIP_UNIT_IN_DOLLAR == Decimal('0.0'):
                print(f"DEBUG_PRINT_BM: PIP_SIZE atau PIP_UNIT_IN_DOLLAR adalah nol. Tidak dapat menghitung volume berbasis risiko.")
                return None

            risk_pips_from_sl = abs(actual_entry_price - adjusted_sl_price) / app_config.PIP_SIZE

            if risk_pips_from_sl > Decimal('0.0'):
                volume_from_risk = risk_per_trade_amount / (risk_pips_from_sl * app_config.PIP_UNIT_IN_DOLLAR)

                min_lot_size = Decimal('0.01')
                total_volume = max(min_lot_size, volume_from_risk).quantize(Decimal('0.01'), rounding=decimal.ROUND_DOWN)

                logger.debug(f"DEBUG BM:_simulate_trade_execution: Calculated volume from risk: {float(total_volume):.2f} (Risk Amount: {float(risk_per_trade_amount):.2f}, Risk Pips: {float(risk_pips_from_sl):.2f})")
            else:
                print(f"DEBUG_PRINT_BM: _simulate_trade_execution: Risk pips for volume calculation is zero. Cannot open trade.")
                return None
        else:
            print(f"DEBUG_PRINT_BM: _simulate_trade_execution: Adjusted SL Price is None/Zero or Point Value is Zero. Using fixed volume from app_config.")
            total_volume = app_config.Trading.AUTO_TRADE_VOLUME

        if total_volume <= Decimal('0.0'):
            print(f"DEBUG_PRINT_BM: _simulate_trade_execution: Calculated volume is zero or negative. Cannot open trade.")
            return None


        simulated_position = SimulatedTrade(
            ticket=f"SIM_{simulated_entry_time.timestamp()}_{signal_action}",
            symbol=self.symbol,
            trade_type=position_type,
            price_open=actual_entry_price,
            time_open=simulated_entry_time,
            sl_price=adjusted_sl_price,
            tp_levels_config=tp_levels_for_sim_trade,
            pip_unit_in_dollar=self.PIP_UNIT_IN_DOLLAR,
            hit_tolerance_points=self.backtest_hit_tolerance_points,
            initial_volume=total_volume,
            commission_per_lot=self.commission_per_lot,
            spread_points=actual_spread_points,
            slippage_points=actual_slippage_points,
            point_value=self.point_value,
            max_hold_duration=self.max_position_hold_time
        )

        self.simulated_trade_events.append({
            'ticket': simulated_position.ticket,
            'type': simulated_position.type,
            'entry_time': simulated_position.time_open,
            'entry_price': simulated_position.price_open,
            'initial_sl': simulated_position.original_sl_price,
            'initial_tp1': tp_levels_for_sim_trade[0]['price'] if tp_levels_for_sim_trade and len(tp_levels_for_sim_trade) > 0 else None,
            'volume': simulated_position.initial_volume,
            'exit_time': None,
            'exit_price': None,
            'pnl': None,
            'exit_reason': None
        })
        print(f"DEBUG_PRINT_BM: _simulate_trade_execution: SimulatedTrade created. Ticket: {simulated_position.ticket}. Appended to simulated_trade_events.")
        print(f"DEBUG_PRINT_BM: _simulate_trade_execution: About to return simulated_position: {simulated_position.ticket}")

        return simulated_position

    def run_backtest(self, start_date: datetime, end_date: datetime,
                     signal_generator_type: str = "aggressive",
                     plot_results: bool = True):
        strategy_name = "Aggressive Strategy" if signal_generator_type == "aggressive" else "Rule-Based Strategy"
        logger.info(f"Memulai backtest untuk {self.symbol} menggunakan {strategy_name} dari {start_date} hingga {end_date}.")
        logger.info(f"DEBUG Backtest: plot_results parameter received: {plot_results}")

        smallest_tf = "M5"

        logger.info(f"Meminta historical candles untuk {smallest_tf} dari DB dari {start_date} sampai {end_date}...")

        all_candles_raw_data = database_manager.get_historical_candles_from_db(
            symbol=self.symbol,
            timeframe=smallest_tf,
            start_time_utc=start_date,
            end_time_utc=end_date,
            limit=None,
            order_asc=True
        )
        if not all_candles_raw_data:
            logger.error(f"Tidak ada data candle {smallest_tf} yang ditemukan untuk periode backtest {start_date} - {end_date}.")
            return {
                "status": "failed",
                "message": "Tidak ada data historis yang cukup."
            }

        logger.info(f"DEBUG BACKTEST: Berhasil mendapatkan {len(all_candles_raw_data)} candles untuk backtest. "
                    f"Waktu candle pertama: {all_candles_raw_data[0]['open_time_utc']}. "
                    f"Waktu candle terakhir: {all_candles_raw_data[-1]['open_time_utc']}.")

        df_candles = pd.DataFrame(all_candles_raw_data)
        df_candles['open_time_utc'] = pd.to_datetime(df_candles['open_time_utc'])
        df_candles.set_index('open_time_utc', inplace=True)
        df_candles.sort_index(inplace=True)

        for col in ['open_price', 'high_price', 'low_price', 'close_price', 'tick_volume', 'spread', 'real_volume']:
            if col in df_candles.columns:
                df_candles[col] = df_candles[col].apply(lambda x: utils.to_decimal_or_none(x) if x is not None else Decimal('0.0')).fillna(Decimal('0.0'))

        initial_df_len = len(df_candles)
        df_candles.dropna(subset=['open_price', 'high_price', 'low_price', 'close_price'], inplace=True)
        if len(df_candles) < initial_df_len:
            logger.warning(f"BACKTEST: Dihapus {initial_df_len - len(df_candles)} baris dengan NaN di kolom OHLC.")

        if df_candles.empty:
            logger.error("BACKTEST: DataFrame kosong setelah pembersihan NaN. Backtest dibatalkan.")
            return {
                "status": "failed",
                "message": "DataFrame kosong setelah pembersihan data OHLC atau tidak ada data valid."
            }

        current_balance = self.initial_balance
        self.balance_history = []
        self.balance_history.append((start_date, current_balance))

        self.closed_trade_profits_losses = []
        self.simulated_trade_events = []
        self.open_positions: dict[str, SimulatedTrade] = {}

        local_total_trades = 0
        local_winning_trades = 0
        local_losing_trades = 0
        local_gross_profit = Decimal('0.0')
        local_gross_loss = Decimal('0.0')

        local_max_drawdown = Decimal('0.0')
        local_peak_balance = self.initial_balance


        local_sl_hit_count = 0
        local_be_hit_count = 0
        local_tp1_hit_count = 0
        local_tp2_hit_count = 0
        local_tp3_hit_count = 0
        local_tsl_hit_count = 0
        local_closed_by_opposite_signal_count = 0
        local_closed_by_time_limit_count = 0

        total_long_trades = 0
        total_short_trades = 0
        consecutive_wins = 0
        consecutive_losses = 0
        max_consecutive_wins = 0
        max_consecutive_losses = 0
        largest_win = Decimal('0.0')
        largest_loss = Decimal('0.0')

        logger.info(f"DEBUG BACKTEST: Memulai iterasi melalui {len(df_candles)} candles untuk backtest...")

        total_iterations = len(df_candles)
        progress_interval = max(1, total_iterations // 10)

        logger.debug(f"DEBUG BM: Mulai run_backtest. Config: MIN_SL_PIPS={float(app_config.Trading.MIN_SL_PIPS):.2f}, MIN_TP_PIPS={float(app_config.Trading.MIN_TP_PIPS):.2f}")
        logger.debug(f"DEBUG BM: Initial df_candles length: {len(df_candles)}")
        if not df_candles.empty:
            logger.debug(f"DEBUG BM: First candle: {df_candles.index[0]}, Close: {df_candles['close_price'].iloc[0]}")

        current_signal_data = None

        for i in range(len(df_candles)):
            candle_series = df_candles.iloc[i]
            current_candle = candle_series.to_dict()
            current_candle['open_time_utc'] = df_candles.index[i]

            current_time_for_signal = current_candle['open_time_utc']

            current_price_for_signal_dec = utils.to_decimal_or_none(current_candle['close_price'])

            print(f"DEBUG_PRINT_BM: === Candle {i+1}/{len(df_candles)} at {current_time_for_signal} ===")
            print(f"DEBUG_PRINT_BM: Candle Close Price (RAW): {current_candle.get('close_price')}")
            print(f"DEBUG_PRINT_BM: Converted Close Price (Decimal): {float(current_price_for_signal_dec) if current_price_for_signal_dec is not None else 'None'}")

            if current_price_for_signal_dec is None:
                print(f"DEBUG_PRINT_BM: Harga penutupan lilin TIDAK VALID ({current_candle['close_price']}) di {current_time_for_signal}. Melewatkan perhitungan sinyal untuk lilin ini.")
                current_signal_data = None
                self.balance_history.append((current_time_for_signal, current_balance))
                continue

            candle_open = current_candle['open_price']
            candle_high = current_candle['high_price']
            candle_low = current_candle['low_price']
            candle_close = current_candle['close_price']

            if (i + 1) % progress_interval == 0 or (i + 1) == total_iterations:
                logger.info(f"Backtest Progress: {((i + 1) / total_iterations * 100):.1f}% selesai. "
                            f"Current Candle Time: {current_time_for_signal.strftime('%Y-%m-%d %H:%M')}. "
                            f"Open Positions: {len(self.open_positions)}. Balance: {float(current_balance):.7f}")

            # Apply swap costs for all open positions
            seconds_per_candle = Decimal(str(utils.timeframe_to_seconds(smallest_tf)))
            hours_per_candle = seconds_per_candle / Decimal('3600')
            fraction_of_day_per_candle = hours_per_candle / Decimal('24')

            for ticket in list(self.open_positions.keys()):
                position = self.open_positions[ticket]
                swap_cost = Decimal('0.0')
                if position.type == 'buy':
                    swap_cost = self.swap_rate_buy_per_day * position.current_volume * fraction_of_day_per_candle / Decimal('0.01')
                elif position.type == 'sell':
                    swap_cost = self.swap_rate_sell_per_day * position.current_volume * fraction_of_day_per_candle / Decimal('0.01')

                if swap_cost != Decimal('0.0'):
                    current_balance -= swap_cost
                    logger.debug(f"DEBUG BM: Swap cost applied for {position.ticket}: {float(swap_cost):.7f}. New Balance: {float(current_balance):.7f}")

                # Update high water mark and attempt TSL/BE for all open positions
                position.update_high_water_mark(candle_high, candle_low)
                position.attempt_trail_stop_loss(
                    self.trailing_stop_pips, self.trailing_stop_step_pips,
                    self.point_value,
                    candle_close
                )

                # Check for TP hits for all open positions
                tp_hit_status, tp_hit_price_at_close = position.check_and_execute_tp_levels(candle_high, candle_low, self.PIP_UNIT_IN_DOLLAR)
                if tp_hit_status:
                    exit_price_for_log = tp_hit_price_at_close
                    exit_reason_for_log = f"TP {tp_hit_status.upper().replace('_FULLY', '')}"

                    if tp_hit_status == 'CLOSED_FULLY':
                        final_pnl_for_trade = self._calculate_total_pnl_for_trade(position, tp_hit_price_at_close)
                        current_balance += final_pnl_for_trade
                        local_total_trades, local_winning_trades, local_losing_trades, local_gross_profit, local_gross_loss, \
                        consecutive_wins, consecutive_losses, largest_win, largest_loss, max_consecutive_wins, max_consecutive_losses = \
                            self._record_closed_trade_metrics(final_pnl_for_trade, current_time_for_signal, position.ticket, exit_price_for_log, exit_reason_for_log, True, local_total_trades, local_winning_trades, local_losing_trades, local_gross_profit, local_gross_loss, consecutive_wins, consecutive_losses, largest_win, largest_loss, max_consecutive_wins, max_consecutive_losses)
                        if 'tp1' in tp_hit_status: local_tp1_hit_count += 1
                        elif 'tp2' in tp_hit_status: local_tp2_hit_count += 1
                        elif 'tp3' in tp_hit_status: local_tp3_hit_count += 1
                        del self.open_positions[ticket]
                        logger.debug(f"DEBUG BM: Posisi {position.type} ({position.ticket}) ditutup HABIS (TP). P/L: {float(final_pnl_for_trade):.7f}. Saldo: {float(current_balance):.7f}")
                    else:
                        if tp_hit_status == 'tp1': local_tp1_hit_count += 1
                        elif tp_hit_status == 'tp2': local_tp2_hit_count += 1
                        elif tp_hit_status == 'tp3': local_tp3_hit_count += 1

                # Check for SL hits for all open positions (if not fully closed by TP)
                if ticket in self.open_positions:
                    sl_hit_price = position.check_sl_hit(candle_high, candle_low)
                    if sl_hit_price is not None:
                        exit_price_for_log = sl_hit_price
                        exit_reason_for_log = ""
                        if position.breakeven_active:
                            exit_reason_for_log = "BE Hit"
                            local_be_hit_count += 1
                        elif position.sl_price != position.original_sl_price:
                            exit_reason_for_log = "TSL Hit"
                            local_tsl_hit_count += 1
                        else:
                            exit_reason_for_log = "SL Hit"
                            local_sl_hit_count += 1

                        final_pnl_for_trade = self._calculate_total_pnl_for_trade(position, sl_hit_price)
                        current_balance += final_pnl_for_trade
                        local_total_trades, local_winning_trades, local_losing_trades, local_gross_profit, local_gross_loss, \
                        consecutive_wins, consecutive_losses, largest_win, largest_loss, max_consecutive_wins, max_consecutive_losses = \
                            self._record_closed_trade_metrics(final_pnl_for_trade, current_time_for_signal, position.ticket, exit_price_for_log, exit_reason_for_log, True, local_total_trades, local_winning_trades, local_losing_trades, local_gross_profit, local_gross_loss, consecutive_wins, consecutive_losses, largest_win, largest_loss, max_consecutive_wins, max_consecutive_losses)
                        del self.open_positions[ticket]
                        logger.debug(f"DEBUG BM: Posisi {position.type} ({position.ticket}) ditutup (oleh {exit_reason_for_log}). P/L: {float(final_pnl_for_trade):.7f}. Saldo: {float(current_balance):.7f}")

                # Check for max hold duration
                if ticket in self.open_positions and position.max_hold_duration is not None:
                    if (current_time_for_signal - position.time_open) >= position.max_hold_duration:
                        print(f"DEBUG_PRINT_BM: Posisi {position.ticket} ditutup karena melewati batas waktu {position.max_hold_duration}.")
                        exit_price_for_log = current_candle['close_price']
                        exit_reason_for_log = "Time Limit"
                        final_pnl_for_trade = self._calculate_total_pnl_for_trade(position, exit_price_for_log)
                        current_balance += final_pnl_for_trade
                        local_total_trades, local_winning_trades, local_losing_trades, local_gross_profit, local_gross_loss, \
                        consecutive_wins, consecutive_losses, largest_win, largest_loss, max_consecutive_wins, max_consecutive_losses = \
                            self._record_closed_trade_metrics(final_pnl_for_trade, current_time_for_signal, position.ticket, exit_price_for_log, exit_reason_for_log, True, local_total_trades, local_winning_trades, local_losing_trades, local_gross_profit, local_gross_loss, consecutive_wins, consecutive_losses, largest_win, largest_loss, max_consecutive_wins, max_consecutive_losses)
                        del self.open_positions[ticket]
                        local_closed_by_time_limit_count += 1
                        logger.debug(f"DEBUG BM: Posisi {position.type} ({position.ticket}) ditutup oleh batas waktu. P/L: {float(final_pnl_for_trade):.7f}. Saldo: {float(current_balance):.7f}")


            # Generate signal after processing open positions
            if signal_generator_type == "aggressive":
                current_signal_data = aggressive_signal_generator.generate_signal(
                    self.symbol, current_time_for_signal, current_price_for_signal_dec
                )
            elif signal_generator_type == "rule_based":
                current_signal_data = rule_based_signal_generator.generate_signal(
                    self.symbol, current_time_for_signal, current_price_for_signal_dec
                )

            action = current_signal_data.get('action', 'HOLD') if current_signal_data else 'HOLD'
            print(f"DEBUG_PRINT_BM: Sinyal dari generator untuk {current_time_for_signal}: {action}. Posisi Terbuka: {len(self.open_positions)}.")

            # Check for opposite signal and close existing positions
            if action in ["BUY", "SELL"]:
                for ticket in list(self.open_positions.keys()):
                    position = self.open_positions[ticket]
                    if (position.type == 'buy' and action == 'SELL') or \
                       (position.type == 'sell' and action == 'BUY'):

                        print(f"DEBUG_PRINT_BM: Sinyal berlawanan {action} untuk posisi {position.type} ({position.ticket}). Menutup posisi.")
                        exit_price_for_log = current_candle['close_price']
                        exit_reason_for_log = "Opposite Signal"

                        final_pnl_for_trade = self._calculate_total_pnl_for_trade(position, exit_price_for_log)
                        current_balance += final_pnl_for_trade
                        local_total_trades, local_winning_trades, local_losing_trades, local_gross_profit, local_gross_loss, \
                        consecutive_wins, consecutive_losses, largest_win, largest_loss, max_consecutive_wins, max_consecutive_losses = \
                            self._record_closed_trade_metrics(final_pnl_for_trade, current_time_for_signal, position.ticket, exit_price_for_log, exit_reason_for_log, True, local_total_trades, local_winning_trades, local_losing_trades, local_gross_profit, local_gross_loss, consecutive_wins, consecutive_losses, largest_win, largest_loss, max_consecutive_wins, max_consecutive_losses)
                        del self.open_positions[ticket]
                        local_closed_by_opposite_signal_count += 1
                        logger.debug(f"DEBUG BM: Posisi {position.type} ({position.ticket}) ditutup penuh oleh sinyal berlawanan {action}. P/L: {float(final_pnl_for_trade):.7f}. Saldo: {float(current_balance):.7f}")

            # Attempt to open a new position if no existing position (or if multiple positions are allowed)
            # and signal is BUY/SELL
            if action in ["BUY", "SELL"]:
                # --- MODIFIKASI: Cara pengambilan SL dan TP levels dari sinyal ---
                sl_sugg = utils.to_decimal_or_none(current_signal_data.get('stop_loss_suggestion'))

                # Ambil daftar lengkap TP levels dari sinyal
                tp_levels_list_from_signal = current_signal_data.get('take_profit_levels_suggestion')

                # Dapatkan TP1 harga dari elemen pertama dalam daftar (untuk validasi dan logging)
                tp1_sugg = None
                if tp_levels_list_from_signal and len(tp_levels_list_from_signal) > 0:
                    tp1_sugg = utils.to_decimal_or_none(tp_levels_list_from_signal[0].get('price'))
                # --- AKHIR MODIFIKASI ---

                print(f"DEBUG_PRINT_BM: Sinyal konversi Decimal: sl_sugg={float(sl_sugg) if sl_sugg else 'None'}, tp1_sugg={float(tp1_sugg) if tp1_sugg else 'None'}. Current Price for Signal: {float(current_price_for_signal_dec):.5f}")

                min_sl_pips_dec = app_config.Trading.MIN_SL_PIPS
                min_tp_pips_dec = app_config.Trading.MIN_TP_PIPS

                trade_rejection_reason = ""
                can_attempt_trade_execution = False

                print(f"DEBUG_PRINT_BM: Konfigurasi validasi: MIN_SL_PIPS={float(min_sl_pips_dec):.2f}, MIN_TP_PIPS={float(min_tp_pips_dec):.2f}.")

                if sl_sugg is None or tp1_sugg is None:
                    trade_rejection_reason = "Sinyal tanpa SL atau TP1 valid."
                    print(f"DEBUG_PRINT_BM: TIDAK MEMBUKA POSISI: {trade_rejection_reason} (SL/TP None).")
                elif (action == "BUY" and sl_sugg >= current_price_for_signal_dec) or \
                     (action == "SELL" and sl_sugg <= current_price_for_signal_dec):
                    trade_rejection_reason = "SL invalid (di sisi yang salah)."
                    print(f"DEBUG_PRINT_BM: TIDAK MEMBUKA POSISI: {trade_rejection_reason} (SL salah sisi).")
                elif (action == "BUY" and tp1_sugg <= current_price_for_signal_dec) or \
                     (action == "SELL" and tp1_sugg >= current_price_for_signal_dec):
                    trade_rejection_reason = "TP1 invalid (di sisi yang salah)."
                    print(f"DEBUG_PRINT_BM: TIDAK MEMBUKA POSISI: {trade_rejection_reason} (TP1 salah sisi).")
                else:
                    # Validasi jarak SL/TP menggunakan self.pip_size
                    if self.pip_size == Decimal('0.0'):
                        trade_rejection_reason = "PIP_SIZE di konfigurasi adalah nol. Tidak dapat memvalidasi jarak SL/TP."
                        print(f"DEBUG_PRINT_BM: TIDAK MEMBUKA POSISI: {trade_rejection_reason}.")
                    else:
                        sl_distance_pips = abs(current_price_for_signal_dec - sl_sugg) / self.pip_size
                        tp_distance_pips = abs(current_price_for_signal_dec - tp1_sugg) / self.pip_size

                        print(f"DEBUG_PRINT_BM: Calculated SL Distance (pips): {sl_distance_pips:.2f}. Required: {float(min_sl_pips_dec):.2f}.")
                        print(f"DEBUG_PRINT_BM: Calculated TP1 Distance (pips): {tp_distance_pips:.2f}. Required: {float(min_tp_pips_dec):.2f}.")

                        if sl_distance_pips < min_sl_pips_dec:
                            trade_rejection_reason = f"SL ({sl_distance_pips:.2f} pips) terlalu dekat."
                            print(f"DEBUG_PRINT_BM: TIDAK MEMBUKA POSISI: {trade_rejection_reason} (SL terlalu dekat).")
                        elif tp_distance_pips < min_tp_pips_dec:
                            trade_rejection_reason = f"TP1 ({tp_distance_pips:.2f} pips) terlalu dekat."
                            print(f"DEBUG_PRINT_BM: TIDAK MEMBUKA POSISI: {trade_rejection_reason} (TP1 terlalu dekat).")
                        else:
                            can_attempt_trade_execution = True
                            print("DEBUG_PRINT_BM: Semua validasi awal dilewati. Siap untuk mencoba eksekusi trade.")

                if can_attempt_trade_execution:
                    sim_pos = self._simulate_trade_execution(
                        current_candle, action,
                        current_signal_data.get('entry_price_suggestion'),
                        sl_sugg,
                        tp_levels_list_from_signal,
                        current_balance
                    )
                    if sim_pos:
                        print(f"DEBUG_PRINT_BM: POSISI BERHASIL DIBUKA: {sim_pos.ticket}. Saldo: {float(current_balance):.2f}")
                        self.open_positions[sim_pos.ticket] = sim_pos
                        if action == "BUY":
                            total_long_trades += 1
                        elif action == "SELL":
                            total_short_trades += 1
                        print(f"DEBUG_PRINT_BM: Sinyal {action} dari {strategy_name}. Membuka posisi simulasi. Saldo saat ini: {float(current_balance):.2f}")
                    else:
                        print(f"DEBUG_PRINT_BM: GAGAL MEMBUKA POSISI SIMULASI (sim_pos is None). Alasan tidak diketahui atau error internal.")
                else:
                    print(f"DEBUG_PRINT_BM: TIDAK MEMBUKA POSISI: {trade_rejection_reason}. Sinyal: {action}.")

            self.balance_history.append((current_time_for_signal, current_balance))
            print(f"DEBUG_PRINT_BM: Saldo akhir candle {current_time_for_signal}: {float(current_balance):.7f}. Open Positions: {len(self.open_positions)}. Total Trades Done So Far: {local_total_trades}")

        # If there are open positions at the end of backtest
        for ticket in list(self.open_positions.keys()):
            open_position = self.open_positions[ticket]
            print(f"DEBUG_PRINT_BM: Menutup posisi terbuka terakhir di AKHIR BACKTEST: {open_position.ticket}")
            exit_price_for_log = df_candles.iloc[-1]['close_price']
            exit_reason_for_log = "End of Backtest"

            final_pnl_total = self._calculate_total_pnl_for_trade(open_position, exit_price_for_log)
            current_balance += final_pnl_total
            local_total_trades, local_winning_trades, local_losing_trades, local_gross_profit, local_gross_loss, \
            consecutive_wins, consecutive_losses, largest_win, largest_loss, max_consecutive_wins, max_consecutive_losses = \
                self._record_closed_trade_metrics(final_pnl_total, df_candles.index[-1], open_position.ticket, exit_price_for_log, exit_reason_for_log, True, local_total_trades, local_winning_trades, local_losing_trades, local_gross_profit, local_gross_loss, consecutive_wins, consecutive_losses, largest_win, largest_loss, max_consecutive_wins, max_consecutive_losses)
            del self.open_positions[ticket]

            logger.debug(f"DEBUG BM: Posisi terakhir ({open_position.ticket}) ditutup pada akhir backtest. Total P/L: {float(final_pnl_total):.7f}. Saldo Akhir: {float(current_balance):.7f}")

        self.balance_history.append((df_candles.index[-1], current_balance))


        net_profit = current_balance - self.initial_balance
        win_rate = (Decimal(str(local_winning_trades)) / Decimal(str(local_total_trades))) * Decimal('100.0') if local_total_trades > 0 else Decimal('0.0')
        profit_factor = local_gross_profit / local_gross_loss if local_gross_loss > 0 else Decimal('inf')

        if not self.balance_history:
            local_max_drawdown = Decimal('0.0')
        else:
            df_temp_balance = pd.DataFrame(self.balance_history, columns=['time', 'balance'])
            df_temp_balance['balance'] = df_temp_balance['balance'].apply(utils.to_decimal_or_none)
            df_temp_balance['peak'] = df_temp_balance['balance'].expanding(min_periods=1).max()
            df_temp_balance['peak'] = df_temp_balance['peak'].apply(utils.to_decimal_or_none)

            df_temp_balance['drawdown'] = df_temp_balance['peak'] - df_temp_balance['balance']
            df_temp_balance['drawdown_percent'] = (df_temp_balance['drawdown'] / df_temp_balance['peak']) * 100
            local_max_drawdown = df_temp_balance['drawdown_percent'].max() if not df_temp_balance['drawdown_percent'].empty else Decimal('0.0')

        recovery_factor = net_profit / local_max_drawdown if local_max_drawdown > 0 else Decimal('inf')

        roic = (net_profit / self.initial_balance) * 100 if self.initial_balance > 0 else Decimal('0.0')

        expected_payoff = net_profit / Decimal(str(local_total_trades)) if local_total_trades > 0 else Decimal('0.0')

        average_win = local_gross_profit / Decimal(str(local_winning_trades)) if local_winning_trades > 0 else Decimal('0.0')
        average_loss = local_gross_loss / Decimal(str(local_losing_trades)) if local_losing_trades > 0 else Decimal('0.0')

        df_balance = pd.DataFrame(self.balance_history, columns=['time', 'balance'])
        df_balance.set_index('time', inplace=True)
        df_balance.index = pd.to_datetime(df_balance.index, utc=True)
        df_balance['balance'] = df_balance['balance'].apply(utils.to_decimal_or_none)

        df_balance_daily = df_balance.resample('D').last().ffill().dropna()
        df_balance_daily['daily_return'] = df_balance_daily['balance'].pct_change().fillna(Decimal('0.0'))

        daily_returns_for_stats = df_balance_daily['daily_return']

        sharpe_ratio = Decimal('0.0')
        sortino_ratio = Decimal('0.0')
        calmar_ratio = Decimal('0.0')

        # Pastikan ada cukup data dan itu adalah angka float yang valid
        # `daily_returns_for_stats` adalah Series Decimal. Konversi ke float untuk .mean() dan .std().
        daily_returns_float = daily_returns_for_stats.apply(utils.to_float_or_none).dropna()

        # Hanya lanjutkan jika ada cukup data untuk perhitungan statistik (minimal 2 untuk std dev)
        if not daily_returns_float.empty and len(daily_returns_float) > 1:
            risk_free_rate_float = float(Decimal('0.0')) # Pastikan risk_free_rate juga float untuk operasi float

            mean_return_float = daily_returns_float.mean()

            # Perhitungan Sharpe Ratio
            daily_std_dev_float = daily_returns_float.std()
            # Pastikan daily_std_dev_float bukan NaN atau nol sebelum digunakan
            if pd.notna(daily_std_dev_float) and daily_std_dev_float > 0:
                sharpe_ratio = Decimal(str((mean_return_float - risk_free_rate_float) / daily_std_dev_float * math.sqrt(252)))
            else:
                logger.warning(f"DEBUG BM: daily_std_dev is {daily_std_dev_float} (NaN or <=0). Sharpe Ratio will remain 0.0.")

            # Perhitungan Sortino Ratio
            negative_returns_float = daily_returns_float[daily_returns_float < 0]

            # --- MODIFIKASI UNTUK Sortino Ratio: Pastikan ada return negatif yang valid ---
            if not negative_returns_float.empty and not negative_returns_float.isnull().all():
                downside_std_dev_float = negative_returns_float.std()

                # Pastikan downside_std_dev_float bukan NaN atau nol sebelum digunakan
                if pd.notna(downside_std_dev_float) and downside_std_dev_float > 0: # Ini adalah baris 913
                    sortino_ratio = Decimal(str((mean_return_float - risk_free_rate_float) / downside_std_dev_float * math.sqrt(252)))
                else:
                    logger.warning(f"DEBUG BM: downside_std_dev is {downside_std_dev_float} (NaN or <=0). Sortino Ratio will remain 0.0.")
            else:
                logger.warning("DEBUG BM: No valid negative returns found for Sortino Ratio calculation. Sortino Ratio will remain 0.0.")
            # --- AKHIR MODIFIKASI Sortino Ratio ---
        else:
            logger.warning("DEBUG BM: Not enough valid daily returns (or all NaN) to calculate Sharpe/Sortino Ratios. They will remain 0.0.")

        # Perhitungan Calmar Ratio
        if local_max_drawdown > 0 and (net_profit / self.initial_balance) > 0:
            calmar_ratio = (net_profit / self.initial_balance) / (local_max_drawdown / Decimal('100'))
        else:
            logger.warning(f"DEBUG BM: Cannot calculate Calmar Ratio. Max Drawdown ({local_max_drawdown}) or Net Profit/Initial Balance ({net_profit/self.initial_balance if self.initial_balance > 0 else 'N/A'}) not valid.")

        final_results = {
            "symbol": self.symbol,
            "strategy_name": strategy_name,
            "start_date": utils.to_iso_format_or_none(start_date),
            "end_date": utils.to_iso_format_or_none(end_date),
            "initial_balance": self.initial_balance,
            "final_balance": current_balance,
            "net_profit": net_profit,
            "total_trades": local_total_trades,
            "winning_trades": local_winning_trades,
            "losing_trades": local_losing_trades,
            "win_rate": win_rate,
            "gross_profit": local_gross_profit,
            "gross_loss": local_gross_loss,
            "profit_factor": profit_factor,
            "max_drawdown_percent": local_max_drawdown,
            "recovery_factor": recovery_factor,
            "roic_percent": f"{roic:.2f}%",
            "expected_payoff": expected_payoff,
            "average_win": average_win,
            "average_loss": average_loss,
            "largest_win": largest_win,
            "largest_loss": largest_loss,
            "max_consecutive_wins": max_consecutive_wins,
            "max_consecutive_losses": max_consecutive_losses,

            "sl_hit_count": local_sl_hit_count,
            "be_hit_count": local_be_hit_count,
            "tp1_hit_count": local_tp1_hit_count,
            "tp2_hit_count": local_tp2_hit_count,
            "tp3_hit_count": local_tp3_hit_count,
            "tsl_hit_count": local_tsl_hit_count,
            "closed_by_opposite_signal_count": local_closed_by_opposite_signal_count,
            "closed_by_time_limit_count": local_closed_by_time_limit_count,

            "total_long_trades": total_long_trades,
            "total_short_trades": total_short_trades,
            "sharpe_ratio": sharpe_ratio,
            "sortino_ratio": sortino_ratio,
            "calmar_ratio": calmar_ratio,
            "status": "success",
            "message": "Backtest berhasil diselesaikan."
        }

        logger.info(f"Backtest selesai untuk {self.symbol}. Hasil akhir: {json.dumps(final_results, indent=2, default=utils._json_default)}")

        chart_filepath = None
        if plot_results:
            logger.info("DEBUG Backtest: plot_results is True. Attempting to generate chart.")
            try:
                logger.debug(f"DEBUG BacktestManager: balance_history length: {len(self.balance_history)}")
                logger.debug(f"DEBUG BacktestManager: closed_trade_profits_losses length: {len(self.closed_trade_profits_losses)}")

                chart_filepath = self._plot_results(
                    self.balance_history,
                    self.closed_trade_profits_losses,
                    strategy_name,
                    local_max_drawdown
                )
                if chart_filepath:
                    logger.info(f"DEBUG Backtest: Chart generated at: {chart_filepath}")
                else:
                    logger.warning("DEBUG Backtest: _plot_results returned None. Chart generation failed or was skipped.")
            except Exception as e:
                logger.error(f"ERROR Backtest: Failed to call _plot_results or process its return: {e}", exc_info=True)
                chart_filepath = None

        final_results['chart_filepath'] = chart_filepath
        final_results['simulated_trade_events'] = self.simulated_trade_events
        candlestick_chart_filepath = None
        if plot_results:
            logger.debug("BACKTEST_MANAGER: Memulai pembuatan chart candlestick dengan trade events.")
            try:
                candles_data_for_chart = df_candles.reset_index().rename(columns={'open_time_utc': 'timestamp'})

                for col in ['open_price', 'high_price', 'low_price', 'close_price']:
                    if col in candles_data_for_chart.columns:
                        candles_data_for_chart[col] = candles_data_for_chart[col].apply(utils.to_float_or_none)

                simulated_trade_events_for_chart = []
                for event in self.simulated_trade_events:
                    chart_event = event.copy()
                    if 'pnl' in chart_event and chart_event['pnl'] is not None:
                        chart_event['pnl'] = utils.to_float_or_none(chart_event['pnl'])
                    if 'entry_price' in chart_event and chart_event['entry_price'] is not None:
                        chart_event['entry_price'] = utils.to_float_or_none(chart_event['entry_price'])
                    if 'exit_price' in chart_event and chart_event['exit_price'] is not None:
                        chart_event['exit_price'] = utils.to_float_or_none(chart_event['exit_price'])
                    if 'initial_sl' in chart_event and chart_event['initial_sl'] is not None:
                        chart_event['initial_sl'] = utils.to_float_or_none(chart_event['initial_sl'])
                    if 'initial_tp1' in chart_event and chart_event['initial_tp1'] is not None:
                        chart_event['initial_tp1'] = utils.to_float_or_none(chart_event['initial_tp1'])
                    simulated_trade_events_for_chart.append(chart_event)


                candlestick_chart_filepath = chart_generator.generate_candlestick_chart(
                    symbol=self.symbol,
                    timeframe=app_config.Trading.DEFAULT_TIMEFRAME,
                    candles_data=candles_data_for_chart.to_dict(orient='records'),
                    simulated_trade_events=simulated_trade_events_for_chart,
                    filename=f"{self.symbol}_{app_config.Trading.DEFAULT_TIMEFRAME}_backtest_trades_{datetime.now().strftime('%Y%m%d%H%M%S')}"
                )
                if candlestick_chart_filepath:
                    final_results['candlestick_chart_filepath'] = candlestick_chart_filepath
                    logger.info(f"BACKTEST_MANAGER: Chart candlestick backtest berhasil dibuat: {candlestick_chart_filepath}")
                else:
                    logger.warning("BACKTEST_MANAGER: Gagal membuat chart candlestick backtest.")
            except Exception as e:
                logger.error(f"BACKTEST_MANAGER: Error saat membuat chart candlestick backtest: {e}", exc_info=True)

        return final_results

    def _record_closed_trade_metrics(self, pnl: Decimal, exit_time: datetime, ticket: str, exit_price: Decimal, exit_reason: str,
                                     is_full_close: bool, local_total_trades: int, local_winning_trades: int, local_losing_trades: int,
                                     local_gross_profit: Decimal, local_gross_loss: Decimal, consecutive_wins: int, consecutive_losses: int,
                                     largest_win: Decimal, largest_loss: Decimal, max_consecutive_wins: int, max_consecutive_losses: int):

        self.closed_trade_profits_losses.append(pnl)

        if is_full_close:
            local_total_trades += 1
            if pnl > 0:
                local_winning_trades += 1
                local_gross_profit += pnl
                consecutive_wins += 1
                consecutive_losses = 0
                largest_win = max(largest_win, pnl)
            else:
                local_losing_trades += 1
                local_gross_loss += abs(pnl)
                consecutive_losses += 1
                consecutive_wins = 0
                largest_loss = min(largest_loss, pnl)

            max_consecutive_wins = max(max_consecutive_wins, consecutive_wins)
            max_consecutive_losses = max(max_consecutive_losses, consecutive_losses)

            for trade_event in self.simulated_trade_events:
                if trade_event['ticket'] == ticket:
                    trade_event['exit_time'] = exit_time
                    trade_event['exit_price'] = exit_price
                    trade_event['exit_reason'] = exit_reason
                    trade_event['pnl'] = pnl
                    break

        return (local_total_trades, local_winning_trades, local_losing_trades, local_gross_profit, local_gross_loss,
                consecutive_wins, consecutive_losses, largest_win, largest_loss, max_consecutive_wins, max_consecutive_losses)


    def _plot_results(self, balance_history: list, closed_trade_profits_losses: list, strategy_name: str, max_drawdown_value: Decimal) -> str:
        logger.debug(f"DEBUG BacktestManager: Starting _plot_results for strategy '{strategy_name}'.")

        try:
            chart_output_dir = os.path.join(os.getcwd(), 'charts_output')
            os.makedirs(chart_output_dir, exist_ok=True)
            logger.debug(f"DEBUG BacktestManager: Ensuring chart output directory exists: {chart_output_dir}")

            if not balance_history:
                logger.warning("DEBUG BacktestManager: balance_history is empty. Cannot create chart.")
                return None

            df_balance = pd.DataFrame(balance_history, columns=['time', 'balance'])
            df_balance.set_index('time', inplace=True)
            df_balance.index = pd.to_datetime(df_balance.index, utc=True)

            df_balance['balance'] = df_balance['balance'].apply(lambda x: float(x) if x is not None else np.nan)

            initial_balance_len = len(df_balance)
            df_balance.dropna(subset=['balance'], inplace=True)
            if len(df_balance) < initial_balance_len:
                logger.warning(f"DEBUG BacktestManager: Dropped {initial_balance_len - len(df_balance)} rows with NaN balances for plotting.")

            profits_losses_float = [float(p) for p in closed_trade_profits_losses if p is not None and not (isinstance(p, Decimal) and p.is_nan())]
            if not profits_losses_float:
                logger.warning("DEBUG BacktestManager: closed_trade_profits_losses is empty or all NaN. P/L histogram will not be created.")

            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            chart_filename = f"{strategy_name.replace(' ', '_')}_{self.symbol}_{timestamp}.png"
            chart_filepath = os.path.join(chart_output_dir, chart_filename)
            logger.debug(f"DEBUG BacktestManager: Chart file name: {chart_filepath}")

            try:
                fig, axes = plt.subplots(nrows=3, ncols=1, figsize=(15, 18), gridspec_kw={'height_ratios': [3, 1, 1]})
                fig.suptitle(f'Backtest Results: {strategy_name} ({self.symbol})', fontsize=16, y=0.98)

                axes[0].plot(df_balance.index, df_balance['balance'], label='Equity Curve', color='blue')
                axes[0].set_title('Equity Curve')
                axes[0].set_xlabel('Time')
                axes[0].set_ylabel('Balance')
                axes[0].grid(True)
                axes[0].legend()

                if len(df_balance) > 1:
                    peak_equity_curve = np.maximum.accumulate(df_balance['balance'])
                    drawdown = ((peak_equity_curve - df_balance['balance']) / peak_equity_curve) * 100

                    axes[1].plot(df_balance.index, drawdown, label='Drawdown (%)', color='red')
                    axes[1].fill_between(df_balance.index, drawdown, 0, color='red', alpha=0.3)
                    axes[1].set_title('Drawdown Curve')
                    axes[1].set_xlabel('Time')
                    axes[1].set_ylabel('Drawdown (%)')
                    axes[1].grid(True)
                    axes[1].legend()
                else:
                    axes[1].text(0.5, 0.5, 'Not enough data for Drawdown Curve',
                                 horizontalalignment='center', verticalalignment='center',
                                 transform=axes[1].transAxes, fontsize=12, color='gray')
                    logger.warning("DEBUG BacktestManager: Not enough data for Drawdown Curve (less than 2 points).")

                if profits_losses_float:
                    axes[2].hist(profits_losses_float, bins=50, edgecolor='black', color='green' if sum(profits_losses_float) > 0 else 'red')
                    axes[2].set_title('Profit/Loss Distribution')
                    axes[2].set_xlabel('Profit/Loss ($)')
                    axes[2].set_ylabel('Frequency')
                    axes[2].grid(True)
                else:
                    axes[2].text(0.5, 0.5, 'No trades to plot P/L distribution',
                                 horizontalalignment='center', verticalalignment='center',
                                 transform=axes[2].transAxes, fontsize=12, color='gray')
                    logger.warning("DEBUG BacktestManager: No closed trades data for P/L distribution.")

                plt.tight_layout(rect=[0, 0.03, 1, 0.96])

                plt.savefig(chart_filepath, dpi=100)
                plt.close(fig)
                logger.info(f"DEBUG BacktestManager: Chart saved successfully to: {chart_filepath}")
                return chart_filepath

            except Exception as chart_e:
                logger.error(f"ERROR BacktestManager: Failed to create or save backtest chart to {chart_filepath}: {chart_e}", exc_info=True)
                if "Permission denied" in str(chart_e):
                    logger.error("HINT: This is likely a folder permission issue. Ensure the bot has write access to 'charts_output' folder.")
                elif "No space left on device" in str(chart_e):
                    logger.error("HINT: Disk is full. Free up some space.")
                elif "Input array contains NaN" in str(chart_e) or "float() argument must be a string or a real number, not 'NoneType'" in str(chart_e):
                    logger.error("HINT: Plotting data issue! Some values passed to Matplotlib (e.g., balances, profits/losses) might be None/NaN or incorrect types despite previous checks.")
                return None

        except Exception as e:
            logger.error(f"ERROR BacktestManager: Unexpected error in _plot_results: {e}", exc_info=True)
            return None

def timeframe_to_seconds(timeframe_str):
    if timeframe_str == "M1": return 60
    if timeframe_str == "M5": return 300
    if timeframe_str == "M15": return 900
    if timeframe_str == "M30": return 1800
    if timeframe_str == "H1": return 3600
    if timeframe_str == "H4": return 14400
    if timeframe_str == "D1": return 86400
    return 0
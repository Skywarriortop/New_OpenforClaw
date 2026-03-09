# File: chart_generator.py

import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime, timezone
from decimal import Decimal
import os
import logging
import re
import numpy as np
import utils
from utils import to_float_or_none, to_iso_format_or_none

logger = logging.getLogger(__name__)

CHART_OUTPUT_DIR = "charts_output"
if not os.path.exists(CHART_OUTPUT_DIR):
    os.makedirs(CHART_OUTPUT_DIR)

def generate_candlestick_chart(symbol: str, timeframe: str, candles_data: list,
                                 signals_data: list = None,
                                 ma_data: dict = None,
                                 fvgs_data: list = None,
                                 sr_levels_data: list = None,
                                 ob_data: list = None,
                                 ms_events_data: list = None,
                                 liquidity_zones_data: list = None,
                                 fibonacci_levels_data: list = None,
                                 volume_profile_data: list = None,
                                 simulated_trade_events: list = None, # <-- TAMBAHKAN PARAMETER INI
                                 filename: str = None) -> str:

    if not candles_data:
        logger.warning(f"CHART_GEN: No candle data to generate candlestick chart for {symbol} {timeframe}.")
        return None

    df = pd.DataFrame(candles_data)
    
    # --- MULAI PERBAIKAN DI SINI ---
    # Pastikan kolom waktu yang digunakan adalah 'timestamp'
    if 'timestamp' not in df.columns:
        logger.error(f"CHART_GEN: Missing 'timestamp' column in DataFrame for {symbol} {timeframe}. Available columns: {df.columns.tolist()}")
        return None # Keluar jika kolom waktu tidak ada

    df['timestamp'] = pd.to_datetime(df['timestamp']) # Gunakan 'timestamp'
    df = df.set_index('timestamp').sort_index() # Set indeks ke 'timestamp'

    # Sekarang, kolom harga di DataFrame seharusnya masih 'open_price', 'high_price', dll.
    # Anda perlu me-rename kolom-kolom ini juga untuk konsistensi internal jika diperlukan oleh kode plotting Matplotlib Anda.
    df = df.rename(columns={
        'open_price': 'open', 'high_price': 'high', 'low_price': 'low',
        'close_price': 'close', 'tick_volume': 'volume' # Sesuaikan dengan semua kolom yang Anda rename
    })

    # Konversi semua kolom harga yang sudah di-rename ke float
    numeric_cols = ['open', 'high', 'low', 'close']
    for col in numeric_cols:
        if col in df.columns: # Pastikan kolom ada setelah rename
            df[col] = df[col].apply(to_float_or_none)
        else:
            logger.error(f"CHART_GEN: Missing critical renamed price column '{col}' in DataFrame for {symbol} {timeframe}.")
            return None # Keluar jika kolom harga utama tidak ada setelah rename

    initial_df_len = len(df)
    df.dropna(subset=numeric_cols, inplace=True)
    if df.empty:
        logger.warning(f"CHART_GEN: DataFrame is empty after dropping NaN values for {symbol} {timeframe}.")
        return None
    if len(df) < initial_df_len:
        logger.warning(f"CHART_GEN: Dropped {initial_df_len - len(df)} rows with NaN OHLC data for {symbol} {timeframe}.")

    fig, ax1 = plt.subplots(figsize=(18, 9))

    # --- MULAI PERBAIKAN: Ubah referensi kolom di loop plotting Candlestick ---
    for i in range(len(df)):
        op = df['open'].iloc[i]   # UBAH dari 'open_price'
        hi = df['high'].iloc[i]   # UBAH dari 'high_price'
        lo = df['low'].iloc[i]    # UBAH dari 'low_price'
        cl = df['close'].iloc[i]  # UBAH dari 'close_price'

        color = 'green' if cl > op else 'red'
        
        ax1.plot([i, i], [lo, hi], color='black', linewidth=0.5)
        ax1.bar(i, abs(cl - op), bottom=min(op, cl), width=0.8, color=color, edgecolor='black')
    # --- AKHIR PERBAIKAN ---


    num_candles = len(df)
    step = max(1, num_candles // 15)
    ax1.set_xticks(range(0, num_candles, step))
    ax1.set_xticklabels([df.index[i].strftime('%Y-%m-%d %H:%M') for i in range(0, num_candles, step)], rotation=45, ha='right')

    ax1.set_title(f"{symbol} - {timeframe} Candlestick Chart with AI Signals and Key Levels", fontsize=16)
    ax1.set_xlabel("Time (UTC)", fontsize=12)
    ax1.set_ylabel("Price", fontsize=12)
    ax1.grid(True, linestyle='--', alpha=0.6)

    y_min_price = df['low'].min()  # UBAH dari 'low_price'
    y_max_price = df['high'].max() # UBAH dari 'high_price'

    all_prices_for_ylim = [y_min_price, y_max_price]

    if signals_data:
        for s in signals_data:
            all_prices_for_ylim.extend([to_float_or_none(s.get('entry_price_suggestion')), to_float_or_none(s.get('stop_loss_suggestion')),
                                         to_float_or_none(s.get('take_profit_suggestion')), to_float_or_none(s.get('tp2_suggestion')),
                                         to_float_or_none(s.get('tp3_suggestion'))])

    if sr_levels_data:
        for lvl in sr_levels_data:
            all_prices_for_ylim.extend([to_float_or_none(lvl.get('price_level')), to_float_or_none(lvl.get('zone_start_price')), to_float_or_none(lvl.get('zone_end_price'))])

    if fvgs_data:
        for fvg in fvgs_data:
            all_prices_for_ylim.extend([to_float_or_none(fvg.get('fvg_top_price')), to_float_or_none(fvg.get('fvg_bottom_price'))])

    if ob_data:
        for ob in ob_data:
            all_prices_for_ylim.extend([to_float_or_none(ob.get('ob_top_price')), to_float_or_none(ob.get('ob_bottom_price'))])

    if liquidity_zones_data:
        for lz in liquidity_zones_data:
            all_prices_for_ylim.append(to_float_or_none(lz.get('price_level')))

    if fibonacci_levels_data:
        for fib in fibonacci_levels_data:
            all_prices_for_ylim.append(to_float_or_none(fib.get('price_level')))

    if ma_data:
        for ma_name, ma_series_obj in ma_data.items():
            if isinstance(ma_series_obj, pd.Series) and not ma_series_obj.empty:
                all_prices_for_ylim.extend(ma_series_obj.apply(to_float_or_none).tolist())

    if volume_profile_data and len(volume_profile_data) > 0 and volume_profile_data[0].get('profile_data_json'):
        vp_bins_raw = volume_profile_data[0]['profile_data_json']
        all_prices_for_ylim.extend([float(bin_data['price']) for bin_data in vp_bins_raw])
        all_prices_for_ylim.extend([to_float_or_none(volume_profile_data[0].get('poc_price')),
                                     to_float_or_none(volume_profile_data[0].get('vah_price')),
                                     to_float_or_none(volume_profile_data[0].get('val_price'))])

    # --- MULAI MODIFIKASI: Memasukkan harga trade events ke batas Y-axis ---
    if simulated_trade_events:
        for trade_event in simulated_trade_events:
            if trade_event.get('entry_price') is not None:
                all_prices_for_ylim.append(to_float_or_none(trade_event['entry_price']))
            if trade_event.get('exit_price') is not None:
                all_prices_for_ylim.append(to_float_or_none(trade_event['exit_price']))
            if trade_event.get('initial_sl') is not None:
                all_prices_for_ylim.append(to_float_or_none(trade_event['initial_sl']))
            if trade_event.get('initial_tp1') is not None:
                all_prices_for_ylim.append(to_float_or_none(trade_event['initial_tp1']))
    # --- AKHIR MODIFIKASI ---


    numeric_prices_for_ylim = [p for p in all_prices_for_ylim if pd.notna(p) and p is not None and p != 0.0]

    if numeric_prices_for_ylim:
        min_overall_price = min(numeric_prices_for_ylim)
        max_overall_price = max(numeric_prices_for_ylim)
        
        price_range = max_overall_price - min_overall_price
        padding = price_range * 0.03 if price_range > 0.0 else (max_overall_price * 0.01 if max_overall_price > 0.0 else 0.01)

        ax1.set_ylim(min_overall_price - padding, max_overall_price + padding)
        logger.debug(f"CHART_GEN: Y-axis set from {min_overall_price - padding:.5f} to {max_overall_price + padding:.5f}")
    else:
        logger.warning("CHART_GEN: No valid numeric prices found for Y-axis scaling. Using default Matplotlib auto-scaling.")
        ax1.autoscale_view()

    # Plotting Volume Profile
    if volume_profile_data and len(volume_profile_data) > 0 and volume_profile_data[0].get('profile_data_json'):
        vp_instance_data = volume_profile_data[0] 
        vp_bins = vp_instance_data['profile_data_json']
        vp_row_height = to_float_or_none(vp_instance_data.get('row_height_value')) 
        
        if vp_row_height is None or vp_row_height <= 0:
            logger.warning("CHART_GEN: Volume Profile row_height_value is invalid or zero. Skipping plotting Volume Profile.")
        else:
            ax2 = ax1.twiny()
            ax2.set_xlabel("Volume", color='blue')
            ax2.tick_params(axis='x', labelcolor='blue')
            ax2.spines['right'].set_color('blue') 
            ax2.spines['top'].set_visible(True)

            max_volume_in_profile = 0.0
            if vp_bins:
                max_volume_in_profile = max([bin_data['volume'] for bin_data in vp_bins])

            if max_volume_in_profile > 0:
                ax2.set_xlim(0, max_volume_in_profile * 1.1)
            else: 
                ax2.set_xlim(0, 1)

            ax2.set_ylim(ax1.get_ylim()) 

            for bin_data in vp_bins:
                price_f = float(bin_data['price'])
                volume_f = float(bin_data['volume'])
                
                if pd.isna(volume_f) or volume_f == 0.0:
                    continue

                ax2.barh(y=price_f - vp_row_height / 2, width=volume_f, height=vp_row_height,
                            left=0, align='center', color='lightblue', edgecolor='blue', alpha=0.5)

            poc_price = to_float_or_none(vp_instance_data.get('poc_price'))
            vah_price = to_float_or_none(vp_instance_data.get('vah_price'))
            val_price = to_float_or_none(vp_instance_data.get('val_price'))

            if poc_price is not None and not np.isnan(poc_price):
                ax2.axhline(y=poc_price, color='darkorange', linestyle='-', linewidth=1.5, alpha=0.8, label='POC')
                ax2.text(ax2.get_xlim()[1] * 0.95, poc_price, 'POC', color='darkorange', fontsize=9, ha='right', va='center')

            if vah_price is not None and not np.isnan(vah_price):
                ax2.axhline(y=vah_price, color='darkgreen', linestyle='--', linewidth=1.0, alpha=0.7, label='VAH')
                ax2.text(ax2.get_xlim()[1] * 0.95, vah_price, 'VAH', color='darkgreen', fontsize=9, ha='right', va='center')

            if val_price is not None and not np.isnan(val_price):
                ax2.axhline(y=val_price, color='darkred', linestyle='--', linewidth=1.0, alpha=0.7, label='VAL')
                ax2.text(ax2.get_xlim()[1] * 0.95, val_price, 'VAL', color='darkred', fontsize=9, ha='right', va='center')
            
            ax2.set_yticklabels([])

    # --- MULAI MODIFIKASI: Plotting Simulated Trade Events ---
    if simulated_trade_events:
        for trade_event in simulated_trade_events:
            entry_time = trade_event.get('entry_time')
            entry_price = to_float_or_none(trade_event.get('entry_price'))
            trade_type = trade_event.get('type') # 'buy' atau 'sell'
            
            exit_time = trade_event.get('exit_time')
            exit_price = to_float_or_none(trade_event.get('exit_price'))
            exit_reason = trade_event.get('exit_reason')
            
            # Hanya plot jika waktu entry dan harga valid
            if entry_time and entry_price is not None and pd.notna(entry_price) and entry_time in df.index:
                x_entry_idx = df.index.get_loc(entry_time)
                
                # Plot penanda entry
                if trade_type == 'buy':
                    ax1.plot(x_entry_idx, entry_price, '^', markersize=10, color='lime', zorder=5, label='Buy Entry')
                    ax1.annotate('BUY', (x_entry_idx, entry_price),
                                 textcoords="offset points", xytext=(-5, 10), ha='center',
                                 fontsize=8, color='lime', weight='bold')
                elif trade_type == 'sell':
                    ax1.plot(x_entry_idx, entry_price, 'v', markersize=10, color='red', zorder=5, label='Sell Entry')
                    ax1.annotate('SELL', (x_entry_idx, entry_price),
                                 textcoords="offset points", xytext=(-5, -15), ha='center',
                                 fontsize=8, color='red', weight='bold')

                # Plot penanda exit (jika trade sudah ditutup)
                if exit_time and exit_price is not None and pd.notna(exit_price) and exit_time in df.index:
                    x_exit_idx = df.index.get_loc(exit_time)
                    
                    marker = 'o'
                    color = 'gray'
                    if 'TP' in str(exit_reason):
                        marker = '*'
                        color = 'blue'
                    elif 'SL' in str(exit_reason) or 'BE' in str(exit_reason) or 'TSL' in str(exit_reason):
                        marker = 'X'
                        color = 'purple'
                    elif 'Opposite Signal' in str(exit_reason):
                        marker = 's'
                        color = 'orange'
                    elif 'End of Backtest' in str(exit_reason):
                        marker = 'd'
                        color = 'darkgray'
                    
                    ax1.plot(x_exit_idx, exit_price, marker, markersize=8, color=color, zorder=4, label='Exit')
                    ax1.annotate(f'Exit ({exit_reason})', (x_exit_idx, exit_price),
                                 textcoords="offset points", xytext=(5, -10 if trade_type == 'buy' else 10), ha='left',
                                 fontsize=7, color=color)
            else:
                logger.warning(f"CHART_GEN: Skipping trade event {trade_event.get('ticket')} due to invalid entry/exit time or price for plotting.")
    # --- AKHIR MODIFIKASI ---

    if signals_data:
        for signal in signals_data:
            signal_time = signal['timestamp']
            action = signal['recommendation_action']
            entry_price = signal['entry_price_suggestion'] # Sudah float

            if signal_time in df.index:
                x_index = df.index.get_loc(signal_time)

                if entry_price is not None and pd.notna(entry_price):
                    if action == 'BUY':
                        ax1.annotate(f'BUY {entry_price:.5f}', (x_index, entry_price),
                                    textcoords="offset points", xytext=(0, -25), ha='center',
                                    arrowprops=dict(facecolor='blue', shrink=0.05),
                                    fontsize=9, color='blue', weight='bold')
                    elif action == 'SELL':
                        ax1.annotate(f'SELL {entry_price:.5f}', (x_index, entry_price),
                                    textcoords="offset points", xytext=(0, 20), ha='center',
                                    arrowprops=dict(facecolor='red', shrink=0.05),
                                    fontsize=9, color='red', weight='bold')
                else:
                    logger.warning(f"CHART_GEN: Signal {action} at {signal_time} skipped due to invalid entry_price: {entry_price}")


    if ma_data:
        for ma_name, ma_series_obj in ma_data.items():
            if isinstance(ma_series_obj, pd.Series) and not ma_series_obj.empty:
                ma_series_aligned = ma_series_obj.reindex(df.index, method='nearest').dropna()
                if not ma_series_aligned.empty:
                    ax1.plot(df.index.get_indexer(ma_series_aligned.index), ma_series_aligned,
                            label=ma_name, linewidth=1.5, alpha=0.8)
                    logger.debug(f"CHART_GEN: Plotting MA: {ma_name}. Data points: {len(ma_series_aligned)}")
                else:
                    logger.warning(f"CHART_GEN: MA {ma_name} series is empty after reindexing for plotting.")

    if fvgs_data:
        last_close_price_f = df['close_price'].iloc[-1]
        fvgs_data_sorted = sorted(fvgs_data, key=lambda x: abs(to_float_or_none(x.get('fvg_bottom_price')) - last_close_price_f) if to_float_or_none(x.get('fvg_bottom_price')) is not None else float('inf'))
        max_fvgs_to_plot = 5
        fvgs_plotted_count = 0

        for fvg in fvgs_data_sorted:
            if fvgs_plotted_count >= max_fvgs_to_plot:
                break

            fvg_bottom = to_float_or_none(fvg.get('fvg_bottom_price'))
            fvg_top = to_float_or_none(fvg.get('fvg_top_price'))
            fvg_formation_time = fvg.get('formation_time_utc')

            if fvg_bottom is None or fvg_top is None or pd.isna(fvg_bottom) or pd.isna(fvg_top):
                logger.warning(f"CHART_GEN: Skipping FVG {fvg.get('id')} due to None/NaN prices: bottom={fvg_bottom}, top={fvg_top}.")
                continue

            if not fvg.get('is_filled') and fvg_formation_time <= df.index.max():

                time_diffs_fvg = abs(df.index - pd.to_datetime(fvg_formation_time))
                closest_idx_pos_fvg = np.argmin(time_diffs_fvg.total_seconds())
                fvg_start_idx = df.index.get_loc(df.index[closest_idx_pos_fvg])

                width = len(df) - fvg_start_idx
                if fvg.get('is_filled') and fvg.get('last_fill_time_utc'):
                    fill_time = pd.to_datetime(fvg.get('last_fill_time_utc'))
                    if fill_time in df.index:
                        fill_idx_pos = df.index.get_loc(fill_time)
                        width = fill_idx_pos - fvg_start_idx + 1

                if fvg_start_idx >= 0 and width > 0:
                    color = 'cyan' if fvg.get('type') == 'Bullish Imbalance' else 'magenta'
                    alpha = 0.2
                    ax1.add_patch(plt.Rectangle((fvg_start_idx - 0.5, fvg_bottom), width, fvg_top - fvg_bottom,
                                               facecolor=color, alpha=alpha, edgecolor=color, linestyle='--'))
                    fvgs_plotted_count += 1
                    logger.debug(f"CHART_GEN: Plotting FVG: {fvg.get('type')} at {fvg_bottom:.5f}-{fvg_top:.5f} (plotted {fvgs_plotted_count}/{max_fvgs_to_plot}).")
                else:
                    logger.warning(f"CHART_GEN: Failed to plot FVG {fvg.get('type')} at {to_iso_format_or_none(fvg_formation_time)} due to invalid index or width.")


    if sr_levels_data:
        sr_levels_data_sorted = sorted(sr_levels_data, key=lambda x: (x.get('is_key_level', False), x.get('strength_score', 0)), reverse=True)
        
        max_sr_labels = 3
        sr_labels_count = 0
        max_sr_to_plot = 15
        sr_plotted_count = 0

        for sr_level in sr_levels_data_sorted:
            if sr_plotted_count >= max_sr_to_plot:
                break

            price_level = to_float_or_none(sr_level.get('price_level'))
            level_type = sr_level.get('level_type')
            
            if price_level is None or pd.isna(price_level):
                logger.warning(f"CHART_GEN: Skipping SR {sr_level.get('id')} due to None/NaN price_level: {price_level}.")
                continue

            if sr_level.get('is_active') and price_level >= ax1.get_ylim()[0] and price_level <= ax1.get_ylim()[1]:
                color = 'blue' if level_type == 'Support' else 'red'
                linestyle = '--'
                linewidth = 1.0

                if sr_level.get('is_key_level', False):
                    linewidth = 1.8
                    color = 'darkblue' if level_type == 'Support' else 'darkred'
                elif sr_level.get('strength_score', 0) >= 5:
                    linewidth = 1.4

                ax1.axhline(y=price_level, color=color, linestyle=linestyle, linewidth=linewidth, alpha=0.7)
                sr_plotted_count += 1

                if (sr_level.get('is_key_level', False) or sr_labels_count < max_sr_labels):
                    text_x_pos = df.index.get_loc(df.index[0]) - 2
                    ax1.text(text_x_pos, price_level, f"{level_type}",
                            verticalalignment='center', horizontalalignment='right', color=color, fontsize=9, weight='bold',
                            bbox=dict(facecolor='white', alpha=0.7, edgecolor='none', pad=1))
                    sr_labels_count += 1
                logger.debug(f"CHART_GEN: Plotting S&R: {level_type} at {price_level:.5f} (Key: {sr_level.get('is_key_level')}, Str: {sr_level.get('strength_score')}, plotted {sr_plotted_count}/{max_sr_to_plot}).")
            elif not sr_level.get('is_active'):
                logger.debug(f"CHART_GEN: S&R {level_type} {to_float_or_none(sr_level.get('price_level'))} skipped because it is not active.")

    if ob_data:
        ob_data_sorted = sorted(ob_data, key=lambda x: x.get('strength_score', 0), reverse=True)
        max_ob_labels = 2
        ob_labels_count = 0
        max_ob_to_plot = 10
        ob_plotted_count = 0

        for ob in ob_data_sorted:
            if ob_plotted_count >= max_ob_to_plot:
                break

            ob_bottom = to_float_or_none(ob.get('ob_bottom_price'))
            ob_top = to_float_or_none(ob.get('ob_top_price'))
            ob_formation_time = ob.get('formation_time_utc')

            if ob_bottom is None or ob_top is None or pd.isna(ob_bottom) or pd.isna(ob_top):
                logger.warning(f"CHART_GEN: Skipping OB {ob.get('id')} due to None/NaN prices: bottom={ob_bottom}, top={ob_top}.")
                continue

            if not ob.get('is_mitigated') and ob_formation_time <= df.index.max():
                time_diffs_ob = abs(df.index - pd.to_datetime(ob_formation_time))
                closest_idx_pos_ob = np.argmin(time_diffs_ob.total_seconds())
                ob_start_idx = df.index.get_loc(df.index[closest_idx_pos_ob])

                width_ob = len(df) - ob_start_idx
                if ob.get('is_mitigated') and ob.get('last_mitigation_time_utc'):
                    mitigation_time = pd.to_datetime(ob.get('last_mitigation_time_utc'))
                    if mitigation_time in df.index:
                        mitigation_idx_pos = df.index.get_loc(mitigation_time)
                        width_ob = mitigation_idx_pos - ob_start_idx + 1

                if ob_start_idx >= 0 and width_ob > 0:
                    color = 'green' if ob.get('type') == 'Bullish' else 'purple'
                    alpha = 0.25
                    ax1.add_patch(plt.Rectangle((ob_start_idx - 0.5, ob_bottom), width_ob, ob_top - ob_bottom,
                                               facecolor=color, alpha=alpha, edgecolor=color, linestyle='-'))
                    ob_plotted_count += 1
                    if ob_labels_count < max_ob_labels:
                        ax1.text(ob_start_idx + width_ob / 2, (ob_bottom + ob_top) / 2, f"{ob.get('type')} OB",
                                verticalalignment='center', horizontalalignment='center', color=color, fontsize=8, weight='bold')
                        ob_labels_count += 1
                    logger.debug(f"CHART_GEN: Plotting OB: {ob.get('type')} at {ob_bottom:.5f}-{ob_top:.5f} (Str: {ob.get('strength_score')}, plotted {ob_plotted_count}/{max_ob_to_plot}).")
                else:
                    logger.warning(f"CHART_GEN: Failed to plot OB {ob.get('type')} at {to_iso_format_or_none(ob_formation_time)} due to invalid index or width.")


    if ms_events_data:
        ms_events_data_sorted = sorted(ms_events_data, key=lambda x: x.get('event_time_utc'), reverse=True)
        max_ms_labels = 3
        ms_labels_count = 0
        max_ms_to_plot = 10
        ms_plotted_count = 0

        for event in ms_events_data_sorted:
            if ms_plotted_count >= max_ms_to_plot:
                break

            event_time = event.get('event_time_utc')
            if event_time is None: continue

            if pd.to_datetime(event_time) in df.index:
                x_idx = df.index.get_loc(pd.to_datetime(event_time))
                price_level = to_float_or_none(event.get('price_level'))
                event_type = event.get('event_type')
                direction = event.get('direction')

                if price_level is None or pd.isna(price_level):
                    logger.warning(f"CHART_GEN: Skipping MS event {event_type} due to None/NaN price_level: {price_level}.")
                    continue

                marker = ''
                color = 'gray'
                text_label = ''
                y_offset_px = 0

                if event_type == 'Swing High':
                    marker = 'v'
                    color = 'darkorange'
                    text_label = 'SH'
                    y_offset_px = 5
                elif event_type == 'Swing Low':
                    marker = '^'
                    color = 'darkgreen'
                    text_label = 'SL'
                    y_offset_px = -5
                elif event_type == 'Break of Structure':
                    marker = 'o'
                    color = 'green' if direction == 'Bullish' else 'red'
                    text_label = 'BOS'
                    ax1.axhline(y=price_level, color=color, linestyle=':', linewidth=0.7, alpha=0.7)
                    y_offset_px = 5 if direction == 'Bullish' else -5
                elif event_type == 'Change of Character':
                    marker = 'X'
                    color = 'blue' if direction == 'Bullish' else 'purple'
                    text_label = 'CHoCH'
                    ax1.axhline(y=price_level, color=color, linestyle=':', linewidth=0.7, alpha=0.7)
                    y_offset_px = 5 if direction == 'Bullish' else -5

                if marker:
                    ax1.plot(x_idx, price_level, marker=marker, color=color, markersize=8, zorder=5)
                    ms_plotted_count += 1
                    if ms_labels_count < max_ms_labels:
                        y_offset_abs = (ax1.get_ylim()[1] - ax1.get_ylim()[0]) * (y_offset_px / 400.0)
                        ax1.text(x_idx, price_level + y_offset_abs,
                                text_label,
                                fontsize=7, color=color, ha='center', va='center')
                        ms_labels_count += 1
                    logger.debug(f"CHART_GEN: Plotting MS Event: {event_type} {direction} at {price_level:.5f} (plotted {ms_plotted_count}/{max_ms_to_plot}).")
            else:
                logger.warning(f"CHART_GEN: MS event at {to_iso_format_or_none(event_time)} not found in df.index. Skipping.")


    if liquidity_zones_data:
        liquidity_zones_data_sorted = sorted(liquidity_zones_data, key=lambda x: x.get('formation_time_utc') if x.get('formation_time_utc') else datetime.min, reverse=True)
        max_liq_labels = 2
        liq_labels_count = 0
        max_liq_to_plot = 8
        liq_plotted_count = 0

        for zone in liquidity_zones_data_sorted:
            if liq_plotted_count >= max_liq_to_plot:
                break

            price_level = to_float_or_none(zone.get('price_level'))
            zone_type = zone.get('zone_type')

            if price_level is None or pd.isna(price_level):
                logger.warning(f"CHART_GEN: Skipping Liquidity Zone {zone.get('id')} due to None/NaN price_level: {price_level}.")
                continue

            if not zone.get('is_tapped') and price_level >= ax1.get_ylim()[0] and price_level <= ax1.get_ylim()[1]:
                color = 'darkgoldenrod' if zone_type == 'Equal Highs' else 'darkcyan'
                linestyle = '-.'
                ax1.axhline(y=price_level, color=color, linestyle=linestyle, linewidth=1.0, alpha=0.7)
                liq_plotted_count += 1
                if liq_labels_count < max_liq_labels:
                    text_x_pos = df.index.get_loc(df.index[-1]) + 2
                    ax1.text(text_x_pos, price_level, zone_type.replace('Equal ', 'E').replace('Highs', 'H').replace('Lows', 'L'),
                            verticalalignment='center', horizontalalignment='left', color=color, fontsize=8, weight='bold',
                            bbox=dict(facecolor='white', alpha=0.7, edgecolor='none', pad=1))
                    liq_labels_count += 1
                logger.debug(f"CHART_GEN: Plotting Liquidity Zone: {zone_type} at {price_level:.5f} (plotted {liq_plotted_count}/{max_liq_to_plot}).")


    if fibonacci_levels_data:
        fibonacci_levels_data_filtered = [fib for fib in fibonacci_levels_data if fib.get('ratio') in [Decimal('0.236'), Decimal('0.382'), Decimal('0.5'), Decimal('0.618'), Decimal('0.786'), Decimal('1.0')]]
        fibonacci_levels_data_sorted = sorted(fibonacci_levels_data_filtered, key=lambda x: x.get('start_time_ref_utc') if x.get('start_time_ref_utc') else datetime.min, reverse=True)
        
        max_fib_labels_per_fibo_set = 1
        labeled_fib_sets_count = {}

        max_fib_to_plot = 10
        fib_plotted_count = 0

        for fib in fibonacci_levels_data_sorted:
            if fib_plotted_count >= max_fib_to_plot:
                break

            price_level = to_float_or_none(fib.get('price_level'))
            ratio = to_float_or_none(fib.get('ratio'))
            
            fibo_set_key = (to_float_or_none(fib.get('high_price_ref')), to_float_or_none(fib.get('low_price_ref')))

            if price_level is None or pd.isna(price_level):
                logger.warning(f"CHART_GEN: Skipping Fibonacci Level {fib.get('id')} due to None/NaN price_level: {price_level}.")
                continue

            if fib.get('is_active') and price_level >= ax1.get_ylim()[0] and price_level <= ax1.get_ylim()[1]:
                color = 'gray' if ratio == 0.5 else 'lightgray'
                linestyle = ':'
                linewidth = 0.7 if ratio != 0.5 else 1.0
                ax1.axhline(y=price_level, color=color, linestyle=linestyle, linewidth=linewidth, alpha=0.7)
                fib_plotted_count += 1

                current_label_count_for_set = labeled_fib_sets_count.get(fibo_set_key, 0)
                if current_label_count_for_set < max_fib_labels_per_fibo_set:
                    text_x_pos = df.index.get_loc(df.index[0]) - 5
                    ax1.text(text_x_pos, price_level, f"Fib {ratio:.3f}",
                            verticalalignment='center', horizontalalignment='right', color='dimgray', fontsize=7,
                            bbox=dict(facecolor='white', alpha=0.7, edgecolor='none', pad=1))
                    labeled_fib_sets_count[fibo_set_key] = current_label_count_for_set + 1
                logger.debug(f"CHART_GEN: Plotting Fibonacci Level: Fib {ratio:.3f} at {price_level:.5f} (plotted {fib_plotted_count}/{max_fib_to_plot}).")
            elif not fib.get('is_active'):
                logger.debug(f"CHART_GEN: Fibonacci Level Fib {to_float_or_none(fib.get('ratio'))} at {to_float_or_none(fib.get('price_level'))} skipped because it is not active.")


    if ma_data:
        if any(isinstance(ma_series_obj, pd.Series) and not ma_series_obj.empty for ma_series_obj in ma_data.values()):
            ax1.legend(loc='best', fontsize=10)

    plt.tight_layout()

    filename = f"{filename or f'{symbol}_{timeframe}_candlestick_{datetime.now().strftime('%Y%m%d%H%M%S')}'}.png"
    filepath = os.path.join(CHART_OUTPUT_DIR, filename)
    try:
        plt.savefig(filepath, dpi=100)
        logger.info(f"CHART_GEN: Candlestick chart saved successfully to: {filepath}")
    except Exception as e:
        logger.error(f"CHART_GEN: Failed to save candlestick chart to {filepath}: {e}", exc_info=True)
        if "Permission denied" in str(e):
            logger.error("HINT: This is likely a folder permission issue. Ensure the bot has write access to the 'charts_output' folder.")
        elif "No space left on device" in str(e):
            logger.error("HINT: Disk is full. Free up some space.")
        elif "float() argument must be a string or a real number, not 'NoneType'" in str(e):
            logger.error("HINT: Plotting data issue! Some values passed to Matplotlib might be None/NaN. Check data source and conversions before plotting.")
        filepath = None
    finally:
        plt.close(fig)

    return filepath
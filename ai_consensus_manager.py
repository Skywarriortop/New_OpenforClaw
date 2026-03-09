import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
import pandas as pd
import talib 
import utils 
import time 
import re   

import database_manager
from config import config
import market_data_processor
import notification_service
import auto_trade_manager
import data_updater 
import mt5_connector 
from sqlalchemy.exc import OperationalError 
from utils import to_float_or_none, to_iso_format_or_none
import fundamental_data_service
logger = logging.getLogger(__name__)

_last_notified_signal_consensus = None
_last_notified_signal_time = None
_NOTIFICATION_COOLDOWN_SECONDS = 3600 * 4 

# Mapping dampak ke emoji
impact_emoji = {
    "Low": "⚪",
    "Medium": "🟡",
    "High": "🔴"
}


# ====================================================================
# DEFINISI FUNGSI-FUNGSI PEMBANTU UNTUK FORMATTING NOTIFIKASI
# ====================================================================

def _format_price(price):
    if price is None:
        return "N/A"
    price_dec = Decimal(str(price))

    if price_dec >= Decimal('100.00'):
        return f"{float(price_dec):.2f}"
    else:
        return f"{float(price_dec):.5f}"


def _format_pip_value(value):
    """Fungsi bantu untuk memformat nilai pip menjadi string."""
    if value is None:
        return "N/A"
    value_dec = Decimal(str(value))
    return f"{int(round(value_dec))}"


def _get_report_header(symbol, current_price, current_rsi):
    header_lines = [
        f"📋 *Trading Plan {symbol}* ",
        f"Harga Saat Ini: `{_format_price(current_price)}`  "
    ]

    rsi_status_emoji = ""
    rsi_level_text = ""
    if current_rsi is not None:
        rsi_level_text = f"RSI (H1): `{_format_price(current_rsi)}`"
        if current_rsi > config.AIAnalysts.RSI_OVERBOUGHT:
            rsi_status_emoji = "⚠️ *Overbought*"
        elif current_rsi < config.AIAnalysts.RSI_OVERSOLD:
            rsi_status_emoji = "⚠️ *Oversold*"

    if rsi_level_text:
        header_lines.append(f"{rsi_level_text} {rsi_status_emoji}")
    header_lines.append("\n---")
    return header_lines


def _get_entry_strategy_section(scenario_type, entry_price, sl_price, tp_prices, confirmations_list, pip_unit_in_dollar):
    """Mengumpulkan dan memformat bagian 'Strategi Entry'."""
    section_lines = []
    if scenario_type != "HOLD":
        section_lines.append("🧭 **Strategi Entry**")
        
        entry_emoji = "🔻" if scenario_type == "SELL" else "🔺"
        section_lines.append(f"{entry_emoji} **{scenario_type} di `${_format_price(entry_price)}`** ")
        
        if sl_price is not None and entry_price is not None:
            entry_price_dec = Decimal(str(entry_price))
            sl_price_dec = Decimal(str(sl_price))
            pip_unit_in_dollar_dec = Decimal(str(pip_unit_in_dollar))

            sl_pips_display = abs(entry_price_dec - sl_price_dec) / pip_unit_in_dollar_dec if pip_unit_in_dollar_dec != Decimal('0') else Decimal('0')

            sl_indicator_emoji = "🔴"
            pip_text_formatted = f"+{_format_pip_value(sl_pips_display)} pips"
            section_lines.append(f"- **Stop Loss (SL):** `${_format_price(sl_price)}` {sl_indicator_emoji} `{pip_text_formatted}`  ")
        else:
            section_lines.append(f"- **Stop Loss (SL):** N/A {sl_indicator_emoji}")
        
        section_lines.append("- **Take Profit (TP):** ")
        if tp_prices:
            for i, tp_price in enumerate(tp_prices):
                if tp_price is not None and entry_price is not None:
                    tp_price_dec = Decimal(str(tp_price))
                    pip_unit_in_dollar_dec = Decimal(str(pip_unit_in_dollar))
                    tp_pips_display = abs(tp_price_dec - entry_price_dec) / pip_unit_in_dollar_dec if pip_unit_in_dollar_dec != Decimal('0') else Decimal('0')
                    
                    pip_prefix = "+" if scenario_type == "BUY" else "-"
                    pip_text_formatted = f"{pip_prefix}{_format_pip_value(tp_pips_display)} pips"
                    
                    section_lines.append(f"  • TP{i+1}: `${_format_price(tp_price)}` 🎯 `{pip_text_formatted}`  ")
        else:
            section_lines.append(f"  • TP: N/A 🎯  ")
            
        if confirmations_list:
            section_lines.append("\n📌 **Konfirmasi:** ")
            for conf in (confirmations_list or []):
                cleaned_conf = conf.replace('\\(', '(').replace('\\)', ')') \
                               .replace('\\.', '.').replace('\\-', '-') \
                               .replace('\\*', '*')
                
                if "RSI Overbought" in cleaned_conf or "RSI Oversold" in cleaned_conf:
                    processed_conf = re.sub(r"(\(>70\))", r"\1", cleaned_conf)
                    processed_conf = re.sub(r"(\(<30\))", r"\1", processed_conf)
                    section_lines.append(f"- ✅ {processed_conf}  ")
                elif "Harga mendekati zona" in cleaned_conf:
                    prices_in_conf_str = re.findall(r'\$(\d+\.\d+)', cleaned_conf)
                    if prices_in_conf_str:
                        prices_dec = [Decimal(p) for p in prices_in_conf_str]
                        min_price = min(prices_dec)
                        max_price = max(prices_dec)
                        if len(prices_dec) > config.Telegram.NOTIF_MAX_LEVELS_PER_TYPE_PER_TF:
                            prices_to_display = sorted(prices_dec, key=lambda x: abs(x - Decimal(str(entry_price))))
                            formatted_prices = ", ".join([f"`${_format_price(p)}`" for p in prices_to_display[:config.Telegram.NOTIF_MAX_LEVELS_PER_TYPE_PER_TF]])
                            section_lines.append(f"- ✅ Harga mendekati zona {'beli' if 'beli' in cleaned_conf else 'jual'} H1 (terdekat: {formatted_prices}, ...).  ")
                        else:
                            formatted_prices = ", ".join([f"`${_format_price(p)}`" for p in prices_dec])
                            section_lines.append(f"- ✅ Harga mendekati zona {'beli' if 'beli' in cleaned_conf else 'jual'} H1 ({formatted_prices}).  ")
                    else:
                        section_lines.append(f"- ✅ {cleaned_conf}  ")
                else: 
                    section_lines.append(f"- ✅ {cleaned_conf}  ")
        else:
            section_lines.append("Tidak ada konfirmasi.")
        
    else: 
        section_lines.append("\n**Status**: HOLD")
        section_lines.append(f"Alasan: Tidak ada setup trading yang jelas.")
    section_lines.append("\n---")
    return section_lines

def _format_tf_levels(tf_name, tf_data, current_price):
    lines = []
    logger.debug(f"\n>>> DEBUG PRINT: Memulai _format_tf_levels untuk TF: {tf_name}. tf_data tersedia: {bool(tf_data)} <<<")

    try:
        # Swing
        try:
            swing_high = tf_data.get('swing_high')
            swing_low = tf_data.get('swing_low')

            logger.debug(f"[_format_tf_levels] {tf_name} Swing High: {to_float_or_none(swing_high)}, Swing Low: {to_float_or_none(swing_low)}")

            if swing_high is not None and swing_low is not None:
                lines.append(f"- **Swing:** High `{_format_price(swing_high)}` / Low `{_format_price(swing_low)}`  ")
            elif swing_high is not None:
                lines.append(f"- **Swing:** High `{_format_price(swing_high)}` / Low N/A  ")
            elif swing_low is not None:
                lines.append(f"- **Swing:** High N/A / Low `{_format_price(swing_low)}`  ")
            else:
                lines.append(f"- **Swing:** N/A  ")
        except Exception as e:
            logger.error(f"[_format_tf_levels] ERROR memproses Swing untuk {tf_name}: {e}", exc_info=True)
            lines.append("- **Swing:** Error. Cek Log.  ")

        # Golden Fibo
        fib_levels_data = tf_data.get('fib_levels', [])
        logger.debug(f"[_format_tf_levels] {tf_name} Fibo data ditemukan: {len(fib_levels_data)}")
        try:
            if fib_levels_data:
                fibs_to_display_filtered = [
                    f for f in fib_levels_data if f.get('ratio') is not None and f.get('ratio') in [
                        Decimal('0.236'), Decimal('0.382'), Decimal('0.5'),
                        Decimal('0.618'), Decimal('0.786'), Decimal('1.0')
                    ]
                ]
                sorted_fib_levels_for_display = sorted(
                    fibs_to_display_filtered,
                    key=lambda x: Decimal(str(x['ratio'])),
                    reverse=True
                )

                notif_max_fibo_per_tf = config.Telegram.NOTIF_MAX_FIBO_PER_TF
                fibs_final_display = sorted_fib_levels_for_display
                if notif_max_fibo_per_tf > 0:
                    fibs_final_display = sorted_fib_levels_for_display[:notif_max_fibo_per_tf]

                lines.append("- **Golden Fibo:** ")
                for fib_lvl_data in fibs_final_display:
                    fib_ratio_float = to_float_or_none(fib_lvl_data.get('ratio', Decimal('0'))) * 100
                    fib_price = fib_lvl_data.get('price_level')
                    if fib_price is not None:
                        lines.append(f"  • {fib_ratio_float:.1f}% = `{_format_price(fib_price)}`  ")
                    else:
                        lines.append(f"  • {fib_ratio_float:.1f}% = N/A  ")
            else:
                lines.append("- **Golden Fibo:** N/A  ")
        except Exception as e:
            logger.error(f"[_format_tf_levels] ERROR memproses Golden Fibo untuk {tf_name}: {e}", exc_info=True)
            lines.append("- **Golden Fibo:** Error. Cek Log.  ")

        # FVG Bearish
        fvgs_bearish = [fvg for fvg in tf_data.get('fvgs', []) if fvg['type'] == 'Bearish Imbalance']
        logger.debug(f"[_format_tf_levels] {tf_name} FVG Bearish data ditemukan: {len(fvgs_bearish)}")
        try:
            if fvgs_bearish:
                valid_fvgs_bearish_for_sort = [f for f in fvgs_bearish if f.get('fvg_top_price') is not None]
                sorted_fvgs_bearish = sorted(valid_fvgs_bearish_for_sort, key=lambda x: abs(Decimal(str(x['fvg_top_price'])) - current_price))
                fvgs_bearish_display = sorted_fvgs_bearish[:config.AIAnalysts.MAX_FVG_DISPLAY]

                fvg_bearish_str = ", ".join([f"`${_format_price(fvg['fvg_top_price'])}`" for fvg in fvgs_bearish_display])
                lines.append(f"- **FVG Bearish:** {fvg_bearish_str}  ")
            else:
                lines.append(f"- **FVG Bearish:** N/A  ")
        except Exception as e:
            logger.error(f"[_format_tf_levels] ERROR memproses FVG Bearish untuk {tf_name}: {e}", exc_info=True)
            lines.append("- **FVG Bearish:** Error. Cek Log.  ")

        # FVG Bullish
        fvgs_bullish = [fvg for fvg in tf_data.get('fvgs', []) if fvg['type'] == 'Bullish Imbalance']
        logger.debug(f"[_format_tf_levels] {tf_name} FVG Bullish data ditemukan: {len(fvgs_bullish)}")
        try:
            if fvgs_bullish:
                valid_fvgs_bullish_for_sort = [f for f in fvgs_bullish if f.get('fvg_bottom_price') is not None]
                sorted_fvgs_bullish = sorted(valid_fvgs_bullish_for_sort, key=lambda x: abs(Decimal(str(x['fvg_bottom_price'])) - current_price))
                fvgs_bullish_display = sorted_fvgs_bullish[:config.AIAnalysts.MAX_FVG_DISPLAY]

                fvg_bullish_str = ", ".join([f"`${_format_price(fvg['fvg_bottom_price'])}`" for fvg in fvgs_bullish_display])
                lines.append(f"- **FVG Bullish:** {fvg_bullish_str}  ")
            else:
                lines.append(f"- **FVG Bullish:** N/A  ")
        except Exception as e:
            logger.error(f"[_format_tf_levels] ERROR memproses FVG Bullish untuk {tf_name}: {e}", exc_info=True)
            lines.append("- **FVG Bullish:** Error. Cek Log.  ")

        # OB Bullish
        obs_bullish = tf_data.get('obs_bullish', [])
        logger.debug(f"[_format_tf_levels] {tf_name} OB Bullish data ditemukan: {len(obs_bullish)}")
        try:
            if obs_bullish:
                valid_obs_bullish_for_sort = [ob for ob in obs_bullish if ob.get('ob_bottom_price') is not None]
                sorted_obs_bullish = sorted(valid_obs_bullish_for_sort, key=lambda x: abs(Decimal(str(x['ob_bottom_price'])) - current_price))
                obs_bullish_display = sorted_obs_bullish[:config.AIAnalysts.MAX_OB_DISPLAY]

                ob_bullish_str = ", ".join([f"`${_format_price(ob['ob_bottom_price'])}`" for ob in obs_bullish_display])
                lines.append(f"- **OB Bullish:** {ob_bullish_str}  ")
            else:
                lines.append(f"- **OB Bullish:** N/A  ")
        except Exception as e:
            logger.error(f"[_format_tf_levels] ERROR memproses OB Bullish untuk {tf_name}: {e}", exc_info=True)
            lines.append("- **OB Bullish:** Error. Cek Log.  ")

        # OB Bearish
        obs_bearish = tf_data.get('obs_bearish', [])
        logger.debug(f"[_format_tf_levels] {tf_name} OB Bearish data ditemukan: {len(obs_bearish)}")
        try:
            if obs_bearish:
                valid_obs_bearish_for_sort = [ob for ob in obs_bearish if ob.get('ob_top_price') is not None]
                sorted_obs_bearish = sorted(valid_obs_bearish_for_sort, key=lambda x: abs(Decimal(str(x['ob_top_price'])) - current_price))
                obs_bearish_display = sorted_obs_bearish[:config.AIAnalysts.MAX_OB_DISPLAY]

                ob_bearish_str = ", ".join([f"`${_format_price(ob['ob_top_price'])}`" for ob in obs_bearish_display])
                lines.append(f"- **OB Bearish:** {ob_bearish_str}  ")
            else:
                lines.append(f"- **OB Bearish:** N/A  ")
        except Exception as e:
            logger.error(f"[_format_tf_levels] ERROR memproses OB Bearish untuk {tf_name}: {e}", exc_info=True)
            lines.append("- **OB Bearish:** Error. Cek Log.  ")

        # Key Level (BoS/ChoCh/Yearly High/Low)
        key_level_data_for_tf = [e for e in tf_data.get('key_level', []) if e['event_type'] in ['Change of Character', 'Break of Structure', 'Yearly High', 'Yearly Low']]
        logger.debug(f"[_format_tf_levels] {tf_name} Key Level data ditemukan: {len(key_level_data_for_tf)}")
        try:
            key_level_str = "N/A"
            if key_level_data_for_tf:
                valid_key_levels_for_sort = [e for e in key_level_data_for_tf if e.get('price_level') is not None and e.get('event_time_utc') is not None]
                key_level_data_for_tf_sorted = sorted(valid_key_levels_for_sort, key=lambda x: x['event_time_utc'], reverse=True)
                key_levels_display = key_level_data_for_tf_sorted[:config.AIAnalysts.MAX_KEY_LEVEL_DISPLAY]

                key_level_str = ", ".join([f"`${_format_price(e['price_level'])}`" for e in key_levels_display])

            lines.append(f"- **Key Level:** {key_level_str}  ")
        except Exception as e:
            logger.error(f"[_format_tf_levels] ERROR memproses Key Level untuk {tf_name}: {e}", exc_info=True)
            lines.append("- **Key Level:** Error. Cek Log.  ")

        # Resistance
        resistances = tf_data.get('resistances', [])
        logger.debug(f"[_format_tf_levels] {tf_name} Resistance data ditemukan: {len(resistances)}")
        try:
            if resistances:
                valid_resistances_for_sort = [r for r in resistances if r.get('price_level') is not None]
                sorted_resistances = sorted(valid_resistances_for_sort, key=lambda x: abs(Decimal(str(x['price_level'])) - current_price))
                resistances_display = sorted_resistances[:config.AIAnalysts.MAX_SR_DISPLAY]

                resistance_str = ", ".join([f"`${_format_price(res['price_level'])}`" for res in resistances_display])
                lines.append(f"- **Resistance:** {resistance_str}  ")
            else:
                lines.append(f"- **Resistance:** N/A  ")
        except Exception as e:
            logger.error(f"[_format_tf_levels] ERROR memproses Resistance untuk {tf_name}: {e}", exc_info=True)
            lines.append("- **Resistance:** Error. Cek Log.  ")

        # Support
        supports = tf_data.get('supports', [])
        logger.debug(f"[_format_tf_levels] {tf_name} Support data ditemukan: {len(supports)}")
        try:
            if supports:
                valid_supports_for_sort = [s for s in supports if s.get('price_level') is not None]
                sorted_supports = sorted(valid_supports_for_sort, key=lambda x: abs(Decimal(str(x['price_level'])) - current_price))
                supports_display = sorted_supports[:config.AIAnalysts.MAX_SR_DISPLAY]

                support_str = ", ".join([f"`${_format_price(sup['price_level'])}`" for sup in supports_display])
                lines.append(f"- **Support:** {support_str}  ")
            else:
                lines.append(f"- **Support:** N/A  ")
        except Exception as e:
            logger.error(f"[_format_tf_levels] ERROR memproses Support untuk {tf_name}: {e}", exc_info=True)
            lines.append("- **Support:** Error. Cek Log.  ")

        # PENAMBAHAN UNTUK LIQUIDITY ZONES
        if config.AIAnalysts.MAX_LIQUIDITY_DISPLAY != 0:
            liquidity_zones = tf_data.get('liquidity_zones', [])
            logger.debug(f"[_format_tf_levels] {tf_name} Liquidity Zones data ditemukan: {len(liquidity_zones)}")
            try:
                if liquidity_zones:
                    display_liquidity = []
                    sorted_liquidity_zones = sorted(liquidity_zones, key=lambda x: x.get('tap_time_utc') if x.get('tap_time_utc') else datetime.min, reverse=True)

                    for lz in sorted_liquidity_zones[:config.AIAnalysts.MAX_LIQUIDITY_DISPLAY]:
                        lz_type = lz.get('zone_type')
                        lz_price = to_float_or_none(lz.get('price_level'))
                        lz_tapped = lz.get('is_tapped')
                        lz_tap_time = lz.get('tap_time_utc')

                        if lz_price is not None:
                            status = "Tapped" if lz_tapped else "Untapped"
                            tap_time_str = f" ({to_iso_format_or_none(lz_tap_time).split('T')[1][:5]})" if lz_tapped and lz_tap_time else ""
                            display_liquidity.append(f"{lz_type} `{lz_price:.2f}` [{status}{tap_time_str}]")
                    if display_liquidity:
                        lines.append(f"- **Liquidity Zones:** {'; '.join(display_liquidity)} ")
                    else:
                        lines.append(f"- **Liquidity Zones:** N/A ")
                else:
                    lines.append(f"- **Liquidity Zones:** N/A ")
            except Exception as e:
                logger.error(f"[_format_tf_levels] ERROR memproses Liquidity Zones untuk {tf_name}: {e}", exc_info=True)
                lines.append("- **Liquidity Zones:** Error. Cek Log. ")
        else:
            logger.debug(f"[_format_tf_levels] {tf_name} Liquidity Zones section display disabled by config.")

        # PENAMBAHAN UNTUK DIVERGENCE
        if config.AIAnalysts.MAX_DIVERGENCE_DISPLAY != 0:
            divergences = tf_data.get('divergences', [])
            logger.debug(f"[_format_tf_levels] {tf_name} Divergence data ditemukan: {len(divergences)}")
            try:
                if divergences:
                    display_divergences = []
                    sorted_divergences = sorted(divergences, key=lambda x: x.get('price_point_time_utc') if x.get('price_point_time_utc') else datetime.min, reverse=True)

                    for div in sorted_divergences[:config.AIAnalysts.MAX_DIVERGENCE_DISPLAY]:
                        div_type = div.get('divergence_type')
                        ind_type = div.get('indicator_type')
                        price1 = to_float_or_none(div.get('price_level_1'))
                        price2 = to_float_or_none(div.get('price_level_2'))
                        ind1 = to_float_or_none(div.get('indicator_value_1'))
                        ind2 = to_float_or_none(div.get('indicator_value_2'))
                        div_time = div.get('price_point_time_utc')

                        if price1 is not None and price2 is not None and ind1 is not None and ind2 is not None:
                            display_divergences.append(f"{div_type} ({ind_type}) P: `{price1:.2f}`->`{price2:.2f}` I: `{ind1:.2f}`->`{ind2:.2f}` ({to_iso_format_or_none(div_time).split('T')[1][:5]})")
                    if display_divergences:
                        lines.append(f"- **Divergences:** {'; '.join(display_divergences)} ")
                    else:
                        lines.append(f"- **Divergences:** N/A ")
                else:
                    lines.append(f"- **Divergences:** N/A ")
            except Exception as e:
                logger.error(f"[_format_tf_levels] ERROR memproses Divergence untuk {tf_name}: {e}", exc_info=True)
                lines.append("- **Divergences:** Error. Cek Log. ")
        else:
            logger.debug(f"[_format_tf_levels] {tf_name} Divergence section display disabled by config.")

    except Exception as e_outer:
        logger.critical(f"[_format_tf_levels] ERROR KRITIS di _format_tf_levels untuk {tf_name}: {e_outer}", exc_info=True)
        return []

    return lines


def _get_key_levels_section(market_data_by_tf, current_price):
    """Mengumpulkan dan memformat bagian 'Key Levels, Fibo, FVG, OB, Support & Resistance' untuk semua TF."""
    section_lines = []
    section_lines.append("📐 *Key Levels, Fibo, FVG, OB, Support & Resistance*")
    
    display_timeframes = ["D1", "H4", "H1", "M15", "M5"]

    for tf_display in display_timeframes:
        tf_data = market_data_by_tf.get(tf_display)
        
        if tf_data and (not tf_data['df_candles'].empty or 
                        tf_data.get('swing_high') is not None or 
                        tf_data.get('swing_low') is not None or 
                        tf_data.get('fib_levels') or 
                        tf_data.get('fvgs') or 
                        tf_data.get('obs_bullish') or 
                        tf_data.get('obs_bearish') or 
                        tf_data.get('key_level') or 
                        tf_data.get('liquidity_zones') or 
                        tf_data.get('resistances') or 
                        tf_data.get('supports') or
                        tf_data.get('divergences')):
            
            section_lines.append(f"\n 🔷 {tf_display}  ")
            section_lines.extend(_format_tf_levels(tf_display, tf_data, current_price))
        else:
            logger.debug(f"Tidak ada data relevan untuk menampilkan level kunci di TF: {tf_display}.")      
    section_lines.append("\n---")
    return section_lines


def _get_notes_section(scenario_notes):
    section_lines = []
    if scenario_notes:
        section_lines.append(f"🧠 **Catatan**")
        section_lines.append(f"```\n{scenario_notes}\n```")
    return section_lines

def get_consensus_recommendation(analyst_results: list, current_market_price: Decimal) -> dict: 
    """
    Mengambil hasil dari berbagai analis AI, memprosesnya untuk mencapai konsensus,
    dan menghasilkan rekomendasi trading akhir.
    """
    if not analyst_results:
        logger.warning("Tidak ada hasil analisis yang berhasil dari analis untuk membentuk konsensus.")
        return {"action": "HOLD", "summary": "Tidak ada konsensus yang dapat dibentuk.", "ai_confidence": "Low", "potential_direction": "Undefined"}

    valid_results = [
        res for res in analyst_results
        if "error" not in res and res.get('trading_recommendation', {}).get('action') in ["BUY", "SELL", "HOLD"]
    ]

    if not valid_results:
        logger.warning("Tidak ada analis yang memberikan rekomendasi yang valid.")
        return {"action": "HOLD", "summary": "Tidak ada rekomendasi valid dari analis.", "ai_confidence": "Low", "potential_direction": "Undefined"}

    actions = [res['trading_recommendation']['action'] for res in valid_results]

    buy_count = actions.count("BUY")
    sell_count = actions.count("SELL")
    hold_count = actions.count("HOLD")

    total_votes = buy_count + sell_count + hold_count

    consensus_action = "HOLD"
    consensus_summary = "Tidak ada konsensus yang kuat."
    consensus_direction = "Undefined"

    min_analysts_for_action = config.AIAnalysts.AI_MIN_ANALYSTS_FOR_CONSENSUS

    if buy_count >= min_analysts_for_action and buy_count > sell_count:
        consensus_action = "BUY"
        consensus_summary = "Beberapa analis merekomendasikan BUY."
        consensus_direction = "Bullish"
    elif sell_count >= min_analysts_for_action and sell_count > buy_count:
        consensus_action = "SELL"
        consensus_summary = "Beberapa analis merekomendasikan SELL."
        consensus_direction = "Bearish"
    elif hold_count >= buy_count and hold_count >= sell_count:
        consensus_action = "HOLD"
        consensus_summary = "Mayoritas atau setidaknya tidak ada aksi yang jelas direkomendasikan."
        consensus_direction = "Sideways"

    high_confidence_analysts = [res for res in valid_results if res.get('ai_confidence') == 'High']
    if len(high_confidence_analysts) >= 2:
        consensus_confidence = "High"
    elif len(valid_results) >= 3 and (buy_count >= 2 or sell_count >= 2):
         consensus_confidence = "Medium"
    else:
        consensus_confidence = "Low"

    entries = [Decimal(str(res['trading_recommendation']['entry_price_suggestion'])) for res in valid_results if res['trading_recommendation']['action'] == consensus_action and res['trading_recommendation'].get('entry_price_suggestion')]
    sls = [Decimal(str(res['trading_recommendation']['stop_loss_suggestion'])) for res in valid_results if res['trading_recommendation']['action'] == consensus_action and res['trading_recommendation'].get('stop_loss_suggestion')]
    tps = [Decimal(str(res['trading_recommendation']['take_profit_suggestion'])) for res in valid_results if res['trading_recommendation']['action'] == consensus_action and res['trading_recommendation'].get('take_profit_suggestion')]

    avg_entry = sum(entries) / len(entries) if entries else current_market_price
    avg_sl = sum(sls) / len(sls) if sls else None
    avg_tp = sum(tps) / len(tps) if tps else None

    emoji = "ℹ️"
    if consensus_action == "BUY": emoji = "📈"
    elif consensus_action == "SELL": emoji = "📉"
    elif consensus_action == "HOLD": emoji = "↔️"

    final_reasoning = f"{emoji} Konsensus: {consensus_summary} Aksi {consensus_action} didukung oleh {total_votes} total suara analis."

    return {
        "summary": consensus_summary,
        "potential_direction": consensus_direction,
        "trading_recommendation": {
            "action": consensus_action,
            "timeframe_for_trade": "H1",
            "entry_price_suggestion": avg_entry,
            "stop_loss_suggestion": avg_sl,
            "take_profit_suggestion": avg_tp,
            "reasoning": final_reasoning
        },
        "ai_confidence": consensus_confidence,
        "analyst_id": "Consensus_Executor"
    }


def analyze_market_and_get_consensus(current_price: Decimal = None, current_price_timestamp: datetime = None):
    """
    Fungsi utama yang akan dipanggil oleh scheduler untuk menjalankan semua analis AI,
    mendapatkan konsensus, dan memicu perdagangan/notifikasi.
    """
    logger.info("Memulai siklus analisis AI (multiple analis & konsensus)...")

    if current_price is None or current_price <= Decimal('0.0'):
        current_market_price_obj, current_market_price_timestamp_obj = data_updater.get_latest_realtime_price()
        if current_market_price_obj is None or current_market_price_obj <= Decimal('0.0'):
            logger.error("Gagal mendapatkan harga pasar terkini dari data_updater. Melewatkan analisis AI.")
            notification_service.notify_error("Analisis AI Gagal: Tidak ada harga real-time terkini.", "AI Consensus Manager")
            return
        current_price = current_market_price_obj
        current_price_timestamp = current_market_price_timestamp_obj
    
    market_context = ai_analyzer.get_market_context_for_ai(config.TRADING_SYMBOL)

    all_analyst_results = []

    for analyst_id, analyst_conf in config.AIAnalysts.ANALYSIS_CONFIGS.items():
        if analyst_conf["enabled"]:
            logger.info(f"Menjalankan Analis AI: '{analyst_id}'...")
            analyst_result = ai_analyzer.run_single_analyst_ai(
                analyst_id=analyst_id,
                prompt_persona=analyst_conf["persona_prompt"],
                market_context=market_context,
                analyst_conf=analyst_conf
            )
            all_analyst_results.append(analyst_result)

            # HAPUS: Logika jeda waktu yang kaku di sini
            if "error" in analyst_result:
                # Logika backoff sekarang ada di run_single_analyst_ai
                # Kita tetap harus menunggu jika ada kesalahan fatal yang tidak dapat diperbaiki
                logger.error(f"Analis '{analyst_id}' gagal menghasilkan analisis: {analyst_result['error']}")
                notification_service.notify_error(f"Analis AI Gagal ({analyst_id}): {analyst_result['error']}", "AI Analyst Error")
            
            if "error" not in analyst_result:
                for attempt in range(config.System.MAX_RETRIES):
                    try:
                        database_manager.save_ai_analysis_result(
                            symbol=config.TRADING_SYMBOL,
                            timestamp=datetime.now(timezone.utc),
                            summary=analyst_result.get('summary', 'N/A'),
                            potential_direction=analyst_result.get('potential_direction', 'Undefined'),
                            recommendation_action=analyst_result.get('trading_recommendation', {}).get('action', 'HOLD'),
                            entry_price=analyst_result.get('trading_recommendation', {}).get('entry_price_suggestion'),
                            stop_loss=analyst_result.get('trading_recommendation', {}).get('stop_loss_suggestion'),
                            take_profit=analyst_result.get('trading_recommendation', {}).get('take_profit_suggestion'),
                            reasoning=analyst_result.get('reasoning', 'N/A'),
                            ai_confidence=analyst_result.get('ai_confidence', 'Low'),
                            raw_response_json=json.dumps(analyst_result),
                            analyst_id=analyst_id
                        )
                        break
                    except (TypeError, ValueError, OperationalError) as oe:
                        logger.warning(f"Database locked atau data tidak serializable saat menyimpan AI Result (Individu {analyst_id}). Coba lagi dalam {config.System.RETRY_DELAY_SECONDS} detik (Percobaan {attempt + 1}/{config.System.MAX_RETRIES}). Error: {oe}", exc_info=True)
                        time.sleep(config.System.RETRY_DELAY_SECONDS)
                    except Exception as db_e:
                        logger.error(f"Error tak terduga saat menyimpan hasil analisis dari '{analyst_id}' ke database: {db_e}", exc_info=True)
                        notification_service.notify_error(f"Gagal menyimpan analisis AI ({analyst_id}) ke DB: {db_e}", "AI Analyst DB Save")
                        break
                else: 
                    logger.error(f"Gagal menyimpan hasil analisis dari '{analyst_id}' setelah {config.System.MAX_RETRIES} percobaan.")
                    notification_service.notify_error(f"Gagal menyimpan analisis AI ({analyst_id}) ke DB (Retry Gagal).", "AI Analyst DB Save")

                logger.info(f"Hasil analisis dari '{analyst_id}' berhasil disimpan ke DB.")

                notification_service.notify_new_ai_signal(
                    analyst_result,
                    min_confidence=analyst_conf["confidence_threshold"],
                    is_individual_analyst_signal=True
                )

            else:
                logger.error(f"Analis '{analyst_id}' gagal menghasilkan analisis: {analyst_result['error']}")
                notification_service.notify_error(f"Analis AI Gagal ({analyst_id}): {analyst_result['error']}", "AI Analyst Error")
        else:
            logger.info(f"Analis AI: '{analyst_id}' dinonaktifkan. Melewatkan analisis.")

    successful_analyst_results = [res for res in all_analyst_results if "error" not in res]
    if successful_analyst_results:
        logger.info(f"Meneruskan {len(successful_analyst_results)} hasil analis ke AI Pengambil Kesimpulan.")

        if current_price is None or current_price <= Decimal('0.0'):
            logger.error("Gagal mendapatkan harga pasar terkini untuk AI Pengambil Kesimpulan. Melewatkan konsensus.")
            notification_service.notify_error("Gagal mendapatkan harga terkini untuk konsensus AI.", "AI Consensus Manager")
            return
        
        final_recommendation = get_consensus_recommendation(
            successful_analyst_results,
            current_market_price=current_price
        )

        global _last_notified_signal_consensus, _last_notified_signal_time

        if final_recommendation and final_recommendation.get('action') in ["BUY", "SELL", "HOLD"]:
            logger.info(f"AI Pengambil Kesimpulan merekomendasikan: {final_recommendation.get('action')} dengan confidence {final_recommendation.get('ai_confidence')}")

            for attempt in range(config.System.MAX_RETRIES):
                try:
                    database_manager.save_ai_analysis_result(
                        symbol=config.TRADING_SYMBOL,
                        timestamp=datetime.now(timezone.utc),
                        summary=final_recommendation.get('summary', 'Keputusan konsensus.'),
                        potential_direction=final_recommendation.get('potential_direction', 'Undefined'),
                        recommendation_action=final_recommendation.get('action', 'HOLD'),
                        entry_price=final_recommendation.get('trading_recommendation', {}).get('entry_price_suggestion'),
                        stop_loss=final_recommendation.get('trading_recommendation', {}).get('stop_loss_suggestion'),
                        take_profit=final_recommendation.get('trading_recommendation', {}).get('take_profit_suggestion'),
                        reasoning=final_recommendation.get('reasoning', 'Berdasarkan konsensus analis.'),
                        ai_confidence=final_recommendation.get('ai_confidence', 'Low'),
                        raw_response_json=json.dumps(final_recommendation, default=utils._json_default),
                        analyst_id="Consensus_Executor"
                    )
                    break
                except (TypeError, ValueError, OperationalError) as oe:
                    logger.warning(f"Database locked atau data tidak serializable saat menyimpan AI Result (Konsensus). Coba lagi dalam {config.System.RETRY_DELAY_SECONDS} detik (Percobaan {attempt + 1}/{config.System.MAX_RETRIES}). Error: {oe}", exc_info=True)
                    time.sleep(config.System.RETRY_DELAY_SECONDS)
                except Exception as db_e:
                    logger.error(f"Error tak terduga saat menyimpan keputusan konsensus ke DB: {db_e}", exc_info=True)
                    notification_service.notify_error(f"Gagal menyimpan konsensus AI ke DB: {db_e}", "AI Consensus DB Save")
                    break
            else:
                logger.error(f"Gagal menyimpan keputusan konsensus setelah {config.System.MAX_RETRIES} percobaan.")
                notification_service.notify_error(f"Gagal menyimpan konsensus AI ke DB (Retry Gagal).", "AI Consensus DB Save")

            logger.info(f"Keputusan final AI Pengambil Kesimpulan berhasil disimpan ke DB.")

            if config.Trading.auto_trade_enabled and final_recommendation.get('action') in ["BUY", "SELL"]:
                logger.info(f"Auto-trade diaktifkan. Meneruskan rekomendasi konsensus ke auto_trade_manager: {final_recommendation['action']}")

                trade_result = auto_trade_manager.execute_ai_trade(
                    symbol=config.TRADING_SYMBOL,
                    action=final_recommendation['action'],
                    volume=config.Trading.AUTO_TRADE_VOLUME,
                    entry_price=final_recommendation.get('trading_recommendation', {}).get('entry_price_suggestion'),
                    stop_loss=final_recommendation.get('trading_recommendation', {}).get('stop_loss_suggestion'),
                    take_profit=final_recommendation.get('trading_recommendation', {}).get('take_profit_suggestion'),
                    magic_number=config.Trading.AUTO_TRADE_MAGIC_NUMBER,
                    slippage=config.Trading.AUTO_TRADE_SLIPPAGE
                )
                notification_service.notify_trade_execution(trade_result)
            
            else: 
                logger.info("Auto-trade dinonaktifkan atau rekomendasi konsensus bukan BUY/SELL (tetap notifikasi semua action, termasuk HOLD).")
        else:
            logger.warning("AI Pengambil Kesimpulan tidak menghasilkan rekomendasi yang valid.")
    else:
        logger.warning("Tidak ada analis AI yang aktif atau berhasil menghasilkan analisis. Tidak ada konsensus yang dapat dibentuk.")

def run_fundamental_analyst_only(symbol: str, query_time_utc: datetime = None,
                                 days_past: int = 1, days_future: int = 1,
                                 min_impact: str = "Low", include_news_topics: list = None,
                                 include_ai_analysis: bool = True):

    logger.info(f"Memicu analisis fundamental khusus untuk {symbol} pada waktu: {query_time_utc} (AI Analysis: {include_ai_analysis})...")

    fund_service = fundamental_data_service.FundamentalDataService()
    comprehensive_fundamental_data = fund_service.get_comprehensive_fundamental_data(
        days_past=days_past,
        days_future=days_future,
        min_impact_level="Low",
        target_currency="USD",
        include_news_topics=["gold", "usd", "suku bunga", "perang", "fed", "inflation", "market", "economy", "geopolitics", "commodities", "finance", "trade"]
    )

    economic_events_for_ai = [item for item in comprehensive_fundamental_data['economic_calendar']]
    news_articles_for_ai = [item for item in comprehensive_fundamental_data['news_article']]

    market_context_for_funda = {
        "current_time_utc": (query_time_utc or datetime.now(timezone.utc)).isoformat(),
        "market_status": config.MarketData.market_status_data,
        "fundamental_data": {
            "economic_events": economic_events_for_ai,
            "news_articles": news_articles_for_ai
        }
    }

    if include_ai_analysis:
        analyst_id = "Fundamental_Analyst"
        analyst_conf = config.AIAnalysts.ANALYSIS_CONFIGS.get(analyst_id)

        if not analyst_conf or not analyst_conf["enabled"]:
            logger.warning(f"Analis '{analyst_id}' tidak ditemukan atau dinonaktifkan di konfigurasi. Tidak dapat menjalankan analisis fundamental khusus.")
            notification_service.notify_error(f"Analisis Fundamental khusus gagal: '{analyst_id}' dinonaktifkan.", "Funda Analyst Trigger")
            return

        logger.info(f"Menjalankan Analis Fundamental: '{analyst_id}'...")
        analyst_result = ai_analyzer.run_single_analyst_ai(
            analyst_id=analyst_id,
            prompt_persona=analyst_conf["persona_prompt"],
            market_context=market_context_for_funda,
            analyst_conf=analyst_conf
        )

        if "error" not in analyst_result:
            for attempt in range(config.System.MAX_RETRIES):
                try:
                    database_manager.save_ai_analysis_result(
                        symbol=symbol,
                        timestamp=datetime.now(timezone.utc),
                        summary=analyst_result.get('summary', 'N/A'),
                        potential_direction=analyst_result.get('potential_direction', 'Undefined'),
                        recommendation_action=analyst_result.get('trading_recommendation', {}).get('action', 'HOLD'),
                        entry_price=analyst_result.get('trading_recommendation', {}).get('entry_price_suggestion'),
                        stop_loss=analyst_result.get('trading_recommendation', {}).get('stop_loss_suggestion'),
                        take_profit=analyst_result.get('trading_recommendation', {}).get('take_profit_suggestion'),
                        reasoning=analyst_result.get('reasoning', 'N/A'),
                        ai_confidence=analyst_result.get('ai_confidence', 'Low'),
                        raw_response_json=json.dumps(analyst_result, default=utils._json_default),
                        analyst_id=analyst_id
                    )
                    break
                except (TypeError, ValueError, OperationalError) as oe:
                    logger.warning(f"Database locked atau data tidak serializable saat menyimpan AI Result (Funda). Coba lagi dalam {config.System.RETRY_DELAY_SECONDS} detik (Percobaan {attempt + 1}/{config.System.MAX_RETRIES}). Error: {oe}", exc_info=True)
                    time.sleep(config.System.RETRY_DELAY_SECONDS)
                except Exception as db_e:
                    logger.error(f"Error tak terduga saat menyimpan hasil analisis fundamental khusus ke database: {db_e}", exc_info=True)
                    notification_service.notify_error(f"Gagal menyimpan analisis Fundamental ({analyst_id}) ke DB: {db_e}", "Funda Analyst DB Save")
                    break
            else:
                logger.error(f"Gagal menyimpan hasil analisis fundamental setelah {config.System.MAX_RETRIES} percobaan.")
                notification_service.notify_error(f"Gagal menyimpan analisis Fundamental ({analyst_id}) ke DB (Retry Gagal).", "Funda Analyst DB Save")

            logger.info(f"Hasil analisis dari '{analyst_id}' berhasil disimpan ke DB.")
            notification_service.notify_new_ai_signal(
                analyst_result,
                min_confidence=analyst_conf["confidence_threshold"],
                is_individual_analyst_signal=True
            )

        else: 
            logger.error(f"Analis Fundamental gagal menghasilkan analisis: {analyst_result['error']}")
            notification_service.notify_error(f"Analis Fundamental Gagal: {analyst_result['error']}", "Funda Analyst Error")
    else: 
        logger.info("Analisis AI Fundamental dilewati karena 'include_ai_analysis' disetel False.")

def _detect_candlestick_pattern(df_candles: pd.DataFrame, pattern_name: str) -> bool:
    """
    Mendeteksi pola candlestick spesifik menggunakan TA-Lib.
    df_candles harus memiliki kolom 'open_price', 'high_price', 'low_price', 'close_price' (float).
    """
    if df_candles.empty or len(df_candles) < 2:
        return False

    op = df_candles['open_price'].astype(float).values
    hi = df_candles['high_price'].astype(float).values
    lo = df_candles['low_price'].astype(float).values
    cl = df_candles['close_price'].astype(float).values

    pattern_detected = False
    if pattern_name == "BEARISH_ENGULFING":
        pattern_detected = talib.CDLENGULFING(op, hi, lo, cl)[-1] < 0
    elif pattern_name == "BULLISH_ENGULFING":
        pattern_detected = talib.CDLENGULFING(op, hi, lo, cl)[-1] > 0
    elif pattern_name == "SHOOTING_STAR":
        pattern_detected = talib.CDLSHOOTINGSTAR(op, hi, lo, cl)[-1] < 0
    elif pattern_name == "HAMMER":
        pattern_detected = talib.CDLHAMMER(op, hi, lo, cl)[-1] > 0

    return pattern_detected != 0

def analyze_and_notify_scenario(symbol: str, timeframe: str = "H1", current_price: Decimal = None, current_time: datetime = None): # Ini adalah baris yang perlu Anda pastikan sama
    """
    Menganalisis skenario pasar berdasarkan level kunci, swing points, dan RSI,
    lalu mengirimkan notifikasi detail ke Telegram.
    Notifikasi ini akan menampilkan detail per timeframe yang diinginkan.
    Args:
        symbol (str): Simbol trading.
        timeframe (str): Timeframe untuk analisis (default "H1").
        current_price (Decimal, opsional): Harga real-time saat ini. Jika None, akan diambil dari data_updater.
        current_time (datetime, opsional): Waktu real-time saat ini. Jika None, akan diambil dari data_updater.
    """
    logger.info(f"Menganalisis skenario strategi komprehensif untuk {symbol} TF {timeframe}...")
    
    # Ambil harga dan waktu terkini dari data_updater jika belum disediakan
    current_price_from_updater = current_price
    current_timestamp_from_updater = current_time

    if current_price_from_updater is None or current_timestamp_from_updater is None:
        temp_price, temp_timestamp = data_updater.get_latest_realtime_price()
        if temp_price is None or temp_price <= Decimal('0.0') or temp_timestamp is None:
            logger.warning("Scenario Analyzer: Tidak ada harga real-time atau timestamp yang valid dari data_updater. Tidak dapat melakukan analisis skenario.")
            notification_service.notify_error("Analisis Skenario Gagal: Tidak ada harga/timestamp real-time terkini.", "Scenario Analyzer")
            return
        current_price_from_updater = temp_price
        current_timestamp_from_updater = temp_timestamp

    current_price = current_price_from_updater
    current_utc_time = current_timestamp_from_updater

    logger.debug(f"Scenario Analyzer: Harga terkini yang diambil dari data_updater: Price={float(current_price):.5f}, Timestamp={to_iso_format_or_none(current_utc_time)}")


    scenario_type = "HOLD"
    scenario_description = "Harga berada di tengah range, tidak ada skenario trading yang jelas."
    scenario_entry_price = None 
    scenario_sl_price = None
    scenario_tp_prices = []
    confirmations_list = []
    entry_reasons = []
    scenario_notes = ""

    scenario_entry_price_for_calc = current_price 

    symbol_point_value = mt5_connector.get_symbol_point_value(symbol)
    if symbol_point_value is None:
        logger.warning(f"Scenario Analyzer: Nilai point untuk {symbol} tidak ditemukan. Menggunakan default 0.00001.")
        symbol_point_value = Decimal('0.00001')
    else:
        symbol_point_value = Decimal(str(symbol_point_value))
    
    PIP_UNIT_IN_DOLLAR = config.PIP_UNIT_IN_DOLLAR
 

    PROXIMITY_THRESHOLD_PIPS = config.RuleBasedStrategy.STRUCTURE_OFFSET_PIPS
    PROXIMITY_THRESHOLD_VALUE = Decimal(str(PROXIMITY_THRESHOLD_PIPS)) * PIP_UNIT_IN_DOLLAR

    MAX_DISPLAY_LEVELS_PER_TYPE_PER_TF = 3
    MAX_AGE_FOR_LEVEL_DATA_DAYS = 90 

    min_display_time_for_levels = current_utc_time - timedelta(days=MAX_AGE_FOR_LEVEL_DATA_DAYS)

    market_data_by_tf = {}
    
    display_timeframes = ["D1", "H4", "H1", "M15", "M5"]

    candles_limit_for_analysis = {
        "D1": 1500, "H4": 2000, "H1": 3000, "M30": 4000, "M15": 5000, "M5": 2000
    }
    level_fetch_limit = 5000 

    PRICE_RANGE_BUFFER_PERCENTAGE = Decimal('0.10')
    min_price_filter_for_levels = current_price - (current_price * PRICE_RANGE_BUFFER_PERCENTAGE)
    max_price_filter_for_levels = current_price + (current_price * PRICE_RANGE_BUFFER_PERCENTAGE)
    logger.info(f"Scenario Analyzer: Filter harga untuk level ditetapkan: Min={to_float_or_none(min_price_filter_for_levels):.5f}, Max={to_float_or_none(max_price_filter_for_levels):.5f} (Buffer: {to_float_or_none(PRICE_RANGE_BUFFER_PERCENTAGE * 100)}%)")
    

    for tf_to_process in display_timeframes:
        logger.debug(f"Mengambil data untuk TF: {tf_to_process}")
        candles_data = database_manager.get_historical_candles_from_db(
            symbol, tf_to_process,
            limit=candles_limit_for_analysis.get(tf_to_process, 500), 
            order_asc=True, 
            end_time_utc=current_utc_time
        )
        
        if not candles_data:
            logger.warning(f"Tidak ada data candle untuk {symbol} {tf_to_process} untuk analisis skenario komprehensif. Melewatkan TF ini.")
            continue

        df_candles = pd.DataFrame(candles_data)
        df_candles['open_time_utc'] = pd.to_datetime(df_candles['open_time_utc'])
        df_candles = df_candles.set_index('open_time_utc').sort_index()

        df_candles = df_candles.rename(columns={
            'open_price': 'open', 
            'high_price': 'high', 
            'low_price': 'low',
            'close_price': 'close', 
            'tick_volume': 'volume' # Juga rename tick_volume ke volume
        })
        # Pastikan juga konversi ke float untuk kolom harga di sini,
        # karena _calculate_rsi dan SMC internal functions sering mengharapkan float.
        for col in ['open', 'high', 'low', 'close', 'volume']:
            if col in df_candles.columns:
                df_candles[col] = df_candles[col].apply(utils.to_float_or_none) # Menggunakan utils.to_float_or_none
        market_data_by_tf[tf_to_process] = {
            'df_candles': df_candles,
            'swing_high': None, 'swing_low': None, 'fib_levels': [],
            'resistances': [], 'supports': [], 'fvgs': [], 'obs_bullish': [],
            'obs_bearish': [], 'key_level': [], 'liquidity_zones': [],
            'divergences': [] 
        }
        
        latest_swing_high_db = database_manager.get_market_structure_events(
            symbol=symbol, timeframe=tf_to_process, event_type='Swing High',
            start_time_utc=min_display_time_for_levels, limit=1
        )
        latest_swing_low_db = database_manager.get_market_structure_events(
            symbol=symbol, timeframe=tf_to_process, event_type='Swing Low',
            start_time_utc=min_display_time_for_levels, limit=1
        )
        
        market_data_by_tf[tf_to_process]['swing_high'] = latest_swing_high_db[0]['price_level'] if latest_swing_high_db else None
        market_data_by_tf[tf_to_process]['swing_low'] = latest_swing_low_db[0]['price_level'] if latest_swing_low_db else None

        market_data_by_tf[tf_to_process]['fvgs'] = database_manager.get_fair_value_gaps(
            symbol=symbol, timeframe=tf_to_process, is_filled=False, limit=level_fetch_limit,
            start_time_utc=min_display_time_for_levels,
            min_price_level=min_price_filter_for_levels, 
            max_price_level=max_price_filter_for_levels  
        )
        market_data_by_tf[tf_to_process]['obs_bullish'] = database_manager.get_order_blocks(
            symbol=symbol, timeframe=tf_to_process, type='Bullish', is_mitigated=False, limit=level_fetch_limit,
            start_time_utc=min_display_time_for_levels,
            min_price_level=min_price_filter_for_levels, 
            max_price_level=max_price_filter_for_levels  
        )
        market_data_by_tf[tf_to_process]['obs_bearish'] = database_manager.get_order_blocks(
            symbol=symbol, timeframe=tf_to_process, type='Bearish', is_mitigated=False, limit=level_fetch_limit,
            start_time_utc=min_display_time_for_levels,
            min_price_level=min_price_filter_for_levels, 
            max_price_level=max_price_filter_for_levels  
        )
        market_data_by_tf[tf_to_process]['liquidity_zones'] = database_manager.get_liquidity_zones(
            symbol=symbol, timeframe=tf_to_process, is_tapped=False, limit=level_fetch_limit,
            start_time_utc=min_display_time_for_levels,
            min_price_level=min_price_filter_for_levels, 
            max_price_level=max_price_filter_for_levels  
        )
        market_data_by_tf[tf_to_process]['key_level'] = database_manager.get_market_structure_events(
            symbol=symbol, timeframe=tf_to_process, event_type=['Change of Character', 'Break of Structure', 'Yearly High', 'Yearly Low'],
            start_time_utc=min_display_time_for_levels, limit=5 
        )
        market_data_by_tf[tf_to_process]['resistances'] = database_manager.get_support_resistance_levels(
            symbol=symbol, timeframe=tf_to_process, is_active=True, level_type='Resistance', limit=level_fetch_limit,
            start_time_utc=min_display_time_for_levels,
            min_price_level=min_price_filter_for_levels, 
            max_price_level=max_price_filter_for_levels  
        )
        market_data_by_tf[tf_to_process]['supports'] = database_manager.get_support_resistance_levels(
            symbol=symbol, timeframe=tf_to_process, is_active=True, level_type='Support', limit=level_fetch_limit,
            start_time_utc=min_display_time_for_levels,
            min_price_level=min_price_filter_for_levels, 
            max_price_level=max_price_filter_for_levels  
        )
        
        all_fibs_in_range = database_manager.get_fibonacci_levels(
            symbol=symbol,
            timeframe=tf_to_process,
            is_active=True,
            limit=level_fetch_limit,
            start_time_utc=min_display_time_for_levels,
            min_price_level=min_price_filter_for_levels, 
            max_price_level=max_price_filter_for_levels,
            ratios_to_include=[Decimal('0.236'), Decimal('0.382'), Decimal('0.5'), Decimal('0.618'), Decimal('0.786')]
        )
        
        relevant_fib_levels_for_display = []
        if all_fibs_in_range:
            fib_sets_by_ref = {}
            for fib_lvl in all_fibs_in_range:
                ref_key = (fib_lvl['high_price_ref'], fib_lvl['low_price_ref'])
                if ref_key not in fib_sets_by_ref:
                    fib_sets_by_ref[ref_key] = []
                fib_sets_by_ref[ref_key].append(fib_lvl)
            
            sorted_fib_sets = sorted(
                fib_sets_by_ref.items(),
                key=lambda item: max(lvl['start_time_ref_utc'] for lvl in item[1] if lvl.get('start_time_ref_utc')),
                reverse=True
            )
            
            if sorted_fib_sets:
                relevant_fib_levels_for_display = sorted_fib_sets[0][1] 
                    
        market_data_by_tf[tf_to_process]['fib_levels'] = relevant_fib_levels_for_display

        market_data_by_tf[tf_to_process]['divergences'] = database_manager.get_divergences(
            symbol=symbol, timeframe=tf_to_process, is_active=True, limit=level_fetch_limit,
            start_time_utc=min_display_time_for_levels,
            min_price_level=min_price_filter_for_levels,
            max_price_level=max_price_filter_for_levels
        )


    bullish_confluences = []
    bearish_confluences = []

    htf_trend_context = market_data_processor.get_trend_context_from_ma(symbol_param=symbol, timeframe_str=timeframe)
    htf_bias = htf_trend_context.get("overall_trend")

    h1_data = market_data_by_tf.get("H1")
    if not h1_data:
        logger.warning("Tidak ada data H1 untuk analisis skenario utama. Menggunakan status HOLD.")
        scenario_type = "HOLD"
        scenario_description = "Tidak ada data H1 untuk analisis skenario utama."
        notification_service.send_telegram_message(
            f"📊 *Update Strategi {symbol}*\n"
            f"Harga Saat Ini: `{to_float_or_none(current_price):.2f}`\n\n"
            f"*Status*: HOLD (Tidak ada data H1 untuk analisis.)\n"
            f"---"
        )
        return

    df_h1_candles = h1_data['df_candles']
    current_rsi = market_data_processor._calculate_rsi(df_h1_candles, config.AIAnalysts.RSI_PERIOD)
    current_rsi = current_rsi.iloc[-1] if not current_rsi.isnull().all() else None

    h1_sell_zones = []
    for ob in h1_data['obs_bearish']:
        if current_price <= ob['ob_top_price'] + PROXIMITY_THRESHOLD_VALUE and abs(ob['ob_top_price'] - current_price) < PROXIMITY_THRESHOLD_VALUE:
            h1_sell_zones.append(ob['ob_top_price'])
    for fvg in h1_data['fvgs']:
        if fvg['type'] == 'Bearish Imbalance' and current_price <= fvg['fvg_top_price'] + PROXIMITY_THRESHOLD_VALUE and abs(fvg['fvg_top_price'] - current_price) < PROXIMITY_THRESHOLD_VALUE:
            h1_sell_zones.append(fvg['fvg_top_price'])
    for res in h1_data['resistances']:
        if res['is_active'] and current_price <= res['price_level'] + PROXIMITY_THRESHOLD_VALUE and abs(res['price_level'] - current_price) < PROXIMITY_THRESHOLD_VALUE:
            h1_sell_zones.append(res['price_level'])
    h1_sell_zones = sorted(list(set(h1_sell_zones)), reverse=True)

    h1_buy_zones = []
    for ob in h1_data['obs_bullish']:
        if current_price >= ob['ob_bottom_price'] - PROXIMITY_THRESHOLD_VALUE and abs(ob['ob_bottom_price'] - current_price) < PROXIMITY_THRESHOLD_VALUE:
            h1_buy_zones.append(ob['ob_bottom_price'])
    for fvg in h1_data['fvgs']:
        if fvg['type'] == 'Bullish Imbalance' and current_price >= fvg['fvg_bottom_price'] - PROXIMITY_THRESHOLD_VALUE and abs(fvg['fvg_bottom_price'] - current_price) < PROXIMITY_THRESHOLD_VALUE:
            h1_buy_zones.append(fvg['fvg_bottom_price'])
    for sup in h1_data['supports']:
        if sup['is_active'] and current_price >= sup['price_level'] - PROXIMITY_THRESHOLD_VALUE and abs(sup['price_level'] - current_price) < PROXIMITY_THRESHOLD_VALUE:
            h1_buy_zones.append(sup['price_level'])
    h1_buy_zones = sorted(list(set(h1_buy_zones)))


    if htf_bias == "Bullish":
        bullish_confluences.append("Bias tren HTF (D1/H4) Bullish.")

    if current_rsi is not None and current_rsi < config.AIAnalysts.RSI_OVERSOLD:
        bullish_confluences.append(f"RSI (H1) Oversold (<{to_float_or_none(config.AIAnalysts.RSI_OVERSOLD):.0f}).")

    if h1_buy_zones:
        sorted_buy_zones_by_proximity = sorted(h1_buy_zones, key=lambda x: abs(x - current_price))
        if len(sorted_buy_zones_by_proximity) > config.Telegram.NOTIF_MAX_LEVELS_PER_TYPE_PER_TF:
            prices_to_display = sorted_buy_zones_by_proximity[:config.Telegram.NOTIF_MAX_LEVELS_PER_TYPE_PER_TF]
            formatted_prices = ", ".join([f"${_format_price(p)}" for p in prices_to_display])
            bullish_confluences.append(f"Harga mendekati zona beli H1 (terdekat: {formatted_prices}, ...).")
        else:
            formatted_prices = ", ".join([f"${_format_price(p)}" for p in sorted_buy_zones_by_proximity])
            bullish_confluences.append(f"Harga mendekati zona beli H1 ({formatted_prices}).")

    latest_bullish_ms = None
    ms_events_h1 = h1_data.get("key_level", []) 
    for event in ms_events_h1:
        if event['event_type'] in ["Change of Character", "Break of Structure"] and event['direction'] == "Bullish":
            event_time = event['event_time_utc']
            time_difference = current_utc_time - event_time
            if time_difference < timedelta(hours=8):
                latest_bullish_ms = event
                break

    if latest_bullish_ms:
        bullish_confluences.append(f"{latest_bullish_ms['event_type']} Bullish (${to_float_or_none(latest_bullish_ms['price_level']):.2f}) terdeteksi baru-baru ini.")

    liquidity_zones_h1 = h1_data.get("liquidity_zones", [])
    logger.debug(f"DEBUG LIQ ALL H1: Total {len(liquidity_zones_h1)} liquidity zones found for H1.")
    for lz in liquidity_zones_h1[:5]:
        logger.debug(f"DEBUG LIQ ALL H1 Item: Type={lz.get('zone_type')}, Price={to_float_or_none(lz.get('price_level')):.2f}, Tapped={lz.get('is_tapped')}, TapTime={to_iso_format_or_none(lz.get('tap_time_utc'))}.")

    for liq_zone in liquidity_zones_h1:
        if liq_zone['zone_type'] == "Equal Lows" and liq_zone['is_tapped'] and liq_zone['tap_time_utc']:
            tap_time = liq_zone['tap_time_utc']
            time_difference = current_utc_time - tap_time
            logger.debug(f"DEBUG LIQ SWEEP BULLISH: Equal Lows @ {to_float_or_none(liq_zone['price_level']):.2f} tapped at {to_iso_format_or_none(tap_time)}, diff: {time_difference}. Current price {to_float_or_none(current_price):.2f}.")
            if time_difference < timedelta(hours=2):
                candles_after_tap = df_h1_candles[df_h1_candles.index >= tap_time]
                if not candles_after_tap.empty:
                    latest_candle_after_tap = candles_after_tap.iloc[-1]
                    logger.debug(f"DEBUG LIQ SWEEP BULLISH: Last candle after tap: Close={to_float_or_none(latest_candle_after_tap['close_price']):.2f}, Open={to_float_or_none(latest_candle_after_tap['open_price']):.2f}. Liq price: {to_float_or_none(liq_zone['price_level']):.2f}, PROX: {to_float_or_none(PROXIMITY_THRESHOLD_VALUE):.2f}")
                    if to_float_or_none(latest_candle_after_tap['close_price']) > to_float_or_none(latest_candle_after_tap['open_price']) and to_float_or_none(latest_candle_after_tap['close_price']) > to_float_or_none(liq_zone['price_level']) + to_float_or_none(PROXIMITY_THRESHOLD_VALUE):
                        bullish_confluences.append(f"Liquidity Sweep (Equal Lows @ ${to_float_or_none(liq_zone['price_level']):.2f}) terdeteksi diikuti pembalikan Bullish (H1).")
                        logger.debug(f"DEBUG LIQ SWEEP BULLISH: Confirmed Bullish Liquidity Sweep.")
                        break 

    fib_levels_h1 = h1_data.get("fib_levels", [])
    ote_fibs_bullish = [f for f in fib_levels_h1 if f['is_active'] and f['ratio'] in [Decimal('0.618'), Decimal('0.786')] and f['high_price_ref'] > f['low_price_ref']]

    if ote_fibs_bullish:
        for fib_lvl in ote_fibs_bullish:
            fib_high_ref = fib_lvl['high_price_ref']
            fib_low_ref = fib_lvl['low_price_ref']
            
            ote_bottom = fib_low_ref + (fib_high_ref - fib_low_ref) * Decimal('0.618')
            ote_top = fib_low_ref + (fib_high_ref - fib_low_ref) * Decimal('0.786')
            
            if current_price >= ote_bottom - PROXIMITY_THRESHOLD_VALUE and current_price <= ote_top + PROXIMITY_THRESHOLD_VALUE:
                bullish_confluences.append(f"Harga berada di zona OTE Fibo (61.8%-78.6%) H1 (${to_float_or_none(ote_bottom):.2f} - ${to_float_or_none(ote_top):.2f}).")
                break 

    divergences_h1_bullish = h1_data.get("divergences", [])
    divergences_m15_bullish = market_data_by_tf.get("M15", {}).get("divergences", [])

    logger.debug(f"DEBUG DIV ALL H1: Total {len(divergences_h1_bullish)} Bullish Regular H1 divergences found.")
    for div_item in divergences_h1_bullish[:5]:
        logger.debug(f"DEBUG DIV ALL H1 Item: Type={div_item.get('divergence_type')}, Price1={to_float_or_none(div_item.get('price_level_1')):.2f}, Price2={to_float_or_none(div_item.get('price_level_2')):.2f}, Time={to_iso_format_or_none(div_item.get('price_point_time_utc'))}, Active={div_item.get('is_active')}.")
    
    logger.debug(f"DEBUG DIV ALL M15: Total {len(divergences_m15_bullish)} Bullish Regular M15 divergences found.")
    for div_item in divergences_m15_bullish[:5]:
        logger.debug(f"DEBUG DIV ALL M15 Item: Type={div_item.get('divergence_type')}, Price1={to_float_or_none(div_item.get('price_level_1')):.2f}, Price2={to_float_or_none(div_item.get('price_level_2')):.2f}, Time={to_iso_format_or_none(div_item.get('price_point_time_utc'))}, Active={div_item.get('is_active')}.")

    if divergences_h1_bullish:
        recent_bullish_divs = [d for d in divergences_h1_bullish if d['divergence_type'] == "Bullish Regular" and d['is_active'] and (current_utc_time - d['price_point_time_utc']) < timedelta(hours=4)]
        if recent_bullish_divs:
            bullish_confluences.append(f"Divergensi Bullish Reguler ({recent_bullish_divs[0]['indicator_type']}) H1 terdeteksi baru-baru ini.")
            logger.debug(f"DEBUG DIV BULLISH H1: Confirmed Bullish Divergence H1.")
    elif divergences_m15_bullish:
        recent_bullish_divs = [d for d in divergences_m15_bullish if d['divergence_type'] == "Bullish Regular" and d['is_active'] and (current_utc_time - d['price_point_time_utc']) < timedelta(minutes=60)]
        if recent_bullish_divs:
            bullish_confluences.append(f"Divergensi Bullish Reguler ({recent_bullish_divs[0]['indicator_type']}) M15 terdeteksi baru-baru ini.")
            logger.debug(f"DEBUG DIV BULLISH M15: Confirmed Bullish Divergence M15.")

    if htf_bias == "Bearish":
        bearish_confluences.append("Bias tren HTF (D1/H4) Bearish.")

    if current_rsi is not None and current_rsi > config.AIAnalysts.RSI_OVERBOUGHT:
        bearish_confluences.append(f"RSI (H1) Overbought (>{to_float_or_none(config.AIAnalysts.RSI_OVERBOUGHT):.0f}).")

    if h1_sell_zones:
        sorted_sell_zones_by_proximity = sorted(h1_sell_zones, key=lambda x: abs(x - current_price))
        if len(sorted_sell_zones_by_proximity) > config.Telegram.NOTIF_MAX_LEVELS_PER_TYPE_PER_TF:
            prices_to_display = sorted_sell_zones_by_proximity[:config.Telegram.NOTIF_MAX_LEVELS_PER_TYPE_PER_TF]
            formatted_prices = ", ".join([f"${_format_price(p)}" for p in prices_to_display])
            bearish_confluences.append(f"Harga mendekati zona jual H1 (terdekat: {formatted_prices}, ...).")
        else:
            formatted_prices = ", ".join([f"${_format_price(p)}" for p in sorted_sell_zones_by_proximity])
            bearish_confluences.append(f"Harga mendekati zona jual H1 ({formatted_prices}).")

    latest_bearish_ms = None
    ms_events_h1 = h1_data.get("key_level", []) 
    for event in ms_events_h1:
        if event['event_type'] in ["Change of Character", "Break of Structure"] and event['direction'] == "Bearish":
            event_time = event['event_time_utc']
            time_difference = current_utc_time - event_time
            if time_difference < timedelta(hours=8):
                latest_bearish_ms = event
                break

    if latest_bearish_ms:
        bearish_confluences.append(f"{latest_bearish_ms['event_type']} Bearish (${to_float_or_none(latest_bearish_ms['price_level']):.2f}) terdeteksi baru-baru ini.")

    for liq_zone in liquidity_zones_h1:
        if liq_zone['zone_type'] == "Equal Highs" and liq_zone['is_tapped'] and liq_zone['tap_time_utc']:
            tap_time = liq_zone['tap_time_utc']
            time_difference = current_utc_time - tap_time
            logger.debug(f"DEBUG LIQ SWEEP BEARISH: Equal Highs @ {to_float_or_none(liq_zone['price_level']):.2f} tapped at {to_iso_format_or_none(tap_time)}, diff: {time_difference}. Current price {to_float_or_none(current_price):.2f}.")
            if time_difference < timedelta(hours=2):
                candles_after_tap = df_h1_candles[df_h1_candles.index >= tap_time]
                if not candles_after_tap.empty:
                    latest_candle_after_tap = candles_after_tap.iloc[-1]
                    logger.debug(f"DEBUG LIQ SWEEP BEARISH: Last candle after tap: Close={to_float_or_none(latest_candle_after_tap['close_price']):.2f}, Open={to_float_or_none(latest_candle_after_tap['open_price']):.2f}. Liq price: {to_float_or_none(liq_zone['price_level']):.2f}, PROX: {to_float_or_none(PROXIMITY_THRESHOLD_VALUE):.2f}")
                    if to_float_or_none(latest_candle_after_tap['close_price']) < to_float_or_none(latest_candle_after_tap['open_price']) and to_float_or_none(latest_candle_after_tap['close_price']) < to_float_or_none(liq_zone['price_level']) - to_float_or_none(PROXIMITY_THRESHOLD_VALUE):
                        bearish_confluences.append(f"Liquidity Sweep (Equal Highs @ ${to_float_or_none(liq_zone['price_level']):.2f}) terdeteksi diikuti pembalikan Bearish (H1).")
                        logger.debug(f"DEBUG LIQ SWEEP BEARISH: Confirmed Bearish Liquidity Sweep.")
                        break 

    ote_fibs_bearish = [f for f in fib_levels_h1 if f['is_active'] and f['ratio'] in [Decimal('0.618'), Decimal('0.786')] and f['high_price_ref'] > f['low_price_ref']]

    if ote_fibs_bearish:
        for fib_lvl in ote_fibs_bearish:
            fib_high_ref = fib_lvl['high_price_ref']
            fib_low_ref = fib_lvl['low_price_ref']
            
            ote_bottom = fib_high_ref - (fib_high_ref - fib_low_ref) * Decimal('0.786')
            ote_top = fib_high_ref - (fib_high_ref - fib_low_ref) * Decimal('0.618')
            
            if current_price >= ote_bottom - PROXIMITY_THRESHOLD_VALUE and current_price <= ote_top + PROXIMITY_THRESHOLD_VALUE:
                bearish_confluences.append(f"Harga berada di zona OTE Fibo (61.8%-78.6%) H1 (${to_float_or_none(ote_bottom):.2f} - ${to_float_or_none(ote_top):.2f}).")
                break 

    divergences_h1_bearish = h1_data.get("divergences", [])
    divergences_m15_bearish = market_data_by_tf.get("M15", {}).get("divergences", [])

    logger.debug(f"DEBUG DIV ALL H1: Total {len(divergences_h1_bearish)} Bearish Regular H1 divergences found.")
    for div_item in divergences_h1_bearish[:5]:
        logger.debug(f"DEBUG DIV ALL H1 Item: Type={div_item.get('divergence_type')}, Price1={to_float_or_none(div_item.get('price_level_1')):.2f}, Price2={to_float_or_none(div_item.get('price_level_2')):.2f}, Time={to_iso_format_or_none(div_item.get('price_point_time_utc'))}, Active={div_item.get('is_active')}.")
    
    logger.debug(f"DEBUG DIV ALL M15: Total {len(divergences_m15_bearish)} Bearish Regular M15 divergences found.")
    for div_item in divergences_m15_bearish[:5]:
        logger.debug(f"DEBUG DIV ALL M15 Item: Type={div_item.get('divergence_type')}, Price1={to_float_or_none(div_item.get('price_level_1')):.2f}, Price2={to_float_or_none(div_item.get('price_level_2')):.2f}, Time={to_iso_format_or_none(div_item.get('price_point_time_utc'))}, Active={div_item.get('is_active')}.")

    if divergences_h1_bearish:
        recent_bearish_divs = [d for d in divergences_h1_bearish if d['divergence_type'] == "Bearish Regular" and d['is_active'] and (current_utc_time - d['price_point_time_utc']) < timedelta(hours=4)]
        if recent_bearish_divs:
            bearish_confluences.append(f"Divergensi Bearish Reguler ({recent_bearish_divs[0]['indicator_type']}) H1 terdeteksi baru-baru ini.")
            logger.debug(f"DEBUG DIV BEARISH H1: Confirmed Bearish Divergence H1.")
    elif divergences_m15_bearish:
        recent_bearish_divs = [d for d in divergences_m15_bearish if d['divergence_type'] == "Bearish Regular" and d['is_active'] and (current_utc_time - d['price_point_time_utc']) < timedelta(minutes=60)]
        if recent_bearish_divs:
            bearish_confluences.append(f"Divergensi Bearish Reguler ({recent_bearish_divs[0]['indicator_type']}) M15 terdeteksi baru-baru ini.")
            logger.debug(f"DEBUG DIV BEARISH M15: Confirmed Bearish Divergence M15.")

    MIN_CONFLUENCE_FOR_STRONG_SIGNAL = 2
    
    scenario_type = "HOLD"
    scenario_description = "Harga berada di tengah range, tidak ada skenario trading yang jelas."
    confirmations_list = []
    entry_reasons = []
    scenario_notes = ""

    scenario_entry_price_for_calc = current_price 

    if len(bullish_confluences) >= MIN_CONFLUENCE_FOR_STRONG_SIGNAL and len(bullish_confluences) > len(bearish_confluences):
        scenario_type = "BUY"
        scenario_description = "Konfluensi Bullish kuat terdeteksi."
        confirmations_list = list(set(bullish_confluences))
        if h1_buy_zones:
            scenario_entry_price_for_calc = min(h1_buy_zones, key=lambda x: abs(x - current_price))
        else:
            scenario_entry_price_for_calc = current_price
        
        scenario_sl_price = scenario_entry_price_for_calc - config.RuleBasedStrategy.DEFAULT_SL_PIPS * PIP_UNIT_IN_DOLLAR
        scenario_tp_prices.append(scenario_entry_price_for_calc + config.RuleBasedStrategy.TP1_PIPS * PIP_UNIT_IN_DOLLAR)
        scenario_tp_prices.append(scenario_entry_price_for_calc + config.RuleBasedStrategy.TP2_PIPS * PIP_UNIT_IN_DOLLAR)
        scenario_tp_prices.append(scenario_entry_price_for_calc + config.RuleBasedStrategy.TP3_PIPS * PIP_UNIT_IN_DOLLAR)

    elif len(bearish_confluences) >= MIN_CONFLUENCE_FOR_STRONG_SIGNAL and len(bearish_confluences) > len(bullish_confluences):
        scenario_type = "SELL"
        scenario_description = "Konfluensi Bearish kuat terdeteksi."
        confirmations_list = list(set(bearish_confluences))
        if h1_sell_zones:
            scenario_entry_price_for_calc = min(h1_sell_zones, key=lambda x: abs(x - current_price))
        else:
            scenario_entry_price_for_calc = current_price
        
        scenario_sl_price = scenario_entry_price_for_calc + config.RuleBasedStrategy.DEFAULT_SL_PIPS * PIP_UNIT_IN_DOLLAR
        scenario_tp_prices.append(scenario_entry_price_for_calc - config.RuleBasedStrategy.TP1_PIPS * PIP_UNIT_IN_DOLLAR)
        scenario_tp_prices.append(scenario_entry_price_for_calc - config.RuleBasedStrategy.TP2_PIPS * PIP_UNIT_IN_DOLLAR)
        scenario_tp_prices.append(scenario_entry_price_for_calc - config.RuleBasedStrategy.TP3_PIPS * PIP_UNIT_IN_DOLLAR)
    else:
        scenario_notes = "Tidak ada konfluensi yang cukup kuat untuk sinyal trading. Pasar mungkin ranging atau menunggu konfirmasi lebih lanjut."
        
    scenario_entry_price = scenario_entry_price_for_calc 

    message_lines = []
    
    message_lines.extend(_get_report_header(symbol, current_price, current_rsi))
    
    message_lines.extend(_get_entry_strategy_section(
        scenario_type, scenario_entry_price, scenario_sl_price, scenario_tp_prices, confirmations_list, PIP_UNIT_IN_DOLLAR
    ))
    
    message_lines.extend(_get_key_levels_section(market_data_by_tf, current_price))
    
    if not scenario_notes: 
        scenario_notes = scenario_description
    
    message_lines.extend(_get_notes_section(scenario_notes))

    full_message = "\n".join(message_lines)
    notification_service.send_telegram_message(full_message)
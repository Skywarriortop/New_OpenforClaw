import datetime
import pandas as pd
import requests
import json
import time
import os # Tambahkan import os untuk cek file

# --- KONFIGURASI API KEY (NewsAPI TIDAK DIGUNAKAN LAGI UNTUK BERITA LOKAL) ---
# NEWS_API_KEY = "YOUR_NEWS_API_KEY"
# newsapi = NewsApiClient(api_key=NEWS_API_KEY)

# --- NGROK URL dari Colab ---
# PASTI GANTI DENGAN URL NGROK YANG ANDA DAPATKAN DARI OUTPUT Colab Sel 5!
NGROK_PUBLIC_URL = "https://09375ecce9d2.ngrok-free.app" # Contoh, GANTI INI!

# --- Fungsi untuk Mengambil Data ---

def fetch_stock_data(symbol: str, period: str = "1d", interval: str = "1h") -> str:
    """Mengambil data harga saham historis dan menghitung indikator sederhana."""
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period=period, interval=interval)
        if hist.empty:
            return f"Tidak ada data ditemukan untuk {symbol} dalam periode {period} interval {interval}."

        latest_data = hist.iloc[-1]
        summary = (
            f"Data terbaru untuk {symbol} ({interval} terakhir):\n"
            f"  Close: {latest_data['Close']:.2f}, Open: {latest_data['Open']:.2f}, High: {latest_data['High']:.2f}, Low: {latest_data['Low']:.2f}\n"
            f"  Volume: {latest_data['Volume']:,}\n"
        )

        price_change = latest_data['Close'] - latest_data['Open']
        if price_change > 0:
            summary += f"  Perubahan Harga: Naik {price_change:.2f} (Bullish)\n"
        elif price_change < 0:
            summary += f"  Perubahan Harga: Turun {abs(price_change):.2f} (Bearish)\n"
        else:
            summary += "  Perubahan Harga: Stabil\n"

        return summary
    except Exception as e:
        return f"Gagal mengambil data saham untuk {symbol}: {str(e)}"

def fetch_latest_financial_news_from_json(json_file_path: str, query_keywords: list = None, num_articles: int = 3) -> str:
    """
    Mengambil berita keuangan terbaru dari file JSON lokal.
    Memfilter berdasarkan kata kunci jika diberikan.
    """
    if not os.path.exists(json_file_path):
        return f"File JSON berita tidak ditemukan di: {json_file_path}"

    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        articles = data.get('news', [])
        if not articles:
            return "Tidak ada berita dalam file JSON."

        filtered_articles = []
        if query_keywords:
            for article in articles:
                # Cek jika ada kata kunci yang cocok di judul atau deskripsi
                if any(keyword.lower() in article.get('title', '').lower() or \
                       keyword.lower() in article.get('summary', '').lower() \
                       for keyword in query_keywords):
                    filtered_articles.append(article)
        else:
            filtered_articles = articles # Jika tidak ada kata kunci, ambil semua

        # Sortir berdasarkan waktu publikasi (terbaru dulu)
        filtered_articles.sort(key=lambda x: x.get('published_time_utc', ''), reverse=True)

        news_summary_list = []
        for i, article in enumerate(filtered_articles[:num_articles]): # Ambil hanya num_articles teratas
            news_summary_list.append(
                f"Berita {i+1}:\n"
                f"  Judul: {article['title']}\n"
                f"  Sumber: {article['source']}\n" # Sumber di JSON Anda adalah string, bukan dict
                f"  Deskripsi: {article['summary'] if article['summary'] else 'Tidak ada deskripsi.'}\n" # Gunakan summary jika ada, jika tidak kosong
            )
        return "\n---\n".join(news_summary_list)
    except Exception as e:
        return f"Gagal membaca atau memproses file JSON berita: {str(e)}"


def get_past_trades_summary(trade_logs: list) -> str:
    """Meringkas riwayat trading dari daftar log."""
    if not trade_logs:
        return "Tidak ada riwayat trading yang tercatat."

    summary_parts = []
    profitable_trades = [t for t in trade_logs if t.get('profit_loss', 0) > 0]
    losing_trades = [t for t in trade_logs if t.get('profit_loss', 0) <= 0]

    summary_parts.append(f"Ringkasan Riwayat Trading ({len(trade_logs)} trade terakhir):")
    summary_parts.append(f"  Profit: {len(profitable_trades)} trade")
    summary_parts.append(f"  Loss: {len(losing_trades)} trade")

    if profitable_trades:
        last_profit = profitable_trades[-1]
        summary_parts.append(f"  Trade terakhir profit: {last_profit.get('symbol')} ({last_profit.get('profit_loss'):.2f} USD)")
    if losing_trades:
        last_loss = losing_trades[-1]
        summary_parts.append(f"  Trade terakhir loss: {last_loss.get('symbol')} ({last_loss.get('profit_loss'):.2f} USD)")

    return "\n".join(summary_parts)

# --- Contoh log trading Anda (ini akan datang dari bot Anda yang sebenarnya) ---
my_trade_logs = [
    {"symbol": "AAPL", "entry_price": 170, "exit_price": 175, "profit_loss": 500, "strategy": "EMA Crossover", "date": "2025-06-20"},
    {"symbol": "MSFT", "entry_price": 400, "exit_price": 405, "profit_loss": 300, "strategy": "RSI Oversold", "date": "2025-06-21"},
    {"symbol": "GME", "entry_price": 25, "exit_price": 23, "profit_loss": -200, "strategy": "Momentum Breakout", "date": "2025-06-22"},
    {"symbol": "EURUSD", "entry_price": 1.0850, "exit_price": 1.0840, "profit_loss": -50, "strategy": "News Trading", "date": "2025-07-01"}
]


# --- Fungsi get_ai_trade_advice (sama seperti sebelumnya) ---
def get_ai_trade_advice(query_text: str, current_market_data: str = None, latest_news_summary: str = None, past_trades_summary: str = None):
    api_endpoint = f"{NGROK_PUBLIC_URL}/trade_advice/"
    headers = {"Content-Type": "application/json"}
    payload = {
        "query": query_text,
        "current_market_data": current_market_data,
        "latest_news_summary": latest_news_summary,
        "past_trades_summary": past_trades_summary
    }

    print(f"\nMengirim permintaan ke AI Assistant: '{query_text[:70]}...'")
    start_time = time.time()

    try:
        response = requests.post(api_endpoint, headers=headers, data=json.dumps(payload), timeout=120)
        response.raise_for_status()
        result = response.json()
        end_time = time.time()
        print(f"Permintaan selesai dalam {end_time - start_time:.2f} detik.")

        if result.get("status") == "success":
            return result.get("advice")
        else:
            print(f"Error dari AI Assistant API: {result.get('message', 'Pesan error tidak diketahui')}")
            return None
    except requests.exceptions.Timeout:
        print("Error: Permintaan ke AI Assistant timeout. Colab mungkin terputus atau terlalu lambat.")
        return None
    except requests.exceptions.ConnectionError:
        print("Error: Gagal terhubung ke AI Assistant API. Pastikan Colab Anda aktif dan ngrok berjalan.")
        return None
    except requests.requests.exceptions.RequestException as e:
        print(f"Terjadi kesalahan saat memanggil AI Assistant API: {e}")
        return None


if __name__ == "__main__":
    print("--- Mengambil data trading aktual (dari sumber lokal/yfinance) ---")
    stock_summary = fetch_stock_data("NVDA", period="1d", interval="1h") # Contoh: ambil data NVDA
    
    # Ganti "yahoo_news_cache.json" dengan path file JSON Anda
    # Gunakan query_keywords untuk memfilter berita yang relevan
    news_summary = fetch_latest_financial_news_from_json("yahoo_news_cache.json", query_keywords=["Nvidia", "Bitcoin", "Apple"])
    
    trades_summary = get_past_trades_summary(my_trade_logs)

    print("\n--- Data yang Terkumpul ---")
    print("Ringkasan Saham/Aset:\n", stock_summary)
    print("\nRingkasan Berita:\n", news_summary)
    print("\nRingkasan Trade Historis:\n", trades_summary)

    # 1. Mengirim permintaan dengan data aktual dari bot
    query_text_1 = "Berdasarkan kondisi pasar terkini dan berita yang ada, apakah ada sinyal untuk trading NVDA? Saya swing trader dengan toleransi risiko moderat dan fokus pada saham teknologi."
    ai_advice_1 = get_ai_trade_advice(
        query_text=query_text_1,
        current_market_data=stock_summary,
        latest_news_summary=news_summary,
        past_trades_summary=trades_summary
    )

    if ai_advice_1:
        print("\n--- Rekomendasi AI Trading Assistant (Skenario Data Aktual) ---")
        print(ai_advice_1)
    else:
        print("Tidak dapat menerima rekomendasi dari AI Assistant untuk Skenario Data Aktual.")

    # 2. Mengirim permintaan hanya dengan memori jangka panjang (tetap di Colab)
    # LLM akan mencari informasi dari `long_term_data` yang diinisialisasi di Colab
    query_text_2 = "Mengingat riwayat trading saya yang berhasil di saham teknologi, apakah saya harus hati-hati dengan saham gorengan seperti GME?"
    ai_advice_2 = get_ai_trade_advice(query_text=query_text_2)

    if ai_advice_2:
        print("\n--- Rekomendasi AI Trading Assistant (Skenario Memori Jangka Panjang) ---")
        print(ai_advice_2)
    else:
        print("Tidak dapat menerima rekomendasi dari AI Assistant untuk Skenario Memori Jangka Panjang.")
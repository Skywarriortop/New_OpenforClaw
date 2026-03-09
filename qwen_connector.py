# qwen_connector.py

import logging
import requests # INI TETAP DIPERLUKAN
import json
import os
import time

# Impor config
from config import config

logger = logging.getLogger(__name__)

# --- IMPLEMENTASI UNTUK LM STUDIO API (KOMPATIBEL OPENAI) ---

def call_qwen_lm_studio_api(prompt_text: str) -> str:
    """
    Memanggil API Qwen yang diekspos oleh LM Studio (kompatibel OpenAI).
    """
    lm_studio_endpoint = config.APIKeys.LM_STUDIO_API_ENDPOINT
    # Untuk LM Studio, API key biasanya tidak diperlukan atau bisa diisi dummy
    # Tapi lebih aman tidak mengirimnya jika tidak perlu.
    # Jika LM Studio Anda mengkonfigurasi API key, Anda bisa menambahkannya di headers:
    # headers = { "Authorization": f"Bearer {API_KEY_ANDA_DI_LM_STUDIO}" }
    headers = { "Content-Type": "application/json" }

    # Payload ini mirip dengan API OpenAI Chat Completions
    payload = {
        "messages": [
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": prompt_text}
        ],
        "max_tokens": 3000,
        "temperature": 0.7,
        "top_p": 0.9,
        "stream": False # Set false untuk mendapatkan respons penuh sekaligus
        # Model name bisa opsional di LM Studio, tapi kadang membantu.
        # "model": "Qwen1.5-8B-Chat" # Ganti dengan nama model yang Anda pakai di LM Studio
    }

    logger.info(f"Mengirim prompt ke LM Studio API ({lm_studio_endpoint}) untuk {len(prompt_text)} karakter...")

    try:
        response = requests.post(lm_studio_endpoint + "/chat/completions", headers=headers, json=payload, timeout=300)
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)

        response_data = response.json()

        if 'choices' in response_data and response_data['choices']:
            return response_data['choices'][0]['message']['content'].strip()
        else:
            logger.error(f"Struktur respons dari LM Studio tidak dikenal: {response_data}")
            return "Error: Respon dari LM Studio tidak memiliki format yang diharapkan."

    except requests.exceptions.HTTPError as errh:
        logger.error(f"HTTP Error dari LM Studio API: {errh} - {errh.response.text}")
        return f"Error API: Gagal terhubung ke LM Studio (HTTP Error)."
    except requests.exceptions.ConnectionError as errc:
        logger.error(f"Error Koneksi ke LM Studio API: {errc}")
        return f"Error API: Gagal terhubung ke LM Studio (Koneksi). Pastikan LM Studio berjalan dan server aktif di {lm_studio_endpoint.replace('/v1', '')}."
    except requests.exceptions.Timeout as errt:
        logger.error(f"Timeout dari LM Studio API: {errt}")
        return f"Error API: LM Studio Timeout."
    except requests.exceptions.RequestException as err:
        logger.error(f"Kesalahan tak terduga saat memanggil LM Studio API: {err}", exc_info=True)
        return f"Error API: Terjadi kesalahan tak terduga saat memanggil LM Studio."
    except json.JSONDecodeError as e:
        logger.error(f"Gagal mendecode JSON dari respons LM Studio API: {e}. Respon mentah: {response.text}")
        return "Error: Gagal memproses respons dari LM Studio."
    except Exception as e:
        logger.error(f"Error tak terduga di call_qwen_lm_studio_api: {e}", exc_info=True)
        return "Error: Terjadi kesalahan tak terduga dalam interaksi AI."

# --- TENTUKAN FUNGSI YANG AKAN DIGUNAKAN SECARA GLOBAL ---
call_qwen3_8b_api = call_qwen_lm_studio_api # <<< INI YANG AKAN DIGUNAKAN
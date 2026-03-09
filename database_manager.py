# database_manager.py
# Modul ini bertanggung jawab untuk semua interaksi dengan database (SQLite).
# Ini berisi fungsi-fungsi untuk menyimpan, mengambil, dan menghapus berbagai jenis data.

import logging
from sqlalchemy import create_engine, text, Column # PASTIKAN 'Column' ADA DI SINI
from sqlalchemy.exc import OperationalError, IntegrityError # Pastikan IntegrityError juga diimpor
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy import create_engine, text, Column, String, DateTime, Integer, ForeignKey, UniqueConstraint, Index, Boolean, DECIMAL, Text, BigInteger
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from sqlalchemy.dialects import sqlite
from sqlalchemy import func
import hashlib
import re
from sqlalchemy.pool import NullPool, QueuePool
import mt5_connector 
import threading 
import psycopg2.errors
import json
import queue
import time
from sqlalchemy.dialects.postgresql import insert
from config import config
import os
from utils import to_float_or_none, to_iso_format_or_none , to_decimal_or_none, to_utc_datetime_or_none, _get_scalar_from_possibly_ndarray
import utils
import numpy as np
import pandas as pd
import uuid


# Import semua model database Anda
from database_models import ( 
    Base,
    DailyOpen, SessionMetadata, SessionSwingData, SessionCandleData, PriceTick,
    MT5Position, MT5Order, MT5DealHistory, MT5AccountInfo, HistoricalCandle,
    SupportResistanceLevel, SupplyDemandZone, FibonacciLevel,
    OrderBlock, FairValueGap, MarketStructureEvent, LiquidityZone,
    MovingAverage, Divergence, VolumeProfile, AIAnalysisResult,
    EconomicEvent, NewsArticle, FeatureBackfillStatus, MACDValue , RSIValue , FeatureBackfillStatus , BigInteger , LLMPerformanceReview
)

# Variabel global untuk engine dan sesi database
engine = None
SessionLocal = None

# Inisialisasi logger untuk modul ini
logger = logging.getLogger(__name__)


_db_write_lock = threading.Lock()
_db_write_queue = queue.Queue()
_db_writer_thread = None
_db_writer_stop_event = threading.Event()


def _convert_to_json_serializable(obj):
    if isinstance(obj, Decimal):
        return to_float_or_none(obj)
    if isinstance(obj, datetime):
        return to_iso_format_or_none(obj)
    # Tambahan: Tangani UUID jika ada
    if isinstance(obj, uuid.UUID):
        return str(obj)
    return obj
def _db_writer_worker():
    logger.info("Database writer worker thread dimulai.")

    while not globals().get('_db_writer_stop_event', threading.Event()).is_set():
        item_to_process = None
        db = None

        try:
            # Ambil tugas dari antrean
            item_to_process = globals().get('_db_write_queue', queue.Queue()).get(timeout=1)

            # Tangani sinyal berhenti (None)
            if item_to_process is None:
                logger.info("Database writer worker menerima sinyal berhenti eksplisit (None item).")
                break # Keluar dari loop

            # --- PERBAIKAN UTAMA: Hapus panggilan task_done() di sini. Hanya validasi. ---
            if not isinstance(item_to_process, dict) or 'op_type' not in item_to_process:
                logger.error(f"ERROR KRITIS di database writer queue: Item tidak memiliki kunci 'op_type' atau bukan dictionary. Item dilewati: {item_to_process}")
                # task_done() AKAN DIPANGGIL DI BLOK FINALLY
                continue # Lanjut ke iterasi berikutnya tanpa memproses item ini
            # --- AKHIR PERBAIKAN UTAMA ---

            # Inisialisasi sesi database di sini, di dalam try block
            db = globals().get('SessionLocal')()

            op_type = item_to_process['op_type']
            model_class = item_to_process['model_class']
            new_items_data = item_to_process.get('new_items_data', [])
            updated_items_data = item_to_process.get('updated_items_data', [])
            unique_columns = item_to_process.get('unique_columns', [])
            pkey_column_name = item_to_process.get('pkey_column_name', 'id')

            single_item_data = item_to_process.get('single_item_data')
            single_item_key_cols = item_to_process.get('single_item_key_columns')

            write_succeeded = False
            for attempt in range(config.System.MAX_RETRIES):
                try:
                    if op_type == 'batch_save_update':
                        _batch_save_and_update(
                            db, model_class, new_items_data, updated_items_data,
                            unique_columns, pkey_column_name
                        )
                    elif op_type == 'single_save_update':
                        if single_item_data:
                            existing_obj = None

                            if pkey_column_name and single_item_data.get(pkey_column_name) is not None:
                                existing_obj = db.query(model_class).get(single_item_data.get(pkey_column_name))

                            if not existing_obj and single_item_key_cols:
                                filter_criteria = {}
                                for col in single_item_key_cols:
                                    value = single_item_data.get(col)
                                    if value is not None:
                                        if hasattr(model_class, col) and \
                                           isinstance(getattr(model_class, col, None), Column) and \
                                           str(getattr(model_class, col).type).startswith('DATETIME') and \
                                           isinstance(value, str):
                                            filter_criteria[col] = utils.to_utc_datetime_or_none(value)
                                        else:
                                            filter_criteria[col] = value
                                if filter_criteria:
                                    existing_obj = db.query(model_class).filter_by(**filter_criteria).first()

                            if existing_obj:
                                for key, value in single_item_data.items():
                                    if key == pkey_column_name and model_class.__table__.c[key].autoincrement:
                                        continue

                                    if hasattr(existing_obj, key) and isinstance(getattr(existing_obj, key, None), Column):
                                        column_type = str(getattr(model_class, key).type)
                                        if column_type.startswith('DECIMAL') and not isinstance(value, Decimal) and value is not None:
                                            setattr(existing_obj, key, utils.to_decimal_or_none(value))
                                        elif column_type.startswith('DATETIME') and not isinstance(value, datetime) and value is not None:
                                            setattr(existing_obj, key, utils.to_utc_datetime_or_none(value))
                                        else:
                                            setattr(existing_obj, key, value)
                                    else:
                                        setattr(existing_obj, key, value)

                                if hasattr(existing_obj, 'last_update_run_time_utc'):
                                    existing_obj.last_update_run_time_utc = datetime.now(timezone.utc)
                                elif hasattr(existing_obj, 'timestamp_recorded'):
                                    existing_obj.timestamp_recorded = datetime.now(timezone.utc)

                                logger.debug(f"DB Writer: Memperbarui {model_class.__tablename__} (ID: {single_item_data.get(pkey_column_name, 'N/A')}).")
                            else:
                                processed_single_item_data = {}
                                for key, value in single_item_data.items():
                                    if hasattr(model_class, key) and isinstance(getattr(model_class, key, None), Column):
                                        column_type = str(getattr(model_class, key).type)
                                        if column_type.startswith('DECIMAL') and not isinstance(value, Decimal) and value is not None:
                                            processed_single_item_data[key] = utils.to_decimal_or_none(value)
                                        elif column_type.startswith('DATETIME') and not isinstance(value, datetime) and value is not None:
                                            processed_single_item_data[key] = utils.to_utc_datetime_or_none(value)
                                        else:
                                            processed_single_item_data[key] = value
                                    else:
                                        processed_single_item_data[key] = value

                                if hasattr(model_class, 'timestamp_recorded') and 'timestamp_recorded' not in processed_single_item_data:
                                    processed_single_item_data['timestamp_recorded'] = datetime.now(timezone.utc)
                                elif hasattr(model_class, 'last_update_run_time_utc') and 'last_update_run_time_utc' not in processed_single_item_data:
                                    processed_single_item_data['last_update_run_time_utc'] = datetime.now(timezone.utc)

                                if pkey_column_name in processed_single_item_data and (processed_single_item_data[pkey_column_name] is None or (isinstance(processed_single_item_data[pkey_column_name], str) and processed_single_item_data[pkey_column_name].strip() == "")):
                                    if model_class.__tablename__ == 'economic_events' and 'name' in processed_single_item_data:
                                        unique_str = f"{processed_single_item_data['name']}_{utils.to_iso_format_or_none(processed_single_item_data.get('event_time_utc', datetime.now(timezone.utc)))}"
                                        processed_single_item_data[pkey_column_name] = hashlib.sha256(unique_str.encode('utf-8')).hexdigest()[:255]
                                    elif model_class.__tablename__ == 'news_articles' and 'url' in processed_single_item_data:
                                        processed_single_item_data[pkey_column_name] = hashlib.sha256(processed_single_item_data['url'].encode('utf-8')).hexdigest()[:255]
                                    else:
                                        processed_single_item_data[pkey_column_name] = str(uuid.uuid4())
                                    logger.warning(f"DB Writer: Primary key for {model_class.__tablename__} was empty, generated fallback: {processed_single_item_data[pkey_column_name]}")


                                new_obj = model_class(**processed_single_item_data)
                                db.add(new_obj)
                                logger.debug(f"DB Writer: Menyimpan baru {model_class.__tablename__} (ID: {processed_single_item_data.get(pkey_column_name, 'N/A')}).")

                            db.commit()

                        else:
                            logger.error(f"DB Writer: single_save_update item_data tidak ditemukan untuk {model_class.__tablename__}.")

                    write_succeeded = True
                    break

                except OperationalError as oe:
                    if "database is locked" in str(oe):
                        logger.warning(f"Database locked saat writer worker mencoba menyimpan {model_class.__tablename__}. Coba lagi dalam {float(config.System.RETRY_DELAY_SECONDS)} detik (Percobaan {attempt + 1}/{config.System.MAX_RETRIES}). Error: {oe}", exc_info=False)
                        if db: db.rollback()
                        time.sleep(float(config.System.RETRY_DELAY_SECONDS))
                    else:
                        logger.error(f"OperationalError (non-lock) saat writer worker menyimpan {model_class.__tablename__}: {oe}", exc_info=True)
                        if db: db.rollback()
                        break
                except IntegrityError as ie:
                    logger.warning(f"DB Writer: IntegrityError (Duplicate) saat menyimpan {model_class.__tablename__}: {ie}. Dilewati.", exc_info=False)
                    if db: db.rollback()
                    write_succeeded = True
                    break
                except Exception as e_db_write:
                    logger.error(f"Error tak terduga saat writer worker menyimpan {model_class.__tablename__}: {e_db_write}", exc_info=True)
                    if db: db.rollback()
                    break

            if not write_succeeded:
                logger.error(f"Writer worker gagal menyimpan {model_class.__tablename__} setelah {config.System.MAX_RETRIES} percobaan.")

        except queue.Empty: # Menangkap queue.Empty jika timeout terjadi
            time.sleep(0.1) # Tidur sebentar sebelum mencoba lagi
            continue # Lanjut ke iterasi berikutnya

        except Exception as e_queue:
            logger.critical(f"ERROR KRITIS di database writer queue: {e_queue}", exc_info=True)
            if db: db.rollback()
            time.sleep(float(config.System.RETRY_DELAY_SECONDS))
            continue

        finally:
            if db:
                db.close()
            # task_done dipanggil di sini untuk memastikan item dari antrean selalu ditandai selesai
            # terlepas dari apakah pemrosesan internal berhasil atau tidak.
            if item_to_process is not None: # Pastikan item_to_process bukan None (yang merupakan sinyal berhenti)
                globals().get('_db_write_queue', queue.Queue()).task_done()

    logger.info("Database writer worker thread dihentikan.")
    # Bersihkan antrean jika ada item yang tersisa saat shutdown
    while not globals().get('_db_write_queue', queue.Queue()).empty():
        try:
            globals().get('_db_write_queue', queue.Queue()).get_nowait()
            globals().get('_db_write_queue', queue.Queue()).task_done()
        except queue.Empty:
            pass
    logger.info("Database write queue dibersihkan saat shutdown.")

def _batch_save_and_update(db: Session, model_class, new_items: list, updated_items: list, unique_columns: list, pkey_column_name='id'):
    """
    Fungsi bantu generik untuk menyimpan item baru dan memperbarui item yang sudah ada dalam batch.
    """
    with globals().get('_db_write_lock', threading.Lock()):
        added_count = 0
        skipped_count = 0
        updated_count = 0

        if not new_items and not updated_items:
            logger.debug(f"Tidak ada item baru atau diperbarui untuk {model_class.__tablename__}.")
            return

        # 1. Proses item baru (INSERT)
        if new_items:
            try:
                # Objek dalam `new_items` sudah seharusnya berupa instance model (misal: `FairValueGap(...)`)
                # yang sudah memiliki tipe data Python yang benar (Decimal, datetime).
                # Jadi, kita tidak perlu konversi manual di sini saat membuat `insert_mappings`.
                # Biarkan SQLAlchemy menangani pemetaan dari objek model ke nilai SQL.
                if db.bind.name == 'postgresql':
                    table = model_class.__table__
                    insert_mappings = []
                    for item in new_items: # item di sini adalah objek model
                        item_dict = {}
                        for column in table.columns:
                            col_name = column.name
                            value = getattr(item, col_name, None) # Ambil nilai dari objek model

                            # Pastikan nilai adalah skalar dan bukan NaN/None dari Pandas/NumPy
                            value = utils._get_scalar_from_possibly_ndarray(value) # Gunakan utils.

                            # Handle auto-incrementing primary keys (tidak perlu disertakan jika None)
                            if column.primary_key and column.autoincrement:
                                if value is None:
                                    continue # Biarkan DB yang meng-generate ID

                            # Handle non-auto-incrementing string primary keys (e.g., UUIDs) jika kosong
                            if column.primary_key and not column.autoincrement and (value is None or (isinstance(value, str) and not value.strip())):
                                if col_name == 'event_id' and model_class.__tablename__ == 'economic_events':
                                    event_name = getattr(item, 'name', f"UnknownEvent_{datetime.now(timezone.utc).isoformat()}")
                                    value = hashlib.sha256(event_name.encode('utf-8')).hexdigest()[:255]
                                elif col_name == 'id' and model_class.__tablename__ == 'news_articles':
                                    article_url = getattr(item, 'url', f"FallbackURL_{uuid.uuid4()}")
                                    value = hashlib.sha256(article_url.encode('utf-8')).hexdigest()[:255]
                                else:
                                    value = str(uuid.uuid4())
                                logger.warning(f"DBWriter: Primary key '{col_name}' for {model_class.__tablename__} was empty, generated fallback: {value}")


                            # PASTIKAN NOT NULL COLUMNS TIDAK MENJADI NONE
                            # Logika fallback ini sudah benar, pertahankan.
                            if not column.nullable and (value is None or (isinstance(value, str) and not value.strip())):
                                if isinstance(column.type, String):
                                    value = "UNKNOWN"
                                elif isinstance(column.type, (Integer, BigInteger)):
                                    value = 0
                                elif isinstance(column.type, DECIMAL):
                                    value = Decimal('0.0')
                                elif isinstance(column.type, DateTime):
                                    value = datetime(1970, 1, 1, tzinfo=timezone.utc)
                                elif isinstance(column.type, Boolean):
                                    value = False
                                else:
                                    value = None
                                logger.warning(f"DBWriter: Kolom NOT NULL '{col_name}' di {model_class.__tablename__} bernilai None/kosong, di-default ke: {value}")

                            # Hapus konversi manual Decimal ke float dan datetime ke ISO string di sini.
                            # Biarkan SQLAlchemy menangani tipe data Python asli.
                            # Pengecualian: jika kolom adalah BigInteger (untuk timestamp di milidetik),
                            # konversi datetime ke int (milidetik).
                            if isinstance(column.type, BigInteger) and isinstance(value, datetime):
                                item_dict[col_name] = int(value.timestamp() * 1000)
                            else:
                                item_dict[col_name] = value # Ini yang paling penting.

                        insert_mappings.append(item_dict)

                    if not unique_columns:
                        logger.warning(f"No unique_columns provided for {model_class.__tablename__} for ON CONFLICT DO NOTHING. This may cause IntegrityError on duplicates.")
                        stmt = insert(table).values(insert_mappings)
                    else:
                        index_cols = [table.c[col] for col in unique_columns if col in table.c]
                        if not index_cols:
                            logger.warning(f"Invalid unique_columns for ON CONFLICT DO NOTHING in {model_class.__tablename__}: {unique_columns}. Proceeding without ON CONFLICT.")
                            stmt = insert(table).values(insert_mappings)
                        else:
                            stmt = insert(table).values(insert_mappings).on_conflict_do_nothing(index_elements=index_cols)

                    result = db.execute(stmt)
                    added_count = result.rowcount
                    skipped_count += len(insert_mappings) - added_count
                    db.commit()
                    logger.debug(f"Batch INSERT for {model_class.__tablename__} (ON CONFLICT DO NOTHING): Added {added_count}, Skipped {skipped_count} (via ON CONFLICT).")

                else: # Fallback untuk SQLite atau database lain (non-PostgreSQL)
                    # Logika fallback ini sudah baik, karena dia langsung menambahkan objek model.
                    logger.warning(f"ON CONFLICT DO NOTHING not supported for {db.bind.name}. Using less efficient individual INSERT fallback.")
                    for item in new_items:
                        try:
                            pk_column = getattr(model_class.__table__.c, pkey_column_name, None)
                            if pk_column is not None and pk_column.primary_key and not pk_column.autoincrement:
                                pk_value = getattr(item, pkey_column_name, None)
                                if pk_value is None or str(pk_value).strip() == "":
                                    logger.critical(f"DBWriter: PK '{pkey_column_name}' for {type(item).__name__} is missing in non-PG fallback. Generating emergency fallback.")
                                    pk_value = str(uuid.uuid4())
                                    setattr(item, pkey_column_name, pk_value)

                            db.add(item)
                            db.flush()
                            added_count += 1
                        except (IntegrityError, psycopg2.errors.UniqueViolation):
                            db.rollback()
                            logger.warning(f"IntegrityError during individual insert for {model_class.__tablename__} (PK: {getattr(item, pkey_column_name, 'N/A')}). Skipping.", exc_info=False)
                            skipped_count += 1
                        except Exception as e:
                            db.rollback()
                            logger.error(f"Error adding individual item to {model_class.__tablename__}: {e}", exc_info=True)
                    db.commit()
            except Exception as e:
                db.rollback()
                logger.error(f"Error processing new_items batch for {model_class.__tablename__}: {e}", exc_info=True)


        # 2. Proses item yang diperbarui (UPDATE)
        if updated_items:
            unique_updated_items_by_pk = {}
            for item_dict in updated_items:
                pk_val = item_dict.get(pkey_column_name)
                if pk_val is None:
                    logger.warning(f"Item update untuk {model_class.__tablename__} dilewati: primary key '{pkey_column_name}' tidak ada atau NULL. Item: {item_dict}")
                    continue
                unique_updated_items_by_pk[pk_val] = item_dict

            updated_data_for_mapping = list(unique_updated_items_by_pk.values())

            if updated_data_for_mapping:
                try:
                    logger.debug(f"Attempting bulk_update_mappings for {len(updated_data_for_mapping)} items in {model_class.__tablename__}.")

                    processed_for_mapping = []
                    for item_dict in updated_data_for_mapping:
                        processed_item_dict = {}
                        for k, v in item_dict.items():
                            # Untuk bulk_update_mappings, SQLAlchemy membutuhkan dict dengan tipe data dasar Python.
                            # Jadi, konversi Decimal ke float dan datetime ke ISO string di sini adalah tepat.
                            if isinstance(v, Decimal):
                                processed_item_dict[k] = float(v)
                            elif isinstance(v, datetime):
                                processed_item_dict[k] = v.isoformat()
                            else:
                                processed_item_dict[k] = v
                        processed_for_mapping.append(processed_item_dict)

                    db.bulk_update_mappings(model_class, processed_for_mapping)
                    updated_count = len(processed_for_mapping)
                    db.commit()
                    logger.debug(f"Bulk_update_mappings for {model_class.__tablename__}: Updated {updated_count}.")

                except (IntegrityError, psycopg2.errors.UniqueViolation) as ie:
                    db.rollback()
                    logger.error(f"INTEGRITY_ERROR during bulk_update_mappings for {model_class.__tablename__}: {ie}. Trying individual updates as fallback.", exc_info=False)
                    individual_updated_count = 0
                    for item_dict in updated_data_for_mapping:
                        try:
                            obj = db.query(model_class).get(item_dict.get(pkey_column_name))
                            if obj:
                                for key, value in item_dict.items():
                                    if key == pkey_column_name: continue

                                    # Untuk individual update, setattr bisa menerima Decimal/datetime Python
                                    if hasattr(model_class, key) and isinstance(getattr(model_class, key, None), Column):
                                        column_type = str(getattr(model_class, key).type)
                                        if column_type.startswith('DECIMAL') and not isinstance(value, Decimal) and value is not None:
                                            setattr(obj, key, utils.to_decimal_or_none(value))
                                        elif column_type.startswith('DATETIME') and not isinstance(value, datetime) and value is not None:
                                            setattr(obj, key, utils.to_utc_datetime_or_none(value))
                                        else:
                                            setattr(obj, key, value)
                                    else:
                                        setattr(obj, key, value)
                                db.flush()
                                individual_updated_count += 1
                            else:
                                logger.warning(f"Item with PK {item_dict.get(pkey_column_name)} not found for individual update in {model_class.__tablename__}.")
                        except (IntegrityError, psycopg2.errors.UniqueViolation):
                            db.rollback()
                            logger.warning(f"IntegrityError during individual update for {model_class.__tablename__} (PK: {item_dict.get(pkey_column_name)}). Skipping.", exc_info=False)
                        except Exception as update_e:
                            db.rollback()
                            logger.error(f"Error updating individual item {model_class.__tablename__} with ID {item_dict.get(pkey_column_name)}: {update_e}", exc_info=True)
                    updated_count = individual_updated_count
                    db.commit()
                except Exception as e:
                    db.rollback()
                    logger.error(f"Error during bulk_update_mappings for {model_class.__tablename__}: {e}", exc_info=True)
            else:
                logger.debug(f"No valid items for bulk_update_mappings in {model_class.__tablename__}.")

        logger.info(f"Batch operation for {model_class.__tablename__}: Added {added_count}, Updated {updated_count}, Skipped {skipped_count}.")



def init_db_writer():
    global _db_writer_thread
    if _db_writer_thread is None or not _db_writer_thread.is_alive():
        _db_writer_stop_event.clear()
        _db_writer_thread = threading.Thread(target=_db_writer_worker, name="DBWriterThread", daemon=True)
        _db_writer_thread.start()
        logger.info("Database writer thread dimulai.")
        return True
    logger.info("Database writer thread sudah berjalan.")
    return False

def stop_db_writer():
    global _db_writer_thread
    if _db_writer_thread and _db_writer_thread.is_alive():
        logger.info("Menghentikan database writer thread...")
        _db_writer_stop_event.set()
        # Masukkan sinyal 'None' ke antrean untuk memastikan worker tidak terjebak jika antrean kosong
        try:
            # Coba masukkan sinyal berhenti, tapi jangan blokir jika antrean penuh saat shutdown
            _db_write_queue.put(None, timeout=1)
        except queue.Full:
            logger.warning("DB writer queue penuh saat shutdown, tidak dapat mengirim sinyal berhenti.")
        except Exception as e:
            logger.error(f"Error mengirim sinyal berhenti ke DB writer queue: {e}", exc_info=True)

        _db_writer_thread.join(timeout=5) # Beri waktu thread untuk selesai
        if _db_writer_thread.is_alive():
            logger.warning("Database writer thread tidak berhenti dalam batas waktu.")
        else:
            logger.info("Database writer thread telah dihentikan.")
        _db_writer_thread = None
    else:
        logger.info("Database writer thread tidak berjalan atau sudah berhenti.")


# --- FUNGSI BANTU UNTUK NORMALISASI DAN HASHING KONTEN ---
def _normalize_and_hash_content(text_to_hash):
    """
    Normalisasi teks dan buat hash SHA256.
    Digunakan untuk deduplikasi konten.
    """
    if not isinstance(text_to_hash, str):
        text_to_hash = str(text_to_hash)
    
    text_to_hash = text_to_hash.strip().lower()
    text_to_hash = re.sub(r'[^a-z0-9\s]', '', text_to_hash) # Hapus non-alphanumeric kecuali spasi
    text_to_hash = re.sub(r'\s+', ' ', text_to_hash).strip() # Ganti multiple spasi dengan single
    if not text_to_hash:
        return None
    return hashlib.sha256(text_to_hash.encode('utf-8')).hexdigest()




# --- Fungsi Inisialisasi Database ---
def init_db_connection(db_url):
    """
    Menginisialisasi koneksi database menggunakan SQLAlchemy.
    """
    global engine, SessionLocal
    try:
        engine = create_engine(
            db_url,
            echo=False,
            # HAPUS BARIS INI KARENA TIDAK RELEVAN/DAPAT MENYEBABKAN MASALAH DI POSTGRESQL
            # connect_args={'timeout': 60}, # BARIS INI HARUS DIHAPUS ATAU DIKOMENTARI
            poolclass=QueuePool,
            pool_size=10,
            max_overflow=20
        )
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        logger.info("Koneksi database berhasil diinisialisasi dan diuji dengan PostgreSQL.")
        return True
    except Exception as e:
        logger.error(f"Gagal menginisialisasi koneksi database: {e}", exc_info=True)
        engine = None
        SessionLocal = None
        return False


def create_all_tables():
    """
    Membuat semua tabel yang didefinisikan dalam Base jika belum ada.
    """
    try:
        Base.metadata.create_all(engine)
        logger.info("Tabel database berhasil dibuat/diperiksa.")
        if 'news_articles' in Base.metadata.tables:
            news_article_table = Base.metadata.tables['news_articles']
            column_names = [column.name for column in news_article_table.columns]
            logger.info(f"Kolom yang terdaftar di tabel news_articles oleh SQLAlchemy: {column_names}")
        if 'session_candle_data' in Base.metadata.tables:
            session_candle_table = Base.metadata.tables['session_candle_data']
            column_names = [column.name for column in session_candle_table.columns]
            logger.info(f"Kolom yang terdaftar di tabel session_candle_data oleh SQLAlchemy: {column_names}")
        if 'session_swing_data' in Base.metadata.tables:
            session_swing_table = Base.metadata.tables['session_swing_data']
            column_names = [column.name for column in session_swing_table.columns]
            logger.info(f"Kolom yang terdaftar di tabel session_swing_data oleh SQLAlchemy: {column_names}")
        if 'daily_opens' in Base.metadata.tables:
            daily_open_table = Base.metadata.tables['daily_opens']
            column_names = [column.name for column in daily_open_table.columns]
            logger.info(f"Kolom yang terdaftar di tabel daily_opens oleh SQLAlchemy: {column_names}")
        if 'price_ticks' in Base.metadata.tables:
            price_tick_table = Base.metadata.tables['price_ticks']
            column_names = [column.name for column in price_tick_table.columns]
            logger.info(f"Kolom yang terdaftar di tabel price_ticks oleh SQLAlchemy: {column_names}")
        return True
    except Exception as e:
        logger.error(f"Gagal membuat atau memeriksa tabel database: {e}", exc_info=True)
        return False

def save_rsi_value(symbol, timeframe, timestamp_utc, value):
    """
    Menyimpan atau memperbarui nilai RSI ke database.
    Memastikan nilai adalah Decimal dan waktu adalah datetime object.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di save_rsi_value. Tidak dapat melanjutkan.")
        return

    try:
        timestamp_utc_processed = None
        if isinstance(timestamp_utc, str):
            try:
                timestamp_utc_processed = datetime.fromisoformat(timestamp_utc).replace(tzinfo=timezone.utc)
            except ValueError:
                logger.error(f"Gagal mengonversi timestamp_utc string '{timestamp_utc}' ke datetime untuk RSI. Melewatkan penyimpanan.")
                return
        elif isinstance(timestamp_utc, datetime):
            timestamp_utc_processed = timestamp_utc.replace(tzinfo=timezone.utc)
        else:
            logger.error(f"Tipe data timestamp_utc tidak dikenal: {type(timestamp_utc)} untuk RSI. Melewatkan penyimpanan.")
            return

        value_processed = Decimal(str(value)) if value is not None else None
        # Pastikan nilai RSI yang diproses tidak None atau NaN untuk disimpan
        if value_processed is None or (isinstance(value_processed, Decimal) and value_processed.is_nan()):
            logger.warning(f"Nilai RSI '{value}' tidak valid atau NaN setelah diproses. Melewatkan penyimpanan.")
            return


        processed_data = {
            "symbol": symbol,
            "timeframe": timeframe,
            "timestamp_utc": timestamp_utc_processed, # <--- KOREKSI: KIRIM OBJEK DATETIME ASLI
            "value": value_processed,                  # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "timestamp_recorded": datetime.now(timezone.utc)
        }

        _db_write_queue.put({
            'op_type': 'single_save_update',
            'model_class': RSIValue,
            'single_item_data': processed_data,
            'single_item_key_columns': ['symbol', 'timeframe', 'timestamp_utc']
        })
        logger.debug(f"RSI untuk {symbol} {timeframe} @ {timestamp_utc} dikirim ke antrean DB.")
    except Exception as e:
        logger.error(f"Gagal menyiapkan RSI untuk antrean DB untuk {symbol} {timeframe}: {e}", exc_info=True)





def get_rsi_values(symbol=None, timeframe=None, limit=None, start_time_utc=None, end_time_utc=None):
    """
    Mengambil nilai RSI dari database.
    Memastikan nilai adalah Decimal dan waktu adalah datetime object.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di get_rsi_values. Tidak dapat melanjutkan.")
        return []
    db = SessionLocal()
    try:
        query = db.query(RSIValue)
        if symbol:
            query = query.filter(RSIValue.symbol == symbol)
        if timeframe:
            query = query.filter(RSIValue.timeframe == timeframe)
        # Modifikasi: Pastikan start_time_utc dan end_time_utc adalah datetime aware UTC untuk filter
        if start_time_utc:
            start_time_utc_processed = to_utc_datetime_or_none(start_time_utc)
            if start_time_utc_processed:
                query = query.filter(RSIValue.timestamp_utc >= start_time_utc_processed)
        if end_time_utc:
            end_time_utc_processed = to_utc_datetime_or_none(end_time_utc)
            if end_time_utc_processed:
                query = query.filter(RSIValue.timestamp_utc <= end_time_utc_processed)

        query = query.order_by(RSIValue.timestamp_utc.desc())

        if limit:
            query = query.limit(limit)

        results = query.all()

        output = []
        for r in results:
            output.append({
                "id": r.id,
                "symbol": r.symbol,
                "timeframe": r.timeframe,
                "timestamp_utc": r.timestamp_utc, # Ini sudah datetime object dari model
                "value": r.value # Ini sudah Decimal dari model
            })
        return output
    except Exception as e:
        logger.error(f"Gagal mengambil nilai RSI dari DB: {e}", exc_info=True)
        return []
    finally:
        db.close()

def save_macd_value(symbol, timeframe, timestamp_utc, macd_line, signal_line, histogram, macd_pcent=None):
    """
    Menyimpan atau memperbarui nilai MACD ke database.
    Memastikan nilai adalah Decimal dan waktu adalah datetime object.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di save_macd_value. Tidak dapat melanjutkan.")
        return

    try:
        # Modifikasi: Gunakan helper to_utc_datetime_or_none untuk memastikan datetime aware UTC
        timestamp_utc_processed = to_utc_datetime_or_none(timestamp_utc)
        if timestamp_utc_processed is None:
            logger.error(f"Gagal mengonversi timestamp_utc '{timestamp_utc}' ke datetime aware UTC untuk MACD. Melewatkan penyimpanan.")
            return

        # Modifikasi: Gunakan helper to_decimal_or_none untuk semua nilai numerik
        macd_line_processed = to_decimal_or_none(macd_line)
        signal_line_processed = to_decimal_or_none(signal_line)
        histogram_processed = to_decimal_or_none(histogram)
        macd_pcent_processed = to_decimal_or_none(macd_pcent) # Menangani macd_pcent

        # Periksa apakah ada nilai yang tidak valid setelah konversi
        if any(v is None for v in [macd_line_processed, signal_line_processed, histogram_processed]):
            logger.warning(f"Beberapa nilai MACD tidak valid setelah konversi untuk {symbol} {timeframe} @ {timestamp_utc}. Melewatkan penyimpanan.")
            return

        processed_data = {
            "symbol": symbol,
            "timeframe": timeframe,
            "timestamp_utc": timestamp_utc_processed, # <--- KOREKSI: KIRIM OBJEK DATETIME ASLI
            "macd_line": macd_line_processed,         # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "signal_line": signal_line_processed,     # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "histogram": histogram_processed,         # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "macd_pcent": macd_pcent_processed,       # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "timestamp_recorded": datetime.now(timezone.utc)
        }

        _db_write_queue.put({
            'op_type': 'single_save_update', # Menggunakan single_save_update untuk setiap titik waktu
            'model_class': MACDValue,
            'single_item_data': processed_data,
            'single_item_key_columns': ['symbol', 'timeframe', 'timestamp_utc']
        })
        logger.debug(f"MACD untuk {symbol} {timeframe} @ {timestamp_utc} dikirim ke antrean DB.")
    except Exception as e:
        logger.error(f"Gagal menyiapkan MACD untuk antrean DB untuk {symbol} {timeframe}: {e}", exc_info=True)





def get_macd_values(symbol=None, timeframe=None, limit=None, start_time_utc=None, end_time_utc=None):
    """
    Mengambil nilai MACD dari database.
    Memastikan nilai adalah Decimal dan waktu adalah datetime object.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di get_macd_values. Tidak dapat melanjutkan.")
        return []
    db = SessionLocal()
    try:
        query = db.query(MACDValue)
        if symbol:
            query = query.filter(MACDValue.symbol == symbol)
        if timeframe:
            query = query.filter(MACDValue.timeframe == timeframe)
        # Modifikasi: Pastikan start_time_utc dan end_time_utc adalah datetime aware UTC untuk filter
        if start_time_utc:
            start_time_utc_processed = to_utc_datetime_or_none(start_time_utc)
            if start_time_utc_processed:
                query = query.filter(MACDValue.timestamp_utc >= start_time_utc_processed)
        if end_time_utc:
            end_time_utc_processed = to_utc_datetime_or_none(end_time_utc)
            if end_time_utc_processed:
                query = query.filter(MACDValue.timestamp_utc <= end_time_utc_processed)

        query = query.order_by(MACDValue.timestamp_utc.desc())

        if limit:
            query = query.limit(limit)

        results = query.all()

        output = []
        for r in results:
            output.append({
                "id": r.id,
                "symbol": r.symbol,
                "timeframe": r.timeframe,
                "timestamp_utc": r.timestamp_utc, # Ini sudah datetime object dari model
                "macd_line": r.macd_line,         # Ini sudah Decimal dari model
                "signal_line": r.signal_line,     # Ini sudah Decimal dari model
                "histogram": r.histogram,         # Ini sudah Decimal dari model
                "macd_pcent": r.macd_pcent        # Ini sudah Decimal dari model
            })
        return output
    except Exception as e:
        logger.error(f"Gagal mengambil nilai MACD dari DB: {e}", exc_info=True)
        return []
    finally:
        db.close()


# --- FUNGSI UNTUK DAILY OPEN PRICE ---
def save_daily_open_price(date_referenced, reference_price, source_candle_time_utc, symbol):
    """
    Menyimpan atau memperbarui harga pembukaan harian untuk simbol dan tanggal tertentu.
    Memastikan reference_price adalah Decimal dan source_candle_time_utc adalah datetime object.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di save_daily_open_price. Tidak dapat melanjutkan.")
        return

    try:
        # Modifikasi: Gunakan helper to_decimal_or_none
        reference_price_processed = to_decimal_or_none(reference_price)
        if reference_price_processed is None:
            logger.warning(f"Harga referensi '{reference_price}' tidak valid untuk Daily Open. Melewatkan penyimpanan.")
            return

        # Modifikasi: Gunakan helper to_utc_datetime_or_none
        source_candle_time_utc_processed = to_utc_datetime_or_none(source_candle_time_utc)
        if source_candle_time_utc_processed is None:
            logger.error(f"Gagal mengonversi source_candle_time_utc '{source_candle_time_utc}' ke datetime aware UTC untuk Daily Open. Melewatkan penyimpanan.")
            return

        processed_data = {
            "symbol": symbol,
            "date_referenced": date_referenced,
            "reference_price": reference_price_processed, # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "source_candle_time_utc": source_candle_time_utc_processed, # <--- KOREKSI: KIRIM OBJEK DATETIME ASLI
            "timestamp_recorded": datetime.now(timezone.utc)
        }

        _db_write_queue.put({
            'op_type': 'single_save_update',
            'model_class': DailyOpen,
            'single_item_data': processed_data,
            'single_item_key_columns': ['symbol', 'date_referenced']
        })
        logger.debug(f"Daily open price untuk {symbol} pada {date_referenced} dikirim ke antrean DB.")
    except Exception as e:
        logger.error(f"Gagal menyiapkan daily open price untuk antrean DB untuk {symbol} pada {date_referenced}: {e}", exc_info=True)

def update_mt5_position_partial_tp_flags(ticket: int, partial_tp_hit_flags: list, tp_levels_config: list): # <--- TAMBAH ARGUMEN tp_levels_config
    """
    Menyimpan atau memperbarui status hit TP parsial dan konfigurasi TP levels untuk posisi MT5 tertentu.
    Mengirimkan tugas ke antrean penulis DB.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di update_mt5_position_partial_tp_flags. Tidak dapat melanjutkan.")
        return

    try:
        flags_json_string = json.dumps(partial_tp_hit_flags)
        tp_config_json_string = json.dumps(tp_levels_config) # <--- Konversi tp_levels_config ke JSON

        processed_data = {
            "ticket": ticket,
            "partial_tp_hit_flags_json": flags_json_string,
            "tp_levels_config_json": tp_config_json_string, # <--- Tambahkan ini
            "timestamp_recorded": datetime.now(timezone.utc)
        }

        _db_write_queue.put({
            'op_type': 'single_save_update',
            'model_class': MT5Position,
            'single_item_data': processed_data,
            'single_item_key_columns': ['ticket'],
            'pkey_column_name': 'ticket'
        })
        logger.debug(f"Status TP parsial dan config untuk posisi {ticket} dikirim ke antrean DB untuk diperbarui.")
    except Exception as e:
        logger.error(f"Gagal menyiapkan update status TP parsial dan config untuk posisi {ticket}: {e}", exc_info=True)



def get_mt5_position_partial_tp_flags(ticket: int) -> dict: # <--- Ubah return type hint ke dict
    """
    Mengambil status hit TP parsial dan konfigurasi TP levels dari database untuk posisi MT5 tertentu.
    Mengembalikan dictionary {'hit_flags': list, 'tp_levels_config': list} atau None jika tidak ditemukan.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di get_mt5_position_partial_tp_flags. Tidak dapat melanjutkan.")
        return None
    db = SessionLocal()
    try:
        position_data = db.query(MT5Position).filter(MT5Position.ticket == ticket).first()
        if position_data and position_data.partial_tp_hit_flags_json and position_data.tp_levels_config_json:
            flags_list = json.loads(position_data.partial_tp_hit_flags_json)
            tp_config_list = json.loads(position_data.tp_levels_config_json)
            logger.debug(f"Status TP parsial dan config untuk posisi {ticket} ditemukan di DB: Flags={flags_list}, Config={tp_config_list}")
            return {'hit_flags': flags_list, 'tp_levels_config': tp_config_list}
        else:
            logger.debug(f"Status TP parsial atau config untuk posisi {ticket} tidak ditemukan di DB atau kosong.")
            return None
    except json.JSONDecodeError as jde:
        logger.error(f"Gagal decode JSON status TP parsial/config untuk posisi {ticket}: {jde}. Mengembalikan None.", exc_info=True)
        return None
    except Exception as e:
        logger.error(f"Gagal mengambil status TP parsial/config untuk posisi {ticket} dari DB: {e}", exc_info=True)
        return None
    finally:
        if db:
            db.close()


def get_daily_open_price(date_ref, symbol_param=None, start_time_utc=None, end_time_utc=None):
    """
    Mengambil harga pembukaan harian untuk simbol dan tanggal tertentu.
    Mengembalikan nilai dalam format Decimal untuk konsistensi.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di get_daily_open_price. Tidak dapat melanjutkan.")
        return None
    db = SessionLocal()
    try:
        query = db.query(DailyOpen).filter(
            DailyOpen.date_referenced == date_ref,
            DailyOpen.symbol == symbol_param
        )
        # Modifikasi: Pastikan start_time_utc dan end_time_utc adalah datetime aware UTC untuk filter
        if start_time_utc:
            start_time_utc_processed = to_utc_datetime_or_none(start_time_utc)
            if start_time_utc_processed:
                query = query.filter(DailyOpen.source_candle_time_utc >= start_time_utc_processed)
        if end_time_utc:
            end_time_utc_processed = to_utc_datetime_or_none(end_time_utc)
            if end_time_utc_processed:
                query = query.filter(DailyOpen.source_candle_time_utc <= end_time_utc_processed)
        
        entry = query.first()
        
        # Modifikasi: Pastikan nilai yang dikembalikan adalah Decimal
        if entry:
            logger.debug(f"Daily open price ditemukan untuk {symbol_param} pada {date_ref}: {to_float_or_none(entry.reference_price):.5f}")
            return entry.reference_price # Ini sudah Decimal dari model
        else:
            logger.debug(f"Daily open price tidak ditemukan untuk {symbol_param} pada {date_ref}.")
            return None
    except Exception as e:
        logger.error(f"Gagal mengambil daily open price dari DB for {date_ref}, symbol {symbol_param}: {e}", exc_info=True)
        return None
    finally:
        db.close()


# --- FUNGSI UNTUK SESI METADATA ---
def get_session_metadata():
    """
    Mengambil semua metadata sesi dari database.
    Returns:
        list: Daftar dictionary dengan detail sesi.
    """
    global SessionLocal # <--- TAMBAHKAN INI
    if SessionLocal is None: # <--- TAMBAHKAN PENGECEKAN INI
        logger.error("SessionLocal belum diinisialisasi di get_session_metadata. Tidak dapat melanjutkan.")
        return []
    db = SessionLocal()
    try:
        sessions = db.query(SessionMetadata).all()
        logger.debug(f"DEBUG DB: get_session_metadata mengembalikan {len(sessions)} sesi.")
        return [{
            "id": s.id,
            "session_name": s.session_name,
            "utc_start_hour": s.utc_start_hour,
            "utc_end_hour": s.utc_end_hour
        } for s in sessions]
    except Exception as e:
        logger.error(f"Gagal mengambil session metadata dari DB: {e}", exc_info=True)
        return []
    finally:
        db.close()

# --- FUNGSI UNTUK SESI CANDLE DATA ---
def save_session_candle_data(session_id, trade_date, candle_time_utc, open_price, high_price, low_price, close_price, tick_volume, spread, real_volume, base_candle_name, symbol):
    """
    Menyimpan atau memperbarui data candle sesi untuk simbol dan tanggal tertentu.
    Memastikan harga adalah Decimal dan waktu adalah datetime object.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di save_session_candle_data. Tidak dapat melanjutkan.")
        return

    try:
        # Modifikasi: Gunakan helper to_utc_datetime_or_none
        candle_time_utc_processed = to_utc_datetime_or_none(candle_time_utc)
        if candle_time_utc_processed is None:
            logger.error(f"Gagal mengonversi candle_time_utc '{candle_time_utc}' ke datetime aware UTC untuk sesi candle. Melewatkan penyimpanan.")
            return

        # Modifikasi: Konversi harga ke Decimal menggunakan helper to_decimal_or_none
        open_price_processed = to_decimal_or_none(open_price)
        high_price_processed = to_decimal_or_none(high_price)
        low_price_processed = to_decimal_or_none(low_price)
        close_price_processed = to_decimal_or_none(close_price)
        tick_volume_processed = utils.to_int_or_none(tick_volume) # Pastikan int
        spread_processed = utils.to_int_or_none(spread) # Pastikan int
        real_volume_processed = utils.to_int_or_none(real_volume) # Pastikan int

        # Modifikasi: Periksa apakah ada nilai harga yang tidak valid setelah konversi
        if any(v is None for v in [open_price_processed, high_price_processed, low_price_processed, close_price_processed]):
            logger.warning(f"Beberapa harga sesi candle tidak valid untuk {symbol} (Session: {session_id}, Date: {trade_date}). Melewatkan penyimpanan.")
            return

        processed_data = {
            "session_id": session_id,
            "trade_date": trade_date,
            "candle_time_utc": candle_time_utc_processed, # <--- KOREKSI: KIRIM OBJEK DATETIME ASLI
            "open_price": open_price_processed,           # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "high_price": high_price_processed,           # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "low_price": low_price_processed,             # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "close_price": close_price_processed,         # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "tick_volume": tick_volume_processed,         # <--- KOREKSI: KIRIM OBJEK INT ASLI
            "spread": spread_processed,                   # <--- KOREKSI: KIRIM OBJEK INT ASLI
            "real_volume": real_volume_processed,         # <--- KOREKSI: KIRIM OBJEK INT ASLI
            "base_candle_name": base_candle_name,
            "symbol": symbol,
            "timestamp_recorded": datetime.now(timezone.utc)
        }

        _db_write_queue.put({
            'op_type': 'single_save_update',
            'model_class': SessionCandleData,
            'single_item_data': processed_data,
            'single_item_key_columns': ['session_id', 'trade_date', 'candle_time_utc', 'base_candle_name', 'symbol']
        })
        logger.debug(f"Session Candle untuk {symbol} (Session: {session_id}, Date: {trade_date}) dikirim ke antrean DB.")
    except Exception as e:
        logger.error(f"Gagal menyiapkan session candle data untuk antrean DB untuk {symbol} (Session: {session_id}, Date: {trade_date}): {e}", exc_info=True)

def ensure_session_metadata_exists():
    """
    Memastikan entri default untuk sesi pasar (Asia, Eropa, New York) ada di database.
    Ini adalah data statis yang dibutuhkan untuk perhitungan sesi.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di ensure_session_metadata_exists. Tidak dapat melanjutkan.")
        return False
    db = SessionLocal()
    try:
        sessions_to_add = [
            {"session_name": "Asia", "utc_start_hour": config.Sessions.ASIA_SESSION_START_HOUR_UTC, "utc_end_hour": config.Sessions.ASIA_SESSION_END_HOUR_UTC},
            {"session_name": "Europe", "utc_start_hour": config.Sessions.EUROPE_SESSION_START_HOUR_UTC, "utc_end_hour": config.Sessions.EUROPE_SESSION_END_HOUR_UTC},
            {"session_name": "New York", "utc_start_hour": config.Sessions.NEWYORK_SESSION_START_HOUR_UTC, "utc_end_hour": config.Sessions.NEWYORK_SESSION_END_HOUR_UTC},
        ]

        for session_data in sessions_to_add:
            existing_session = db.query(SessionMetadata).filter_by(session_name=session_data["session_name"]).first()
            if not existing_session:
                new_session = SessionMetadata(
                    session_name=session_data["session_name"],
                    utc_start_hour=session_data["utc_start_hour"],
                    utc_end_hour=session_data["utc_end_hour"],
                    timestamp_recorded=datetime.now(timezone.utc)
                )
                db.add(new_session)
                logger.info(f"Menambahkan metadata sesi baru: {session_data['session_name']}")
            else:
                # Perbarui jika ada perubahan pada jam sesi
                if (existing_session.utc_start_hour != session_data["utc_start_hour"] or
                    existing_session.utc_end_hour != session_data["utc_end_hour"]):
                    
                    existing_session.utc_start_hour = session_data["utc_start_hour"]
                    existing_session.utc_end_hour = session_data["utc_end_hour"]
                    existing_session.timestamp_recorded = datetime.now(timezone.utc)
                    logger.info(f"Memperbarui metadata sesi yang sudah ada: {session_data['session_name']}")
        
        db.commit()
        logger.info("Verifikasi metadata sesi pasar selesai. Semua entri yang diperlukan ada.")
        return True
    except Exception as e:
        db.rollback()
        logger.error(f"Gagal memastikan metadata sesi pasar ada: {e}", exc_info=True)
        return False
    finally:
        db.close()

def get_mt5_positions_from_db(symbol: str = None) -> list:
    """
    Mengambil posisi MT5 yang sedang aktif dari database.
    Mengembalikan daftar dictionary posisi.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di get_mt5_positions_from_db. Tidak dapat melanjutkan.")
        return []
    db = SessionLocal()
    try:
        query = db.query(MT5Position)
        if symbol:
            query = query.filter(MT5Position.symbol == symbol)

        positions = query.order_by(MT5Position.time_open.desc()).all() # Pastikan query ini menghasilkan list objek Position

        output = []
        for p in positions:
            # <<< MULAI BLOK KODE KOREKSI DI SINI >>>
            if p is None: # Penting: Pastikan objek p itu sendiri bukan None
                logger.warning("Menemukan objek posisi MT5 yang None dari database. Melewatkan baris ini.")
                continue

            # Ambil nilai string JSON dari atribut, default ke '[]' jika None
            # Ini menangani kasus kolom NULL di DB yang diterjemahkan jadi None di Python
            partial_tp_hit_flags_json_str = p.partial_tp_hit_flags_json if p.partial_tp_hit_flags_json is not None else '[]'
            tp_levels_config_json_str = p.tp_levels_config_json if p.tp_levels_config_json is not None else '[]'

            # Coba parse JSON dengan error handling
            partial_tp_flags = []
            try:
                partial_tp_flags = json.loads(partial_tp_hit_flags_json_str)
            except json.JSONDecodeError as jde:
                logger.error(f"Gagal mendecode partial_tp_hit_flags_json untuk tiket {p.ticket}: {jde}. Data: '{partial_tp_hit_flags_json_str[:50]}...'. Menggunakan default [].", exc_info=False)

            tp_levels_config = []
            try:
                tp_levels_config = json.loads(tp_levels_config_json_str)
            except json.JSONDecodeError as jde:
                logger.error(f"Gagal mendecode tp_levels_config_json untuk tiket {p.ticket}: {jde}. Data: '{tp_levels_config_json_str[:50]}...'. Menggunakan default [].", exc_info=false)
            # <<< AKHIR BLOK KODE KOREKSI >>>

            output.append({
                "ticket": p.ticket,
                "symbol": p.symbol,
                "type": p.type,
                "volume": p.volume,
                "price_open": p.price_open,
                "time_open": p.time_open,
                "current_price": p.current_price,
                "profit": p.profit,
                "swap": p.swap,
                "comment": p.comment,
                "magic": p.magic,
                "sl_price": p.sl_price,
                "tp_price": p.tp_price,
                "partial_tp_hit_flags": partial_tp_flags,
                "tp_levels_config": tp_levels_config,
                "timestamp_recorded": p.timestamp_recorded
            })
        return output
    except Exception as e:
        logger.error(f"Gagal mengambil posisi MT5 dari DB: {e}", exc_info=True)
        return []
    finally:
        if db:
            db.close()

def calculate_and_update_session_data(symbol_param, current_mt5_datetime):
    """
    Menghitung dan memperbarui data sesi (Asia, Eropa, New York) dan menyimpannya.
    Ini termasuk sesi lilin dan data ayunan.
    """
    logger.info(f"Menghitung dan memperbarui data sesi untuk {symbol_param} pada {current_mt5_datetime.strftime('%Y-%m-%d %H:%M:%S UTC')}...")
    current_date = current_mt5_datetime.date()
    
    sessions = database_manager.get_session_metadata()
    logger.debug(f"market_data_processor: get_session_metadata mengembalikan: {sessions}")
    
    active_sessions_names = [] 
    detailed_sessions = [] 
    
    if not sessions:
        logger.warning("market_data_processor: Metadata sesi kosong dari database_manager. Tidak dapat menghitung status sesi.")
        with config.MarketData._market_status_data_lock:
            config.MarketData.market_status_data = {
                "session_status": [],
                "overlap_status": "Error: No Session Meta",
                "current_utc_time": current_mt5_datetime.isoformat(),
                "detailed_sessions": []
            }
        return

    for session in sessions:
        session_name = session['session_name'] 
        start_hour = session['utc_start_hour']
        end_hour = session['utc_end_hour']

        is_active = False
        session_start_dt_today = current_mt5_datetime.replace(hour=start_hour, minute=0, second=0, microsecond=0)
        session_end_dt_today = current_mt5_datetime.replace(hour=end_hour, minute=0, second=0, microsecond=0)
        session_start_dt_effective = session_start_dt_today
        session_end_dt_effective = session_end_dt_today
        if start_hour >= end_hour:
            if current_mt5_datetime.hour >= start_hour:
                session_end_dt_effective = session_end_dt_effective + timedelta(days=1)
            else:
                session_start_dt_effective = session_start_dt_effective - timedelta(days=1)
            if session_start_dt_effective <= current_mt5_datetime < session_end_dt_effective:
                is_active = True
        else:
            if session_start_dt_today <= current_mt5_datetime < session_end_dt_today:
                is_active = True
        logger.debug(f"market_data_processor: Sesi {session_name} ({start_hour:02d}:00-{end_hour:02d}:00 UTC) aktif: {is_active} pada waktu {current_mt5_datetime.strftime('%H:%M:%S UTC')}")
        if is_active:
            active_sessions_names.append(session_name)
        detailed_sessions.append({
            "name": session_name,
            "status": "Open" if is_active else "Closed",
            "start_time_utc": f"{start_hour:02d}:00", 
            "end_time_utc": f"{end_hour:02d}:00" 
        })
        window_end_dt = session_end_dt_effective + timedelta(minutes=30)
        if not (session_start_dt_effective <= current_mt5_datetime < window_end_dt):
            logger.debug(f"Sesi {session_name} tidak aktif atau tidak dalam jendela update. Melewatkan pembaruan data sesi candle/swing.")
            continue
        try:
            session_ticks = database_manager.get_price_history(
                symbol_param,
                start_time_utc=session_start_dt_effective,
                end_time_utc=session_end_dt_effective
            )
            if not session_ticks:
                logger.warning(f"Tidak ada data tick untuk sesi {session_name} ({session_start_dt_effective} - {session_end_dt_effective}).")
                continue
            session_open = session_ticks[0]['last_price']
            session_close = session_ticks[-1]['last_price']
            session_high = max(t['last_price'] for t in session_ticks)
            session_low = min(t['last_price'] for t in session_ticks)
            database_manager.save_session_candle_data(
                session_id=session['id'],
                trade_date=current_date.isoformat(),
                candle_time_utc=current_mt5_datetime,
                open_price=session_open,
                high_price=session_high,
                low_price=session_low,
                close_price=session_close,
                tick_volume=len(session_ticks),
                spread=0,
                real_volume=0,
                base_candle_name=f"{session_name}_session_ohlc",
                symbol=symbol_param
            )
            logger.info(f"Candle sesi {session_name} berhasil disimpan untuk {current_date} ({symbol_param}).")
            swing_high_time_utc = None
            for t in session_ticks:
                if t['last_price'] == session_high:
                    # MODIFIKASI INI:
                    # 't['time']' seharusnya sudah datetime object dari database_manager.get_price_history
                    swing_high_time_utc = t['time'] 
                    break
            swing_low_time_utc = None
            for t in session_ticks:
                if t['last_price'] == session_low:
                    # MODIFIKASI INI:
                    # 't['time']' seharusnya sudah datetime object dari database_manager.get_price_history
                    swing_low_time_utc = t['time'] 
                    break
            if swing_high_time_utc is None:
                logger.warning(f"Tidak dapat menemukan timestamp untuk swing high {session_high} di sesi {session_name}. Menggunakan waktu saat ini sebagai fallback.")
                swing_high_time_utc = current_mt5_datetime
            if swing_low_time_utc is None:
                logger.warning(f"Tidak dapat menemukan timestamp untuk swing low {session_low} di sesi {session_name}. Menggunakan waktu saat ini sebagai fallback.")
                swing_low_time_utc = current_mt5_datetime
            database_manager.save_session_swing_data(
                session_id=session['id'],
                trade_date=current_date.isoformat(),
                swing_high=swing_high,
                swing_low=swing_low,
                symbol=symbol_param,
                swing_high_timestamp_utc=swing_high_time_utc,
                swing_low_timestamp_utc=swing_low_time_utc
            )
            logger.info(f"Data swing sesi {session_name} berhasil disimpan untuk {current_date} ({symbol_param}).")
        except Exception as e:
            logger.error(f"Gagal memproses data sesi untuk {session_name}: {e}", exc_info=True)

    def is_in_overlap(current_h, current_m, overlap_start_h, overlap_start_m, overlap_end_h, overlap_end_m):
        start_time_total_minutes = overlap_start_h * 60 + overlap_start_m
        end_time_total_minutes = overlap_end_h * 60 + overlap_end_m
        current_time_total_minutes = current_h * 60 + current_m
        if start_time_total_minutes < end_time_total_minutes:
            return start_time_total_minutes <= current_time_total_minutes < end_time_total_minutes
        else:
            return start_time_total_minutes <= current_time_total_minutes or current_time_total_minutes < end_time_total_minutes

    overlap_status_text = "No Overlap"
    is_asia_open = "Asia" in active_sessions_names
    is_europe_open = "Europe" in active_sessions_names
    is_ny_open = "New York" in active_sessions_names
    asia_start = config.Sessions.ASIA_SESSION_START_HOUR_UTC
    asia_end = config.Sessions.ASIA_SESSION_END_HOUR_UTC
    europe_start = config.Sessions.EUROPE_SESSION_START_HOUR_UTC
    europe_end = config.Sessions.EUROPE_SESSION_END_HOUR_UTC
    ny_start = config.Sessions.NEWYORK_SESSION_START_HOUR_UTC
    ny_end = config.Sessions.NEWYORK_SESSION_END_HOUR_UTC
    current_hour = current_mt5_datetime.hour
    current_minute = current_mt5_datetime.minute
    
    if is_asia_open and is_europe_open and \
       is_in_overlap(current_hour, current_minute, europe_start, 0, asia_end, 0):
        overlap_status_text = "Asia-Europe Overlap"
    elif is_europe_open and is_ny_open and \
         is_in_overlap(current_hour, current_minute, ny_start, 0, europe_end, 0):
        overlap_status_text = "Europe-New York Overlap"
    
    with config.MarketData._market_status_data_lock:
        config.MarketData.market_status_data = {
            "session_status": active_sessions_names, 
            "overlap_status": overlap_status_text,
            "current_utc_time": current_mt5_datetime.isoformat(),
            "detailed_sessions": detailed_sessions 
        }
    logger.info(f"Status Pasar Diperbarui: Sesi Aktif={active_sessions_names}, Overlap={overlap_status_text}")



def get_session_candle_data_for_date(trade_date, symbol_param=None, start_time_utc=None, end_time_utc=None):
    """
    Mengambil data candle sesi untuk tanggal dan simbol tertentu.
    Memastikan harga adalah Decimal dan waktu adalah datetime object.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di get_session_candle_data_for_date. Tidak dapat melanjutkan.")
        return []
    db = SessionLocal()
    try:
        query = db.query(SessionCandleData, SessionMetadata).\
                    join(SessionMetadata).\
                    filter(SessionCandleData.trade_date == trade_date)
        if symbol_param:
            query = query.filter(SessionCandleData.symbol == symbol_param)
        # Modifikasi: Pastikan start_time_utc dan end_time_utc adalah datetime aware UTC untuk filter
        if start_time_utc:
            start_time_utc_processed = to_utc_datetime_or_none(start_time_utc)
            if start_time_utc_processed:
                query = query.filter(SessionCandleData.candle_time_utc >= start_time_utc_processed)
        if end_time_utc:
            end_time_utc_processed = to_utc_datetime_or_none(end_time_utc)
            if end_time_utc_processed:
                query = query.filter(SessionCandleData.candle_time_utc <= end_time_utc_processed)
        
        candles = query.all()
        result = []
        for candle_db, meta_db in candles:
            result.append({
                "session_name": meta_db.session_name,
                "symbol": candle_db.symbol,
                "candle_time_utc": candle_db.candle_time_utc, # Ini sudah datetime object dari model
                "open_price": candle_db.open_price, # Ini sudah Decimal dari model
                "high_price": candle_db.high_price, # Ini sudah Decimal dari model
                "low_price": candle_db.low_price,   # Ini sudah Decimal dari model
                "close_price": candle_db.close_price, # Ini sudah Decimal dari model
                "tick_volume": candle_db.tick_volume,
                "spread": candle_db.spread,
                "real_volume": candle_db.real_volume,
                "base_candle_name": candle_db.base_candle_name
            })
        return result
    except Exception as e:
        logger.error(f"Gagal mengambil session candle data dari DB for {trade_date}, symbol {symbol_param}: {e}", exc_info=True)
        return []
    finally:
        db.close()

def save_price_tick(symbol, time, time_utc_datetime, last_price, bid_price, daily_open_price, delta_point, change, change_percent):
    """
    Menyimpan satu tick harga ke database.
    Memastikan harga adalah Decimal dan waktu adalah datetime object.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di save_price_tick. Tidak dapat melanjutkan.")
        return

    try:
        # Modifikasi: Gunakan helper to_utc_datetime_or_none
        time_utc_datetime_processed = to_utc_datetime_or_none(time_utc_datetime)
        if time_utc_datetime_processed is None:
            logger.error(f"Gagal mengonversi time_utc_datetime '{time_utc_datetime}' ke datetime aware UTC untuk price tick. Melewatkan penyimpanan.")
            return

        # Modifikasi: Konversi harga dan delta ke Decimal menggunakan helper to_decimal_or_none
        last_price_processed = to_decimal_or_none(last_price)
        bid_price_processed = to_decimal_or_none(bid_price)
        daily_open_price_processed = to_decimal_or_none(daily_open_price)
        delta_point_processed = to_decimal_or_none(delta_point)
        change_processed = to_decimal_or_none(change)
        change_percent_processed = to_decimal_or_none(change_percent)

        # Modifikasi: Periksa apakah ada nilai harga yang tidak valid setelah konversi
        # daily_open_price, delta_point, change, change_percent bisa None, jadi hanya cek last_price dan bid_price
        if any(v is None for v in [last_price_processed, bid_price_processed]):
             logger.warning(f"Beberapa harga tick tidak valid untuk {symbol} @ {time_utc_datetime}. Melewatkan penyimpanan.")
             return

        processed_data = {
            "symbol": symbol,
            "time": time, # 'time' ini adalah BigInteger timestamp dari MT5, tidak perlu konversi
            "time_utc_datetime": time_utc_datetime_processed, # <--- KOREKSI: KIRIM OBJEK DATETIME ASLI
            "last_price": last_price_processed,             # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "bid_price": bid_price_processed,               # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "daily_open_price": daily_open_price_processed, # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "delta_point": delta_point_processed,           # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "change": change_processed,                     # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "change_percent": change_percent_processed,     # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "timestamp_recorded": datetime.now(timezone.utc)
        }

        _db_write_queue.put({
            'op_type': 'single_save_update',
            'model_class': PriceTick,
            'single_item_data': processed_data,
            'single_item_key_columns': ['symbol', 'time']
        })
        logger.debug(f"Price tick untuk {symbol} @ {time_utc_datetime} dikirim ke antrean DB.")
    except Exception as e:
        logger.error(f"Gagal menyiapkan price tick untuk antrean DB untuk {symbol} @ {time_utc_datetime}: {e}", exc_info=True)



def save_price_tick_batch(ticks_data_list):
    """
    Menyimpan daftar tick harga ke database dalam satu batch transaksi.
    Memastikan semua data harga adalah Decimal dan waktu adalah datetime object.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di save_price_tick_batch. Tidak dapat melanjutkan.")
        return

    if not ticks_data_list:
        logger.debug("Tidak ada tick dalam batch untuk disimpan.")
        return
    logger.debug(f"DB Batch Save: save_price_tick_batch menerima {len(ticks_data_list)} tick.")
    if ticks_data_list:
        first_tick = ticks_data_list[0]
        last_tick = ticks_data_list[-1]
        # Modifikasi: Pastikan harga diformat dengan benar untuk logging
        logger.debug(
            f"DB Batch Save: Contoh tick pertama: "
            f"Time_UTC_DT={utils.to_iso_format_or_none(first_tick['time_utc_datetime'])}, Last={utils.to_float_or_none(first_tick['last_price']):.5f}, Bid={utils.to_float_or_none(first_tick['bid_price']):.5f}. "
            f"Contoh tick terakhir: "
            f"Time_UTC_DT={utils.to_iso_format_or_none(last_tick['time_utc_datetime'])}, Last={utils.to_float_or_none(last_tick['last_price']):.5f}, Bid={utils.to_float_or_none(last_tick['bid_price']):.5f}."
        )

    new_tick_objects = []
    for tick_data in ticks_data_list:
        try:
            # Modifikasi: Gunakan helper to_utc_datetime_or_none
            time_utc_datetime_obj = utils.to_utc_datetime_or_none(tick_data.get("time_utc_datetime")) # Gunakan .get() untuk keamanan
            if time_utc_datetime_obj is None:
                logger.warning(f"Gagal mengonversi time_utc_datetime '{tick_data.get('time_utc_datetime')}' ke datetime aware UTC untuk batch tick. Melewatkan tick ini.")
                continue

            # Modifikasi: Konversi harga dan delta ke Decimal. Tangani None dengan baik.
            last_price_dec = utils.to_decimal_or_none(tick_data.get("last_price"))
            bid_price_dec = utils.to_decimal_or_none(tick_data.get("bid_price"))
            daily_open_price_dec = utils.to_decimal_or_none(tick_data.get("daily_open_price"))
            delta_point_dec = utils.to_decimal_or_none(tick_data.get("delta_point"))
            change_dec = utils.to_decimal_or_none(tick_data.get("change"))
            change_percent_dec = utils.to_decimal_or_none(tick_data.get("change_percent"))

            # Modifikasi: Periksa apakah ada nilai harga yang tidak valid setelah konversi
            if any(v is None for v in [last_price_dec, bid_price_dec]):
                logger.warning(f"Beberapa harga tick tidak valid untuk batch {tick_data.get('symbol')} @ {tick_data.get('time_utc_datetime')}. Melewatkan tick ini.")
                continue

            # Buat objek PriceTick dengan data yang sudah diproses
            new_tick_objects.append(PriceTick(
                symbol=tick_data.get("symbol"),
                time=tick_data.get("time"), # 'time' ini adalah BigInteger timestamp dari MT5, tidak perlu konversi
                time_utc_datetime=time_utc_datetime_obj,
                last_price=last_price_dec,
                bid_price=bid_price_dec,
                daily_open_price=daily_open_price_dec,
                delta_point=delta_point_dec,
                change=change_dec,
                change_percent=change_percent_dec,
                timestamp_recorded=datetime.now(timezone.utc) # Pastikan ini datetime object
            ))
        except KeyError as ke:
            logger.error(f"Kunci data tick tidak ditemukan di batch: {ke}. Item dilewati: {tick_data}", exc_info=True)
            continue
        except Exception as e:
            logger.error(f"Error saat menyiapkan tick untuk penyimpanan batch DB (item dilewati): {e}. Data: {tick_data}", exc_info=True)
            continue

    if new_tick_objects:
        _db_write_queue.put({
            'op_type': 'batch_save_update',
            'model_class': PriceTick,
            'new_items_data': new_tick_objects,
            'updated_items_data': [], # Tick biasanya hanya di-insert, jarang di-update oleh batch ini
            'unique_columns': ['symbol', 'time'],
            'pkey_column_name': 'id'
        })
        logger.debug(f"Price tick batch ({len(new_tick_objects)} ticks) dikirim ke antrean DB.")
    else:
        logger.debug("Tidak ada tick valid yang berhasil disiapkan untuk dikirim ke antrean DB.")




def save_session_swing_data(session_id, trade_date, swing_high, swing_high_timestamp_utc, swing_low, swing_low_timestamp_utc, symbol):
    """
    Menyimpan atau memperbarui data swing sesi untuk simbol dan tanggal tertentu.
    Memastikan swing_high/low adalah Decimal dan timestamp adalah datetime object.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di save_session_swing_data. Tidak dapat melanjutkan.")
        return

    try:
        # Modifikasi: Gunakan helper to_decimal_or_none
        swing_high_processed = to_decimal_or_none(swing_high)
        swing_low_processed = to_decimal_or_none(swing_low)

        # Modifikasi: Gunakan helper to_utc_datetime_or_none
        swing_high_timestamp_utc_processed = to_utc_datetime_or_none(swing_high_timestamp_utc)
        swing_low_timestamp_utc_processed = to_utc_datetime_or_none(swing_low_timestamp_utc)

        # Modifikasi: Periksa apakah ada nilai yang tidak valid setelah konversi
        if swing_high_processed is None or swing_low_processed is None:
            logger.warning(f"Nilai swing high atau low tidak valid untuk {symbol} (Session: {session_id}, Date: {trade_date}). Melewatkan penyimpanan.")
            return
        if swing_high_timestamp_utc_processed is None or swing_low_timestamp_utc_processed is None:
            logger.warning(f"Timestamp swing high atau low tidak valid untuk {symbol} (Session: {session_id}, Date: {trade_date}). Melewatkan penyimpanan.")
            return

        processed_data = {
            "session_id": session_id,
            "trade_date": trade_date,
            "swing_high": swing_high_processed, # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "swing_high_timestamp_utc": swing_high_timestamp_utc_processed, # <--- KOREKSI: KIRIM OBJEK DATETIME ASLI
            "swing_low": swing_low_processed, # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "swing_low_timestamp_utc": swing_low_timestamp_utc_processed, # <--- KOREKSI: KIRIM OBJEK DATETIME ASLI
            "symbol": symbol,
            "timestamp_recorded": datetime.now(timezone.utc)
        }

        _db_write_queue.put({
            'op_type': 'single_save_update',
            'model_class': SessionSwingData,
            'single_item_data': processed_data,
            'single_item_key_columns': ['session_id', 'trade_date', 'symbol']
        })
        logger.debug(f"Session Swing untuk {symbol} (Session: {session_id}, Date: {trade_date}) dikirim ke antrean DB.")
    except Exception as e:
        logger.error(f"Gagal menyiapkan session swing data untuk antrean DB untuk {symbol} (Session: {session_id}, Date: {trade_date}): {e}", exc_info=True)



def get_session_swing_data_for_date(trade_date, symbol_param=None, start_time_utc=None, end_time_utc=None):
    """
    Mengambil data swing sesi untuk tanggal dan simbol tertentu.
    Memastikan swing_high/low adalah Decimal dan timestamp adalah datetime object.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di get_session_swing_data_for_date. Tidak dapat melanjutkan.")
        return []
    db = SessionLocal()
    try:
        query = db.query(SessionSwingData, SessionMetadata).\
                   join(SessionMetadata).\
                   filter(SessionSwingData.trade_date == trade_date)
        if symbol_param:
            query = query.filter(SessionSwingData.symbol == symbol_param)
        # Modifikasi: Pastikan start_time_utc dan end_time_utc adalah datetime aware UTC untuk filter
        if start_time_utc:
            start_time_utc_processed = to_utc_datetime_or_none(start_time_utc)
            if start_time_utc_processed:
                query = query.filter(SessionSwingData.swing_high_timestamp_utc >= start_time_utc_processed)
        if end_time_utc:
            end_time_utc_processed = to_utc_datetime_or_none(end_time_utc)
            if end_time_utc_processed:
                query = query.filter(SessionSwingData.swing_high_timestamp_utc <= end_time_utc_processed)
        
        swings = query.all()
        result = []
        for swing_db, meta_db in swings:
            result.append({
                "session_name": meta_db.session_name,
                "symbol": swing_db.symbol,
                "swing_high": swing_db.swing_high, # Ini sudah Decimal dari model
                "swing_high_timestamp_utc": swing_db.swing_high_timestamp_utc, # Ini sudah datetime object dari model
                "swing_low": swing_db.swing_low, # Ini sudah Decimal dari model
                "swing_low_timestamp_utc": swing_db.swing_low_timestamp_utc, # Ini sudah datetime object dari model
                "trade_date": swing_db.trade_date
            })
        return result
    except Exception as e:
        logger.error(f"Gagal mengambil session swing data from DB for {trade_date}, symbol {symbol_param}: {e}", exc_info=True)
        return []
    finally:
        db.close()


def save_historical_candles(candles_data_list):
    """
    Menyimpan daftar data candle historis ke database dalam satu batch transaksi.
    Memastikan semua data harga adalah Decimal dan waktu adalah datetime object.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di save_historical_candles. Tidak dapat melanjutkan.")
        return

    if not candles_data_list:
        logger.debug("Tidak ada candle dalam batch untuk disimpan.")
        return

    new_candle_objects = []
    for candle_data in candles_data_list:
        try:
            # Modifikasi: Gunakan helper to_utc_datetime_or_none
            open_time_utc_obj = utils.to_utc_datetime_or_none(candle_data.get("open_time_utc"))
            if open_time_utc_obj is None:
                logger.warning(f"Gagal mengonversi open_time_utc '{candle_data.get('open_time_utc')}' ke datetime aware UTC untuk historical candle. Melewatkan candle ini.")
                continue

            # Modifikasi: Konversi harga ke Decimal. Tangani None jika ada.
            open_price_dec = utils.to_decimal_or_none(candle_data.get("open_price"))
            high_price_dec = utils.to_decimal_or_none(candle_data.get("high_price"))
            low_price_dec = utils.to_decimal_or_none(candle_data.get("low_price"))
            close_price_dec = utils.to_decimal_or_none(candle_data.get("close_price"))

            # MODIFIKASI KRITIS INI: Validasi semua kolom NOT NULL sebelum membuat objek
            # Kolom NOT NULL di HistoricalCandle: symbol, timeframe, open_time_utc, open_price, high_price, low_price, close_price, tick_volume, spread
            required_not_null_fields = [
                ("symbol", candle_data.get("symbol")),
                ("timeframe", candle_data.get("timeframe")),
                ("open_time_utc", open_time_utc_obj),
                ("open_price", open_price_dec),
                ("high_price", high_price_dec),
                ("low_price", low_price_dec),
                ("close_price", close_price_dec)
            ]

            missing_fields = []
            for field_name, value in required_not_null_fields:
                if value is None or (isinstance(value, str) and not value.strip()):
                    missing_fields.append(field_name)

            # Pastikan tick_volume dan spread juga tidak None, berikan default aman jika kosong
            tick_volume_int = utils.to_int_or_none(candle_data.get("tick_volume"))
            if tick_volume_int is None: tick_volume_int = 0

            spread_int = utils.to_int_or_none(candle_data.get("spread"))
            if spread_int is None: spread_int = 0

            real_volume_int = utils.to_int_or_none(candle_data.get("real_volume"))
            if real_volume_int is None: real_volume_int = 0

            if missing_fields:
                logger.error(f"Data candle historis TIDAK LENGKAP untuk {candle_data.get('symbol')} {candle_data.get('timeframe')} @ {candle_data.get('open_time_utc')}. Kolom hilang/NULL: {', '.join(missing_fields)}. Melewatkan candle ini.")
                continue # Lewati seluruh candle jika ada data NOT NULL yang hilang

            # Buat objek HistoricalCandle dengan data yang sudah diproses dan divalidasi
            new_candle_objects.append(HistoricalCandle(
                symbol=candle_data.get("symbol"),
                timeframe=candle_data.get("timeframe"),
                open_time_utc=open_time_utc_obj,
                open_price=open_price_dec,
                high_price=high_price_dec,
                low_price=low_price_dec,
                close_price=close_price_dec,
                tick_volume=tick_volume_int,
                spread=spread_int,
                real_volume=real_volume_int,
                timestamp_recorded=datetime.now(timezone.utc)
            ))
        except KeyError as ke:
            logger.error(f"Kunci data candle historis tidak ditemukan di batch: {ke}. Item dilewati: {candle_data}", exc_info=True)
            continue
        except Exception as e:
            logger.error(f"Error saat menyiapkan historical candle untuk penyimpanan batch DB (item dilewati): {e}. Data: {candle_data}", exc_info=True)
            continue

    if new_candle_objects:
        _db_write_queue.put({
            'op_type': 'batch_save_update',
            'model_class': HistoricalCandle,
            'new_items_data': new_candle_objects,
            'updated_items_data': [], # Candle biasanya hanya di-insert, jarang di-update
            'unique_columns': ['symbol', 'timeframe', 'open_time_utc'],
            'pkey_column_name': 'id'
        })
        logger.debug(f"Historical candles batch ({len(new_candle_objects)} candles) dikirim ke antrean DB.")
    else:
        logger.debug("Tidak ada historical candle valid yang berhasil disiapkan untuk dikirim ke antrean DB.")




def get_historical_candles_from_db(symbol: str, timeframe: str, # Pastikan parameternya 'symbol'
                                    start_time_utc: datetime = None,
                                    end_time_utc: datetime = None,
                                    min_open_time_utc: datetime = None,
                                    limit: int = None,
                                    order_asc: bool = True):

    """
    Mengambil candle historis dari database.
    Memastikan semua data harga adalah Decimal dan waktu adalah datetime object.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di get_historical_candles_from_db. Tidak dapat melanjutkan.")
        return []
    db = SessionLocal()
    try:
        query = db.query(HistoricalCandle).filter(
            HistoricalCandle.symbol == symbol,
            HistoricalCandle.timeframe == timeframe
        )
        # Modifikasi: Pastikan start_time_utc, end_time_utc, dan min_open_time_utc adalah datetime aware UTC untuk filter
        if start_time_utc:
            start_time_utc_processed = to_utc_datetime_or_none(start_time_utc)
            if start_time_utc_processed:
                query = query.filter(HistoricalCandle.open_time_utc >= start_time_utc_processed)
        if end_time_utc:
            end_time_utc_processed = to_utc_datetime_or_none(end_time_utc)
            if end_time_utc_processed:
                query = query.filter(HistoricalCandle.open_time_utc <= end_time_utc_processed)
        if min_open_time_utc:
            min_open_time_utc_processed = to_utc_datetime_or_none(min_open_time_utc)
            if min_open_time_utc_processed:
                query = query.filter(HistoricalCandle.open_time_utc >= min_open_time_utc_processed)

        # --- Bagian order_by dan limit SQLAlchemy ---
        if order_asc:
            query = query.order_by(HistoricalCandle.open_time_utc.asc())
        else:
            query = query.order_by(HistoricalCandle.open_time_utc.desc())

        # Tetap panggil query.limit() di sini
        if limit is not None:
            query = query.limit(limit)

        candles = query.all() # Fetch the data

        # --- PENTING: PENEGAKAN LIMIT MANUAL SETELAH DATA DITARIK ---
        # Ini adalah fallback jika query.limit() tidak efektif
        # Pastikan `candles` sudah dalam urutan yang diinginkan (ASC) sebelum memotong
        if not order_asc: # Jika awalnya DESC, balikkan menjadi ASC untuk pemotongan yang benar
            candles.reverse()

        if limit is not None and limit > 0 and len(candles) > limit:
            # Ambil `limit` candle terbaru (akhir dari list yang sudah ASC)
            candles = candles[-limit:]
            logger.debug(f"DEBUG DB: Limit manual diterapkan. Awalnya {len(candles) + limit}, sekarang {len(candles)}.")


        # --- AKHIR PENEGAKAN LIMIT MANUAL ---

        result = []
        for c in candles:
            if c is None:
                logger.warning(f"DEBUG DB: Found a None candle object for {symbol_param} {timeframe_str}. Skipping.")
                continue
            if not hasattr(c, 'open_time_utc'):
                logger.error(f"DEBUG DB: Candle object for {symbol_param} {timeframe_str} is missing 'open_time_utc'. Object: {c}")
                continue

            result.append({
                "symbol": c.symbol,
                "timeframe": c.timeframe,
                "open_time_utc": c.open_time_utc, # Ini sudah datetime object dari model
                "open_price": c.open_price, # Ini sudah Decimal dari model
                "high_price": c.high_price, # Ini sudah Decimal dari model
                "low_price": c.low_price,   # Ini sudah Decimal dari model
                "close_price": c.close_price, # Ini sudah Decimal dari model
                "tick_volume": c.tick_volume,
                "spread": c.spread,
                "real_volume": c.real_volume
            })
        return result
    except Exception as e:
        logger.error(f"Gagal mengambil historical candles dari DB untuk {symbol_param} {timeframe_str}: {e}", exc_info=True)
        return []
    finally:
        db.close()

def get_price_history(symbol_param, limit=None, start_time_utc=None, end_time_utc=None):
    """
    Mengambil riwayat harga (tick) dari database.
    Memastikan semua data harga adalah Decimal dan waktu adalah datetime object.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di get_price_history. Tidak dapat melanjutkan.")
        return []
    db = SessionLocal()
    try:
        query = db.query(PriceTick).filter(
            PriceTick.symbol == symbol_param
        )
        # Modifikasi: Pastikan start_time_utc dan end_time_utc adalah datetime aware UTC untuk filter
        if start_time_utc:
            start_time_utc_processed = to_utc_datetime_or_none(start_time_utc)
            if start_time_utc_processed:
                query = query.filter(PriceTick.time_utc_datetime >= start_time_utc_processed)
        if end_time_utc:
            end_time_utc_processed = to_utc_datetime_or_none(end_time_utc)
            if end_time_utc_processed:
                query = query.filter(PriceTick.time_utc_datetime <= end_time_utc_processed)
        
        query = query.order_by(PriceTick.time_utc_datetime.asc())
        
        if limit:
            query = query.limit(limit)
        
        ticks = query.all()
        
        result = []
        for t in ticks:
            result.append({
                "symbol": t.symbol,
                "time": t.time_utc_datetime, # Ini sudah datetime object dari model
                "time_utc_datetime": t.time_utc_datetime, # Ini juga sudah datetime object dari model
                "last_price": t.last_price, # Ini sudah Decimal dari model
                "bid_price": t.bid_price,   # Ini sudah Decimal dari model
                "daily_open_price": t.daily_open_price, # Ini sudah Decimal dari model
                "delta_point": t.delta_point, # Ini sudah Decimal dari model
                "change": t.change,           # Ini sudah Decimal dari model
                "change_percent": t.change_percent # Ini sudah Decimal dari model
            })
        return result
    except Exception as e:
        logger.error(f"Gagal mengambil price history dari DB untuk {symbol_param}: {e}", exc_info=True)
        return []
    finally:
        db.close()


def get_mt5_account_info_from_db():
    """
    Mengambil informasi akun MT5 terbaru dari database.
    Memastikan semua nilai moneter adalah Decimal dan timestamp adalah datetime object.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di get_mt5_account_info_from_db. Tidak dapat melanjutkan.")
        return None
    db = SessionLocal()
    try:
        account_info = db.query(MT5AccountInfo).order_by(MT5AccountInfo.timestamp_recorded.desc()).first()
        if account_info:
            return {
                "login": account_info.login,
                "name": account_info.name,
                "server": account_info.server,
                "balance": account_info.balance, # Ini sudah Decimal dari model
                "equity": account_info.equity,   # Ini sudah Decimal dari model
                "profit": account_info.profit,   # Ini sudah Decimal dari model
                "free_margin": account_info.free_margin, # Ini sudah Decimal dari model
                "currency": account_info.currency,
                "leverage": account_info.leverage,
                "company": account_info.company,
                "timestamp_recorded": account_info.timestamp_recorded # Ini sudah datetime object dari model
            }
        return None
    except Exception as e:
        logger.error(f"Gagal mengambil info akun MT5 dari DB: {e}", exc_info=True)
        return None
    finally:
        db.close()



def get_latest_price_tick(symbol_param):
    """
    Mengambil tick harga terbaru untuk simbol tertentu dari database.
    Memastikan semua data harga adalah Decimal dan waktu adalah datetime object.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di get_latest_price_tick. Tidak dapat melanjutkan.")
        return None
    db = SessionLocal()
    try:
        latest_tick = db.query(PriceTick).filter(PriceTick.symbol == symbol_param).order_by(PriceTick.time.desc()).first()
        if latest_tick:
            # Modifikasi: Pastikan harga diformat dengan benar untuk logging
            logger.debug(
                f"DB Query: get_latest_price_tick untuk {symbol_param}: "
                f"Time_UTC_DT={utils.to_iso_format_or_none(latest_tick.time_utc_datetime)}, "
                f"Last_Price={utils.to_float_or_none(latest_tick.last_price):.5f}, "
                f"Bid_Price={utils.to_float_or_none(latest_tick.bid_price):.5f}"
            )
            return {
                "symbol": latest_tick.symbol,
                "time": latest_tick.time_utc_datetime, # Ini sudah datetime object dari model
                "last_price": latest_tick.last_price, # Ini sudah Decimal dari model
                "bid_price": latest_tick.bid_price,   # Ini sudah Decimal dari model
                "daily_open_price": latest_tick.daily_open_price, # Ini sudah Decimal dari model
                "delta_point": latest_tick.delta_point, # Ini sudah Decimal dari model
                "change": latest_tick.change,           # Ini sudah Decimal dari model
                "change_percent": latest_tick.change_percent # Ini sudah Decimal dari model
            }
        else: # Modifikasi: Tambahkan else block untuk logging jika tidak ada tick
            logger.debug(f"DB Query: get_latest_price_tick untuk {symbol_param}: Tidak ada tick ditemukan.")
            return None
    except Exception as e:
        logger.error(f"Gagal mengambil tick harga terbaru dari DB untuk {symbol_param}: {e}", exc_info=True)
        return None
    finally:
        db.close()


def update_mt5_trade_data_periodically(symbol_param):
    """
    Mengambil dan memperbarui informasi akun, posisi, dan order dari MT5
    serta menyimpan riwayat deal yang baru.
    Memastikan semua data dikonversi ke tipe data yang benar (Decimal, datetime).
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di update_mt5_trade_data_periodically. Tidak dapat melanjutkan.")
        return

    new_positions_data = []
    new_orders_data = []
    new_deals_data = []
    updated_account_data_dict = None

    # --- Bagian 1: Update Account Info ---
    account_info_data = mt5_connector.get_mt5_account_info_raw()
    if account_info_data:
        try:
            balance_dec = utils.to_decimal_or_none(account_info_data.get('balance'))
            equity_dec = utils.to_decimal_or_none(account_info_data.get('equity'))
            profit_dec = utils.to_decimal_or_none(account_info_data.get('profit'))
            free_margin_dec = utils.to_decimal_or_none(account_info_data.get('margin_free'))

            updated_account_data_dict = {
                "login": account_info_data.get('login'),
                "name": account_info_data.get('name'),
                "server": account_info_data.get('server'),
                "balance": balance_dec,
                "equity": equity_dec,
                "profit": profit_dec,
                "free_margin": free_margin_dec,
                "currency": account_info_data.get('currency'),
                "leverage": account_info_data.get('leverage'),
                "company": account_info_data.get('company'),
                "timestamp_recorded": datetime.now(timezone.utc)
            }
            logger.debug("MT5 Account Info disiapkan untuk update.")
        except Exception as e:
            logger.error(f"Gagal memproses data akun MT5 mentah: {e}", exc_info=True)
            updated_account_data_dict = None
    else:
        logger.warning("Tidak dapat mengambil info akun MT5.")

    # --- Bagian 2: Update Positions ---
    db_session_for_delete = None
    try:
        with _db_write_lock: # PENTING: ini harus memegang lock
            db_session_for_delete = SessionLocal()
            db_session_for_delete.execute(
                text("DELETE FROM mt5_positions WHERE symbol = :symbol_param"),
                {'symbol_param': symbol_param}
            )
            db_session_for_delete.commit()
            logger.debug(f"Posisi lama untuk {symbol_param} berhasil dihapus.")
    except Exception as e:
        logger.error(f"Gagal menghapus posisi lama untuk {symbol_param}: {e}", exc_info=True)
        if db_session_for_delete: db_session_for_delete.rollback()
    finally:
        if db_session_for_delete: db_session_for_delete.close()

    positions_data = mt5_connector.get_mt5_positions_raw()
    if positions_data:
        for pos in positions_data:
            if pos.get('symbol') == symbol_param:
                try:
                    # --- START MODIFIKASI: Pastikan partial_tp_hit_flags_json dan tp_levels_config_json selalu ada ---
                    # Coba ambil status yang sudah ada di DB untuk posisi ini
                    # Fungsi get_mt5_position_partial_tp_flags ada di database_manager.py
                    existing_flags_and_config = get_mt5_position_partial_tp_flags(pos.get('ticket'))

                    # Inisialisasi dengan default jika tidak ada di DB, atau ambil dari yang sudah ada
                    initial_hit_flags_json = json.dumps(existing_flags_and_config.get('hit_flags', [])) if existing_flags_and_config else "[]"
                    initial_tp_config_json = json.dumps(existing_flags_and_config.get('tp_levels_config', [])) if existing_flags_and_config else "[]"
                    # --- END MODIFIKASI ---

                    new_positions_data.append({
                        "ticket": pos.get('ticket'),
                        "symbol": pos.get('symbol'),
                        "type": "buy" if pos.get('type') == mt5_connector.mt5.POSITION_TYPE_BUY else "sell",
                        "volume": utils.to_decimal_or_none(pos.get('volume')),
                        "price_open": utils.to_decimal_or_none(pos.get('price_open')),
                        "time_open": utils.to_utc_datetime_or_none(pos.get('time')),
                        "current_price": utils.to_decimal_or_none(pos.get('price_current')),
                        "profit": utils.to_decimal_or_none(pos.get('profit')),
                        "swap": utils.to_decimal_or_none(pos.get('swap')),
                        "comment": pos.get('comment'),
                        "magic": pos.get('magic'),
                        "sl_price": utils.to_decimal_or_none(pos.get('sl')),
                        "tp_price": utils.to_decimal_or_none(pos.get('tp')),
                        # --- START MODIFIKASI: Tambahkan kolom baru di sini ---
                        "partial_tp_hit_flags_json": initial_hit_flags_json,
                        "tp_levels_config_json": initial_tp_config_json,
                        # --- END MODIFIKASI ---
                        "timestamp_recorded": datetime.now(timezone.utc)
                    })
                except Exception as e:
                    logger.error(f"Gagal memproses data posisi mentah untuk {pos.get('symbol')} (ticket {pos.get('ticket')}): {e}", exc_info=True)
                    continue
        logger.info(f"Menyiapkan {len(new_positions_data)} posisi terbuka untuk {symbol_param} untuk dikirim ke antrean.")
    else:
        logger.debug(f"Tidak ada posisi terbuka untuk {symbol_param}.")

    # --- Bagian 3: Update Orders (Pending Orders) ---
    db_session_for_delete = None
    try:
        with _db_write_lock: # PENTING: ini harus memegang lock
            db_session_for_delete = SessionLocal()
            db_session_for_delete.execute(
                text("DELETE FROM mt5_orders WHERE symbol = :symbol_param"),
                {'symbol_param': symbol_param}
            )
            db_session_for_delete.commit()
            logger.debug(f"Order lama untuk {symbol_param} berhasil dihapus.")
    except Exception as e:
        logger.error(f"Gagal menghapus order lama untuk {symbol_param}: {e}", exc_info=True)
        if db_session_for_delete: db_session_for_delete.rollback()
    finally:
        if db_session_for_delete: db_session_for_delete.close()

    orders_data = mt5_connector.get_mt5_orders_raw()
    if orders_data:
        for order in orders_data: # 'order' di sini sudah dictionary
            if order.get('symbol') == symbol_param:
                try:
                    new_orders_data.append({
                        "ticket": order.get('ticket'),
                        "symbol": order.get('symbol'),
                        "type": str(order.get('type')),
                        "price_open": utils.to_decimal_or_none(order.get('price_open')),
                        "volume_initial": utils.to_decimal_or_none(order.get('volume_initial')),
                        "volume_current": utils.to_decimal_or_none(order.get('volume_current')),
                        "time_setup": utils.to_utc_datetime_or_none(order.get('time_setup')),
                        "expiration": utils.to_utc_datetime_or_none(order.get('time_expiration')),
                        "comment": order.get('comment'),
                        "magic": order.get('magic'),
                        "state": str(order.get('state')),
                        "timestamp_recorded": datetime.now(timezone.utc)
                    })
                except Exception as e:
                    logger.error(f"Gagal memproses data order mentah untuk {order.get('symbol')} (ticket {order.get('ticket')}): {e}", exc_info=True)
                    continue
        logger.info(f"Menyiapkan {len(new_orders_data)} order pending untuk {symbol_param} untuk dikirim ke antrean.")
    else:
        logger.debug(f"Tidak ada order pending untuk {symbol_param}.")

    # --- Bagian 4: Update Deal History (Hanya ambil deal baru) ---
    db_session = SessionLocal()
    last_deal_time = None
    try:
        last_deal_from_db = db_session.query(MT5DealHistory).filter_by(symbol=symbol_param).order_by(MT5DealHistory.time.desc()).first()
        if last_deal_from_db:
            last_deal_time = last_deal_from_db.time # Ini sudah datetime object
        else:
            last_deal_time = datetime.now(timezone.utc) - timedelta(days=7) # Default 7 hari ke belakang
    except Exception as e:
        logger.error(f"Gagal mengambil deal terakhir untuk {symbol_param} dari DB: {e}", exc_info=True)
        last_deal_time = datetime.now(timezone.utc) - timedelta(days=7) # Fallback jika ada error
    finally:
        if db_session: db_session.close()

    from_time_dt = last_deal_time
    current_time_dt = datetime.now(timezone.utc)

    deals_data = mt5_connector.get_mt5_deals_history_raw(from_time_dt, current_time_dt, group=symbol_param)

    if deals_data:
        for deal in deals_data:
            try:
                new_deals_data.append({
                    "ticket": deal.get('ticket'),
                    "order_ticket": deal.get('order'),
                    "symbol": deal.get('symbol'),
                    "type": str(deal.get('type')),
                    "entry_type": str(deal.get('entry')),
                    "price": utils.to_decimal_or_none(deal.get('price')),
                    "volume": utils.to_decimal_or_none(deal.get('volume')),
                    "profit": utils.to_decimal_or_none(deal.get('profit', '0.0')),
                    "time": utils.to_utc_datetime_or_none(deal.get('time')),
                    "comment": deal.get('comment'),
                    "magic": deal.get('magic'),
                    "timestamp_recorded": datetime.now(timezone.utc)
                })
            except Exception as e:
                logger.error(f"Gagal memproses data deal mentah untuk {deal.get('symbol', 'N/A')} (ticket {deal.get('ticket', 'N/A')}): {e}", exc_info=True)
                continue
        logger.info(f"Menyiapkan {len(new_deals_data)} deal history baru untuk {symbol_param} untuk dikirim ke antrean.")
    else:
        logger.debug(f"Tidak ada deal history baru untuk {symbol_param}.")

    # --- Bagian 5: Kirim Semua Data yang Dikumpulkan ke Antrean Penulis DB ---
    if updated_account_data_dict:
        _db_write_queue.put({
            'op_type': 'single_save_update',
            'model_class': MT5AccountInfo,
            'single_item_data': updated_account_data_dict,
            'single_item_key_columns': ['login'],
            'pkey_column_name': 'login'
        })
        logger.debug(f"MT5 Account Info dikirim ke antrean DB.")

    if new_positions_data:
        new_positions_objects = []
        for pos_dict in new_positions_data:
            try:
                new_positions_objects.append(MT5Position(
                    ticket=pos_dict['ticket'],
                    symbol=pos_dict['symbol'],
                    type=pos_dict['type'],
                    volume=pos_dict['volume'],
                    price_open=pos_dict['price_open'],
                    time_open=pos_dict['time_open'],
                    current_price=pos_dict['current_price'],
                    profit=pos_dict['profit'],
                    swap=pos_dict['swap'],
                    comment=pos_dict['comment'],
                    magic=pos_dict['magic'],
                    sl_price=pos_dict['sl_price'],
                    tp_price=pos_dict['tp_price'],
                    partial_tp_hit_flags_json=pos_dict['partial_tp_hit_flags_json'], # Sudah ditambahkan
                    tp_levels_config_json=pos_dict['tp_levels_config_json'], # Sudah ditambahkan
                    timestamp_recorded=pos_dict['timestamp_recorded']
                ))
            except Exception as e:
                logger.error(f"Gagal membuat objek MT5Position dari dict: {e}. Item dilewati: {pos_dict}", exc_info=True)
                continue

        _db_write_queue.put({
            'op_type': 'batch_save_update',
            'model_class': MT5Position,
            'new_items_data': new_positions_objects,
            'updated_items_data': [],
            'unique_columns': ['ticket'],
            'pkey_column_name': 'ticket'
        })
        logger.debug(f"MT5 Positions ({len(new_positions_data)}) dikirim ke antrean DB.")

    if new_orders_data:
        new_orders_objects = []
        for order_dict in new_orders_data:
            try:
                new_orders_objects.append(MT5Order(
                    ticket=order_dict['ticket'],
                    symbol=order_dict['symbol'],
                    type=order_dict['type'],
                    price_open=order_dict['price_open'],
                    volume_initial=order_dict['volume_initial'],
                    volume_current=order_dict['volume_current'],
                    time_setup=order_dict['time_setup'],
                    expiration=order_dict['expiration'],
                    comment=order_dict['comment'],
                    magic=order_dict['magic'],
                    state=order_dict['state'],
                    timestamp_recorded=order_dict['timestamp_recorded']
                ))
            except Exception as e:
                logger.error(f"Gagal membuat objek MT5Order dari dict: {e}. Item dilewati: {order_dict}", exc_info=True)
                continue

        _db_write_queue.put({
            'op_type': 'batch_save_update',
            'model_class': MT5Order,
            'new_items_data': new_orders_objects,
            'updated_items_data': [],
            'unique_columns': ['ticket'],
            'pkey_column_name': 'ticket'
        })
        logger.debug(f"MT5 Orders ({len(new_orders_data)}) dikirim ke antrean DB.")

    if new_deals_data:
        new_deals_objects = []
        for deal_dict in new_deals_data:
            try:
                new_deals_objects.append(MT5DealHistory(
                    ticket=deal_dict['ticket'],
                    order_ticket=deal_dict['order_ticket'],
                    symbol=deal_dict['symbol'],
                    type=deal_dict['type'],
                    entry_type=deal_dict['entry_type'],
                    price=deal_dict['price'],
                    volume=deal_dict['volume'],
                    profit=deal_dict['profit'],
                    time=deal_dict['time'],
                    comment=deal_dict['comment'],
                    magic=deal_dict['magic'],
                    timestamp_recorded=deal_dict['timestamp_recorded']
                ))
            except Exception as e:
                logger.error(f"Gagal membuat objek MT5DealHistory dari dict: {e}. Item dilewati: {deal_dict}", exc_info=True)
                continue

        _db_write_queue.put({
            'op_type': 'batch_save_update',
            'model_class': MT5DealHistory,
            'new_items_data': new_deals_objects,
            'updated_items_data': [],
            'unique_columns': ['ticket'],
            'pkey_column_name': 'ticket'
        })
        logger.debug(f"MT5 Deals ({len(new_deals_data)}) dikirim ke antrean DB.")

    logger.info(f"Pembaruan data perdagangan MT5 untuk {symbol_param} selesai dan dikirim ke antrean.")


def get_ai_analysis_results(symbol=None, start_time_utc=None, end_time_utc=None, limit=None, analyst_id=None):
    """
    Mengambil hasil analisis AI dari database.
    Memastikan semua nilai harga adalah Decimal dan waktu adalah datetime object.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di get_ai_analysis_results. Tidak dapat melanjutkan.")
        return []
    db = SessionLocal()
    try:
        query = db.query(AIAnalysisResult)
        if symbol:
            query = query.filter(AIAnalysisResult.symbol == symbol)
        # Modifikasi: Pastikan start_time_utc dan end_time_utc adalah datetime aware UTC untuk filter
        if start_time_utc:
            start_time_utc_processed = to_utc_datetime_or_none(start_time_utc)
            if start_time_utc_processed:
                query = query.filter(AIAnalysisResult.timestamp >= start_time_utc_processed)
        if end_time_utc:
            end_time_utc_processed = to_utc_datetime_or_none(end_time_utc)
            if end_time_utc_processed:
                query = query.filter(AIAnalysisResult.timestamp <= end_time_utc_processed)
        if analyst_id:
            query = query.filter(AIAnalysisResult.analyst_id == analyst_id)

        query = query.order_by(AIAnalysisResult.timestamp.desc())

        if limit:
            query = query.limit(limit)

        results = query.all()

        output = []
        for r in results:
            output.append({
                "id": r.id,
                "symbol": r.symbol,
                "timestamp": r.timestamp, # Ini sudah datetime object dari model
                "analyst_id": r.analyst_id,
                "summary": r.summary,
                "potential_direction": r.potential_direction,
                "recommendation_action": r.recommendation_action,
                "entry_price": r.entry_price, # Ini sudah Decimal dari model
                "stop_loss": r.stop_loss,     # Ini sudah Decimal dari model
                "take_profit": r.take_profit, # Ini sudah Decimal dari model
                "reasoning": r.reasoning,
                "ai_confidence": r.ai_confidence,
                "raw_response_json": r.raw_response_json
            })
        return output
    except Exception as e:
        logger.error(f"Gagal mengambil hasil analisis AI dari DB: {e}", exc_info=True)
        return []
    finally:
        db.close()

# --- FUNGSI UNTUK HASIL ANALISIS AI ---
def save_ai_analysis_result(symbol, timestamp, analyst_id, summary, potential_direction, recommendation_action, entry_price, stop_loss, take_profit, reasoning, ai_confidence, raw_response_json):
    """
    Menyimpan atau memperbarui hasil analisis AI ke database.
    Memastikan harga adalah Decimal dan waktu adalah datetime object.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di save_ai_analysis_result. Tidak dapat melanjutkan.")
        return

    try:
        # Modifikasi: Gunakan helper to_utc_datetime_or_none
        timestamp_processed = to_utc_datetime_or_none(timestamp)
        if timestamp_processed is None:
            logger.error(f"Gagal mengonversi timestamp '{timestamp}' ke datetime aware UTC untuk AI Analysis Result. Melewatkan penyimpanan.")
            return

        # Modifikasi: Konversi harga ke Decimal. Tangani None jika ada.
        entry_price_processed = to_decimal_or_none(entry_price)
        stop_loss_processed = to_decimal_or_none(stop_loss)
        take_profit_processed = to_decimal_or_none(take_profit)

        processed_data = {
            "symbol": symbol,
            "timestamp": timestamp_processed, # <--- KOREKSI: KIRIM OBJEK DATETIME ASLI
            "analyst_id": analyst_id,
            "summary": summary,
            "potential_direction": potential_direction,
            "recommendation_action": recommendation_action,
            "entry_price": entry_price_processed, # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "stop_loss": stop_loss_processed,     # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "take_profit": take_profit_processed, # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "reasoning": reasoning,
            "ai_confidence": ai_confidence,
            "raw_response_json": raw_response_json, # Ini sudah string JSON, tidak perlu diubah
            "timestamp_recorded": datetime.now(timezone.utc)
        }

        _db_write_queue.put({
            'op_type': 'single_save_update',
            'model_class': AIAnalysisResult,
            'single_item_data': processed_data,
            'single_item_key_columns': ['symbol', 'timestamp', 'analyst_id']
        })
        logger.debug(f"AI Analysis Result untuk {symbol} oleh {analyst_id} @ {timestamp} dikirim ke antrean DB.")
    except Exception as e:
        logger.error(f"Gagal menyiapkan AI Analysis Result untuk antrean DB untuk {symbol} oleh {analyst_id} @ {timestamp}: {e}", exc_info=True)


# --- FUNGSI UNTUK DATA INDIKATOR & LEVEL HARGA ---
def save_moving_average(symbol, timeframe, ma_type, period, timestamp_utc, value):
    """
    Menyimpan atau memperbarui nilai Moving Average ke database.
    Memastikan value adalah Decimal dan timestamp_utc adalah datetime object.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di save_moving_average. Tidak dapat melanjutkan.")
        return

    try:
        # Modifikasi: Gunakan helper to_utc_datetime_or_none
        timestamp_utc_processed = to_utc_datetime_or_none(timestamp_utc)
        if timestamp_utc_processed is None:
            logger.error(f"Gagal mengonversi timestamp_utc '{timestamp_utc}' ke datetime aware UTC untuk Moving Average. Melewatkan penyimpanan.")
            return

        # Modifikasi: Konversi value ke Decimal. Tangani None jika ada.
        value_processed = to_decimal_or_none(value)
        if value_processed is None:
            logger.warning(f"Nilai Moving Average '{value}' tidak valid. Melewatkan penyimpanan.")
            return

        processed_data = {
            "symbol": symbol,
            "timeframe": timeframe,
            "ma_type": ma_type,
            "period": period,
            "timestamp_utc": timestamp_utc_processed, # <--- KOREKSI: KIRIM OBJEK DATETIME ASLI
            "value": value_processed,                  # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "timestamp_recorded": datetime.now(timezone.utc)
        }

        _db_write_queue.put({
            'op_type': 'single_save_update',
            'model_class': MovingAverage,
            'single_item_data': processed_data,
            'single_item_key_columns': ['symbol', 'timeframe', 'ma_type', 'period', 'timestamp_utc']
        })
        logger.debug(f"MA {ma_type}-{period} untuk {symbol} {timeframe} @ {timestamp_utc} dikirim ke antrean DB.")
    except Exception as e:
        logger.error(f"Gagal menyiapkan Moving Average untuk antrean DB untuk {symbol} {timeframe} {ma_type}-{period}: {e}", exc_info=True)

def get_moving_averages(symbol=None, timeframe=None, ma_type=None, period=None, limit=None, start_time_utc=None, end_time_utc=None):
    """
    Mengambil nilai Moving Average dari database.
    Memastikan nilai adalah Decimal dan waktu adalah datetime object.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di get_moving_averages. Tidak dapat melanjutkan.")
        return []
    db = SessionLocal()
    try:
        query = db.query(MovingAverage)
        if symbol:
            query = query.filter(MovingAverage.symbol == symbol)
        if timeframe:
            query = query.filter(MovingAverage.timeframe == timeframe)
        if ma_type:
            query = query.filter(MovingAverage.ma_type == ma_type)
        if period:
            query = query.filter(MovingAverage.period == period)
        # Modifikasi: Pastikan start_time_utc dan end_time_utc adalah datetime aware UTC untuk filter
        if start_time_utc:
            start_time_utc_processed = to_utc_datetime_or_none(start_time_utc)
            if start_time_utc_processed:
                query = query.filter(MovingAverage.timestamp_utc >= start_time_utc_processed)
        if end_time_utc:
            end_time_utc_processed = to_utc_datetime_or_none(end_time_utc)
            if end_time_utc_processed:
                query = query.filter(MovingAverage.timestamp_utc <= end_time_utc_processed)

        query = query.order_by(MovingAverage.timestamp_utc.desc())

        if limit:
            query = query.limit(limit)

        results = query.all()

        output = []
        for r in results:
            output.append({
                "id": r.id,
                "symbol": r.symbol,
                "timeframe": r.timeframe,
                "ma_type": r.ma_type,
                "period": r.period,
                "timestamp_utc": r.timestamp_utc, # Ini sudah datetime object dari model
                "value": r.value # Ini sudah Decimal dari model
            })
                # --- TAMBAHKAN LOG DEBUG INI ---
        if output:
            logger.debug(f"DB Query: get_moving_averages({symbol}, {timeframe}, {ma_type}, {period}, limit={limit}) - Ditemukan {len(output)} MA. Contoh terbaru: Type={output[0]['ma_type']}-{output[0]['period']}, Value={float(output[0]['value']):.5f}, Time={output[0]['timestamp_utc'].isoformat()}")
        else:
            logger.debug(f"DB Query: get_moving_averages({symbol}, {timeframe}, {ma_type}, {period}, limit={limit}) - Tidak ada MA ditemukan.")
        # --- AKHIR LOG TAMBAHAN ---

        return output
    except Exception as e:
        logger.error(f"Gagal mengambil Moving Averages dari DB: {e}", exc_info=True)
        return []
    finally:
        db.close()

def get_fair_value_gaps(symbol=None, timeframe=None, is_filled=None, limit=None, start_time_utc=None, end_time_utc=None, min_price_level=None, max_price_level=None,
                        order_by_price_asc=False, order_by_price_desc=False,
                        type=None): # <-- Tambahkan parameter 'type' di sini

    """
    Mengambil Fair Value Gap (FVG) dari database.
    Memastikan harga adalah Decimal dan waktu adalah datetime object.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di get_fair_value_gaps. Tidak dapat melanjutkan.")
        return []
    db = SessionLocal()
    try:
        query = db.query(FairValueGap)
        if symbol:
            query = query.filter(FairValueGap.symbol == symbol)
        if timeframe:
            query = query.filter(FairValueGap.timeframe == timeframe)
        if is_filled is not None:
            query = query.filter(FairValueGap.is_filled == is_filled)
        if type: # Logika ini sudah ada, kita hanya perlu parameter di definisi fungsi
            query = query.filter(FairValueGap.type == type) # Memfilter berdasarkan type (Bullish Imbalance/Bearish Imbalance)
        if start_time_utc:
            start_time_utc_processed = to_utc_datetime_or_none(start_time_utc)
            if start_time_utc_processed:
                query = query.filter(FairValueGap.formation_time_utc >= start_time_utc_processed)
        if end_time_utc:
            end_time_utc_processed = to_utc_datetime_or_none(end_time_utc)
            if end_time_utc_processed:
                query = query.filter(FairValueGap.formation_time_utc <= end_time_utc_processed)

        if min_price_level is not None:
            min_price_level_processed = to_decimal_or_none(min_price_level)
            if min_price_level_processed is not None:
                query = query.filter(FairValueGap.fvg_top_price >= min_price_level_processed) # Gunakan fvg_top_price
        if max_price_level is not None:
            max_price_level_processed = to_decimal_or_none(max_price_level)
            if max_price_level_processed is not None:
                query = query.filter(FairValueGap.fvg_bottom_price <= max_price_level_processed) # Gunakan fvg_bottom_price

        # --- LOGIKA PENGURUTAN BARU (sudah kita tambahkan di modifikasi sebelumnya) ---
        if order_by_price_asc:
            query = query.order_by(FairValueGap.fvg_bottom_price.asc()) # FVG diurutkan dari bawah ke atas
        elif order_by_price_desc:
            query = query.order_by(FairValueGap.fvg_top_price.desc()) # FVG diurutkan dari atas ke bawah
        else: # Default order
            query = query.order_by(FairValueGap.formation_time_utc.desc())
        # --- AKHIR LOGIKA PENGURUTAN BARU ---

        if limit is None:
            limit = 10000
        if limit:
            query = query.limit(limit)

        results = query.all()

        output = []
        for r in results:
            output.append({
                "id": r.id,
                "symbol": r.symbol,
                "timeframe": r.timeframe,
                "type": r.type, # Memastikan type ada di output
                "fvg_top_price": r.fvg_top_price,
                "fvg_bottom_price": r.fvg_bottom_price,
                "formation_time_utc": r.formation_time_utc,
                "is_filled": r.is_filled,
                "last_fill_time_utc": r.last_fill_time_utc,
                "retest_count": r.retest_count,
                "last_retest_time_utc": r.last_retest_time_utc
            })
        return output
    except Exception as e:
        logger.error(f"Gagal mengambil Fair Value Gaps dari DB: {e}", exc_info=True)
        return []
    finally:
        db.close()

def get_market_structure_events(symbol=None, timeframe=None, event_type=None, direction=None, limit=None, start_time_utc=None, end_time_utc=None, min_price_level=None, max_price_level=None):
    """
    Mengambil event struktur pasar dari database.
    event_type bisa berupa string tunggal atau list of strings.
    Memastikan semua nilai harga adalah Decimal dan waktu adalah datetime object.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di get_market_structure_events. Tidak dapat melanjutkan.")
        return []
    db = SessionLocal()
    try:
        query = db.query(MarketStructureEvent)
        if symbol:
            query = query.filter(MarketStructureEvent.symbol == symbol)
        if timeframe:
            query = query.filter(MarketStructureEvent.timeframe == timeframe)

        if event_type:
            if isinstance(event_type, list):
                query = query.filter(MarketStructureEvent.event_type.in_(event_type))
            else:
                query = query.filter(MarketStructureEvent.event_type == event_type)

        if direction:
            query = query.filter(MarketStructureEvent.direction == direction)
        # Modifikasi: Pastikan start_time_utc dan end_time_utc adalah datetime aware UTC untuk filter
        if start_time_utc:
            start_time_utc_processed = to_utc_datetime_or_none(start_time_utc)
            if start_time_utc_processed:
                query = query.filter(MarketStructureEvent.event_time_utc >= start_time_utc_processed)
        if end_time_utc:
            end_time_utc_processed = to_utc_datetime_or_none(end_time_utc)
            if end_time_utc_processed:
                query = query.filter(MarketStructureEvent.event_time_utc <= end_time_utc_processed)

        # Modifikasi: Pastikan min_price_level dan max_price_level adalah Decimal untuk filter
        if min_price_level is not None:
            min_price_level_processed = to_decimal_or_none(min_price_level)
            if min_price_level_processed is not None:
                query = query.filter(MarketStructureEvent.price_level >= min_price_level_processed)
        if max_price_level is not None:
            max_price_level_processed = to_decimal_or_none(max_price_level)
            if max_price_level_processed is not None:
                query = query.filter(MarketStructureEvent.price_level <= max_price_level_processed)

        query = query.order_by(MarketStructureEvent.event_time_utc.desc())

        if limit:
            query = query.limit(limit)

        results = query.all()

        # --- MODIFIKASI DEBUG LOG DI SINI ---
        if results:
            logger.debug(
                f"DB Query: get_market_structure_events ({symbol}, {timeframe}, {event_type}, limit={limit}): "
                f"Ditemukan {len(results)} event. Contoh terbaru: "
                f"Type={results[0].event_type}, Dir={results[0].direction}, "
                f"Price={utils.to_float_or_none(results[0].price_level):.5f}, " # Harga tetap Decimal, formatting di sini hanya untuk log
                f"Time={utils.to_iso_format_or_none(results[0].event_time_utc)}" # Waktu tetap datetime, formatting di sini hanya untuk log
            )
        else:
            logger.debug(
                f"DB Query: get_market_structure_events ({symbol}, {timeframe}, {event_type}, limit={limit}): "
                f"Tidak ada event ditemukan."
            )
        # --- MODIFIKASI SELESAI ---

        output = []
        for r in results:
            output.append({
                "id": r.id,
                "symbol": r.symbol,
                "timeframe": r.timeframe,
                "event_type": r.event_type,
                "direction": r.direction,
                "price_level": r.price_level, # Ini sudah Decimal dari model
                "event_time_utc": r.event_time_utc, # Ini sudah datetime object dari model
                "swing_high_ref_time": r.swing_high_ref_time, # Ini sudah datetime object dari model
                "swing_low_ref_time": r.swing_low_ref_time # Ini sudah datetime object dari model
            })
        return output
    except Exception as e:
        logger.error(f"Gagal mengambil Market Structure Events dari DB: {e}", exc_info=True)
        return []
    finally:
        db.close()
def get_support_resistance_levels(
    symbol=None,
    timeframe=None,
    is_active=None,
    limit=None,
    start_time_utc=None,
    end_time_utc=None,
    min_price_level=None,
    max_price_level=None,
    min_strength_score=None,
    is_key_level=None,
    level_type=None,
    # PASTIKAN DUA PARAMETER INI ADA DI get_support_resistance_levels
    order_by_price_asc=False, # <-- Ini harus ada
    order_by_price_desc=False # <-- Ini harus ada
):
    """
    Mengambil level Support & Resistance dari database.
    Memastikan semua nilai harga adalah Decimal dan waktu adalah datetime object.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di get_support_resistance_levels. Tidak dapat melanjutkan.")
        return []
    db = SessionLocal()
    try:
        query = db.query(SupportResistanceLevel)
        if symbol:
            query = query.filter(SupportResistanceLevel.symbol == symbol)
        if timeframe:
            query = query.filter(SupportResistanceLevel.timeframe == timeframe)
        if is_active is not None:
            query = query.filter(SupportResistanceLevel.is_active == is_active)
        # Modifikasi: Pastikan start_time_utc dan end_time_utc adalah datetime aware UTC untuk filter
        if start_time_utc:
            start_time_utc_processed = utils.to_utc_datetime_or_none(start_time_utc)
            if start_time_utc_processed:
                query = query.filter(SupportResistanceLevel.formation_time_utc >= start_time_utc_processed)
        if end_time_utc:
            end_time_utc_processed = utils.to_utc_datetime_or_none(end_time_utc)
            if end_time_utc_processed:
                query = query.filter(SupportResistanceLevel.formation_time_utc <= end_time_utc_processed)

        if level_type:
            query = query.filter(SupportResistanceLevel.level_type == level_type)

        if min_price_level is not None:
            min_price_level_processed = utils.to_decimal_or_none(min_price_level)
            if min_price_level_processed is not None:
                query = query.filter(SupportResistanceLevel.price_level >= min_price_level_processed)
        if max_price_level is not None:
            max_price_level_processed = utils.to_decimal_or_none(max_price_level)
            if max_price_level_processed is not None:
                query = query.filter(SupportResistanceLevel.price_level <= max_price_level_processed)

        if min_strength_score is not None:
            query = query.filter(SupportResistanceLevel.strength_score >= min_strength_score)
        if is_key_level is not None:
            query = query.filter(SupportResistanceLevel.is_key_level == is_key_level)

        # LOGIKA PENGURUTAN BARU: Prioritaskan berdasarkan harga jika diminta, jika tidak, berdasarkan waktu formasi
        if order_by_price_asc:
            query = query.order_by(SupportResistanceLevel.price_level.asc())
        elif order_by_price_desc:
            query = query.order_by(SupportResistanceLevel.price_level.desc())
        else: # Default order
            query = query.order_by(SupportResistanceLevel.formation_time_utc.desc())

        if limit is None:
            limit = 10000
        if limit:
            query = query.limit(limit)

        results = query.all()

        output = []
        for r in results:
            output.append({
                "id": r.id,
                "symbol": r.symbol,
                "timeframe": r.timeframe,
                "level_type": r.level_type,
                "price_level": r.price_level, # Ini sudah Decimal dari model
                "zone_start_price": r.zone_start_price, # Ini sudah Decimal dari model
                "zone_end_price": r.zone_end_price, # Ini sudah Decimal dari model
                "is_active": r.is_active,
                "formation_time_utc": r.formation_time_utc, # Ini sudah datetime object dari model
                "last_test_time_utc": r.last_test_time_utc, # Ini sudah datetime object dari model
                "is_key_level": r.is_key_level,
                "confluence_score": r.confluence_score
            })
        return output
    except Exception as e:
        logger.error(f"Gagal mengambil level Support & Resistance dari DB: {e}", exc_info=True)
        return []
    finally:
        db.close()

def get_order_blocks(symbol=None, timeframe=None, is_mitigated=None, limit=None, start_time_utc=None, end_time_utc=None, min_price_level=None, max_price_level=None, min_strength_score=None, type=None,
                     order_by_price_asc=False, order_by_price_desc=False): # <-- Tambahkan parameter ini
    """
    Mengambil Order Blocks dari database.
    Memastikan semua nilai harga adalah Decimal dan waktu adalah datetime object.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di get_order_blocks. Tidak dapat melanjutkan.")
        return []
    db = SessionLocal()
    try:
        query = db.query(OrderBlock)
        if symbol:
            query = query.filter(OrderBlock.symbol == symbol)
        if timeframe:
            query = query.filter(OrderBlock.timeframe == timeframe)
        if is_mitigated is not None:
            query = query.filter(OrderBlock.is_mitigated == is_mitigated)
        if start_time_utc:
            start_time_utc_processed = to_utc_datetime_or_none(start_time_utc)
            if start_time_utc_processed:
                query = query.filter(OrderBlock.formation_time_utc >= start_time_utc_processed)
        if end_time_utc:
            end_time_utc_processed = to_utc_datetime_or_none(end_time_utc)
            if end_time_utc_processed:
                query = query.filter(OrderBlock.formation_time_utc <= end_time_utc_processed)

        if type:
            query = query.filter(OrderBlock.type == type)

        if min_price_level is not None:
            min_price_level_processed = to_decimal_or_none(min_price_level)
            if min_price_level_processed is not None:
                # Untuk Order Blocks, filter harga min/max harus mencakup range OB
                query = query.filter(OrderBlock.ob_top_price >= min_price_level_processed)
        if max_price_level is not None:
            max_price_level_processed = to_decimal_or_none(max_price_level)
            if max_price_level_processed is not None:
                # Untuk Order Blocks, filter harga min/max harus mencakup range OB
                query = query.filter(OrderBlock.ob_bottom_price <= max_price_level_processed)
        
        logger.debug(f"DB Query: get_order_blocks({symbol}, {timeframe}) - Filters: Active={is_mitigated}, Time: {start_time_utc}-{end_time_utc}, Price Range: {min_price_level}-{max_price_level}")
        
        if min_strength_score is not None:
            query = query.filter(OrderBlock.strength_score >= min_strength_score)

        # --- LOGIKA PENGURUTAN BARU (SAMA DENGAN S&R) ---
        if order_by_price_asc:
            # Jika bullish (bottom up), urutkan berdasarkan ob_bottom_price ASC. Jika bearish (top down), urutkan berdasarkan ob_top_price ASC
            # Ini mungkin perlu logika lebih kompleks jika ingin selalu urut 'terdekat' ke suatu harga
            query = query.order_by(OrderBlock.ob_bottom_price.asc()) # Default ASC jika diminta
        elif order_by_price_desc:
            query = query.order_by(OrderBlock.ob_top_price.desc()) # Default DESC jika diminta
        else: # Default order jika tidak ada pengurutan harga spesifik
            query = query.order_by(OrderBlock.formation_time_utc.desc())
        # --- AKHIR LOGIKA PENGURUTAN BARU ---

        if limit is None:
            limit = 10000
        if limit:
            query = query.limit(limit)

        results = query.all()
        logger.debug(f"DB Query: get_order_blocks - Found {len(results)} OB. First 5 prices: {[to_float_or_none(r.ob_bottom_price) for r in results[:5]] if results else 'N/A'}")


        output = []
        for r in results:
            output.append({
                "id": r.id,
                "symbol": r.symbol,
                "timeframe": r.timeframe,
                "type": r.type,
                "ob_top_price": r.ob_top_price, # Ini sudah Decimal dari model
                "ob_bottom_price": r.ob_bottom_price, # Ini sudah Decimal dari model
                "formation_time_utc": r.formation_time_utc, # Ini sudah datetime object dari model
                "is_mitigated": r.is_mitigated,
                "last_mitigation_time_utc": r.last_mitigation_time_utc, # Ini sudah datetime object dari model
                "strength_score": r.strength_score,
                "confluence_score": r.confluence_score
            })
        return output
    except Exception as e:
        logger.error(f"Gagal mengambil Order Blocks dari DB: {e}", exc_info=True)
        return []
    finally:
        db.close()

def save_order_block(symbol, timeframe, ob_type, ob_top_price, ob_bottom_price,
                     formation_time_utc, is_mitigated, strength_score=None,
                     last_mitigation_time_utc=None, confluence_score=0):
    """
    Menyimpan atau memperbarui Order Block (OB) ke database.
    Memastikan harga adalah Decimal dan waktu adalah datetime object.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di save_order_block. Tidak dapat melanjutkan.")
        return

    try:
        # Modifikasi: Gunakan helper to_utc_datetime_or_none
        formation_time_utc_processed = to_utc_datetime_or_none(formation_time_utc)
        if formation_time_utc_processed is None:
            logger.error(f"Gagal mengonversi formation_time_utc '{formation_time_utc}' ke datetime aware UTC untuk OB. Melewatkan penyimpanan.")
            return

        # Modifikasi: Gunakan helper to_utc_datetime_or_none untuk timestamp opsional
        last_mitigation_time_utc_processed = to_utc_datetime_or_none(last_mitigation_time_utc)

        # Modifikasi: Konversi harga ke Decimal. Tangani None jika ada.
        ob_top_price_processed = to_decimal_or_none(ob_top_price)
        ob_bottom_price_processed = to_decimal_or_none(ob_bottom_price)

        # Modifikasi: Periksa apakah ada nilai harga yang tidak valid setelah konversi
        if ob_top_price_processed is None or ob_bottom_price_processed is None:
            logger.warning(f"Harga OB tidak valid untuk {symbol} {timeframe} {ob_type}. Melewatkan penyimpanan.")
            return

        processed_data = {
            "symbol": symbol,
            "timeframe": timeframe,
            "type": ob_type,
            "ob_top_price": _convert_to_json_serializable(ob_top_price_processed),
            "ob_bottom_price": _convert_to_json_serializable(ob_bottom_price_processed),
            "formation_time_utc": _convert_to_json_serializable(formation_time_utc_processed),
            "is_mitigated": is_mitigated,
            "strength_score": strength_score,
            "last_mitigation_time_utc": _convert_to_json_serializable(last_mitigation_time_utc_processed),
            "confluence_score": confluence_score,
            "timestamp_recorded": _convert_to_json_serializable(datetime.now(timezone.utc))
        }

        _db_write_queue.put({
            'op_type': 'single_save_update',
            'model_class': OrderBlock,
            'single_item_data': processed_data,
            'single_item_key_columns': ['symbol', 'timeframe', 'formation_time_utc', 'type']
        })
        logger.debug(f"Order Block {ob_type} untuk {symbol} {timeframe} @ {formation_time_utc} dikirim ke antrean DB.")
    except Exception as e:
        logger.error(f"Gagal menyiapkan Order Block untuk antrean DB untuk {symbol} {timeframe} {ob_type}: {e}", exc_info=True)



def save_fair_value_gap(symbol, timeframe, fvg_type, fvg_top_price, fvg_bottom_price, formation_time_utc, is_filled, last_fill_time_utc=None, retest_count=0, last_retest_time_utc=None):
    """
    Menyimpan atau memperbarui Fair Value Gap (FVG) ke database.
    Memastikan harga adalah Decimal dan waktu adalah datetime object.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di save_fair_value_gap. Tidak dapat melanjutkan.")
        return

    try:
        # Modifikasi: Gunakan helper to_utc_datetime_or_none
        formation_time_utc_processed = to_utc_datetime_or_none(formation_time_utc)
        if formation_time_utc_processed is None:
            logger.error(f"Gagal mengonversi formation_time_utc '{formation_time_utc}' ke datetime aware UTC untuk FVG. Melewatkan penyimpanan.")
            return

        # Modifikasi: Gunakan helper to_utc_datetime_or_none untuk timestamp opsional
        last_fill_time_utc_processed = to_utc_datetime_or_none(last_fill_time_utc)
        last_retest_time_utc_processed = to_utc_datetime_or_none(last_retest_time_utc)

        # Modifikasi: Konversi harga ke Decimal. Tangani None jika ada.
        fvg_top_price_processed = to_decimal_or_none(fvg_top_price)
        fvg_bottom_price_processed = to_decimal_or_none(fvg_bottom_price)

        # Modifikasi: Periksa apakah ada nilai harga yang tidak valid setelah konversi
        if fvg_top_price_processed is None or fvg_bottom_price_processed is None:
            logger.warning(f"Harga FVG tidak valid untuk {symbol} {timeframe} {fvg_type}. Melewatkan penyimpanan.")
            return


        processed_data = {
            "symbol": symbol,
            "timeframe": timeframe,
            "type": fvg_type,
            "fvg_top_price": fvg_top_price_processed,       # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "fvg_bottom_price": fvg_bottom_price_processed, # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "formation_time_utc": formation_time_utc_processed, # <--- KOREKSI: KIRIM OBJEK DATETIME ASLI
            "is_filled": is_filled,
            "last_fill_time_utc": last_fill_time_utc_processed, # <--- KOREKSI: KIRIM OBJEK DATETIME ASLI
            "retest_count": retest_count,
            "last_retest_time_utc": last_retest_time_utc_processed, # <--- KOREKSI: KIRIM OBJEK DATETIME ASLI
            "timestamp_recorded": datetime.now(timezone.utc)
        }

        _db_write_queue.put({
            'op_type': 'single_save_update',
            'model_class': FairValueGap,
            'single_item_data': processed_data,
            'single_item_key_columns': ['symbol', 'timeframe', 'formation_time_utc', 'type']
        })
        logger.debug(f"FVG {fvg_type} untuk {symbol} {timeframe} @ {formation_time_utc} dikirim ke antrean DB.")
    except Exception as e:
        logger.error(f"Gagal menyiapkan FVG untuk antrean DB untuk {symbol} {timeframe} {fvg_type}: {e}", exc_info=True)


def save_support_resistance_level(symbol, timeframe, level_type, price_level, is_active, formation_time_utc, strength_score=None, zone_start_price=None, zone_end_price=None, last_test_time_utc=None, is_key_level=False, confluence_score=0):
    """
    Menyimpan atau memperbarui level Support & Resistance ke database.
    Memastikan harga adalah Decimal dan waktu adalah datetime object.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di save_support_resistance_level. Tidak dapat melanjutkan.")
        return

    try:
        # Modifikasi: Gunakan helper to_utc_datetime_or_none
        formation_time_utc_processed = to_utc_datetime_or_none(formation_time_utc)
        if formation_time_utc_processed is None:
            logger.error(f"Gagal mengonversi formation_time_utc '{formation_time_utc}' ke datetime aware UTC untuk S&R. Melewatkan penyimpanan.")
            return

        # Modifikasi: Gunakan helper to_utc_datetime_or_none untuk timestamp opsional
        last_test_time_utc_processed = to_utc_datetime_or_none(last_test_time_utc)

        # Modifikasi: Konversi harga ke Decimal. Tangani None jika ada.
        price_level_processed = to_decimal_or_none(price_level)
        zone_start_price_processed = to_decimal_or_none(zone_start_price)
        zone_end_price_processed = to_decimal_or_none(zone_end_price)

        # Modifikasi: Periksa apakah ada nilai harga yang tidak valid setelah konversi
        if price_level_processed is None:
            logger.warning(f"Price level tidak valid untuk S&R {level_type}. Melewatkan penyimpanan.")
            return


        processed_data = {
            "symbol": symbol,
            "timeframe": timeframe,
            "level_type": level_type,
            "price_level": price_level_processed,               # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "zone_start_price": zone_start_price_processed,     # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "zone_end_price": zone_end_price_processed,         # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "strength_score": strength_score,
            "is_active": is_active,
            "formation_time_utc": formation_time_utc_processed, # <--- KOREKSI: KIRIM OBJEK DATETIME ASLI
            "last_test_time_utc": last_test_time_utc_processed, # <--- KOREKSI: KIRIM OBJEK DATETIME ASLI
            "is_key_level": is_key_level,
            "confluence_score": confluence_score,
            "timestamp_recorded": datetime.now(timezone.utc)
        }

        _db_write_queue.put({
            'op_type': 'single_save_update',
            'model_class': SupportResistanceLevel,
            'single_item_data': processed_data,
            'single_item_key_columns': ['symbol', 'timeframe', 'price_level', 'level_type']
        })
        logger.debug(f"S&R {level_type} untuk {symbol} {timeframe} @ {price_level} dikirim ke antrean DB.")
    except Exception as e:
        logger.error(f"Gagal menyiapkan S&R Level untuk antrean DB untuk {symbol} {timeframe} {level_type}: {e}", exc_info=True)


def save_supply_demand_zone(symbol, timeframe, zone_type, base_type, zone_top_price, zone_bottom_price, formation_time_utc, is_mitigated, strength_score=None, is_key_level=False, confluence_score=0, last_retest_time_utc=None):
    """
    Menyimpan atau memperbarui zona Supply & Demand (S&D) ke database.
    Memastikan harga adalah Decimal dan waktu adalah datetime object.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di save_supply_demand_zone. Tidak dapat melanjutkan.")
        return

    try:
        # Modifikasi: Gunakan helper to_utc_datetime_or_none
        formation_time_utc_processed = to_utc_datetime_or_none(formation_time_utc)
        if formation_time_utc_processed is None:
            logger.error(f"Gagal mengonversi formation_time_utc '{formation_time_utc}' ke datetime aware UTC untuk S&D. Melewatkan penyimpanan.")
            return

        # Modifikasi: Gunakan helper to_utc_datetime_or_none untuk timestamp opsional
        last_retest_time_utc_processed = to_utc_datetime_or_none(last_retest_time_utc)

        # Modifikasi: Konversi harga ke Decimal. Tangani None jika ada.
        zone_top_price_processed = to_decimal_or_none(zone_top_price)
        zone_bottom_price_processed = to_decimal_or_none(zone_bottom_price)

        # Modifikasi: Periksa apakah ada nilai harga yang tidak valid setelah konversi
        if zone_top_price_processed is None or zone_bottom_price_processed is None:
            logger.warning(f"Harga zona S&D tidak valid untuk {symbol} {timeframe} {zone_type}. Melewatkan penyimpanan.")
            return

        processed_data = {
            "symbol": symbol,
            "timeframe": timeframe,
            "zone_type": zone_type,
            "base_type": base_type,
            "zone_top_price": zone_top_price_processed,         # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "zone_bottom_price": zone_bottom_price_processed,   # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "formation_time_utc": formation_time_utc_processed, # <--- KOREKSI: KIRIM OBJEK DATETIME ASLI
            "is_mitigated": is_mitigated,
            "strength_score": strength_score,
            "is_key_level": is_key_level,
            "confluence_score": confluence_score,
            "last_retest_time_utc": last_retest_time_utc_processed, # <--- KOREKSI: KIRIM OBJEK DATETIME ASLI
            "timestamp_recorded": datetime.now(timezone.utc)
        }

        _db_write_queue.put({
            'op_type': 'single_save_update',
            'model_class': SupplyDemandZone,
            'single_item_data': processed_data,
            'single_item_key_columns': ['symbol', 'timeframe', 'zone_type', 'formation_time_utc']
        })
        logger.debug(f"S&D Zone {zone_type} untuk {symbol} {timeframe} @ {formation_time_utc} dikirim ke antrean DB.")
    except Exception as e:
        logger.error(f"Gagal menyiapkan S&D Zone untuk antrean DB untuk {symbol} {timeframe} {zone_type}: {e}", exc_info=True)

def get_supply_demand_zones(symbol=None, timeframe=None, is_mitigated=None, limit=None, start_time_utc=None, end_time_utc=None, min_price_level=None, max_price_level=None, min_strength_score=None, is_key_level=None, zone_type=None): # <-- Tambahan: zone_type=None

    """
    Mengambil zona Supply & Demand (S&D) dari database.
    Memastikan semua nilai harga adalah Decimal dan waktu adalah datetime object.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di get_supply_demand_zones. Tidak dapat melanjutkan.")
        return []
    db = SessionLocal()
    try:
        query = db.query(SupplyDemandZone)
        if symbol:
            query = query.filter(SupplyDemandZone.symbol == symbol)
        if timeframe:
            query = query.filter(SupplyDemandZone.timeframe == timeframe)
        if is_mitigated is not None:
            query = query.filter(SupplyDemandZone.is_mitigated == is_mitigated)
        # Modifikasi: Pastikan start_time_utc dan end_time_utc adalah datetime aware UTC untuk filter
        if start_time_utc:
            start_time_utc_processed = to_utc_datetime_or_none(start_time_utc)
            if start_time_utc_processed:
                query = query.filter(SupplyDemandZone.formation_time_utc >= start_time_utc_processed)
        if end_time_utc:
            end_time_utc_processed = to_utc_datetime_or_none(end_time_utc)
            if end_time_utc_processed:
                query = query.filter(SupplyDemandZone.formation_time_utc <= end_time_utc_processed)

        # --- TAMBAHAN KODE INI: Logika filter untuk 'zone_type' ---
        if zone_type:
            query = query.filter(SupplyDemandZone.zone_type == zone_type)
        # --- AKHIR TAMBAHAN KODE ---

        # --- Modifikasi: Pastikan min_price_level dan max_price_level adalah Decimal untuk filter ---
        if min_price_level is not None:
            min_price_level_processed = to_decimal_or_none(min_price_level)
            if min_price_level_processed is not None:
                query = query.filter(SupplyDemandZone.zone_bottom_price >= min_price_level_processed)
        if max_price_level is not None:
            max_price_level_processed = to_decimal_or_none(max_price_level)
            if max_price_level_processed is not None:
                query = query.filter(SupplyDemandZone.zone_top_price <= max_price_level_processed)
        # --- Akhir Modifikasi ---

        # --- Filter Signifikansi (Baru) ---
        if min_strength_score is not None:
            query = query.filter(SupplyDemandZone.strength_score >= min_strength_score)
        if is_key_level is not None:
            query = query.filter(SupplyDemandZone.is_key_level == is_key_level)
        # --- Akhir Filter Signifikansi ---

        query = query.order_by(SupplyDemandZone.formation_time_utc.desc())

        # --- Tambahkan LIMIT DEFAULT ---
        if limit is None:
            limit = 10000 # Batas default
        # --- Akhir Tambahan LIMIT DEFAULT ---

        if limit:
            query = query.limit(limit)

        results = query.all()

        output = []
        for r in results:
            output.append({
                "id": r.id,
                "symbol": r.symbol,
                "timeframe": r.timeframe,
                "zone_type": r.zone_type,
                "base_type": r.base_type,
                "zone_top_price": r.zone_top_price, # Ini sudah Decimal dari model
                "zone_bottom_price": r.zone_bottom_price, # Ini sudah Decimal dari model
                "formation_time_utc": r.formation_time_utc, # Ini sudah datetime object dari model
                "last_retest_time_utc": r.last_retest_time_utc, # Ini sudah datetime object dari model
                "is_mitigated": r.is_mitigated,
                "strength_score": r.strength_score,
                "is_key_level": r.is_key_level,
                "confluence_score": r.confluence_score
            })
        return output
    except Exception as e:
        logger.error(f"Gagal mengambil zona Supply & Demand dari DB: {e}", exc_info=True)
        return []
    finally:
        db.close()

def save_fibonacci_level(symbol, timeframe, type, high_price_ref, low_price_ref,
                         start_time_ref_utc, end_time_ref_utc, ratio, price_level,
                         is_active, id=None, confluence_score=0,
                         current_retracement_percent=None,
                         deepest_retracement_percent=None, retracement_direction=None,
                         last_test_time_utc=None):
    """
    Menyimpan atau memperbarui level Fibonacci ke database.
    Memastikan nilai adalah Decimal dan waktu adalah datetime object.
    """
    global SessionLocal
    if SessionLocal is None:
        logging.error("SessionLocal belum diinisialisasi di save_fibonacci_level. Tidak dapat melanjutkan.")
        return

    try:
        # MODIFIKASI INI: Proses semua parameter menjadi _processed
        high_price_ref_processed = utils.to_decimal_or_none(high_price_ref)
        low_price_ref_processed = utils.to_decimal_or_none(low_price_ref)
        start_time_ref_utc_processed = utils.to_utc_datetime_or_none(start_time_ref_utc)
        end_time_ref_utc_processed = utils.to_utc_datetime_or_none(end_time_ref_utc)
        ratio_processed = utils.to_decimal_or_none(ratio)
        price_level_processed = utils.to_decimal_or_none(price_level)
        current_retracement_percent_processed = utils.to_decimal_or_none(current_retracement_percent)
        deepest_retracement_percent_processed = utils.to_decimal_or_none(deepest_retracement_percent)
        # retracement_direction sudah int/None, tidak perlu proses Decimal atau _convert_to_json_serializable
        retracement_direction_processed = retracement_direction
        last_test_time_utc_processed = utils.to_utc_datetime_or_none(last_test_time_utc)

        # Validasi minimal untuk data yang tidak boleh None (kolom NOT NULL di model)
        if any(v is None for v in [high_price_ref_processed, low_price_ref_processed,
                                   start_time_ref_utc_processed, end_time_ref_utc_processed,
                                   ratio_processed, price_level_processed]):
            logging.warning(f"Data Fibonacci Level tidak lengkap untuk {symbol} {timeframe}. Melewatkan penyimpanan. Missing: "
                            f"high_ref={high_price_ref_processed}, low_ref={low_price_ref_processed}, "
                            f"start_time={start_time_ref_utc_processed}, end_time={end_time_ref_utc_processed}, "
                            f"ratio={ratio_processed}, price_level={price_level_processed}")
            return

        processed_data = {
            "symbol": symbol,
            "timeframe": timeframe,
            "type": type,
            "high_price_ref": high_price_ref_processed,           # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "low_price_ref": low_price_ref_processed,             # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "start_time_ref_utc": start_time_ref_utc_processed,   # <--- KOREKSI: KIRIM OBJEK DATETIME ASLI
            "end_time_ref_utc": end_time_ref_utc_processed,       # <--- KOREKSI: KIRIM OBJEK DATETIME ASLI
            "ratio": ratio_processed,                             # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "price_level": price_level_processed,                 # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "is_active": is_active,
            "confluence_score": confluence_score,                 # <--- KOREKSI: JANGAN _convert_to_json_serializable
            "timestamp_recorded": datetime.now(timezone.utc),
            "current_retracement_percent": current_retracement_percent_processed, # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "deepest_retracement_percent": deepest_retracement_percent_processed, # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "retracement_direction": retracement_direction_processed, # <--- KOREKSI: JANGAN _convert_to_json_serializable
            "last_test_time_utc": last_test_time_utc_processed    # <--- KOREKSI: KIRIM OBJEK DATETIME ASLI
        }

        # Jika ID disediakan, ini adalah operasi update
        if id is not None:
            processed_data['id'] = id # Pastikan ID ada untuk update

        # Kirim ke antrean penulis DB
        logging.debug(f"Fibonacci Level untuk {symbol} {timeframe} @ {price_level} dikirim ke antrean DB.")

        # Menggunakan 'single_save_update' karena kita tidak memiliki 'batch_update_mappings' di save_fibonacci_levels_batch
        # dan ini adalah update individual dari _update_existing_fibonacci_levels_status
        # pkey_column_name harus 'id' untuk model ini
        globals().get('_db_write_queue').put({
            'op_type': 'single_save_update',
            'model_class': globals().get('FibonacciLevel'), # Akses model dari globals()
            'single_item_data': processed_data,
            'single_item_key_columns': ['symbol', 'timeframe', 'type', 'ratio', 'start_time_ref_utc', 'end_time_ref_utc'], # Kunci unik untuk mencari
            'pkey_column_name': 'id' # Primary key untuk update
        })

    except Exception as e:
        logging.error(f"Gagal menyiapkan Fibonacci Level untuk antrean DB untuk {symbol} {timeframe}: {e}", exc_info=True)


def get_liquidity_zones(symbol=None, timeframe=None, is_tapped=None, limit=None, start_time_utc=None, end_time_utc=None, min_price_level=None, max_price_level=None,
                        order_by_price_asc=False, order_by_price_desc=False,
                        zone_type=None): # <-- Tambahkan parameter 'zone_type' di sini

    """
    Mengambil Zona Likuiditas dari database.
    Memastikan semua nilai harga adalah Decimal dan waktu adalah datetime object.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di get_liquidity_zones. Tidak dapat melanjutkan.")
        return []
    db = SessionLocal()
    try:
        query = db.query(LiquidityZone)
        if symbol:
            query = query.filter(LiquidityZone.symbol == symbol)
        if timeframe:
            query = query.filter(LiquidityZone.timeframe == timeframe)
        if is_tapped is not None:
            query = query.filter(LiquidityZone.is_tapped == is_tapped)
        if zone_type: # Logika ini sudah ada, kita hanya perlu parameter di definisi fungsi
            query = query.filter(LiquidityZone.zone_type == zone_type) # Memfilter berdasarkan zone_type (Equal Highs/Equal Lows)
        if start_time_utc:
            start_time_utc_processed = to_utc_datetime_or_none(start_time_utc)
            if start_time_utc_processed:
                query = query.filter(LiquidityZone.formation_time_utc >= start_time_utc_processed)
        if end_time_utc:
            end_time_utc_processed = to_utc_datetime_or_none(end_time_utc)
            if end_time_utc_processed:
                query = query.filter(LiquidityZone.formation_time_utc <= end_time_utc_processed)
        
        if min_price_level is not None:
            min_price_level_processed = to_decimal_or_none(min_price_level)
            if min_price_level_processed is not None:
                query = query.filter(LiquidityZone.price_level >= min_price_level_processed)
        if max_price_level is not None:
            max_price_level_processed = to_decimal_or_none(max_price_level)
            if max_price_level_processed is not None:
                query = query.filter(LiquidityZone.price_level <= max_price_level_processed)

        # --- LOGIKA PENGURUTAN BARU (sudah kita tambahkan di modifikasi sebelumnya) ---
        if order_by_price_asc:
            query = query.order_by(LiquidityZone.price_level.asc())
        elif order_by_price_desc:
            query = query.order_by(LiquidityZone.price_level.desc())
        else: # Default order
            query = query.order_by(LiquidityZone.formation_time_utc.desc())
        # --- AKHIR LOGIKA PENGURUTAN BARU ---

        if limit is None:
            limit = 10000
        if limit:
            query = query.limit(limit)

        results = query.all()

        output = []
        for r in results:
            output.append({
                "id": r.id,
                "symbol": r.symbol,
                "timeframe": r.timeframe,
                "zone_type": r.zone_type, # Memastikan zone_type ada di output
                "price_level": r.price_level,
                "formation_time_utc": r.formation_time_utc,
                "is_tapped": r.is_tapped,
                "tap_time_utc": r.tap_time_utc
            })
        return output
    except Exception as e:
        logger.error(f"Gagal mengambil Zona Likuiditas dari DB: {e}", exc_info=True)
        return []
    finally:
        db.close()
        
def get_fibonacci_levels(symbol=None, timeframe=None, is_active=None, limit=None, start_time_utc=None, end_time_utc=None, min_price_level=None, max_price_level=None, ratios_to_include=None): # <--- TAMBAHKAN PARAMETER INI
    """
    Mengambil level Fibonacci dari database.
    Memastikan semua nilai harga/rasio adalah Decimal dan waktu adalah datetime object.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di get_fibonacci_levels. Tidak dapat melanjutkan.")
        return []
    db = SessionLocal()
    try:
        query = db.query(FibonacciLevel)
        if symbol:
            query = query.filter(FibonacciLevel.symbol == symbol)
        if timeframe:
            query = query.filter(FibonacciLevel.timeframe == timeframe)
        if is_active is not None:
            query = query.filter(FibonacciLevel.is_active == is_active)
        # Modifikasi: Pastikan start_time_utc dan end_time_utc adalah datetime aware UTC untuk filter
        if start_time_utc:
            start_time_utc_processed = to_utc_datetime_or_none(start_time_utc)
            if start_time_utc_processed:
                query = query.filter(FibonacciLevel.start_time_ref_utc >= start_time_utc_processed)
        if end_time_utc:
            end_time_utc_processed = to_utc_datetime_or_none(end_time_utc)
            if end_time_utc_processed:
                query = query.filter(FibonacciLevel.end_time_ref_utc <= end_time_utc_processed)

        # --- Modifikasi: Pastikan min_price_level dan max_price_level adalah Decimal untuk filter ---
        if min_price_level is not None:
            min_price_level_processed = to_decimal_or_none(min_price_level)
            if min_price_level_processed is not None:
                query = query.filter(FibonacciLevel.price_level >= min_price_level_processed)
        if max_price_level is not None:
            max_price_level_processed = to_decimal_or_none(max_price_level)
            if max_price_level_processed is not None:
                query = query.filter(FibonacciLevel.price_level <= max_price_level_processed)
        # --- Akhir Modifikasi ---

        # --- Tambahkan Filter Rasio Fibonacci (Baru) ---
        if ratios_to_include is not None:
            # Pastikan rasio dalam list juga dikonversi ke Decimal untuk perbandingan yang akurat
            ratios_to_include_processed = [to_decimal_or_none(r) for r in ratios_to_include if to_decimal_or_none(r) is not None]
            if ratios_to_include_processed:
                query = query.filter(FibonacciLevel.ratio.in_(ratios_to_include_processed))
        # --- Akhir Filter Rasio Fibonacci ---

        query = query.order_by(FibonacciLevel.start_time_ref_utc.desc())

        # --- TAMBAHKAN LIMIT DEFAULT ---
        if limit is None:
            limit = 10000 # Batas default
        # --- Akhir Tambahan LIMIT DEFAULT ---

        if limit:
            query = query.limit(limit)

        results = query.all()
        output = []
        for r in results:
            output.append({
                "id": r.id,
                "symbol": r.symbol,
                "timeframe": r.timeframe,
                "type": r.type,
                "high_price_ref": r.high_price_ref, # Ini sudah Decimal dari model
                "low_price_ref": r.low_price_ref,   # Ini sudah Decimal dari model
                "start_time_ref_utc": r.start_time_ref_utc, # Ini sudah datetime object dari model
                "end_time_ref_utc": r.end_time_ref_utc,     # Ini sudah datetime object dari model
                "ratio": r.ratio,                   # Ini sudah Decimal dari model
                "price_level": r.price_level,       # Ini sudah Decimal dari model
                "is_active": r.is_active,
                "last_test_time_utc": r.last_test_time_utc, # Ini sudah datetime object dari model
                "current_retracement_percent": r.current_retracement_percent, # Ini sudah Decimal dari model
                "deepest_retracement_percent": r.deepest_retracement_percent, # Ini sudah Decimal dari model
                "retracement_direction": r.retracement_direction # Ini sudah Integer dari model
            })
        return output
    except Exception as e:
        logger.error(f"Gagal mengambil level Fibonacci dari DB: {e}", exc_info=True)
        return []
    finally:
        db.close()

def save_volume_profile(symbol, timeframe, period_start_utc, period_end_utc, poc_price, vah_price, val_price, total_volume, profile_data_json, row_height_value=None):
    """
    Menyimpan atau memperbarui data Volume Profile ke database.
    Memastikan harga/volume adalah Decimal dan waktu adalah datetime object.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di save_volume_profile. Tidak dapat melanjutkan.")
        return

    try:
        # Pastikan period_start_utc adalah datetime object dengan timezone info
        period_start_utc_processed = utils.to_utc_datetime_or_none(period_start_utc)
        if period_start_utc_processed is None:
            logger.error(f"Gagal mengonversi period_start_utc '{period_start_utc}' ke datetime untuk Volume Profile. Melewatkan penyimpanan.")
            return

        # Pastikan period_end_utc adalah datetime object (jika ada)
        period_end_utc_processed = utils.to_utc_datetime_or_none(period_end_utc)
        if period_end_utc_processed is None:
            logger.warning(f"Period_end_utc '{period_end_utc}' tidak valid. Menggunakan period_start_utc sebagai fallback.")
            period_end_utc_processed = period_start_utc_processed


        # Konversi harga dan total volume ke Decimal. Tangani None jika ada.
        poc_price_processed = utils.to_decimal_or_none(poc_price)
        vah_price_processed = utils.to_decimal_or_none(vah_price)
        val_price_processed = utils.to_decimal_or_none(val_price)
        total_volume_processed = utils.to_decimal_or_none(total_volume)
        row_height_value_processed = utils.to_decimal_or_none(row_height_value) # Tangani ini juga

        # Modifikasi: Periksa apakah ada nilai penting yang tidak valid
        if any(v is None for v in [poc_price_processed, vah_price_processed, val_price_processed, total_volume_processed]):
            logger.warning(f"Beberapa nilai harga/volume Volume Profile tidak valid untuk {symbol} {timeframe}. Melewatkan penyimpanan.")
            return

        processed_data = {
            "symbol": symbol,
            "timeframe": timeframe,
            "period_start_utc": period_start_utc_processed,   # <--- KOREKSI: KIRIM OBJEK DATETIME ASLI
            "period_end_utc": period_end_utc_processed,       # <--- KOREKSI: KIRIM OBJEK DATETIME ASLI
            "poc_price": poc_price_processed,                 # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "vah_price": vah_price_processed,                 # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "val_price": val_price_processed,                 # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "total_volume": total_volume_processed,           # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "profile_data_json": profile_data_json, # Ini diharapkan sudah JSON string
            "row_height_value": row_height_value_processed,   # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "timestamp_recorded": datetime.now(timezone.utc)
        }

        _db_write_queue.put({
            'op_type': 'single_save_update',
            'model_class': VolumeProfile,
            'single_item_data': processed_data,
            'single_item_key_columns': ['symbol', 'timeframe', 'period_start_utc']
        })
        logger.debug(f"Volume Profile untuk {symbol} {timeframe} @ {period_start_utc} dikirim ke antrean DB.")
    except Exception as e:
        logger.error(f"Gagal menyiapkan Volume Profile untuk antrean DB untuk {symbol} {timeframe} @ {period_start_utc}: {e}", exc_info=True)


def get_volume_profiles(symbol=None, timeframe=None, limit=None, start_time_utc=None, end_time_utc=None):
    """
    Mengambil data Volume Profiles dari database.
    Memastikan semua nilai harga/volume adalah Decimal dan waktu adalah datetime object.
    """
    global SessionLocal 
    if SessionLocal is None: 
        logger.error("SessionLocal belum diinisialisasi di get_volume_profiles. Tidak dapat melanjutkan.")
        return []
    db = SessionLocal()
    try:
        query = db.query(VolumeProfile)
        if symbol:
            query = query.filter(VolumeProfile.symbol == symbol)
        if timeframe:
            query = query.filter(VolumeProfile.timeframe == timeframe)
        # Modifikasi: Pastikan start_time_utc dan end_time_utc adalah datetime aware UTC untuk filter
        if start_time_utc:
            start_time_utc_processed = to_utc_datetime_or_none(start_time_utc)
            if start_time_utc_processed:
                query = query.filter(VolumeProfile.period_start_utc >= start_time_utc_processed)
        if end_time_utc:
            end_time_utc_processed = to_utc_datetime_or_none(end_time_utc)
            if end_time_utc_processed:
                query = query.filter(VolumeProfile.period_end_utc <= end_time_utc_processed)

        query = query.order_by(VolumeProfile.period_start_utc.desc())

        if limit:
            query = query.limit(limit)

        results = query.all()

        output = []
        for r in results:
            output.append({
                "id": r.id,
                "symbol": r.symbol,
                "timeframe": r.timeframe,
                "period_start_utc": r.period_start_utc, # Ini sudah datetime object dari model
                "period_end_utc": r.period_end_utc,     # Ini sudah datetime object dari model
                "poc_price": r.poc_price,               # Ini sudah Decimal dari model
                "vah_price": r.vah_price,               # Ini sudah Decimal dari model
                "val_price": r.val_price,               # Ini sudah Decimal dari model
                "total_volume": r.total_volume,         # Ini sudah Decimal dari model
                "profile_data_json": json.loads(r.profile_data_json) if r.profile_data_json else None,
                "row_height_value": r.row_height_value  # Ini sudah Decimal dari model
            })
        return output
    except Exception as e:
        logger.error(f"Gagal mengambil Volume Profiles dari DB: {e}", exc_info=True)
        return []
    finally:
        db.close()


def save_divergence(symbol, timeframe, indicator_type, divergence_type, price_point_time_utc, indicator_point_time_utc, price_level_1=None, price_level_2=None, indicator_value_1=None, indicator_value_2=None, is_active=True):
    """
    Menyimpan atau memperbarui Divergensi ke database.
    Memastikan harga/nilai indikator adalah Decimal dan waktu adalah datetime object.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di save_divergence. Tidak dapat melanjutkan.")
        return

    try:
        # Modifikasi: Gunakan helper to_utc_datetime_or_none
        price_point_time_utc_processed = to_utc_datetime_or_none(price_point_time_utc)
        if price_point_time_utc_processed is None:
            logger.error(f"Gagal mengonversi price_point_time_utc '{price_point_time_utc}' ke datetime aware UTC untuk Divergence. Melewatkan penyimpanan.")
            return

        # Modifikasi: Gunakan helper to_utc_datetime_or_none untuk timestamp opsional
        indicator_point_time_utc_processed = to_utc_datetime_or_none(indicator_point_time_utc)

        # Modifikasi: Konversi harga dan nilai indikator ke Decimal. Tangani None jika ada.
        price_level_1_processed = to_decimal_or_none(price_level_1)
        price_level_2_processed = to_decimal_or_none(price_level_2)
        indicator_value_1_processed = to_decimal_or_none(indicator_value_1)
        indicator_value_2_processed = to_decimal_or_none(indicator_value_2)

        # Modifikasi: Periksa apakah ada nilai yang tidak valid setelah konversi
        if any(v is None for v in [price_level_1_processed, price_level_2_processed, indicator_value_1_processed, indicator_value_2_processed]):
            logger.warning(f"Beberapa nilai harga/indikator divergensi tidak valid untuk {symbol} {timeframe} {divergence_type}. Melewatkan penyimpanan.")
            return

        processed_data = {
            "symbol": symbol,
            "timeframe": timeframe,
            "indicator_type": indicator_type,
            "divergence_type": divergence_type,
            "price_point_time_utc": price_point_time_utc_processed,     # <--- KOREKSI: KIRIM OBJEK DATETIME ASLI
            "indicator_point_time_utc": indicator_point_time_utc_processed, # <--- KOREKSI: KIRIM OBJEK DATETIME ASLI
            "price_level_1": price_level_1_processed,                   # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "price_level_2": price_level_2_processed,                   # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "indicator_value_1": indicator_value_1_processed,           # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "indicator_value_2": indicator_value_2_processed,           # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "is_active": is_active,
            "timestamp_recorded": datetime.now(timezone.utc)
        }

        _db_write_queue.put({
            'op_type': 'single_save_update',
            'model_class': Divergence,
            'single_item_data': processed_data,
            'single_item_key_columns': ['symbol', 'timeframe', 'indicator_type', 'divergence_type', 'price_point_time_utc', 'indicator_point_time_utc']
        })
        logger.debug(f"Divergence {divergence_type} untuk {symbol} {timeframe} dikirim ke antrean DB.")
    except Exception as e:
        logger.error(f"Gagal menyiapkan Divergence untuk antrean DB untuk {symbol} {timeframe} {divergence_type}: {e}", exc_info=True)


def save_divergences_batch(new_items_data: list, updated_items_data: list):
    """
    Menyimpan Divergensi baru dan memperbarui yang sudah ada dalam batch.
    Memastikan data diubah ke objek model/format yang benar sebelum dikirim ke antrean.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di save_divergences_batch. Tidak dapat melanjutkan.")
        return

    # Siapkan data BARU: Konversi list of dictionaries menjadi list of Divergence objects
    new_div_objects = []
    if new_items_data:
        for item_data in new_items_data:
            try:
                # Modifikasi: Konversi harga dan nilai indikator ke Decimal
                price_level_1_dec = utils.to_decimal_or_none(item_data.get('price_level_1'))
                price_level_2_dec = utils.to_decimal_or_none(item_data.get('price_level_2'))
                indicator_value_1_dec = utils.to_decimal_or_none(item_data.get('indicator_value_1'))
                indicator_value_2_dec = utils.to_decimal_or_none(item_data.get('indicator_value_2'))

                # Modifikasi: Konversi waktu ke datetime objects (UTC aware)
                price_point_time_utc_obj = utils.to_utc_datetime_or_none(item_data.get('price_point_time_utc'))
                indicator_point_time_utc_obj = utils.to_utc_datetime_or_none(item_data.get('indicator_point_time_utc'))

                # Modifikasi: Periksa apakah ada nilai penting yang tidak valid
                if any(v is None for v in [price_level_1_dec, price_level_2_dec, indicator_value_1_dec, indicator_value_2_dec, price_point_time_utc_obj, indicator_point_time_utc_obj]):
                    logger.warning(f"Beberapa nilai divergensi baru tidak valid untuk batch {item_data.get('symbol')} {item_data.get('timeframe')}. Melewatkan item ini.")
                    continue

                new_div_objects.append(Divergence(
                    symbol=item_data.get('symbol'), # Gunakan .get() untuk keamanan
                    timeframe=item_data.get('timeframe'),
                    indicator_type=item_data.get('indicator_type'),
                    divergence_type=item_data.get('divergence_type'),
                    price_point_time_utc=price_point_time_utc_obj,       # <--- KOREKSI: KIRIM OBJEK DATETIME ASLI
                    indicator_point_time_utc=indicator_point_time_utc_obj, # <--- KOREKSI: KIRIM OBJEK DATETIME ASLI
                    price_level_1=price_level_1_dec,                   # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
                    price_level_2=price_level_2_dec,                   # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
                    indicator_value_1=indicator_value_1_dec,           # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
                    indicator_value_2=indicator_value_2_dec,           # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
                    is_active=item_data.get('is_active'),
                    timestamp_recorded=datetime.now(timezone.utc) # Pastikan ini datetime object
                ))
            except KeyError as ke:
                logger.error(f"Kunci data Divergence baru tidak ditemukan di batch: {ke}. Item dilewati: {item_data}", exc_info=True)
                continue
            except Exception as e:
                logger.error(f"Error saat menyiapkan Divergence baru untuk penyimpanan batch DB (item dilewati): {e}. Data: {item_data}", exc_info=True)
                continue

    # Siapkan data yang DIPERBARUI: Pastikan dictionary sudah di-JSON-serializable-kan jika perlu
    # (nilai Decimal -> float, datetime -> ISO string)
    processed_updated_items = []
    if updated_items_data:
        for item_dict in updated_items_data:
            try:
                processed_item_dict = item_dict.copy()
                # Loop melalui item_dict dan konversi jika tipe data sesuai
                for k, v in processed_item_dict.items():
                    if isinstance(v, Decimal):
                        processed_item_dict[k] = float(v) # Konversi ke float untuk bulk_update_mappings
                    elif isinstance(v, datetime):
                        processed_item_dict[k] = v.isoformat() # Konversi ke ISO string
                processed_updated_items.append(processed_item_dict)
            except Exception as e:
                logger.error(f"Error saat memproses item Divergence yang diperbarui untuk batch: {e}. Item dilewati: {item_dict}", exc_info=True)
                continue

    if new_div_objects or processed_updated_items:
        _db_write_queue.put({
            'op_type': 'batch_save_update',
            'model_class': Divergence,
            'new_items_data': new_div_objects,
            'updated_items_data': processed_updated_items,
            'unique_columns': ['symbol', 'timeframe', 'indicator_type', 'divergence_type', 'price_point_time_utc', 'indicator_point_time_utc'],
            'pkey_column_name': 'id'
        })
        logger.debug(f"Divergences batch put into queue: new={len(new_div_objects)}, updated={len(processed_updated_items)}")
    else:
        logger.debug("Tidak ada Divergences valid yang berhasil disiapkan untuk dikirim ke antrean DB.")



def get_divergences(symbol=None, timeframe=None, is_active=None, indicator_type=None, divergence_type=None, limit=None, start_time_utc=None, end_time_utc=None, min_price_level=None, max_price_level=None):
    """
    Mengambil Divergensi antara harga dan indikator dari database.
    Memastikan semua nilai harga/indikator adalah Decimal dan waktu adalah datetime object.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di get_divergences. Tidak dapat melanjutkan.")
        return []
    db = SessionLocal()
    try:
        query = db.query(Divergence)
        if symbol:
            query = query.filter(Divergence.symbol == symbol)
        if timeframe:
            query = query.filter(Divergence.timeframe == timeframe)
        if is_active is not None:
            query = query.filter(Divergence.is_active == is_active)
        if indicator_type:
            query = query.filter(Divergence.indicator_type == indicator_type)
        if divergence_type:
            query = query.filter(Divergence.divergence_type == divergence_type)
        # Modifikasi: Pastikan start_time_utc dan end_time_utc adalah datetime aware UTC untuk filter
        if start_time_utc:
            start_time_utc_processed = to_utc_datetime_or_none(start_time_utc)
            if start_time_utc_processed:
                query = query.filter(Divergence.price_point_time_utc >= start_time_utc_processed)
        if end_time_utc:
            end_time_utc_processed = to_utc_datetime_or_none(end_time_utc)
            if end_time_utc_processed:
                query = query.filter(Divergence.price_point_time_utc <= end_time_utc_processed)

        # --- Modifikasi: Pastikan min_price_level dan max_price_level adalah Decimal untuk filter ---
        # Untuk Divergensi, kita memfilter berdasarkan rentang yang dibentuk oleh price_level_1 dan price_level_2.
        # Jika salah satu atau kedua price level berada dalam rentang filter, maka divergensi tersebut relevan.
        if min_price_level is not None:
            min_price_level_processed = to_decimal_or_none(min_price_level)
            if min_price_level_processed is not None:
                query = query.filter(
                    (Divergence.price_level_1 >= min_price_level_processed) |
                    (Divergence.price_level_2 >= min_price_level_processed)
                )
        if max_price_level is not None:
            max_price_level_processed = to_decimal_or_none(max_price_level)
            if max_price_level_processed is not None:
                query = query.filter(
                    (Divergence.price_level_1 <= max_price_level_processed) |
                    (Divergence.price_level_2 <= max_price_level_processed)
                )
        # --- Akhir Modifikasi ---

        query = query.order_by(Divergence.price_point_time_utc.desc())

        # --- TAMBAHKAN LIMIT DEFAULT ---
        if limit is None:
            limit = 10000 # Batas default
        # --- AKHIR TAMBAHAN LIMIT DEFAULT ---

        if limit:
            query = query.limit(limit)

        results = query.all()

        output = []
        for r in results:
            output.append({
                "id": r.id,
                "symbol": r.symbol,
                "timeframe": r.timeframe,
                "indicator_type": r.indicator_type,
                "divergence_type": r.divergence_type,
                "price_point_time_utc": r.price_point_time_utc, # Ini sudah datetime object dari model
                "indicator_point_time_utc": r.indicator_point_time_utc, # Ini sudah datetime object dari model
                "price_level_1": r.price_level_1, # Ini sudah Decimal dari model
                "price_level_2": r.price_level_2, # Ini sudah Decimal dari model
                "indicator_value_1": r.indicator_value_1, # Ini sudah Decimal dari model
                "indicator_value_2": r.indicator_value_2, # Ini sudah Decimal dari model
                "is_active": r.is_active
            })
        return output
    except Exception as e:
        logger.error(f"Gagal mengambil Divergensi dari DB: {e}", exc_info=True)
        return []
    finally:
        db.close()

def save_news_articles(articles_data: list):
    """
    Menyimpan daftar artikel berita ke database.
    Sekarang memastikan setiap artikel memiliki ID yang valid dan unik
    sebelum dikirim ke antrean penulis DB.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di save_news_articles. Tidak dapat melanjutkan.")
        return

    if not articles_data:
        logger.debug("Tidak ada artikel berita dalam batch untuk disimpan.")
        return

    new_article_objects = []
    for article_item in articles_data:
        try:
            processed_article_item = article_item.copy()

            # Hapus kunci 'type' jika ada (untuk mencegah konflik dengan kolom model)
            if 'type' in processed_article_item:
                del processed_article_item['type']

            # Jika 'name' ada tapi 'title' tidak, pindahkan 'name' ke 'title'
            if 'name' in processed_article_item and 'title' not in processed_article_item:
                processed_article_item['title'] = processed_article_item.pop('name')
            # Jika 'name' masih ada setelah pemindahan atau 'title' sudah ada, hapus 'name'
            if 'name' in processed_article_item:
                del processed_article_item['name']

            # Pastikan title selalu ada
            if not processed_article_item.get('title'):
                processed_article_item['title'] = "No Title Provided"
                logger.warning(f"save_news_articles: Judul kosong untuk artikel dari '{processed_article_item.get('source', 'N/A')}'. Menggunakan default.")

            # Pastikan URL selalu ada
            if not processed_article_item.get('url'):
                processed_article_item['url'] = f"https://fallback.url/{uuid.uuid4()}" # Fallback URL generik
                logger.warning(f"save_news_articles: URL kosong untuk artikel '{processed_article_item.get('title')}'. Menggunakan URL fallback.")

            # --- PENTING: LOGIKA PEMBUATAN/VALIDASI ID ARTIKEL ---
            final_article_id = processed_article_item.get('id')

            # Prioritas ID: 1. ID dari scraper, 2. Hash URL, 3. Hash konten, 4. UUID random
            if not final_article_id:
                # Coba buat ID dari URL (paling stabil jika URL unik)
                if processed_article_item['url'] and processed_article_item['url'].strip():
                    final_article_id = hashlib.sha256(processed_article_item['url'].encode('utf-8')).hexdigest()
                    logger.debug(f"save_news_articles: Membuat ID dari URL: {final_article_id[:10]}...")
                else:
                    # Fallback ke hash konten jika URL juga tidak ada
                    content_to_hash = f"{processed_article_item['title']} {processed_article_item.get('summary', '')}"
                    final_article_id = _normalize_and_hash_content(content_to_hash)
                    if final_article_id:
                        logger.debug(f"save_news_articles: Membuat ID dari konten: {final_article_id[:10]}...")
                    else:
                        # Fallback terakhir: UUID random (ini sangat jarang terjadi)
                        final_article_id = str(uuid.uuid4())
                        logger.critical(f"save_news_articles: Tidak dapat membuat ID dari URL atau konten. Menggunakan UUID random: {final_article_id}. Tolong periksa scraper untuk artikel: {processed_article_item.get('title')[:50]}...")

            # Potong ID agar sesuai dengan batas kolom `String(255)`
            final_article_id = final_article_id[:255]

            # Jika masih kosong setelah semua upaya, lewati item ini
            if not final_article_id or not final_article_id.strip():
                logger.critical(f"save_news_articles: FINAL ID masih kosong setelah semua upaya fallback untuk '{processed_article_item.get('title', 'N/A')}'. MELEWATI ITEM INI. Item: {article_item}")
                continue # Lanjut ke item berikutnya

            processed_article_item['id'] = final_article_id
            # --- AKHIR LOGIKA PEMBUATAN/VALIDASI ID ARTIKEL ---


            # Modifikasi: Pastikan published_time_utc adalah datetime object yang valid
            published_time_utc_for_db = utils.to_utc_datetime_or_none(processed_article_item.get('published_time_utc'))
            if published_time_utc_for_db is None:
                published_time_utc_for_db = datetime.now(timezone.utc)
                logger.warning(f"save_news_articles: published_time_utc kosong atau tidak valid untuk '{processed_article_item.get('title', 'N/A')}'. Menggunakan waktu saat ini.")
            processed_article_item['published_time_utc'] = published_time_utc_for_db

            # Set timestamp_recorded
            processed_article_item['timestamp_recorded'] = datetime.now(timezone.utc)

            # Membuat objek model NewsArticle
            # Tidak perlu _convert_to_json_serializable di sini, karena model akan menangani tipe aslinya
            new_article_objects.append(NewsArticle(
                id=processed_article_item.get('id'),
                source=processed_article_item.get('source'),
                title=processed_article_item.get('title'),
                summary=processed_article_item.get('summary'),
                url=processed_article_item.get('url'),
                published_time_utc=processed_article_item.get('published_time_utc'), # Datetime
                relevance_score=processed_article_item.get('relevance_score'),
                thumbnail_url=processed_article_item.get('thumbnail_url'),
                timestamp_recorded=processed_article_item.get('timestamp_recorded') # Datetime
            ))
        except Exception as e:
            logger.error(f"Gagal menyiapkan artikel berita untuk penyimpanan DB (item dilewati): {article_item.get('url', 'N/A')}. Error: {e}", exc_info=True)
            continue # Lanjutkan ke item berikutnya jika ada error pada satu item

    if new_article_objects:
        _db_write_queue.put({
            'op_type': 'batch_save_update',
            'model_class': NewsArticle,
            'new_items_data': new_article_objects,
            'updated_items_data': [], # Artikel biasanya hanya di-insert, jarang di-update
            'unique_columns': ['url'], # Konflik pada URL untuk deduplikasi utama
            'pkey_column_name': 'id' # Primary key, akan digunakan untuk `get()` jika perlu
        })
        logger.debug(f"News articles batch ({len(new_article_objects)} articles) dikirim ke antrean DB.")
    else:
        logger.debug("Tidak ada News articles valid yang berhasil disiapkan untuk dikirim ke antrean DB.")





def save_economic_events(events_data: list):
    """
    Menyimpan daftar event ekonomi ke database.
    Sekarang memastikan setiap event memiliki ID yang valid dan unik
    sebelum dikirim ke antrean penulis DB.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di save_economic_events. Tidak dapat melanjutkan.")
        return

    if not events_data:
        logger.debug("Tidak ada event ekonomi dalam batch untuk disimpan.")
        return

    new_event_objects = []
    for event_item in events_data:
        try:
            processed_event_item = event_item.copy()

            # Hapus kunci 'type' jika ada (untuk mencegah konflik dengan kolom model)
            if 'type' in processed_event_item:
                del processed_event_item['type']

            # Jika 'title' ada tapi 'name' tidak, pindahkan 'title' ke 'name'
            if 'title' in processed_event_item and 'name' not in processed_event_item:
                processed_event_item['name'] = processed_event_item.pop('title')
            # Jika 'title' masih ada setelah pemindahan atau 'name' sudah ada, hapus 'title'
            if 'title' in processed_event_item:
                del processed_event_item['title']

            # Pastikan 'name' (nama event) selalu ada
            if not processed_event_item.get('name'):
                processed_event_item['name'] = "Unknown Economic Event"
                logger.warning(f"save_economic_events: Nama event kosong. Menggunakan default.")

            # --- PENTING: LOGIKA PEMBUATAN/VALIDASI event_id ---
            final_event_id = processed_event_item.get('event_id')

            # Prioritas ID: 1. event_id dari scraper, 2. Hash kombinasi data, 3. UUID random
            if not final_event_id:
                # Coba buat ID dari kombinasi nama event, mata uang, dan waktu event
                event_name_safe = processed_event_item['name'] # Sudah dipastikan ada
                currency_safe = processed_event_item.get('currency', 'N/A')

                # Modifikasi: Pastikan event_time_for_id dikonversi dengan helper
                event_time_for_id = utils.to_utc_datetime_or_none(processed_event_item.get('event_time_utc'))

                if event_time_for_id is None:
                    event_time_for_id = datetime.now(timezone.utc)
                    logger.warning(f"save_economic_events: event_time_utc untuk ID kosong atau tidak valid untuk '{event_name_safe}'. Menggunakan waktu saat ini.")

                # Buat hash dari kombinasi data kunci
                unique_id_source = f"{event_name_safe}_{currency_safe}_{event_time_for_id.strftime('%Y%m%d%H%M%S%f')}"
                final_event_id = hashlib.sha256(unique_id_source.encode('utf-8')).hexdigest()
                logger.debug(f"save_economic_events: Membuat ID dari kombinasi data: {final_event_id[:10]}...")

                if not final_event_id: # Fallback terakhir jika hash gagal (sangat jarang)
                    final_event_id = str(uuid.uuid4())
                    logger.critical(f"save_economic_events: Tidak dapat membuat ID dari kombinasi data. Menggunakan UUID random: {final_event_id}. Tolong periksa scraper untuk event: {processed_event_item.get('name')[:50]}...")

            # Potong ID agar sesuai dengan batas kolom `String(255)`
            final_event_id = final_event_id[:255]

            # Jika masih kosong setelah semua upaya, lewati item ini
            if not final_event_id or not final_event_id.strip():
                logger.critical(f"save_economic_events: FINAL ID masih kosong setelah semua upaya fallback untuk '{processed_event_item.get('name', 'N/A')}'. MELEWATI ITEM INI.")
                continue # Lanjut ke item berikutnya

            processed_event_item['event_id'] = final_event_id
            # --- AKHIR LOGIKA PEMBUATAN/VALIDASI event_id ---

            # Modifikasi: Pastikan event_time_utc adalah datetime object yang valid untuk kolom DB
            event_time_utc_for_db = utils.to_utc_datetime_or_none(processed_event_item.get('event_time_utc'))
            if event_time_utc_for_db is None:
                event_time_utc_for_db = datetime.now(timezone.utc)
                logger.warning(f"save_economic_events: event_time_utc kosong atau tidak valid untuk '{processed_event_item.get('name', 'N/A')}'. Menggunakan waktu saat ini.")
            processed_event_item['event_time_utc'] = event_time_utc_for_db

            # Konversi nilai aktual, perkiraan, dan sebelumnya ke Decimal
            actual_value_processed = utils.to_decimal_or_none(processed_event_item.get('actual_value'))
            forecast_value_processed = utils.to_decimal_or_none(processed_event_item.get('forecast_value'))
            previous_value_processed = utils.to_decimal_or_none(processed_event_item.get('previous_value'))

            # Set timestamp_recorded
            processed_event_item['timestamp_recorded'] = datetime.now(timezone.utc)

            # Membuat objek model EconomicEvent
            # Tidak perlu _convert_to_json_serializable di sini
            new_event_objects.append(EconomicEvent(
                event_id=processed_event_item['event_id'],
                name=processed_event_item.get('name'),
                country=processed_event_item.get('country'),
                currency=processed_event_item.get('currency'),
                impact=processed_event_item.get('impact'),
                event_time_utc=processed_event_item['event_time_utc'], # Datetime
                actual_value=actual_value_processed,     # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
                forecast_value=forecast_value_processed, # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
                previous_value=previous_value_processed, # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
                timestamp_recorded=processed_event_item['timestamp_recorded'] # Datetime
            ))

        except Exception as e:
            logger.error(f"Gagal menyiapkan event ekonomi untuk penyimpanan DB (event dilewati): {event_item.get('name', 'N/A')}. Error: {e}", exc_info=True)
            continue # Lanjutkan ke item berikutnya

    if new_event_objects:
        _db_write_queue.put({
            'op_type': 'batch_save_update',
            'model_class': EconomicEvent,
            'new_items_data': new_event_objects,
            'updated_items_data': [], # Event biasanya hanya di-insert, jarang di-update
            'unique_columns': ['event_id'], # Menggunakan event_id yang sudah dijamin unik
            'pkey_column_name': 'event_id'
        })
        logger.debug(f"Economic events batch ({len(new_event_objects)} events) dikirim ke antrean DB.")
    else:
        logger.debug("Tidak ada Economic events valid yang berhasil disiapkan untuk dikirim ke antrean DB.")

def get_upcoming_events_from_db(days_past=0, days_future=0, start_time_utc=None, end_time_utc=None, min_impact=None, target_currency=None, limit=None):
    """
    Mengambil event ekonomi dan artikel berita dari database dalam rentang waktu tertentu.
    Jika start_time_utc dan end_time_utc diberikan, parameter days_past dan days_future diabaikan.
    Mengembalikan daftar gabungan event dan artikel.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di get_upcoming_events_from_db. Tidak dapat melanjutkan.")
        return []
    db = SessionLocal()
    try:
        now_utc = datetime.now(timezone.utc)
        
        if start_time_utc is None:
            start_time_utc_effective = now_utc - timedelta(days=days_past)
        else:
            start_time_utc_processed = utils.to_utc_datetime_or_none(start_time_utc)
            if start_time_utc_processed:
                start_time_utc_effective = start_time_utc_processed
            else:
                logger.warning(f"start_time_utc '{start_time_utc}' tidak valid. Menggunakan default days_past.")
                start_time_utc_effective = now_utc - timedelta(days=days_past)

        if end_time_utc is None:
            end_time_utc_effective = now_utc + timedelta(days=days_future)
        else:
            end_time_utc_processed = utils.to_utc_datetime_or_none(end_time_utc)
            if end_time_utc_processed:
                end_time_utc_effective = end_time_utc_processed
            else:
                logger.warning(f"end_time_utc '{end_time_utc}' tidak valid. Menggunakan default days_future.")
                end_time_utc_effective = now_utc + timedelta(days=days_future)

        all_events = []

        event_query = db.query(EconomicEvent).filter(
            EconomicEvent.event_time_utc >= start_time_utc_effective,
            EconomicEvent.event_time_utc <= end_time_utc_effective
        )
        if min_impact:
            impact_order = {'Low': 1, 'Medium': 2, 'High': 3}
            min_impact_value = impact_order.get(min_impact, 1)
            event_query = event_query.filter(EconomicEvent.impact.in_([
                impact for impact, value in impact_order.items() if value >= min_impact_value
            ]))
        if target_currency:
            event_query = event_query.filter(EconomicEvent.currency == target_currency)
        
        event_query = event_query.order_by(EconomicEvent.event_time_utc.asc())
        # Limit per query type (EconomicEvent)
        if limit:
            event_query = event_query.limit(limit)
            
        economic_events_db = event_query.all()
        for event in economic_events_db:
            all_events.append({
                "type": "economic_calendar",
                "id": event.event_id,
                "name": event.name,
                "country": event.country,
                "currency": event.currency,
                "impact": event.impact,
                "event_time_utc": event.event_time_utc,
                "actual_value": event.actual_value,
                "forecast_value": event.forecast_value,
                "previous_value": event.previous_value
            })
        logger.info(f"Mengambil {len(economic_events_db)} event ekonomi dari DB.")

        news_query = db.query(NewsArticle).filter(
            NewsArticle.published_time_utc >= start_time_utc_effective,
            NewsArticle.published_time_utc <= end_time_utc_effective
        )
        news_query = news_query.order_by(NewsArticle.published_time_utc.desc())
        # Limit per query type (NewsArticle)
        if limit:
            news_query = news_query.limit(limit)

        news_articles_db = news_query.all()
        for article in news_articles_db:
            all_events.append({
                "type": "news_article",
                "id": article.id,
                "source": article.source,
                "title": article.title,
                "summary": article.summary,
                "url": article.url,
                "published_time_utc": article.published_time_utc,
                "relevance_score": article.relevance_score,
                "thumbnail_url": article.thumbnail_url
            })
        logger.info(f"Mengambil {len(news_articles_db)} artikel berita dari DB.")

        return all_events

    except Exception as e:
        logger.error(f"Gagal mengambil event fundamental dari DB: {e}", exc_info=True)
        return []
    finally:
        db.close()

def clean_old_price_ticks(days_to_keep=7):
    """
    Menghapus tick harga yang lebih lama dari N hari.
    Memastikan cutoff_time menggunakan datetime timezone-aware UTC yang konsisten.
    """
    global SessionLocal # Pastikan ini ada
    if SessionLocal is None: # Pastikan pengecekan inisialisasi sesi ada
        logger.error("SessionLocal belum diinisialisasi di clean_old_price_ticks. Tidak dapat melanjutkan.")
        return
    db = SessionLocal()
    try:
        # Modifikasi: Pastikan cutoff_time menggunakan datetime.now(timezone.utc)
        cutoff_time = datetime.now(timezone.utc) - timedelta(days=days_to_keep)
        deleted_count = db.query(PriceTick).filter(PriceTick.time_utc_datetime < cutoff_time).delete()
        db.commit()
        logger.info(f"Dibersihkan {deleted_count} tick harga lebih lama dari {days_to_keep} hari.")
    except Exception as e:
        db.rollback()
        logger.error(f"Gagal membersihkan tick harga lama: {e}", exc_info=True)
    finally:
        db.close()


def clean_old_mt5_trade_data(days_to_keep=30):
    """
    Menghapus data posisi, order, dan deal history MT5 yang lebih lama dari N hari.
    Catatan: Posisi dan order umumnya hanya relevan saat aktif. Ini lebih untuk deal history.
    Memastikan cutoff_time menggunakan datetime timezone-aware UTC yang konsisten.
    """
    global SessionLocal 
    if SessionLocal is None: 
        logger.error("SessionLocal belum diinisialisasi di clean_old_mt5_trade_data. Tidak dapat melanjutkan.")
        return
    db = SessionLocal()
    try:
        # Modifikasi: Pastikan cutoff_time menggunakan datetime.now(timezone.utc)
        cutoff_time = datetime.now(timezone.utc) - timedelta(days=days_to_keep)
        
        # Posisi dan Order biasanya hanya disimpan saat aktif, tidak perlu pembersihan histori berdasarkan waktu
        # Tetapi untuk deal history, kita bisa membersihkan yang lama
        deleted_deals_count = db.query(MT5DealHistory).filter(MT5DealHistory.time < cutoff_time).delete()
        db.commit()
        logger.info(f"Dibersihkan {deleted_deals_count} deal history MT5 lebih lama dari {days_to_keep} hari.")
        
        # Anda mungkin tidak ingin membersihkan account info kecuali sudah terlalu banyak duplikat
        # deleted_account_info_count = db.query(MT5AccountInfo).filter(MT5AccountInfo.timestamp_recorded < cutoff_time).delete()
        # db.commit()
        # logger.info(f"Dibersihkan {deleted_account_info_count} account info lama.")

    except Exception as e:
        db.rollback()
        logger.error(f"Gagal membersihkan data perdagangan MT5 lama: {e}", exc_info=True)
    finally:
        db.close()

def clean_old_historical_candles(days_to_keep=90): # Parameter days_to_keep tidak lagi digunakan, kebijakan di config.py
    """
    Menghapus candle historis berdasarkan kebijakan retensi yang ditentukan di config.py.
    """
    global SessionLocal # <--- TAMBAHKAN INI
    if SessionLocal is None: # <--- TAMBAHKAN PENGECEKAN INI
        logger.error("SessionLocal belum diinisialisasi di clean_old_historical_candles. Tidak dapat melanjutkan.")
        return
    db = SessionLocal()
    try:
        from config import config # Import di sini untuk menghindari circular import jika ada
        
        retention_policy = config.MarketData.HISTORICAL_DATA_RETENTION_DAYS
        
        total_deleted_count = 0

        for tf_name, days_to_retain in retention_policy.items():
            if days_to_retain > 0: # Hanya hapus jika days_to_retain lebih dari 0
                cutoff_time = datetime.now(timezone.utc) - timedelta(days=days_to_retain)
                
                deleted_count_tf = db.query(HistoricalCandle).filter(
                    HistoricalCandle.timeframe == tf_name, # Filter berdasarkan timeframe
                    HistoricalCandle.open_time_utc < cutoff_time
                ).delete(synchronize_session=False) # Penting untuk bulk delete
                
                total_deleted_count += deleted_count_tf
                logger.info(f"Dibersihkan {deleted_count_tf} candle historis {tf_name} lebih lama dari {days_to_retain} hari.")
            else:
                logger.info(f"Kebijakan retensi untuk {tf_name} adalah 'simpan semua' (0 hari). Tidak ada penghapusan.")

        db.commit()
        logger.info(f"Pembersihan candle historis total: {total_deleted_count} record dihapus.")

    except Exception as e:
        db.rollback()
        logger.error(f"Gagal membersihkan candle historis lama: {e}", exc_info=True)
    finally:
        db.close()

def get_last_feature_backfill_time(symbol, timeframe):
    """
    Mengambil waktu candle terakhir yang diproses untuk fitur historis di timeframe tertentu.
    Mengembalikan datetime object atau None jika belum pernah diproses.
    """
    global SessionLocal # <--- TAMBAHKAN INI
    if SessionLocal is None: # <--- TAMBAHKAN PENGECEKAN INI
        logger.error("SessionLocal belum diinisialisasi di get_last_feature_backfill_time. Tidak dapat melanjutkan.")
        return None
    db = SessionLocal()
    try:
        status_entry = db.query(FeatureBackfillStatus).filter_by(
            symbol=symbol, timeframe=timeframe
        ).first()
        return status_entry.last_processed_time_utc if status_entry else None
    except Exception as e:
        logger.error(f"Gagal mengambil status backfill fitur untuk {symbol} {timeframe}: {e}", exc_info=True)
        return None
    finally:
        db.close()

def save_feature_backfill_status(symbol, timeframe, last_processed_time_utc):
    """
    Menyimpan atau memperbarui waktu candle terakhir yang diproses untuk fitur historis.
    --- PERUBAHAN: Sekarang mengirimkan tugas ke antrean penulis DB. ---
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di save_feature_backfill_status. Tidak dapat melanjutkan.")
        return

    processed_data = {
        "symbol": symbol,
        "timeframe": timeframe,
        "last_processed_time_utc": _convert_to_json_serializable(last_processed_time_utc),
        "last_update_run_time_utc": _convert_to_json_serializable(datetime.now(timezone.utc))
    }

    _db_write_queue.put({
        'op_type': 'single_save_update',
        'model_class': FeatureBackfillStatus,
        'single_item_data': processed_data,
        'single_item_key_columns': ['symbol', 'timeframe']
    })
    logger.debug(f"Feature backfill status untuk {symbol} {timeframe} dikirim ke antrean DB.")

def _convert_decimal_in_dict(item_dict):
    """Mengonversi Decimal dalam dictionary menjadi float untuk SQLAlchemy update mappings"""
    for key, value in item_dict.items():
        if isinstance(value, Decimal):
            item_dict[key] = float(value)
    return item_dict

# --- MULAI BLOK KODE KOREKSI UNTUK FUNGSI _batch_save_and_update ---

def save_fair_value_gaps_batch(new_items_data: list, updated_items_data: list):
    """
    Menyimpan FVG baru dan memperbarui FVG yang sudah ada dalam batch.
    Memastikan data diubah ke objek model/format yang benar sebelum dikirim ke antrean.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di save_fair_value_gaps_batch. Tidak dapat melanjutkan.")
        return

    # Siapkan data BARU: Konversi list of dictionaries menjadi list of FairValueGap objects
    new_fvg_objects = []
    if new_items_data:
        for item_data in new_items_data:
            try:
                # Konversi harga ke Decimal
                fvg_top_price_dec = utils.to_decimal_or_none(item_data.get('fvg_top_price'))
                fvg_bottom_price_dec = utils.to_decimal_or_none(item_data.get('fvg_bottom_price'))

                # Konversi waktu ke datetime objects (UTC aware)
                formation_time_utc_obj = utils.to_utc_datetime_or_none(item_data.get('formation_time_utc'))
                last_fill_time_utc_obj = utils.to_utc_datetime_or_none(item_data.get('last_fill_time_utc'))
                last_retest_time_utc_obj = utils.to_utc_datetime_or_none(item_data.get('last_retest_time_utc'))

                # Validasi nilai penting yang tidak boleh None
                if fvg_top_price_dec is None or fvg_bottom_price_dec is None or formation_time_utc_obj is None:
                    logger.warning(f"Data FVG tidak lengkap (harga atau waktu formasi kosong) untuk {item_data.get('symbol')} {item_data.get('timeframe')}. Melewatkan item ini.")
                    continue

                new_fvg_objects.append(FairValueGap(
                    symbol=item_data['symbol'],
                    timeframe=item_data['timeframe'],
                    type=item_data['type'],
                    fvg_top_price=fvg_top_price_dec,
                    fvg_bottom_price=fvg_bottom_price_dec,
                    formation_time_utc=formation_time_utc_obj,
                    is_filled=item_data.get('is_filled', False), # Gunakan .get dengan default
                    last_fill_time_utc=last_fill_time_utc_obj,
                    retest_count=item_data.get('retest_count', 0), # Gunakan .get dengan default
                    last_retest_time_utc=last_retest_time_utc_obj,
                    strength_score=item_data.get('strength_score', 0), # Tambahkan strength_score dengan default
                    timestamp_recorded=datetime.now(timezone.utc)
                ))
            except KeyError as ke:
                logger.error(f"Kunci data FVG baru tidak ditemukan di batch: {ke}. Item dilewati: {item_data}", exc_info=True)
                continue
            except Exception as e:
                logger.error(f"Error saat menyiapkan FVG baru untuk penyimpanan batch DB (item dilewati): {e}. Data: {item_data}", exc_info=True)
                continue

    # Siapkan data yang DIPERBARUI: (Logika ini sudah benar untuk bulk_update_mappings)
    processed_updated_items = []
    if updated_items_data:
        for item_dict in updated_items_data:
            try:
                processed_item_dict = item_dict.copy()
                for k, v in processed_item_dict.items():
                    if isinstance(v, Decimal):
                        processed_item_dict[k] = float(v)
                    elif isinstance(v, datetime):
                        processed_item_dict[k] = v.isoformat()
                processed_updated_items.append(processed_item_dict)
            except Exception as e:
                logger.error(f"Error saat memproses item FVG yang diperbarui untuk batch: {e}. Item dilewati: {item_dict}", exc_info=True)
                continue

    if new_fvg_objects or processed_updated_items:
        _db_write_queue.put({
            'op_type': 'batch_save_update',
            'model_class': FairValueGap,
            'new_items_data': new_fvg_objects,
            'updated_items_data': processed_updated_items,
            'unique_columns': ['symbol', 'timeframe', 'formation_time_utc', 'type'],
            'pkey_column_name': 'id'
        })
        logger.debug(f"FVG batch put into queue: new={len(new_fvg_objects)}, updated={len(processed_updated_items)}")
    else:
        logger.debug("Tidak ada FVG valid yang berhasil disiapkan untuk dikirim ke antrean DB.")





def save_order_blocks_batch(new_items_data: list, updated_items_data: list):
    """
    Menyimpan Order Blocks baru dan memperbarui yang sudah ada dalam batch.
    Memastikan data diubah ke objek model/format yang benar sebelum dikirim ke antrean.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di save_order_blocks_batch. Tidak dapat melanjutkan.")
        return

    # Siapkan data BARU: Konversi list of dictionaries menjadi list of OrderBlock objects
    new_ob_objects = []
    if new_items_data:
        for item_data in new_items_data:
            try:
                # Modifikasi: Konversi harga ke Decimal
                ob_top_price_dec = utils.to_decimal_or_none(item_data.get('ob_top_price'))
                ob_bottom_price_dec = utils.to_decimal_or_none(item_data.get('ob_bottom_price'))

                # Modifikasi: Konversi waktu ke datetime objects (UTC aware)
                formation_time_utc_obj = utils.to_utc_datetime_or_none(item_data.get('formation_time_utc'))
                last_mitigation_time_utc_obj = utils.to_utc_datetime_or_none(item_data.get('last_mitigation_time_utc'))

                # Modifikasi: Periksa apakah ada nilai penting yang tidak valid
                if ob_top_price_dec is None or ob_bottom_price_dec is None or formation_time_utc_obj is None:
                    logger.warning(f"Beberapa nilai Order Block baru tidak valid untuk batch {item_data.get('symbol')} {item_data.get('timeframe')}. Melewatkan item ini.")
                    continue

                new_ob_objects.append(OrderBlock(
                    symbol=item_data['symbol'],
                    timeframe=item_data['timeframe'],
                    type=item_data['type'],
                    ob_top_price=ob_top_price_dec,
                    ob_bottom_price=ob_bottom_price_dec,
                    formation_time_utc=formation_time_utc_obj,
                    is_mitigated=item_data['is_mitigated'],
                    last_mitigation_time_utc=last_mitigation_time_utc_obj,
                    strength_score=item_data.get('strength_score'),
                    confluence_score=item_data.get('confluence_score', 0),
                    timestamp_recorded=datetime.now(timezone.utc) # Pastikan ini datetime object
                ))
            except KeyError as ke:
                logger.error(f"Kunci data OB baru tidak ditemukan di batch: {ke}. Item dilewati: {item_data}", exc_info=True)
                continue
            except Exception as e:
                logger.error(f"Error saat menyiapkan OB baru untuk penyimpanan batch DB (item dilewati): {e}. Data: {item_data}", exc_info=True)
                continue

    # Siapkan data yang DIPERBARUI: Pastikan dictionary sudah di-JSON-serializable-kan jika perlu
    processed_updated_items = []
    if updated_items_data:
        for item_dict in updated_items_data:
            try:
                processed_item_dict = item_dict.copy()
                for k, v in processed_item_dict.items():
                    if isinstance(v, Decimal):
                        processed_item_dict[k] = utils.to_float_or_none(v) # Menggunakan helper
                    elif isinstance(v, datetime):
                        processed_item_dict[k] = utils.to_iso_format_or_none(v) # Menggunakan helper
                processed_updated_items.append(processed_item_dict)
            except Exception as e:
                logger.error(f"Error saat memproses item OB yang diperbarui untuk batch: {e}. Item dilewati: {item_dict}", exc_info=True)
                continue

    if new_ob_objects or processed_updated_items:
        _db_write_queue.put({
            'op_type': 'batch_save_update',
            'model_class': OrderBlock,
            'new_items_data': new_ob_objects,
            'updated_items_data': processed_updated_items,
            'unique_columns': ['symbol', 'timeframe', 'formation_time_utc', 'type'],
            'pkey_column_name': 'id'
        })
        logger.debug(f"Order Blocks batch put into queue: new={len(new_ob_objects)}, updated={len(processed_updated_items)}")
    else:
        logger.debug("Tidak ada Order Blocks valid yang berhasil disiapkan untuk dikirim ke antrean DB.")


def save_support_resistance_levels_batch(new_items_data: list, updated_items_data: list):
    """
    Menyimpan S&R baru dan memperbarui yang sudah ada dalam batch.
    Memastikan data diubah ke objek model/format yang benar sebelum dikirim ke antrean.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di save_support_resistance_levels_batch. Tidak dapat melanjutkan.")
        return

    # Siapkan data BARU: Konversi list of dictionaries menjadi list of SupportResistanceLevel objects
    new_sr_objects = []
    if new_items_data:
        for item_data in new_items_data:
            try:
                # Modifikasi: Konversi harga ke Decimal
                price_level_dec = utils.to_decimal_or_none(item_data.get('price_level'))
                zone_start_price_dec = utils.to_decimal_or_none(item_data.get('zone_start_price'))
                zone_end_price_dec = utils.to_decimal_or_none(item_data.get('zone_end_price'))

                # Modifikasi: Konversi waktu ke datetime objects (UTC aware)
                formation_time_utc_obj = utils.to_utc_datetime_or_none(item_data.get('formation_time_utc'))
                last_test_time_utc_obj = utils.to_utc_datetime_or_none(item_data.get('last_test_time_utc'))

                # Modifikasi: Periksa apakah ada nilai penting yang tidak valid
                if price_level_dec is None or formation_time_utc_obj is None:
                    logger.warning(f"Beberapa nilai S&R baru tidak valid untuk batch {item_data.get('symbol')} {item_data.get('timeframe')}. Melewatkan item ini.")
                    continue

                new_sr_objects.append(SupportResistanceLevel(
                    symbol=item_data['symbol'],
                    timeframe=item_data['timeframe'],
                    level_type=item_data['level_type'],
                    price_level=price_level_dec,
                    zone_start_price=zone_start_price_dec,
                    zone_end_price=zone_end_price_dec,
                    strength_score=item_data.get('strength_score'),
                    is_active=item_data['is_active'],
                    formation_time_utc=formation_time_utc_obj,
                    last_test_time_utc=last_test_time_utc_obj,
                    is_key_level=item_data.get('is_key_level', False),
                    confluence_score=item_data.get('confluence_score', 0),
                    timestamp_recorded=datetime.now(timezone.utc) # Pastikan ini datetime object
                ))
            except KeyError as ke:
                logger.error(f"Kunci data S&R baru tidak ditemukan di batch: {ke}. Item dilewati: {item_data}", exc_info=True)
                continue
            except Exception as e:
                logger.error(f"Error saat menyiapkan S&R baru untuk penyimpanan batch DB (item dilewati): {e}. Data: {item_data}", exc_info=True)
                continue

    # Siapkan data yang DIPERBARUI: Pastikan dictionary sudah di-JSON-serializable-kan jika perlu
    processed_updated_items = []
    if updated_items_data:
        for item_dict in updated_items_data:
            try:
                processed_item_dict = item_dict.copy()
                for k, v in processed_item_dict.items():
                    if isinstance(v, Decimal):
                        processed_item_dict[k] = utils.to_float_or_none(v) # Menggunakan helper
                    elif isinstance(v, datetime):
                        processed_item_dict[k] = utils.to_iso_format_or_none(v) # Menggunakan helper
                processed_updated_items.append(processed_item_dict)
            except Exception as e:
                logger.error(f"Error saat memproses item S&R yang diperbarui untuk batch: {e}. Item dilewati: {item_dict}", exc_info=True)
                continue

    if new_sr_objects or processed_updated_items:
        _db_write_queue.put({
            'op_type': 'batch_save_update',
            'model_class': SupportResistanceLevel,
            'new_items_data': new_sr_objects,
            'updated_items_data': processed_updated_items,
            'unique_columns': ['symbol', 'timeframe', 'price_level', 'level_type'],
            'pkey_column_name': 'id'
        })
        logger.debug(f"S&R Levels batch put into queue: new={len(new_sr_objects)}, updated={len(processed_updated_items)}")
    else:
        logger.debug("Tidak ada S&R Levels valid yang berhasil disiapkan untuk dikirim ke antrean DB.")

def save_supply_demand_zones_batch(new_items_data: list, updated_items_data: list):
    """
    Menyimpan S&D baru dan memperbarui yang sudah ada dalam batch.
    Memastikan data diubah ke objek model/format yang benar sebelum dikirim ke antrean.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di save_supply_demand_zones_batch. Tidak dapat melanjutkan.")
        return

    # Siapkan data BARU: Konversi list of dictionaries menjadi list of SupplyDemandZone objects
    new_sd_objects = []
    if new_items_data:
        for item_data in new_items_data:
            try:
                # Konversi harga ke Decimal
                zone_top_price_dec = Decimal(str(item_data['zone_top_price'])) if item_data.get('zone_top_price') is not None else None
                zone_bottom_price_dec = Decimal(str(item_data['zone_bottom_price'])) if item_data.get('zone_bottom_price') is not None else None

                # Konversi waktu ke datetime objects
                formation_time_utc_obj = item_data['formation_time_utc']
                if isinstance(formation_time_utc_obj, str):
                    formation_time_utc_obj = datetime.fromisoformat(formation_time_utc_obj).replace(tzinfo=timezone.utc)
                elif isinstance(formation_time_utc_obj, datetime) and formation_time_utc_obj.tzinfo is None:
                    formation_time_utc_obj = formation_time_utc_obj.replace(tzinfo=timezone.utc)

                last_retest_time_utc_obj = item_data.get('last_retest_time_utc')
                if isinstance(last_retest_time_utc_obj, str):
                    last_retest_time_utc_obj = datetime.fromisoformat(last_retest_time_utc_obj).replace(tzinfo=timezone.utc)
                elif isinstance(last_retest_time_utc_obj, datetime) and last_retest_time_utc_obj.tzinfo is None:
                    last_retest_time_utc_obj = last_retest_time_utc_obj.replace(tzinfo=timezone.utc)


                new_sd_objects.append(SupplyDemandZone(
                    symbol=item_data['symbol'],
                    timeframe=item_data['timeframe'],
                    zone_type=item_data['zone_type'],
                    base_type=item_data['base_type'],
                    zone_top_price=zone_top_price_dec,
                    zone_bottom_price=zone_bottom_price_dec,
                    formation_time_utc=formation_time_utc_obj,
                    is_mitigated=item_data['is_mitigated'],
                    strength_score=item_data.get('strength_score'),
                    is_key_level=item_data.get('is_key_level', False),
                    confluence_score=item_data.get('confluence_score', 0),
                    last_retest_time_utc=last_retest_time_utc_obj,
                    timestamp_recorded=datetime.now(timezone.utc) # Pastikan ini datetime object
                ))
            except KeyError as ke:
                logger.error(f"Kunci data S&D baru tidak ditemukan di batch: {ke}. Item dilewati: {item_data}", exc_info=True)
                continue
            except Exception as e:
                logger.error(f"Error saat menyiapkan S&D baru untuk penyimpanan batch DB (item dilewati): {e}. Data: {item_data}", exc_info=True)
                continue

    # Siapkan data yang DIPERBARUI: Pastikan dictionary sudah di-JSON-serializable-kan jika perlu
    processed_updated_items = []
    if updated_items_data:
        for item_dict in updated_items_data:
            try:
                processed_item_dict = item_dict.copy()
                for k, v in processed_item_dict.items():
                    if isinstance(v, Decimal):
                        processed_item_dict[k] = float(v)
                    elif isinstance(v, datetime):
                        processed_item_dict[k] = v.isoformat()
                processed_updated_items.append(processed_item_dict)
            except Exception as e:
                logger.error(f"Error saat memproses item S&D yang diperbarui untuk batch: {e}. Item dilewati: {item_dict}", exc_info=True)
                continue

    if new_sd_objects or processed_updated_items:
        _db_write_queue.put({
            'op_type': 'batch_save_update',
            'model_class': SupplyDemandZone,
            'new_items_data': new_sd_objects,
            'updated_items_data': processed_updated_items,
            'unique_columns': ['symbol', 'timeframe', 'zone_type', 'formation_time_utc'],
            'pkey_column_name': 'id'
        })
        logger.debug(f"S&D Zones batch put into queue: new={len(new_sd_objects)}, updated={len(processed_updated_items)}")
    else:
        logger.debug("Tidak ada S&D Zones valid yang berhasil disiapkan untuk dikirim ke antrean DB.")



def save_market_structure_events_batch(new_items_data: list, updated_items_data: list): # <--- TAMBAHKAN updated_items_data
    """
    Menyimpan Market Structure Events baru dan memperbarui yang sudah ada dalam batch.
    Memastikan data diubah ke objek model/format yang benar sebelum dikirim ke antrean.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di save_market_structure_events_batch. Tidak dapat melanjutkan.")
        return

    # --- START: Proses NEW items (insert) ---
    new_ms_objects = []
    if new_items_data:
        for item_data in new_items_data:
            try:
                price_level_dec = utils.to_decimal_or_none(item_data.get('price_level'))
                event_time_utc_obj = utils.to_utc_datetime_or_none(item_data.get('event_time_utc'))
                if event_time_utc_obj is None:
                    logger.warning(f"Event time UTC kosong atau tidak valid untuk MS Event {item_data.get('event_type')}. Melewatkan item ini.")
                    continue
                swing_high_ref_time_obj = utils.to_utc_datetime_or_none(item_data.get('swing_high_ref_time'))
                swing_low_ref_time_obj = utils.to_utc_datetime_or_none(item_data.get('swing_low_ref_time'))

                new_ms_objects.append(MarketStructureEvent(
                    symbol=item_data.get('symbol'),
                    timeframe=item_data.get('timeframe'),
                    event_type=item_data.get('event_type'),
                    direction=item_data.get('direction'),
                    price_level=price_level_dec,
                    event_time_utc=event_time_utc_obj,
                    swing_high_ref_time=swing_high_ref_time_obj,
                    swing_low_ref_time=swing_low_ref_time_obj,
                    # Pastikan confluence_score juga disimpan jika ada di new_items_data
                    confluence_score=item_data.get('confluence_score', 0), 
                    timestamp_recorded=datetime.now(timezone.utc)
                ))
            except KeyError as ke:
                logger.error(f"Kunci data Market Structure Event baru tidak ditemukan di batch: {ke}. Item dilewati: {item_data}", exc_info=True)
                continue
            except Exception as e:
                logger.error(f"Error saat menyiapkan Market Structure Event baru untuk penyimpanan batch DB (item dilewati): {e}. Data: {item_data}", exc_info=True)
                continue
    # --- END: Proses NEW items (insert) ---

    # --- START: Proses UPDATED items (update) ---
    # Logika untuk updated_items_data (serupa dengan batch_save_update internal)
    processed_updated_items = []
    if updated_items_data:
        for item_dict in updated_items_data:
            try:
                processed_item_dict = item_dict.copy()
                for k, v in processed_item_dict.items():
                    if isinstance(v, Decimal):
                        processed_item_dict[k] = utils.to_float_or_none(v)
                    elif isinstance(v, datetime):
                        processed_item_dict[k] = utils.to_iso_format_or_none(v)
                processed_updated_items.append(processed_item_dict)
            except Exception as e:
                logger.error(f"Error saat memproses item Market Structure Event yang diperbarui untuk batch: {e}. Item dilewati: {item_dict}", exc_info=True)
                continue
    # --- END: Proses UPDATED items (update) ---

    if new_ms_objects or processed_updated_items: # <--- Periksa kedua list
        _db_write_queue.put({
            'op_type': 'batch_save_update',
            'model_class': MarketStructureEvent,
            'new_items_data': new_ms_objects,
            'updated_items_data': processed_updated_items, # <--- Teruskan updated_items_data
            'unique_columns': ['symbol', 'timeframe', 'event_time_utc', 'event_type', 'direction'],
            'pkey_column_name': 'id'
        })
        logger.debug(f"Market Structure Events batch put into queue: new={len(new_ms_objects)}, updated={len(processed_updated_items)}")
    else:
        logger.debug("Tidak ada Market Structure Events valid yang berhasil disiapkan untuk dikirim ke antrean DB.")




def save_liquidity_zone(symbol, timeframe, zone_type, price_level, formation_time_utc, is_tapped, tap_time_utc=None):
    """
    Menyimpan atau memperbarui zona Likuiditas ke database.
    Memastikan price_level adalah Decimal dan waktu adalah datetime object.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di save_liquidity_zone. Tidak dapat melanjutkan.")
        return

    try:
        # Modifikasi: Gunakan helper to_utc_datetime_or_none
        formation_time_utc_processed = to_utc_datetime_or_none(formation_time_utc)
        if formation_time_utc_processed is None:
            logger.error(f"Gagal mengonversi formation_time_utc '{formation_time_utc}' ke datetime aware UTC untuk Liquidity Zone. Melewatkan penyimpanan.")
            return

        # Modifikasi: Gunakan helper to_utc_datetime_or_none untuk timestamp opsional
        tap_time_utc_processed = to_utc_datetime_or_none(tap_time_utc)

        # Modifikasi: Konversi price_level ke Decimal. Tangani None jika ada.
        price_level_processed = to_decimal_or_none(price_level)
        if price_level_processed is None:
            logger.warning(f"Price level tidak valid untuk Liquidity Zone {zone_type}. Melewatkan penyimpanan.")
            return

        processed_data = {
            "symbol": symbol,
            "timeframe": timeframe,
            "zone_type": zone_type,
            "price_level": price_level_processed,               # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
            "formation_time_utc": formation_time_utc_processed, # <--- KOREKSI: KIRIM OBJEK DATETIME ASLI
            "is_tapped": is_tapped,
            "tap_time_utc": tap_time_utc_processed,             # <--- KOREKSI: KIRIM OBJEK DATETIME ASLI
            "timestamp_recorded": datetime.now(timezone.utc)
        }

        _db_write_queue.put({
            'op_type': 'single_save_update',
            'model_class': LiquidityZone,
            'single_item_data': processed_data,
            'single_item_key_columns': ['symbol', 'timeframe', 'zone_type', 'price_level', 'formation_time_utc']
        })
        logger.debug(f"Liquidity Zone {zone_type} untuk {symbol} {timeframe} @ {price_level} dikirim ke antrean DB.")
    except Exception as e:
        logger.error(f"Gagal menyiapkan Liquidity Zone untuk antrean DB untuk {symbol} {timeframe} {zone_type}: {e}", exc_info=True)

def save_liquidity_zones_batch(new_items_data: list, updated_items_data: list):
    """
    Menyimpan Liquidity Zones baru dan memperbarui yang sudah ada dalam batch.
    Memastikan data diubah ke objek model/format yang benar sebelum dikirim ke antrean.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di save_liquidity_zones_batch. Tidak dapat melanjutkan.")
        return

    # Siapkan data BARU: Konversi list of dictionaries menjadi list of LiquidityZone objects
    new_liq_objects = []
    if new_items_data:
        for item_data in new_items_data:
            try:
                # Modifikasi: Konversi price_level ke Decimal
                price_level_dec = utils.to_decimal_or_none(item_data.get('price_level'))

                # Modifikasi: Konversi waktu ke datetime objects
                formation_time_utc_obj = utils.to_utc_datetime_or_none(item_data.get('formation_time_utc'))
                tap_time_utc_obj = utils.to_utc_datetime_or_none(item_data.get('tap_time_utc'))

                new_liq_objects.append(LiquidityZone(
                    symbol=item_data.get('symbol'),
                    timeframe=item_data.get('timeframe'),
                    zone_type=item_data.get('zone_type'),
                    price_level=price_level_dec,               # <--- KOREKSI: KIRIM OBJEK DECIMAL ASLI
                    formation_time_utc=formation_time_utc_obj, # <--- KOREKSI: KIRIM OBJEK DATETIME ASLI
                    is_tapped=item_data.get('is_tapped'),
                    tap_time_utc=tap_time_utc_obj,             # <--- KOREKSI: KIRIM OBJEK DATETIME ASLI
                    timestamp_recorded=datetime.now(timezone.utc) # Pastikan ini datetime object
                ))
            except KeyError as ke:
                logger.error(f"Kunci data Liquidity Zone baru tidak ditemukan di batch: {ke}. Item dilewati: {item_data}", exc_info=True)
                continue
            except Exception as e:
                logger.error(f"Error saat menyiapkan Liquidity Zone baru untuk penyimpanan batch DB (item dilewati): {e}. Data: {item_data}", exc_info=True)
                continue

    # Siapkan data yang DIPERBARUI: Pastikan dictionary sudah di-JSON-serializable-kan jika perlu
    processed_updated_items = []
    if updated_items_data:
        for item_dict in updated_items_data:
            try:
                processed_item_dict = item_dict.copy()
                # Loop melalui item_dict dan konversi jika tipe data sesuai
                for k, v in processed_item_dict.items():
                    if isinstance(v, Decimal):
                        processed_item_dict[k] = float(v) # Konversi ke float untuk bulk_update_mappings
                    elif isinstance(v, datetime):
                        processed_item_dict[k] = v.isoformat() # Konversi ke ISO string
                processed_updated_items.append(processed_item_dict)
            except Exception as e:
                logger.error(f"Error saat memproses item Liquidity Zone yang diperbarui untuk batch: {e}. Item dilewati: {item_dict}", exc_info=True)
                continue

    if new_liq_objects or processed_updated_items:
        _db_write_queue.put({
            'op_type': 'batch_save_update',
            'model_class': LiquidityZone,
            'new_items_data': new_liq_objects,
            'updated_items_data': processed_updated_items,
            'unique_columns': ['symbol', 'timeframe', 'zone_type', 'price_level', 'formation_time_utc'],
            'pkey_column_name': 'id'
        })
        logger.debug(f"Liquidity Zones batch put into queue: new={len(new_liq_objects)}, updated={len(processed_updated_items)}")
    else:
        logger.debug("Tidak ada Liquidity Zones valid yang berhasil disiapkan untuk dikirim ke antrean DB.")


def delete_feature_backfill_status(symbol_param: str, timeframe_str: str):
    """
    Menghapus status backfill untuk symbol dan timeframe tertentu.
    Ini memaksa backfill untuk memulai dari awal (atau dari custom start date).
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di delete_feature_backfill_status. Tidak dapat melanjutkan.")
        return

    try:
        db = SessionLocal()
        # Hapus status backfill spesifik untuk symbol dan timeframe
        deleted_status_count = db.query(FeatureBackfillStatus).filter(
            FeatureBackfillStatus.symbol == symbol_param,
            FeatureBackfillStatus.timeframe == timeframe_str
        ).delete()
        logger.info(f"Dibersihkan {deleted_status_count} status backfill untuk {symbol_param} {timeframe_str}.")

        # --- KODE KRITIKAL INI: HAPUS JUGA EVENT 'OVERALL TREND' LAMA ---
        # Ini akan menghapus event 'Overall Trend' untuk timeframe spesifik ini
        deleted_overall_trend_events = db.query(MarketStructureEvent).filter(
            MarketStructureEvent.symbol == symbol_param,
            MarketStructureEvent.timeframe == timeframe_str,
            MarketStructureEvent.event_type == 'Overall Trend'
        ).delete()
        db.commit() # Commit setelah semua penghapusan
        logger.info(f"Dibersihkan {deleted_overall_trend_events} event 'Overall Trend' lama untuk {symbol_param} {timeframe_str} dari database.")
        # --- AKHIR KODE KRITIKAL ---

    except Exception as e:
        logger.error(f"Gagal menghapus status backfill atau event tren untuk {symbol_param} {timeframe_str}: {e}", exc_info=True)
        db.rollback()
    finally:
        db.close()





def save_rsi_value(symbol, timeframe, timestamp_utc, value):
    """
    Menyimpan atau memperbarui nilai RSI ke database.
    Memastikan nilai adalah Decimal dan waktu adalah datetime object.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di save_rsi_value. Tidak dapat melanjutkan.")
        return

    try:
        timestamp_utc_processed = None
        if isinstance(timestamp_utc, str):
            try:
                timestamp_utc_processed = datetime.fromisoformat(timestamp_utc).replace(tzinfo=timezone.utc)
            except ValueError:
                logger.error(f"Gagal mengonversi timestamp_utc string '{timestamp_utc}' ke datetime untuk RSI. Melewatkan penyimpanan.")
                return
        elif isinstance(timestamp_utc, datetime):
            timestamp_utc_processed = timestamp_utc.replace(tzinfo=timezone.utc)
        else:
            logger.error(f"Tipe data timestamp_utc tidak dikenal: {type(timestamp_utc)} untuk RSI. Melewatkan penyimpanan.")
            return

        value_processed = Decimal(str(value)) if value is not None else None

        processed_data = {
            "symbol": symbol,
            "timeframe": timeframe,
            "timestamp_utc": _convert_to_json_serializable(timestamp_utc_processed),
            "value": _convert_to_json_serializable(value_processed),
            "timestamp_recorded": _convert_to_json_serializable(datetime.now(timezone.utc))
        }

        _db_write_queue.put({
            'op_type': 'single_save_update',
            'model_class': RSIValue,
            'single_item_data': processed_data,
            'single_item_key_columns': ['symbol', 'timeframe', 'timestamp_utc']
        })
        logger.debug(f"RSI untuk {symbol} {timeframe} @ {timestamp_utc} dikirim ke antrean DB.")
    except Exception as e:
        logger.error(f"Gagal menyiapkan RSI untuk antrean DB untuk {symbol} {timeframe}: {e}", exc_info=True)


def get_rsi_values(symbol=None, timeframe=None, limit=None, start_time_utc=None, end_time_utc=None):
    """
    Mengambil nilai RSI dari database.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di get_rsi_values. Tidak dapat melanjutkan.")
        return []
    db = SessionLocal()
    try:
        query = db.query(RSIValue)
        if symbol:
            query = query.filter(RSIValue.symbol == symbol)
        if timeframe:
            query = query.filter(RSIValue.timeframe == timeframe)
        if start_time_utc:
            query = query.filter(RSIValue.timestamp_utc >= start_time_utc)
        if end_time_utc:
            query = query.filter(RSIValue.timestamp_utc <= end_time_utc)

        query = query.order_by(RSIValue.timestamp_utc.desc())

        if limit:
            query = query.limit(limit)

        results = query.all()

        output = []
        for r in results:
            output.append({
                "id": r.id,
                "symbol": r.symbol,
                "timeframe": r.timeframe,
                "timestamp_utc": r.timestamp_utc,
                "value": r.value
            })
        return output
    except Exception as e:
        logger.error(f"Gagal mengambil nilai RSI dari DB: {e}", exc_info=True)
        return []
    finally:
        db.close()
# --- AKHIR FUNGSI BARU UNTUK RSI ---



def _detect_and_save_macd_historically(symbol_param: str, timeframe_str: str, candles_df: pd.DataFrame) -> int:
    """
    Menghitung MACD secara historis dan menyimpannya ke database.
    Args:
        symbol_param (str): Simbol trading.
        timeframe_str (str): Timeframe.
        candles_df (pd.DataFrame): DataFrame candle historis.
    Returns:
        int: Jumlah nilai MACD yang berhasil dihitung dan dikirim ke antrean DB.
    """
    logger.info(f"Mendeteksi dan menyimpan MACD historis untuk {symbol_param} {timeframe_str}...")
    processed_count = 0

    macd_fast = config.AIAnalysts.MACD_FAST_PERIOD
    macd_slow = config.AIAnalysts.MACD_SLOW_PERIOD
    macd_signal = config.AIAnalysts.MACD_SIGNAL_PERIOD

    # Pastikan candles_df memiliki kolom yang sesuai dan tipe data yang benar untuk _calculate_macd
    # _calculate_macd mengharapkan DataFrame dengan kolom 'close' dalam float.
    # Namun, candles_df yang masuk ke sini adalah DataFrame Decimal.
    # Kita perlu membuat salinan dan mengonversinya ke float untuk _calculate_macd.
    ohlc_df_for_macd_calc_float = candles_df.copy()
    ohlc_df_for_macd_calc_float = ohlc_df_for_macd_calc_float.rename(columns={
        'open_price': 'open', 'high_price': 'high', 'low_price': 'low',
        'close_price': 'close', 'tick_volume': 'volume'
    })
    for col in ['open', 'high', 'low', 'close', 'volume']:
        if col in ohlc_df_for_macd_calc_float.columns:
            # Konversi ke float untuk TA-Lib
            ohlc_df_for_macd_calc_float[col] = ohlc_df_for_macd_calc_float[col].apply(to_float_or_none)


    if ohlc_df_for_macd_calc_float.empty:
        logger.warning(f"Tidak ada data candle untuk {timeframe_str}. Tidak dapat menghitung dan menyimpan MACD.")
        return 0

    macd_results = _calculate_macd(ohlc_df_for_macd_calc_float, macd_fast, macd_slow, macd_signal)

    if macd_results and not macd_results['macd_line'].empty:
        for idx in range(len(macd_results['macd_line'])):
            timestamp = ohlc_df_for_macd_calc_float.index[idx].to_pydatetime()
            macd_val = macd_results['macd_line'].iloc[idx]
            signal_val = macd_results['signal_line'].iloc[idx]
            hist_val = macd_results['histogram'].iloc[idx]
            pcent_val = macd_results['macd_pcent'].iloc[idx] # Ambil nilai macd_pcent

            if pd.notna(macd_val) and pd.notna(signal_val) and pd.notna(hist_val):
                try:
                    database_manager.save_macd_value(
                        symbol=symbol_param,
                        timeframe=timeframe_str,
                        timestamp_utc=timestamp,
                        macd_line=to_decimal_or_none(macd_val),
                        signal_line=to_decimal_or_none(signal_val),
                        histogram=to_decimal_or_none(hist_val),
                        macd_pcent=to_decimal_or_none(pcent_val) # Simpan macd_pcent
                    )
                    processed_count += 1
                except Exception as e:
                    logger.error(f"Gagal menyimpan nilai MACD untuk {timeframe_str} pada {timestamp}: {e}", exc_info=True)
        logger.info(f"Selesai menghitung dan menyimpan MACD untuk {timeframe_str}. Disimpan: {processed_count} titik data.")
    else:
        logger.warning(f"MACD results kosong atau tidak valid untuk {timeframe_str}.")

    return processed_count

def save_fibonacci_levels_batch(new_items_data: list, updated_items_data: list):
    """
    Menyimpan Fibonacci Levels baru dan memperbarui yang sudah ada dalam batch.
    Memastikan data diubah ke objek model/format yang benar sebelum dikirim ke antrean.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di save_fibonacci_levels_batch. Tidak dapat melanjutkan.")
        return

    # Siapkan data BARU: Konversi list of dictionaries menjadi list of FibonacciLevel objects
    new_fib_objects = []
    if new_items_data:
        for item_data in new_items_data:
            try:
                # Konversi harga dan rasio ke Decimal
                high_price_ref_dec = utils.to_decimal_or_none(item_data.get('high_price_ref'))
                low_price_ref_dec = utils.to_decimal_or_none(item_data.get('low_price_ref'))
                ratio_dec = utils.to_decimal_or_none(item_data.get('ratio'))
                price_level_dec = utils.to_decimal_or_none(item_data.get('price_level'))

                # Konversi waktu ke datetime objects
                start_time_ref_utc_obj = utils.to_utc_datetime_or_none(item_data.get('start_time_ref_utc'))
                end_time_ref_utc_obj = utils.to_utc_datetime_or_none(item_data.get('end_time_ref_utc'))

                # Validasi kritis: Pastikan timestamp referensi tidak None, karena ini kolom NOT NULL
                if start_time_ref_utc_obj is None:
                    logger.warning(
                        f"Timestamp 'start_time_ref_utc' kosong atau tidak valid untuk Fibonacci Level baru "
                        f"({item_data.get('symbol')} {item_data.get('timeframe')}). Melewatkan item ini."
                    )
                    continue # Lewati item ini jika timestamp penting hilang

                if end_time_ref_utc_obj is None:
                    logger.warning(
                        f"Timestamp 'end_time_ref_utc' kosong atau tidak valid untuk Fibonacci Level baru "
                        f"({item_data.get('symbol')} {item_data.get('timeframe')}). Melewatkan item ini."
                    )
                    continue # Lewati item ini jika timestamp penting hilang

                # Validasi minimal untuk data harga dan rasio
                if any(v is None for v in [high_price_ref_dec, low_price_ref_dec, ratio_dec, price_level_dec]):
                    logger.warning(
                        f"Data harga/rasio Fibonacci Level tidak lengkap untuk {item_data.get('symbol')} {item_data.get('timeframe')}. Melewatkan item ini."
                    )
                    continue # Lewati item ini jika data harga/rasio penting hilang

                new_fib_objects.append(FibonacciLevel(
                    symbol=item_data['symbol'],
                    timeframe=item_data['timeframe'],
                    type=item_data['type'],
                    high_price_ref=high_price_ref_dec,
                    low_price_ref=low_price_ref_dec,
                    start_time_ref_utc=start_time_ref_utc_obj, # Sudah divalidasi tidak None
                    end_time_ref_utc=end_time_ref_utc_obj,     # Sudah divalidasi tidak None
                    ratio=ratio_dec,
                    price_level=price_level_dec,
                    is_active=item_data.get('is_active', True), # Pastikan ada default jika tidak disediakan
                    confluence_score=item_data.get('confluence_score', 0), # Pastikan ada default
                    current_retracement_percent=utils.to_decimal_or_none(item_data.get('current_retracement_percent')),
                    deepest_retracement_percent=utils.to_decimal_or_none(item_data.get('deepest_retracement_percent')),
                    retracement_direction=item_data.get('retracement_direction'),
                    last_test_time_utc=utils.to_utc_datetime_or_none(item_data.get('last_test_time_utc')),
                    timestamp_recorded=datetime.now(timezone.utc)
                ))
            except KeyError as ke:
                logger.error(f"Kunci data Fibonacci Level baru tidak ditemukan di batch: {ke}. Item dilewati: {item_data}", exc_info=True)
                continue
            except Exception as e:
                logger.error(f"Error saat menyiapkan Fibonacci Level baru untuk penyimpanan batch DB (item dilewati): {e}. Data: {item_data}", exc_info=True)
                continue

    # Siapkan data yang DIPERBARUI: Pastikan dictionary sudah di-JSON-serializable-kan jika perlu
    processed_updated_items = []
    if updated_items_data:
        for item_dict in updated_items_data:
            try:
                processed_item_dict = item_dict.copy()
                for k, v in processed_item_dict.items():
                    if isinstance(v, Decimal):
                        processed_item_dict[k] = float(v)
                    elif isinstance(v, datetime):
                        processed_item_dict[k] = v.isoformat()
                processed_updated_items.append(processed_item_dict)
            except Exception as e:
                logger.error(f"Error saat memproses item Fibonacci Level yang diperbarui untuk batch: {e}. Item dilewati: {item_dict}", exc_info=True)
                continue

    if new_fib_objects or processed_updated_items:
        _db_write_queue.put({
            'op_type': 'batch_save_update',
            'model_class': FibonacciLevel,
            'new_items_data': new_fib_objects,
            'updated_items_data': processed_updated_items,
            # BARIS INI TELAH DIMODIFIKASI UNTUK SAMA PERSIS DENGAN UniqueConstraint DI MODEL
            'unique_columns': ['symbol', 'timeframe', 'type', 'high_price_ref', 'low_price_ref', 'ratio'],
            'pkey_column_name': 'id'
        })
        logger.debug(f"Fibonacci Levels batch put into queue: new={len(new_fib_objects)}, updated={len(processed_updated_items)}")
    else:
        logger.debug("Tidak ada Fibonacci Levels valid yang berhasil disiapkan untuk dikirim ke antrean DB.")


def save_volume_profiles_batch(new_items_data: list):
    """Menyimpan Volume Profiles baru dalam batch.
    Memastikan semua data harga/volume adalah Decimal dan waktu adalah datetime object.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di save_volume_profiles_batch. Tidak dapat melanjutkan.")
        return

    if not new_items_data:
        logger.debug("Tidak ada Volume Profiles dalam batch untuk disimpan.")
        return

    new_vp_objects = []
    for item_data in new_items_data:
        try:
            # Modifikasi: Gunakan helper to_utc_datetime_or_none
            period_start_utc_processed = utils.to_utc_datetime_or_none(item_data.get('period_start_utc'))
            period_end_utc_processed = utils.to_utc_datetime_or_none(item_data.get('period_end_utc'))

            if period_start_utc_processed is None or period_end_utc_processed is None:
                logger.warning(f"Timestamp Volume Profile tidak valid untuk {item_data.get('symbol')} {item_data.get('timeframe')}. Melewatkan item ini.")
                continue

            # Modifikasi: Konversi ke Decimal, menangani None
            poc_price_dec = utils.to_decimal_or_none(item_data.get('poc_price'))
            vah_price_dec = utils.to_decimal_or_none(item_data.get('vah_price'))
            val_price_dec = utils.to_decimal_or_none(item_data.get('val_price'))
            total_volume_dec = utils.to_decimal_or_none(item_data.get('total_volume'))
            row_height_value_dec = utils.to_decimal_or_none(item_data.get('row_height_value')) 
            
            # Modifikasi: Periksa apakah ada nilai penting yang tidak valid
            if any(v is None for v in [poc_price_dec, vah_price_dec, val_price_dec, total_volume_dec]):
                logger.warning(f"Beberapa nilai harga/volume Volume Profile tidak valid untuk {item_data.get('symbol')} {item_data.get('timeframe')}. Melewatkan item ini.")
                continue

            new_vp_objects.append(VolumeProfile(
                symbol=item_data['symbol'],
                timeframe=item_data['timeframe'],
                period_start_utc=period_start_utc_processed,
                period_end_utc=period_end_utc_processed,
                poc_price=poc_price_dec,
                vah_price=vah_price_dec,
                val_price=val_price_dec,
                total_volume=total_volume_dec,
                profile_data_json=item_data.get('profile_data_json'),
                row_height_value=row_height_value_dec,
                timestamp_recorded=datetime.now(timezone.utc)
            ))
        except KeyError as ke:
            logger.error(f"Kunci data Volume Profile baru tidak ditemukan di batch: {ke}. Item dilewati: {item_data}", exc_info=True)
            continue
        except Exception as e:
            logger.error(f"Error saat menyiapkan Volume Profile baru untuk penyimpanan batch DB (item dilewati): {e}. Data: {item_data}", exc_info=True)
            continue

    if new_vp_objects:
        _db_write_queue.put({
            'op_type': 'batch_save_update',
            'model_class': VolumeProfile,
            'new_items_data': new_vp_objects,
            'updated_items_data': [],
            'unique_columns': ['symbol', 'timeframe', 'period_start_utc'],
            'pkey_column_name': 'id'
        })
        logger.debug(f"Volume Profiles batch put into queue: new={len(new_vp_objects)}")
    else:
        logger.debug("Tidak ada Volume Profiles valid yang berhasil disiapkan untuk dikirim ke antrean DB.")
def save_llm_proposal(
    symbol: str,
    timeframe: str,
    recommendation_action: str,
    entry_price: Decimal | None,
    stop_loss: Decimal | None,
    take_profit: Decimal | None,
    confidence_score: int,
    summary: str,
    reasoning: str,
    proposal_timestamp: datetime,
    status: str = "proposed",
    external_id: str | None = None # Untuk ID eksternal jika ada (misal dari LLM API)
):
    """
    Menyimpan proposal LLM ke database.
    """
    if _db_write_queue is None:
        logger.error("Database writer queue belum diinisialisasi. Tidak dapat menyimpan proposal LLM.")
        return

    # Pastikan data dalam format yang bisa diserialkan JSON jika disimpan sebagai TEXT/JSON
    try:
        proposal_data = {
            "symbol": symbol,
            "timeframe": timeframe,
            "recommendation_action": recommendation_action,
            "entry_price": float(entry_price) if entry_price is not None else None,
            "stop_loss": float(stop_loss) if stop_loss is not None else None,
            "take_profit": float(take_profit) if take_profit is not None else None,
            "confidence_score": confidence_score,
            "summary": summary,
            "reasoning": reasoning,
            "proposal_timestamp": proposal_timestamp.isoformat(),
            "status": status,
            "external_id": external_id
        }
        # Gunakan parameter binding atau f-string yang aman untuk SQL jika tidak menggunakan ORM penuh
        # Contoh SQL (sesuaikan dengan tabel dan kolom Anda)
        query = text("""
            INSERT INTO llm_proposals (
                symbol, timeframe, recommendation_action, entry_price, stop_loss, take_profit,
                confidence_score, summary, reasoning, proposal_timestamp, status, external_id
            ) VALUES (
                :symbol, :timeframe, :recommendation_action, :entry_price, :stop_loss, :take_profit,
                :confidence_score, :summary, :reasoning, :proposal_timestamp, :status, :external_id
            );
        """)
        
        # Tambahkan ke antrean penulisan database
        _db_write_queue.put_nowait({
            'query': query,
            'params': proposal_data,
            'log_msg': f"Menyimpan proposal LLM untuk {symbol} {timeframe} (Aksi: {recommendation_action})"
        })
        logger.debug(f"Proposal LLM untuk {symbol} {timeframe} ditambahkan ke antrean penulisan database.")
    except Exception as e:
        logger.error(f"Gagal menambahkan proposal LLM ke antrean penulisan database: {e}", exc_info=True)



def save_llm_performance_review(review_data: dict):
    """
    Menyimpan hasil review kinerja LLM ke database.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di save_llm_performance_review. Tidak dapat melanjutkan.")
        return

    try:
        processed_data = {
            "review_time_utc": to_utc_datetime_or_none(review_data.get('review_time_utc', datetime.now(timezone.utc))),
            "review_period_start_utc": to_utc_datetime_or_none(review_data.get('review_period_start_utc')),
            "review_period_end_utc": to_utc_datetime_or_none(review_data.get('review_period_end_utc')),
            "symbol": review_data.get('symbol'),
            "total_proposals_reviewed": review_data.get('total_proposals_reviewed', 0),
            "profitable_proposals": review_data.get('profitable_proposals', 0),
            "losing_proposals": review_data.get('losing_proposals', 0),
            "drawdown_proposals": review_data.get('drawdown_proposals', 0),
            "accuracy_score_proposals": to_decimal_or_none(review_data.get('accuracy_score_proposals')),
            "llm_diagnosis_summary": review_data.get('llm_diagnosis_summary'),
            "llm_identified_strengths": review_data.get('llm_identified_strengths'),
            "llm_identified_weaknesses": review_data.get('llm_identified_weaknesses'),
            "llm_improvement_suggestions": review_data.get('llm_improvement_suggestions'),
            "raw_llm_response_json": json.dumps(review_data.get('raw_llm_response_json')) if review_data.get('raw_llm_response_json') else None,
            "timestamp_recorded": datetime.now(timezone.utc)
        }

        # Validasi minimal untuk kolom NOT NULL
        if any(v is None for v in [processed_data['review_period_start_utc'], processed_data['review_period_end_utc'], processed_data['symbol']]):
            logger.error(f"Data review kinerja LLM tidak lengkap (start/end/symbol kosong). Melewatkan penyimpanan: {review_data}")
            return

        _db_write_queue.put({
            'op_type': 'single_save_update',
            'model_class': LLMPerformanceReview,
            'single_item_data': processed_data,
            'single_item_key_columns': ['symbol', 'review_period_start_utc', 'review_period_end_utc']
        })
        logger.debug(f"LLM Performance Review untuk {processed_data['symbol']} periode {processed_data['review_period_start_utc']}-{processed_data['review_period_end_utc']} dikirim ke antrean DB.")
    except Exception as e:
        logger.error(f"Gagal menyiapkan LLM Performance Review untuk antrean DB: {e}", exc_info=True)

def get_llm_proposals_for_review(
    symbol: str,
    timeframe: str | None = None,
    start_time_utc: datetime | None = None,
    end_time_utc: datetime | None = None,
    limit: int = 100
) -> list:
    """
    Mengambil proposal LLM dari database untuk keperluan review atau diagnosa mandiri.
    """
    if _Session is None:
        logger.error("Sesi database belum diinisialisasi. Tidak dapat mengambil proposal LLM.")
        return []

    session = _Session()
    try:
        query = text("""
            SELECT
                id, symbol, timeframe, recommendation_action, entry_price, stop_loss, take_profit,
                confidence_score, summary, reasoning, proposal_timestamp, status, external_id
            FROM llm_proposals
            WHERE symbol = :symbol
        """)
        params = {"symbol": symbol}

        if timeframe:
            query = query.text + " AND timeframe = :timeframe"
            params["timeframe"] = timeframe
        if start_time_utc:
            query = query.text + " AND proposal_timestamp >= :start_time_utc"
            params["start_time_utc"] = start_time_utc
        if end_time_utc:
            query = query.text + " AND proposal_timestamp <= :end_time_utc"
            params["end_time_utc"] = end_time_utc

        query = query.text + " ORDER BY proposal_timestamp DESC LIMIT :limit"
        params["limit"] = limit

        result = session.execute(query, params).fetchall()
        proposals = []
        for row in result:
            proposals.append({
                "id": row.id,
                "symbol": row.symbol,
                "timeframe": row.timeframe,
                "recommendation_action": row.recommendation_action,
                "entry_price": Decimal(str(row.entry_price)) if row.entry_price is not None else None,
                "stop_loss": Decimal(str(row.stop_loss)) if row.stop_loss is not None else None,
                "take_profit": Decimal(str(row.take_profit)) if row.take_profit is not None else None,
                "confidence_score": row.confidence_score,
                "summary": row.summary,
                "reasoning": row.reasoning,
                "proposal_timestamp": row.proposal_timestamp,
                "status": row.status,
                "external_id": row.external_id
            })
        logger.debug(f"Mengambil {len(proposals)} proposal LLM untuk review.")
        return proposals
    except exc.SQLAlchemyError as e:
        logger.error(f"Kesalahan database saat mengambil proposal LLM untuk review: {e}", exc_info=True)
        return []
    except Exception as e:
        logger.error(f"Kesalahan tak terduga saat mengambil proposal LLM untuk review: {e}", exc_info=True)
        return []
    finally:
        session.close()

def get_llm_performance_reviews(symbol=None, start_time_utc=None, end_time_utc=None, limit=None):
    """
    Mengambil review kinerja LLM dari database.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di get_llm_performance_reviews. Tidak dapat melanjutkan.")
        return []
    db = SessionLocal()
    try:
        query = db.query(LLMPerformanceReview)
        if symbol:
            query = query.filter(LLMPerformanceReview.symbol == symbol)
        if start_time_utc:
            start_time_utc_processed = to_utc_datetime_or_none(start_time_utc)
            if start_time_utc_processed:
                query = query.filter(LLMPerformanceReview.review_time_utc >= start_time_utc_processed)
        if end_time_utc:
            end_time_utc_processed = to_utc_datetime_or_none(end_time_utc)
            if end_time_utc_processed:
                query = query.filter(LLMPerformanceReview.review_time_utc <= end_time_utc_processed)

        query = query.order_by(LLMPerformanceReview.review_time_utc.desc())
        if limit:
            query = query.limit(limit)

        results = query.all()
        output = []
        for r in results:
            output.append({
                "id": r.id,
                "review_time_utc": r.review_time_utc,
                "review_period_start_utc": r.review_period_start_utc,
                "review_period_end_utc": r.review_period_end_utc,
                "symbol": r.symbol,
                "total_proposals_reviewed": r.total_proposals_reviewed,
                "profitable_proposals": r.profitable_proposals,
                "losing_proposals": r.losing_proposals,
                "drawdown_proposals": r.drawdown_proposals,
                "accuracy_score_proposals": r.accuracy_score_proposals,
                "llm_diagnosis_summary": r.llm_diagnosis_summary,
                "llm_identified_strengths": r.llm_identified_strengths,
                "llm_identified_weaknesses": r.llm_identified_weaknesses,
                "llm_improvement_suggestions": r.llm_improvement_suggestions,
                "raw_llm_response_json": json.loads(r.raw_llm_response_json) if r.raw_llm_response_json else None,
                "timestamp_recorded": r.timestamp_recorded
            })
        return output
    except Exception as e:
        logger.error(f"Gagal mengambil review kinerja LLM dari DB: {e}", exc_info=True)
        return []
    finally:
        if db:
            db.close()

def get_economic_events_for_llm_prompt(
    days_past: int = 0,
    days_future: int = 0,
    target_currency: str = None,
    limit: int = None,
    end_time_utc: datetime = None
) -> list:
    """
    Mengambil event ekonomi terbaru dari database khusus untuk prompt LLM.
    Memastikan hanya event ekonomi yang diambil dan jumlahnya dibatasi oleh 'limit'.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di get_economic_events_for_llm_prompt. Tidak dapat melanjutkan.")
        return []
    db = SessionLocal()
    try:
        now_utc = datetime.now(timezone.utc)
        effective_end_time_utc = end_time_utc if end_time_utc else now_utc
        effective_start_time_utc = effective_end_time_utc - timedelta(days=days_past)
        if days_future > 0: # Jika ada days_future, rentang start_time bisa lebih jauh ke masa lalu
            effective_start_time_utc = effective_end_time_utc - timedelta(days=days_past + days_future) # Sesuaikan rentang awal

        query = db.query(EconomicEvent).filter(
            EconomicEvent.event_time_utc >= effective_start_time_utc,
            EconomicEvent.event_time_utc <= effective_end_time_utc
        )
        if target_currency:
            query = query.filter(EconomicEvent.currency == target_currency)
        
        query = query.order_by(EconomicEvent.event_time_utc.desc()) # Urutkan dari terbaru
        if limit is not None:
            query = query.limit(limit) # Terapkan limit di sini

        results = query.all()
        output = []
        for event in results:
            output.append({
                "type": "economic_calendar", # Tambahkan tipe untuk konsistensi
                "event_id": event.event_id,
                "name": event.name,
                "country": event.country,
                "currency": event.currency,
                "impact": event.impact,
                "event_time_utc": event.event_time_utc,
                "actual_value": event.actual_value,
                "forecast_value": event.forecast_value,
                "previous_value": event.previous_value
            })
        logger.info(f"Mengambil {len(output)} event ekonomi dari DB untuk LLM prompt.")
        return output
    except Exception as e:
        logger.error(f"Gagal mengambil event ekonomi untuk LLM prompt dari DB: {e}", exc_info=True)
        return []
    finally:
        if db:
            db.close()

def get_news_articles_for_llm_prompt(
    days_past: int = 0,
    limit: int = None,
    end_time_utc: datetime = None
) -> list:
    """
    Mengambil artikel berita terbaru dari database khusus untuk prompt LLM.
    Memastikan hanya artikel berita yang diambil dan jumlahnya dibatasi oleh 'limit'.
    """
    global SessionLocal
    if SessionLocal is None:
        logger.error("SessionLocal belum diinisialisasi di get_news_articles_for_llm_prompt. Tidak dapat melanjutkan.")
        return []
    db = SessionLocal()
    try:
        now_utc = datetime.now(timezone.utc)
        effective_end_time_utc = end_time_utc if end_time_utc else now_utc
        effective_start_time_utc = effective_end_time_utc - timedelta(days=days_past)

        query = db.query(NewsArticle).filter(
            NewsArticle.published_time_utc >= effective_start_time_utc,
            NewsArticle.published_time_utc <= effective_end_time_utc
        )
        
        query = query.order_by(NewsArticle.published_time_utc.desc()) # Urutkan dari terbaru
        if limit is not None:
            query = query.limit(limit) # Terapkan limit di sini

        results = query.all()
        output = []
        for article in results:
            output.append({
                "type": "news_article", # Tambahkan tipe untuk konsistensi
                "id": article.id,
                "source": article.source,
                "title": article.title,
                "summary": article.summary,
                "url": article.url,
                "published_time_utc": article.published_time_utc,
                "relevance_score": article.relevance_score,
                "thumbnail_url": article.thumbnail_url
            })
        logger.info(f"Mengambil {len(output)} artikel berita dari DB untuk LLM prompt.")
        return output
    except Exception as e:
        logger.error(f"Gagal mengambil artikel berita untuk LLM prompt dari DB: {e}", exc_info=True)
        return []
    finally:
        if db:
            db.close()
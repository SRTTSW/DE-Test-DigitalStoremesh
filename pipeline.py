"""
pipeline.py
กระบวนการ Prefect ETL สำหรับการย้ายข้อมูลของ ShopData Inc.

ทำหน้าที่ดึงข้อมูลลูกค้า / คำสั่งซื้อ / อัตราแลกเปลี่ยน จากตาราง View ในฐานข้อมูลเดิม shopdata.db (SQLite), 
ทำความสะอาดข้อมูลตามกฎธุรกิจที่กำหนดไว้ใน transforms.py และนำผลลัพธ์ไปบันทึกลงฐานข้อมูลวิเคราะห์ตัวใหม่ analytics.db (SQLite) 
โดยแยกเป็น 2 ตารางคือ dim_customers และ fct_orders

"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

import pandas as pd
from prefect import flow, task
from prefect.logging import get_run_logger

import transforms as t

SOURCE_DB = "shopdata.db"
TARGET_DB = "analytics.db"


# ---------------------------------------------------------------------
# EXTRACT
# ---------------------------------------------------------------------
@task(name="extract-customers", retries=2, retry_delay_seconds=5)
def extract_customers(db_path: str) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info("Extracting customers from %s", db_path)
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql("SELECT * FROM vw_raw_customers", conn)
    logger.info("Extracted %d raw customer rows", len(df))
    return df


@task(name="extract-orders", retries=2, retry_delay_seconds=5)
def extract_orders(db_path: str) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info("Extracting orders from %s", db_path)
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql("SELECT * FROM vw_raw_orders", conn)
    logger.info("Extracted %d raw order rows", len(df))
    return df


@task(name="extract-exchange-rates", retries=2, retry_delay_seconds=5)
def extract_exchange_rates(db_path: str) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info("Extracting exchange rates from %s", db_path)
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql("SELECT * FROM vw_exchange_rates", conn)
    logger.info("Extracted %d exchange-rate rows", len(df))
    return df


# ---------------------------------------------------------------------
# TRANSFORM
# ---------------------------------------------------------------------
@task(name="clean-customers")
def clean_customers_task(customers_df: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    try:
        cleaned = t.clean_customers(customers_df)
        logger.info(
            "Cleaned customers: %d raw -> %d deduplicated rows",
            len(customers_df),
            len(cleaned),
        )
        return cleaned
    except Exception:
        logger.exception("Failed to clean customer data")
        raise


@task(name="clean-orders")
def clean_orders_task(
    orders_df: pd.DataFrame, rates_df: pd.DataFrame
) -> pd.DataFrame:
    logger = get_run_logger()
    try:
        cleaned = t.clean_orders(orders_df, rates_df)
        logger.info(
            "Cleaned orders: %d raw -> %d valid rows after filtering system errors",
            len(orders_df),
            len(cleaned),
        )
        return cleaned
    except Exception:
        logger.exception("Failed to clean order data")
        raise


# ---------------------------------------------------------------------
# LOAD
# ---------------------------------------------------------------------
@task(name="load-to-analytics-db")
def load_to_db(
    customers_df: pd.DataFrame,
    orders_df: pd.DataFrame,
    db_path: str = TARGET_DB,
) -> None:
    logger = get_run_logger()
    try:
        with sqlite3.connect(db_path) as conn:
            customers_df.to_sql("dim_customers", conn, if_exists="replace", index=False)
            orders_df.to_sql("fct_orders", conn, if_exists="replace", index=False)
        logger.info(
            "Loaded %d customers into dim_customers and %d orders into fct_orders (%s)",
            len(customers_df),
            len(orders_df),
            db_path,
        )
    except Exception:
        logger.exception(
            "Failed to write to %s -- falling back to local CSV files", db_path
        )
        customers_df.to_csv("clean_customers.csv", index=False)
        orders_df.to_csv("clean_orders.csv", index=False)
        logger.info("Wrote fallback CSV files: clean_customers.csv, clean_orders.csv")


# ---------------------------------------------------------------------
# FLOW
# ---------------------------------------------------------------------
@flow(name="shopdata-etl-pipeline", log_prints=True)
def etl_flow(source_db: str = SOURCE_DB, target_db: str = TARGET_DB) -> None:
    """
    End-to-end ETL flow:
      Extract  -> raw customers, orders, exchange rates from shopdata.db
      Transform-> dedupe/clean customers, filter+convert orders to USD
      Load     -> dim_customers, fct_orders tables in analytics.db
    """
    if not Path(source_db).exists():
        raise FileNotFoundError(f"Source database not found: {source_db}")

    raw_customers = extract_customers(source_db)
    raw_orders = extract_orders(source_db)
    rates = extract_exchange_rates(source_db)

    clean_customers = clean_customers_task(raw_customers)
    clean_orders = clean_orders_task(raw_orders, rates)

    load_to_db(clean_customers, clean_orders, target_db)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    etl_flow()

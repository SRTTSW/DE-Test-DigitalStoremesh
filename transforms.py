"""
transforms.py
=============
ฟังก์ชันสำหรับการแปลงข้อมูล (Data Transformation) ใน ShopData ETL

ไม่มี Prefect decorator และไม่มีการเชื่อมต่อฐานข้อมูล โดยจะรับและส่งคืนข้อมูลแบบ pandas DataFrame (หรือค่า Scalar) เท่านั้น

นำ DataFrame จำลองเข้าไปทดสอบได้ทันทีโดยไม่ต้องเชื่อมต่อฐานข้อมูล SQLite 

"""

from __future__ import annotations

import re
import pandas as pd


DEFAULT_EMAIL_PLACEHOLDER = "unknown@domain.com"


# ---------------------------------------------------------------------
# Customers
# ---------------------------------------------------------------------
def standardize_phone(phone) -> str | None:
    """
    ทำให้phone number เหลือแค่ตัวเลข 

    Examples
    --------
    "+1 (555) 123-4567" -> "15551234567"
    "555-987-6543"       -> "5559876543"
    "1-800-555-DINO"     -> "1800555"      (letters are simply dropped)
    "Ext 444"            -> "444"
    None / NaN / ""      -> None
    """
    if phone is None:
        return None
    if isinstance(phone, float) and pd.isna(phone):
        return None
    phone_str = str(phone).strip()
    if not phone_str or phone_str.lower() == "nan":
        return None

    digits = re.sub(r"\D", "", phone_str)
    return digits if digits else None


def fill_missing_email(email, placeholder: str = DEFAULT_EMAIL_PLACEHOLDER) -> str:
    """Replace a missing/blank email with a placeholder value."""
    if email is None:
        return placeholder
    if isinstance(email, float) and pd.isna(email):
        return placeholder
    email_str = str(email).strip()
    if not email_str or email_str.lower() == "nan":
        return placeholder
    return email_str


def deduplicate_customers(df: pd.DataFrame) -> pd.DataFrame:
    """
    ฟังก์ชันลบรายชื่อลูกค้าที่ซ้ำกัน โดยเลือกเก็บเฉพาะแถวที่มีวันที่สมัครล่าสุดไว้
    """
    if df.empty:
        return df.copy()

    working = df.copy()
    working["_signup_date_parsed"] = pd.to_datetime(
        working["signup_date"], errors="coerce"
    )
    working = working.sort_values(
        by=["customer_id", "_signup_date_parsed"],
        ascending=[True, True],
    )
    # keep='last' -> the row with the max parsed signup_date wins
    deduped = working.drop_duplicates(subset="customer_id", keep="last")
    deduped = deduped.drop(columns=["_signup_date_parsed"]).reset_index(drop=True)
    return deduped


def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = deduplicate_customers(df)
    cleaned["phone"] = cleaned["phone"].apply(standardize_phone)
    cleaned["email"] = cleaned["email"].apply(fill_missing_email)
    return cleaned.reset_index(drop=True)


# ---------------------------------------------------------------------
# Orders
# ---------------------------------------------------------------------
def filter_invalid_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove orders that are system errors: total_amount <= 0.
    """
    if df.empty:
        return df.copy()
    return df[df["total_amount"] > 0].reset_index(drop=True)


def convert_orders_to_usd(
    orders_df: pd.DataFrame, rates_df: pd.DataFrame
) -> pd.DataFrame:
    """
    เพิ่มคอลัมน์ usd_amount (ยอดเงินสกุล USD) เข้าไปในตารางคำสั่งซื้อ (orders_df) 
    โดยแปลงมูลค่าจากคอลัมน์ total_amount ด้วยอัตราแลกเปลี่ยนรายวันจากตาราง rates_df ที่ตรงกับ วันที่สั่งซื้อ (order_date) และ สกุลเงิน (currency)

    Business rule: หากเจอ 2 เคสนี้ ให้ถือว่ายอดเงินนั้นเป็นสกุลเงิน USD อยู่แล้ว ให้ปล่อยผ่านได้เลบ
    1. สกุลเงินไม่มีข้อมูล / เป็นค่าว่าง (missing/blank)
    2. ไม่พบข้อมูลอัตราแลกเปลี่ยนของสกุลเงินนั้นในตาราง ณ วันที่สั่งซื้อสินค้าพอดี (no exchange rate available)
    """
    if orders_df.empty:
        result = orders_df.copy()
        result["usd_amount"] = pd.Series(dtype="float64")
        return result

    orders = orders_df.copy()
    orders["_currency_norm"] = orders["currency"].apply(
        lambda c: None if (c is None or (isinstance(c, float) and pd.isna(c)) or str(c).strip() == "")
        else str(c).strip().upper()
    )

    if rates_df.empty:
        # No exchange rates available at all -> everything assumed already USD
        orders["usd_amount"] = orders["total_amount"]
        return orders.drop(columns=["_currency_norm"]).reset_index(drop=True)

    rates = rates_df.copy()
    rates["currency"] = rates["currency"].astype(str).str.strip().str.upper()
    rates = rates.rename(columns={"date": "_rate_date", "currency": "_rate_currency"})

    merged = orders.merge(
        rates,
        how="left",
        left_on=["order_date", "_currency_norm"],
        right_on=["_rate_date", "_rate_currency"],
    )

    def _compute_usd(row):
        currency = row["_currency_norm"]
        rate = row.get("rate_to_usd")
        if currency is None or currency == "USD":
            return row["total_amount"]
        if pd.isna(rate):
            # No matching exchange rate found for this currency/date -> assume USD
            return row["total_amount"]
        return round(row["total_amount"] * rate, 2)

    merged["usd_amount"] = merged.apply(_compute_usd, axis=1)

    # Keep only the original order columns + usd_amount
    keep_cols = list(orders_df.columns) + ["usd_amount"]
    return merged[keep_cols].reset_index(drop=True)


def clean_orders(orders_df: pd.DataFrame, rates_df: pd.DataFrame) -> pd.DataFrame:
    """
    Full order cleaning pipeline:
      1. Filter out orders with total_amount <= 0 (system errors).
      2. Convert every order amount to USD.
    """
    filtered = filter_invalid_orders(orders_df)
    converted = convert_orders_to_usd(filtered, rates_df)
    return converted

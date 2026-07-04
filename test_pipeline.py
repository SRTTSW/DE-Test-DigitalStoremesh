"""
test_pipeline.py
=================
Unit tests for the pure transformation logic in transforms.py.

These tests use dummy pandas DataFrames / scalar values only -- no
live database connection is required, per the assignment's Part 3
requirement to test the business logic in isolation.

Run with either:
    pytest tests/
    python -m unittest discover -s tests
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

# Make the project root importable when running `pytest tests/`
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import transforms as t


# ---------------------------------------------------------------------
# standardize_phone
# ---------------------------------------------------------------------
class TestStandardizePhone:
    def test_strips_symbols_and_spaces(self):
        assert t.standardize_phone("+1 (555) 123-4567") == "15551234567"

    def test_strips_dashes(self):
        assert t.standardize_phone("555-987-6543") == "5559876543"

    def test_already_digits_only(self):
        assert t.standardize_phone("1234567890") == "1234567890"

    def test_drops_embedded_letters(self):
        # letters are simply removed, per the assignment's cleaning rule
        assert t.standardize_phone("1-800-555-DINO") == "1800555"

    def test_extension_prefix_text(self):
        assert t.standardize_phone("Ext 444") == "444"

    def test_none_returns_none(self):
        assert t.standardize_phone(None) is None

    def test_nan_returns_none(self):
        assert t.standardize_phone(float("nan")) is None

    def test_blank_string_returns_none(self):
        assert t.standardize_phone("   ") is None

    def test_international_format(self):
        assert t.standardize_phone("+44 20 7123 1234") == "442071231234"


# ---------------------------------------------------------------------
# fill_missing_email
# ---------------------------------------------------------------------
class TestFillMissingEmail:
    def test_none_gets_placeholder(self):
        assert t.fill_missing_email(None) == "unknown@domain.com"

    def test_nan_gets_placeholder(self):
        assert t.fill_missing_email(float("nan")) == "unknown@domain.com"

    def test_blank_gets_placeholder(self):
        assert t.fill_missing_email("   ") == "unknown@domain.com"

    def test_valid_email_untouched(self):
        assert t.fill_missing_email("alice@example.com") == "alice@example.com"

    def test_custom_placeholder(self):
        assert t.fill_missing_email(None, placeholder="na@shop.com") == "na@shop.com"


# ---------------------------------------------------------------------
# deduplicate_customers / clean_customers
# ---------------------------------------------------------------------
class TestDeduplicateCustomers:
    def test_keeps_most_recent_signup_date(self):
        df = pd.DataFrame(
            {
                "customer_id": [1, 1],
                "full_name": ["Alice Smith", "Alice Smith"],
                "email": ["alice@example.com", "alice.smith@example.com"],
                "phone": ["+1 (555) 123-4567", "15551234567"],
                "signup_date": ["2023-01-15", "2023-06-01"],
            }
        )
        result = t.deduplicate_customers(df)
        assert len(result) == 1
        assert result.iloc[0]["signup_date"] == "2023-06-01"
        assert result.iloc[0]["email"] == "alice.smith@example.com"

    def test_no_duplicates_unchanged(self):
        df = pd.DataFrame(
            {
                "customer_id": [1, 2],
                "full_name": ["Alice", "Bob"],
                "email": ["a@example.com", "b@example.com"],
                "phone": ["111", "222"],
                "signup_date": ["2023-01-01", "2023-02-01"],
            }
        )
        result = t.deduplicate_customers(df)
        assert len(result) == 2

    def test_empty_dataframe(self):
        df = pd.DataFrame(
            columns=["customer_id", "full_name", "email", "phone", "signup_date"]
        )
        result = t.deduplicate_customers(df)
        assert result.empty


class TestCleanCustomers:
    def test_end_to_end_cleaning(self):
        df = pd.DataFrame(
            {
                "customer_id": [1, 1, 2],
                "full_name": ["Alice Smith", "Alice Smith", "Bob Jones"],
                "email": ["alice@example.com", None, None],
                "phone": ["+1 (555) 123-4567", "15551234567", "555-987-6543"],
                "signup_date": ["2023-01-15", "2023-06-01", "2023-02-20"],
            }
        )
        result = t.clean_customers(df)
        # Deduplicated down to 2 unique customers
        assert len(result) == 2
        alice = result[result["customer_id"] == 1].iloc[0]
        assert alice["phone"] == "15551234567"
        # Alice's most-recent row had a NULL email -> filled with placeholder
        assert alice["email"] == "unknown@domain.com"
        bob = result[result["customer_id"] == 2].iloc[0]
        assert bob["phone"] == "5559876543"


# ---------------------------------------------------------------------
# filter_invalid_orders
# ---------------------------------------------------------------------
class TestFilterInvalidOrders:
    def test_removes_non_positive_amounts(self):
        df = pd.DataFrame(
            {
                "order_id": [1, 2, 3, 4],
                "total_amount": [100.0, -50.0, 0.0, 25.5],
                "currency": ["USD", "USD", "USD", "USD"],
                "order_date": ["2023-05-01"] * 4,
            }
        )
        result = t.filter_invalid_orders(df)
        assert len(result) == 2
        assert set(result["order_id"]) == {1, 4}

    def test_empty_dataframe(self):
        df = pd.DataFrame(columns=["order_id", "total_amount", "currency", "order_date"])
        result = t.filter_invalid_orders(df)
        assert result.empty


# ---------------------------------------------------------------------
# convert_orders_to_usd / clean_orders
# ---------------------------------------------------------------------
class TestConvertOrdersToUsd:
    @pytest.fixture
    def rates(self):
        return pd.DataFrame(
            {
                "currency": ["EUR", "GBP", "JPY"],
                "rate_to_usd": [1.10, 1.25, 0.0070],
                "date": ["2023-05-01", "2023-05-01", "2023-05-01"],
            }
        )

    def test_usd_order_unchanged(self, rates):
        orders = pd.DataFrame(
            {
                "order_id": [1],
                "order_date": ["2023-05-01"],
                "total_amount": [150.0],
                "currency": ["USD"],
            }
        )
        result = t.convert_orders_to_usd(orders, rates)
        assert result.iloc[0]["usd_amount"] == 150.0

    def test_eur_order_converted(self, rates):
        orders = pd.DataFrame(
            {
                "order_id": [2],
                "order_date": ["2023-05-01"],
                "total_amount": [200.0],
                "currency": ["EUR"],
            }
        )
        result = t.convert_orders_to_usd(orders, rates)
        assert result.iloc[0]["usd_amount"] == pytest.approx(220.0)

    def test_jpy_order_converted(self, rates):
        orders = pd.DataFrame(
            {
                "order_id": [3],
                "order_date": ["2023-05-01"],
                "total_amount": [10000.0],
                "currency": ["JPY"],
            }
        )
        result = t.convert_orders_to_usd(orders, rates)
        assert result.iloc[0]["usd_amount"] == pytest.approx(69.0)

    def test_missing_currency_assumed_usd(self, rates):
        orders = pd.DataFrame(
            {
                "order_id": [4],
                "order_date": ["2023-05-01"],
                "total_amount": [120.0],
                "currency": [None],
            }
        )
        result = t.convert_orders_to_usd(orders, rates)
        assert result.iloc[0]["usd_amount"] == 120.0

    def test_currency_with_no_rate_for_date_assumed_usd(self, rates):
        # GBP order on a date with no matching rate row
        orders = pd.DataFrame(
            {
                "order_id": [5],
                "order_date": ["2023-05-08"],
                "total_amount": [1200.0],
                "currency": ["GBP"],
            }
        )
        result = t.convert_orders_to_usd(orders, rates)
        assert result.iloc[0]["usd_amount"] == 1200.0

    def test_empty_rates_table_assumes_usd(self):
        orders = pd.DataFrame(
            {
                "order_id": [6],
                "order_date": ["2023-05-01"],
                "total_amount": [75.0],
                "currency": ["EUR"],
            }
        )
        empty_rates = pd.DataFrame(columns=["currency", "rate_to_usd", "date"])
        result = t.convert_orders_to_usd(orders, empty_rates)
        assert result.iloc[0]["usd_amount"] == 75.0


class TestCleanOrders:
    def test_end_to_end_filters_and_converts(self):
        orders = pd.DataFrame(
            {
                "order_id": [1, 2, 3],
                "order_date": ["2023-05-01", "2023-05-01", "2023-05-01"],
                "total_amount": [150.0, -50.0, 200.0],
                "currency": ["USD", "USD", "EUR"],
            }
        )
        rates = pd.DataFrame(
            {"currency": ["EUR"], "rate_to_usd": [1.10], "date": ["2023-05-01"]}
        )
        result = t.clean_orders(orders, rates)
        # order_id 2 (negative amount) should be filtered out
        assert len(result) == 2
        assert set(result["order_id"]) == {1, 3}
        eur_row = result[result["order_id"] == 3].iloc[0]
        assert eur_row["usd_amount"] == pytest.approx(220.0)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))

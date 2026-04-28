from pathlib import Path

import duckdb
import polars as pl

SQL_PATH = Path(__file__).parent.parent.parent / "sql"
DB_PATH = Path(__file__).parent.parent.parent / "data"


def data_path(filename: str) -> Path:
    """Sets the database file to read and out-puts it as a path"""
    data_set = DB_PATH / filename
    if not data_set.exists():
        raise FileNotFoundError(f"Data set not found {data_set}")
    return Path(__file__).parent.parent.parent / "data" / filename


def read_sql(filename: str) -> str:
    """Read a .sql file from SQL_DIR and return its contents as a string."""
    sql_path = SQL_PATH / filename
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")
    return sql_path.read_text(encoding="utf-8")


def get_payment_information(
    conn: duckdb.DuckDBPyConnection,
    payment_installment: int,
) -> pl.DataFrame:
    """Get payment information filtered by number of installments."""
    return conn.sql(
        read_sql("Payment_information.sql"),
        params=[payment_installment],
    ).pl()


def get_price_shipping(
    conn: duckdb.DuckDBPyConnection,
    seller_city: str,
) -> pl.DataFrame:
    """Get shipping price ratting ."""
    return conn.sql(
        read_sql("Price_shipping.sql"),
        params=[seller_city],
    ).pl()


def get_product_reviews(
    conn: duckdb.DuckDBPyConnection,
    review_score: str,
) -> pl.DataFrame:
    """Get product reviews filtered by review score."""
    return conn.sql(
        read_sql("Product_reviews.sql"),
        params=[review_score],
    ).pl()


def get_seller_consumer_location(
    conn: duckdb.DuckDBPyConnection,
    seller_city: str,
) -> pl.DataFrame:
    return conn.sql(
        read_sql("seller_consumer_location.sql"),
        params=[seller_city],
    ).pl()


if __name__ == "__main__":
    print(get_payment_information)  # verifys that data is properly pulled

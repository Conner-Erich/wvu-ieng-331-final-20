import logging
from datetime import datetime

import duckdb

from .queries import data_path, read_sql

data_set = data_path(
    "olist.duckdb"
)  # sets the data path as olist.duckdb and pulls the function from queries.py


MIN_ROW_COUNTS = {
    "orders": 1_000,
    "order_items": 1_000,
    "customers": 1_000,
}

EXPECTED_TABLES = [
    "orders",
    "order_items",
    "customers",
    "products",
    "sellers",
    "order_reviews",
    "order_payments",
    "product_category_name_translation",
    "geolocation",
]
KEY_COLUMNS = {
    "orders": ["order_id", "customer_id"],
    "order_items": ["order_id", "product_id", "seller_id"],
    "customers": ["customer_id"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
}

DATE_COLUMN = "order_purchase_timestamp"
DATE_TABLE = "orders"
DATE_MIN = datetime(2015, 1, 1)
DATE_MAX = datetime.today()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

db_connect = duckdb.connect(data_set, read_only=True)


def check_tables_exist(conn: duckdb.DuckDBPyConnection) -> list[str]:
    """Verify all 9 expected tables are present in the database."""
    failures = []
    existing = {row[0].lower() for row in conn.sql("SHOW TABLES").fetchall()}
    for table in EXPECTED_TABLES:
        if table.lower() not in existing:
            msg = f"Missing table: '{table}' not found in database."
            log.warning(msg)
            failures.append(msg)
        else:
            log.info(f"Table check passed: '{table}' exists.")
    return failures


def check_columns_not_null(conn: duckdb.DuckDBPyConnection) -> list[str]:
    """Check that key columns are not entirely NULL in their respective tables."""
    failures = []
    for table, columns in KEY_COLUMNS.items():
        for column in columns:
            result = conn.sql(read_sql("data_not_null.sql")).fetchone()
            non_null_count = result[0] if result else 0
            if non_null_count == 0:
                msg = f"NULL column: '{table}.{column}' is entirely NULL (0 non-null rows)."
                log.warning(msg)
                failures.append(msg)
            else:
                log.info(
                    f"NULL check passed: '{table}.{column}' has {non_null_count:,} non-null rows."
                )
    return failures


def check_date_range(conn: duckdb.DuckDBPyConnection) -> list[str]:
    """Validate that order_purchase_timestamp is not empty and contains no future dates."""
    failures = []
    result = conn.sql(read_sql("data_validation_timerange.sql")).fetchone()

    if not result or result[0] == 0:
        msg = f"Date check failed: '{DATE_TABLE}.{DATE_COLUMN}' has no non-null dates."
        log.warning(msg)
        failures.append(msg)
        return failures

    total_rows, min_date, max_date, future_count = result

    if min_date < DATE_MIN:
        msg = (
            f"Date check failed: earliest date {min_date} in "
            f"'{DATE_TABLE}.{DATE_COLUMN}' is before {DATE_MIN.date()} — "
            f"possible data quality issue."
        )
        log.warning(msg)
        failures.append(msg)
    else:
        log.info(f"Date range check passed: earliest date is {min_date}.")

    if future_count > 0:
        msg = (
            f"Date check failed: {future_count:,} future-dated row(s) found in "
            f"'{DATE_TABLE}.{DATE_COLUMN}'."
        )
        log.warning(msg)
        failures.append(msg)
    else:
        log.info(
            f"Future date check passed: no future-dated rows in '{DATE_TABLE}.{DATE_COLUMN}'."
        )

    log.info(f"Date range: {min_date} → {max_date} ({total_rows:,} dated rows).")
    return failures


def check_row_counts(conn: duckdb.DuckDBPyConnection) -> list[str]:
    failures = []
    for table, minimum in MIN_ROW_COUNTS.items():
        result = conn.sql(read_sql("data_row_counts.sql")).fetchone()
        count = int(result[0]) if result else 0

        if count < minimum:
            msg = (
                f"Row count check failed: '{table}' has {count:,} rows "
                f"(minimum expected: {minimum:,})."
            )
            log.warning(msg)
            failures.append(msg)
        else:
            log.info(f"Row count check passed: '{table}' has {count:,} rows.")
    return failures


if __name__ == "__main__":
    check_row_counts(db_connect)

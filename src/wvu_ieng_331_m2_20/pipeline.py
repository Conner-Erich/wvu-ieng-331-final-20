import argparse
from pathlib import Path

import altair as alt
import duckdb
import polars as pl
from loguru import logger

from .queries import (
    DB_PATH,
    get_payment_information,
    get_price_shipping,
    get_product_reviews,
    get_seller_consumer_location,
)
from .validation import (
    check_columns_not_null,
    check_date_range,
    check_row_counts,
    check_tables_exist,
)

OUTPUT_DIR = Path(__file__).parent.parent.parent / "outputs"


def ensure_output_dir() -> None:
    """Create the outputs directory if it does not already exist."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"Output directory created or found: {OUTPUT_DIR}")


def summary_csv(x: pl.DataFrame) -> Path:
    """
    puts all of the payment information into a csv file found in the output folder
    """
    logger.info("creating summary csv")
    seen = {}
    new_names = []
    for col in x.columns:
        if col in seen:
            seen[col] += 1
            new_names.append(f"{col}_{seen[col]}")
        else:
            seen[col] = 0
            new_names.append(col)
    x = x.rename(dict(zip(x.columns, new_names)))

    summary = (
        x.group_by("payment_installments")
        .agg(
            [
                pl.col("payment_type").first().alias("payment_type"),
                pl.col("order_customer_id").len().alias("customer_count"),
                pl.col("product_id").len().alias("product_count"),
                pl.col("type_of_installment").count().alias("type_of_installment"),
            ]
        )
        .sort("customer_count", descending=True)
    )
    for col in summary.columns:
        if summary[col].dtype == pl.List(pl.Utf8) or isinstance(
            summary[col].dtype, pl.List
        ):
            summary = summary.with_columns(pl.col(col).list.join(", ").alias(col))
        elif isinstance(summary[col].dtype, (pl.Struct, pl.Array)):
            summary = summary.with_columns(pl.col(col).cast(pl.Utf8).alias(col))
    output_path = OUTPUT_DIR / "summary.csv"
    summary.write_csv(output_path)
    return output_path


def write_detail_parquet(df: pl.DataFrame) -> Path:
    """
    Outputs the seller consumer location as a detail.parquet
    """
    logger.info("creating parquet")
    output_path = OUTPUT_DIR / "detail.parquet"
    df.write_parquet(output_path)
    print(f"Written: {output_path}  ({len(df)} rows)")
    return output_path


def write_chart_html(df: pl.DataFrame, limit: int = 50) -> Path:
    """

    creats a html chart file that looks at the products price and the cost it takes to ship it and out-puts the file into the data folder

    Args:
        df    : Polars DataFrame from get_price_shipping()
        limit : Number of top products by price_per_density to display.
                Pass 0 to show all products. (default: 50) so that the graph is not unreadble
    """
    logger.info(f"creating html chart (limit={limit})")

    if limit > 0:
        df = df.sort("price_per_density", descending=True).head(limit)
        title = f"Top {limit} Products by Price per Density"
    else:
        df = df.sort("price_per_density", descending=True)
        title = "All Products by Price per Density"

    chart_data = df.to_pandas()

    chart = (
        alt.Chart(chart_data, title=title)
        .mark_bar()
        .encode(
            x=alt.X(
                "product_id:N",
                title="Product ID",
                axis=alt.Axis(labelAngle=-45, labels=False, ticks=False),
                sort=alt.SortField(field="price_per_density", order="descending"),
            ),
            y=alt.Y(
                "price_per_density:Q",
                title="Price per Density (g/cm³)",
                scale=alt.Scale(zero=False),
            ),
            color=alt.Color(
                "shipping_price_rating:N",
                title="Shipping Rating",
                scale=alt.Scale(
                    domain=["cheap", "moderate", "expensive", "error"],
                    range=["#1D9E75", "#EF9F27", "#E24B4A", "#888780"],
                ),
            ),
            tooltip=[
                alt.Tooltip("product_id:N", title="Product ID"),
                alt.Tooltip(
                    "price_per_density:Q", title="Price per Density", format=",.4f"
                ),
                alt.Tooltip("freight_value:Q", title="Freight Value", format=",.2f"),
                alt.Tooltip("price:Q", title="Item Price", format=",.2f"),
                alt.Tooltip("payment_value:Q", title="Payment Value", format=",.2f"),
                alt.Tooltip("shipping_price_rating:N", title="Shipping Rating"),
            ],
        )
        .properties(width=700, height=400)
        .interactive()
    )

    output_path = OUTPUT_DIR / "chart.html"
    chart.save(str(output_path))
    return output_path


def parse_args() -> argparse.Namespace:
    """creates the arguments for the database, and for the queries payment_installment, shipping_price_rating, review_rating, and seller city."""
    parser = argparse.ArgumentParser(
        prog="pipeline.py",
        description="Olist database reporting.",
        epilog="Running with no arguments produces the preset data values.",
    )

    parser.add_argument(
        "--db",
        type=str,
        default="olist.duckdb",
        metavar="FILENAME",
        help="Database filename inside the /data folder. (default: olist.duckdb)",
    )
    parser.add_argument(
        "--chart_limit",
        type=int,
        default=50,
        help="Number of products to show in the chart. (default: 50)",
    )
    parser.add_argument(
        "--payment_installment",
        type=int,
        default=1,
        help="Filter by number of payment installments. (default: 1)",
    )
    parser.add_argument(
        "--shipping_price_rating",
        type=str,
        default="cheap",
        help="Filter by shipping price rating e.g. cheap, expensive. (default: cheap)",
    )
    parser.add_argument(
        "--review_rating",
        type=str,
        default="bad",
        help="Filter by review rating e.g. bad, good. (default: bad)",
    )
    parser.add_argument(
        "--seller_city",
        type=str,
        default="sao paulo",
        help="Filter by seller city. (default: sao paulo)",
    )

    return parser.parse_args()


def get_connection(db_filename: str = "olist.duckdb") -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection to the given database file."""
    db_path = Path(__file__).parent.parent.parent / "data" / db_filename
    if not db_path.exists():
        logger.error(f"Database not found: {db_path}")
        raise FileNotFoundError(f"Database not found: {db_path}")
    logger.info(f"Connecting to database: {db_path}")
    return duckdb.connect(str(db_path), read_only=True)


def run_validation(conn: duckdb.DuckDBPyConnection) -> None:
    """Run all data quality checks before the main analysis."""
    logger.info("Running validation...")
    try:
        check_tables_exist(conn)
        check_columns_not_null(conn)
        check_date_range(conn)
        check_row_counts(conn)
    except RuntimeError as e:
        logger.error(f"Validation failed:\n{e}")
        raise SystemExit(1)
    logger.info("Validation complete.")


def pipeline() -> None:
    """applys arguments, validates data, runs the analysis, creates a csv, html chart, and a parquet."""

    args = parse_args()

    conn = get_connection(args.db)
    ensure_output_dir()

    run_validation(conn)

    logger.info("Querying payment information...")
    df_payments = get_payment_information(conn, args.payment_installment)
    logger.info(f"Payment information: {len(df_payments)} rows returned.")
    summary_csv(df_payments)

    logger.info("Querying seller consumer location...")
    df_cities = get_seller_consumer_location(conn, args.seller_city)
    logger.info(f"Seller consumer location: {len(df_cities)} rows returned.")
    write_detail_parquet(df_cities)

    logger.info("Querying price shipping data...")
    df_shipping = get_price_shipping(conn, args.shipping_price_rating)
    logger.info(f"Price shipping: {len(df_shipping)} rows returned.")
    write_chart_html(df_shipping, limit=args.chart_limit)

    conn.close()
    logger.info("Pipeline complete. All outputs written.")

main = pipeline
if __name__ == "__main__":
    pipeline()

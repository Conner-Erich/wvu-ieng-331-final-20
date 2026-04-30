from pathlib import Path

import altair as alt
import polars as pl
from loguru import logger

OUTPUT_DIR = Path(__file__).parent.parent.parent / "outputs"


def _chart_to_embed(chart: alt.Chart) -> str:
    """
    Extract the vegaEmbed div and script block from an Altair chart's
    full HTML so it can be safely inlined into a larger HTML document.
    Returns just the <div> and <script> tags — no <html>, <head>, or
    duplicate vega/vega-lite/vega-embed script imports.
    """
    full_html = chart.to_html()

    # Extract the chart div id
    div_start = full_html.find('<div id="vis"')
    div_end = full_html.find("</div>", div_start) + len("</div>")
    div_block = full_html[div_start:div_end]

    # Extract the vegaEmbed script block
    script_start = full_html.find('<script type="text/javascript">')
    script_end = full_html.find("</script>", script_start) + len("</script>")
    script_block = full_html[script_start:script_end]

    # Give each chart a unique div id to avoid conflicts
    return div_block, script_block


def write_report_html(
    df_payments: pl.DataFrame,
    df_shipping: pl.DataFrame,
    df_cities: pl.DataFrame,
    limit: int = 50,
) -> Path:
    """
    Combines all three charts into a single self-contained HTML report
    with descriptive text and saves it to outputs/report.html.

    Args:
        df_payments : Polars DataFrame from get_payment_information()
        df_shipping : Polars DataFrame from get_price_shipping()
        df_cities   : Polars DataFrame from get_seller_consumer_location()
        limit       : Number of top items to show per chart. (default: 50)
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    logger.info("Building combined HTML report...")

    payment_df = (
        df_payments.group_by(["payment_type", "type_of_installment"])
        .agg(
            [
                pl.col("order_customer_id").len().alias("customer_count"),
                pl.col("product_id").len().alias("product_count"),
            ]
        )
        .sort("customer_count", descending=True)
    ).to_pandas()

    chart1 = (
        alt.Chart(payment_df, title="Payment Types by Customer Count")
        .mark_bar()
        .encode(
            x=alt.X(
                "payment_type:N",
                title="Payment Type",
                axis=alt.Axis(labelAngle=-45),
                sort="-y",
            ),
            y=alt.Y("customer_count:Q", title="Number of Customers"),
            color=alt.Color(
                "type_of_installment:N",
                title="Installment Type",
                scale=alt.Scale(
                    domain=[
                        "onetime purchase",
                        "multiple installments",
                        "not paid for or error",
                    ],
                    range=["#1D9E75", "#EF9F27", "#E24B4A"],
                ),
            ),
            tooltip=[
                alt.Tooltip("payment_type:N", title="Payment Type"),
                alt.Tooltip("type_of_installment:N", title="Installment Type"),
                alt.Tooltip("customer_count:Q", title="Customer Count"),
                alt.Tooltip("product_count:Q", title="Product Count"),
            ],
        )
        .properties(width=700, height=400)
        .interactive()
    )

    shipping_df = (
        df_shipping.group_by(["product_id", "shipping_price_rating"])
        .agg(
            [
                pl.col("price_per_density").mean().alias("avg_price_per_density"),
                pl.col("freight_value").mean().alias("avg_freight_value"),
                pl.col("price").mean().alias("avg_item_price"),
            ]
        )
        .sort("avg_price_per_density", descending=True)
        .head(limit)
    ).to_pandas()

    chart2 = (
        alt.Chart(shipping_df, title=f"Top {limit} Products by Shipping Cost")
        .mark_bar()
        .encode(
            x=alt.X(
                "product_id:N",
                title="Product ID",
                axis=alt.Axis(labels=False, ticks=False),
                sort=alt.SortField(field="avg_price_per_density", order="descending"),
            ),
            y=alt.Y(
                "avg_price_per_density:Q",
                title="Avg Price per Density (g/cm³)",
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
                alt.Tooltip("shipping_price_rating:N", title="Shipping Rating"),
                alt.Tooltip(
                    "avg_price_per_density:Q", title="Avg Price/Density", format=",.4f"
                ),
                alt.Tooltip(
                    "avg_freight_value:Q", title="Avg Freight Value", format=",.2f"
                ),
                alt.Tooltip("avg_item_price:Q", title="Avg Item Price", format=",.2f"),
            ],
        )
        .properties(width=700, height=400)
        .interactive()
    )

    top_ids = (
        df_cities.group_by("product_id")
        .agg(pl.len().alias("total_orders"))
        .sort("total_orders", descending=True)
        .head(10)["product_id"]
    )

    location_df = (
        df_cities.filter(pl.col("product_id").is_in(top_ids))
        .group_by(["order_month", "product_id"])
        .agg(pl.len().alias("order_count"))
        .sort("order_month")
    ).to_pandas()
    location_df["order_month"] = location_df["order_month"].astype(str)

    chart3 = (
        alt.Chart(location_df, title="Top 10 Products Ordered Over Time")
        .mark_line(point=True)
        .encode(
            x=alt.X(
                "order_month:T",
                title="Month",
                axis=alt.Axis(labelAngle=-45, format="%b %Y"),
            ),
            y=alt.Y("order_count:Q", title="Number of Orders"),
            color=alt.Color("product_id:N", title="Product ID"),
            tooltip=[
                alt.Tooltip("order_month:T", title="Month", format="%B %Y"),
                alt.Tooltip("product_id:N", title="Product ID"),
                alt.Tooltip("order_count:Q", title="Order Count"),
            ],
        )
        .properties(width=700, height=400)
        .interactive()
    )

    chart1_spec = chart1.to_json()
    chart2_spec = chart2.to_json()
    chart3_spec = chart3.to_json()

    html_report = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Olist Data Analysis Report</title>
    <script src="https://cdn.jsdelivr.net/npm/vega@5"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-lite@5"></script>
    <script src="https://cdn.jsdelivr.net/npm/vega-embed@6"></script>
    <style>
        body {{
            font-family: Georgia, serif;
            max-width: 900px;
            margin: 0 auto;
            padding: 40px 20px;
            color: #2c2c2c;
            background-color: #fafafa;
        }}
        h1 {{ font-size: 2em; border-bottom: 3px solid #1D9E75; padding-bottom: 10px; }}
        h2 {{ font-size: 1.4em; border-bottom: 1px solid #ccc; padding-bottom: 6px; margin-top: 50px; }}
        p  {{ line-height: 1.7; font-size: 1em; }}
        .chart-container {{ margin: 30px 0 50px 0; }}
        .insight {{
            background: #f0f9f5;
            border-left: 4px solid #1D9E75;
            padding: 12px 16px;
            margin: 16px 0;
            font-style: italic;
        }}
    </style>
</head>
<body>

    <h1>Olist E-Commerce Analysis Report</h1>
    <p>
        This Report analysis the Olist E-Commerce data set looking secifically
        at the payment infromation and the cost the is incured when shipping to
        different customers and the price of shipping each of the products offered.
    </p>

    <h2>1. Payment Types by Customer Count</h2>
    <p>
        The following chart shows the payment type and the payment installation made
        by our customers.
        The vast majority of our one time purchases fall with in the credit_card and
        the boleto payment types. While very few customers make a single installment
        payment with a debit_card.
    </p>

    <div id="chart1" class="chart-container"></div>

    <h2>2. Product Shipping Cost by Density</h2>
    <p>
        This chart shows the top {limit} products by their average price-per-density
        ratio. This is a measure of how economical each product is to ship relative to
        its size and weight. Products with a high price-per-density are expensive
        to ship per unit of mass, while low values indicate efficient shipping.
        These 50 products offered are very cheap to ship costing less than $1 to ship
        based on weight.
    </p>

    <div id="chart2" class="chart-container"></div>

    <h2>3. Product Demand Over Time by City</h2>
    <p>
        This chart tracks how the top 10 most ordered products in the selected
        seller city. The following graph shows that most of the products shipped
        are one time unique products that are not reordered or ordered by different
        customers.
    </p>

    <div id="chart3" class="chart-container"></div>

    <hr style="margin-top: 60px; border: none; border-top: 1px solid #ccc;">
    <p style="font-size: 0.85em; color: #888; text-align: center;">
        Generated by the Olist analysis pipeline. Data source: Olist E-Commerce Dataset.
    </p>

</body>
<script>
    // Render each chart spec into its named div using vegaEmbed
    vegaEmbed("#chart1", {chart1_spec}, {{renderer: "svg", actions: false}});
    vegaEmbed("#chart2", {chart2_spec}, {{renderer: "svg", actions: false}});
    vegaEmbed("#chart3", {chart3_spec}, {{renderer: "svg", actions: false}});
</script>
</html>"""

    output_path = OUTPUT_DIR / "report.html"
    output_path.write_text(html_report, encoding="utf-8")
    logger.info(f"Written: {output_path}  (self-contained HTML report)")
    return output_path

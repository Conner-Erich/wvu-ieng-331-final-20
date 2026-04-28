# wvu-ieng-331-m2-20
IENG_331_Milestone_2
# Milestone 2: Python Pipeline

**Team {20}**: {Conner Erich}

## How to Run

Instructions to run the pipeline from a fresh clone:

```bash
git clone https://github.com/{Coner-Erich}/wvu-ieng-331-m2-{20}.git
cd wvu-ieng-331-m2-{20}
uv sync
# place olist.duckdb in the data/ directory 
uv run wvu-ieng-331-m2-{20}
uv run wvu-ieng-331-m2-{20} --db --chart_limit --payment_installment --shipping_price_rating --review_rating --seller_city sao paulo
```

## Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `--olist.duckdb` | file | 'olist.duckdb' | used to change the data base to any file that is in the data folder |
|'--chart_limit| int | 50| used to set the number of products present in the chart|
| `--payment_installment` | int | 1 (single payments)| shows the number of paymentinstallments that the customer has chosen |
| '--seller_city' | str | sao paulo | shows all of the sellers and consumers in one city |

## Outputs

Describe each output file: what it contains, what format, and how to interpret it.
The SQL folder contains all of the sql files these files are written in sql and the files convert those sql files in to polars files so that it can be used in the other files

The validation file takes the information found in the SQL files and checks if the data is present or is null. 

pipeline.py contains functions that analysis the data provided in the SQL and Validation Files, It also contains functions to create csv, html charts, parquets. It runs terminal arguements to better refine the data.

## Validation Checks

check_tables_exist; checks all of the tables in the chosen data set so that the sql queries have data to pull. If it fails the pipeline is haulted as it is imposible to move forward.

check_columns_not_null; checks for the key columns and sees if they are not null. If it fails the pipeline is haulted as it is imposible to move forward.

check_date_range;checks for the date columns and sees if they are not null or the time set is to far-forward or to far-back. If it fails a warning is given.

check_row_counts;checks for the row count and sees if they are not null and how many rows there are. If it fails a warning is given.


## Analysis Summary

The data that is output from the pipeline shows that most of the products are very cost effective to ship to and from consumers and which products are cheapest to ship based on the space that they take up allowing for more revenue by focusing and shipping those products.
The pipeline also shows which products get what reviews and which customers give what types of reviews alloing to see if specific customers always rate products lower or what products are of poor or great quality.
The pipeline also shows the payment infromation to show what customer make one payment in full and what products are normally paid for at once. 

## Limitations & Caveats

Pipeline can not handle a database that is not duckdb and can not handle files that are not written in sql as the code is written for those two specific file types. The code is limited to the argparse groupings so to refine the data anyway you wanted would require them to be rewritten with the wanted arguments.

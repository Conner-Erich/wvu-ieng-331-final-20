# Design Rationale

## Parameter Flow
  ### shipping_price_rating
 `--shipping_price_rating` travees through the code by starting with being called from the terminal with the argument that it want to be run the defaul is 'cheap'. The get_price_shipping function is imported from the queries.py and is called with the argument shipping_price_rating. This argument then replaces the place holder value in the SQL code that value being $1. The SQL code is turned from sql code to python so that the placeholder can be replaced with the proper arguemnt given in the terminal or the default being cheap, this allows the sql code to now be run against the database and pull out the wanted infromation.
 

## SQL Parameterization
 ### Price_shipping.sql
 This is what the raw code looks with the $1 place holder. The place holder is applied to the where clause.
-- This CTE Finds the price to ship the prodcut based on it dimensions to find what prodcuts are most ecinomical to ship.
with product_shipping_values as (
select
  (p.product_length_cm * p.product_height_cm * p.product_width_cm) as product_volume,
  product_volume / oi.freight_value as price_per_cm3,
  p.product_weight_g / product_volume as price_per_density,
  op.payment_value,
  oi.freight_value,
  oi.price,
  p.product_id,
    case
  when price_per_density <= 1 then 'cheap'
  when price_per_density >1 and price_per_density <= 10 then 'moderate'
  when price_per_density > 10 then 'expensive'
  else 'error'
  end as shipping_price_rating
from
  products as p
  join order_items as oi on oi.product_id = p.product_id
  join order_payments as op on op.order_id = oi.order_id
where shipping_price_rating = $1
order by price_per_density desc
  )
  select *
  from product_shipping_values

queries.py reads the file as a string and then retuns the sql code as text (str) and then takes the params argument at the end of the code and imputs the wanted value in the place of the place holder, so params[cheap] repleaces $1 with 'cheap', this is done so that it can be connceted to the duckdb database and run with the chosen parameter. Parameters are chosen over f strings as parameters can be chosen from the terminal rather then only form the file its self.
SQL lives in its own file so that the DB syntax can be easily shown so that it is easier to debug rather than trying to debug it in python where it is simply text. It also lives in its own sql file so that it can be run in a DB directly to check if the code produce the proper output before trying to debug and trouble shoot in the python enviornment where python errors could be the issue and not the sql code. 
The SQL and also be called multiple times more easily than if it was simply python code.

## Validation Logic

check_tables_exist; checks all of the tables in the chosen data set so that the sql queries have data to pull. If it fails the pipeline is haulted as it is imposible to move forward.

check_columns_not_null; checks for the key columns and sees if they are not null. If it fails the pipeline is haulted as it is imposible to move forward.

check_date_range;checks for the date columns and sees if they are not null or the time set is to far-forward or to far-back. If it fails a warning is given.

check_row_counts;checks for the row count and sees if they are not null and how many rows there are. If it fails a warning is given.

The minimum threshold was chosen as 1000 as that was what was given in the template and it checks if there are at least some computable rows with out over loading the program with every single row that is in the table. 
## Error Handling
### get_connection
inside of the pipeline.py there is the get-connection function. Inside of that function there is a FileNotFoundError to catch if the database is improperly called or if it does not exist, when it is raised it logs an error message, and haults the pipeline. If it was a bare except no error would be displayed and the code would not produce an out-put.

### run_validation
inside of the pipeline.py there is the run_validation function. Inside of that function there is a except RuntimeError that is pulled from the validation.py and all of the functions in there where they incounter an error flag the RuntimeError. When is function is raised it stops the pipeline function and returns an error. A bare except would hide which validation check failed. 

## Scaling & Adaptation

Answer both:
1.The first part of the pipeline that would slow down would be the validation check row count as counting all those rows would start to slow the process down, next it would be pulling up the data tables from the sql as there is no limit currently written in the sql code so it would pull all of the data, the fix would simple be to put a limit of 50,000 on it and then make than an argparse to change it to the wanted amount of data points.

2.It would be added in the pipeline.py with the rest of the output formats, I would simply need to write the code on how the data needs to be displayed and then choose which SQL query to put in there. No function would need to be modified other than the pipeline function that runs and outputs the data.

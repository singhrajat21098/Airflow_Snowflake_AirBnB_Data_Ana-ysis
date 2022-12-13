import os
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from airflow import AirflowException
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


#########################################################
#
#   Load Environment Variables
#
#########################################################
# Connection variables
snowflake_conn_id = "snowflake_conn_id"

########################################################
#
#   DAG Settings
#
#########################################################

dag_default_args = {
    'owner': 'singhrajat',
    'start_date': datetime.now() + timedelta(minutes=10),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='load_listing_data_dag',
    default_args=dag_default_args,
    schedule_interval= '@once',
    catchup=False,
    max_active_runs=1,
    concurrency=5
)


#########################################################
#
#   Custom Logics for Operator
#
#########################################################



query_setup = f"""

use bde_at3_1;

"""



query_create_listing = f"""

use bde_at3_1;

create or replace external table raw.listing
with location = @stage_gcp/listings
file_format = file_format_csv
pattern = '.*[.]csv';



create or replace table staging.listings as
select
value:c1::int as LISTING_ID,
value:c2::double as SCRAPE_ID,
to_date(value:c3::varchar) as SCRAPED_DATE,
value:c4::int as HOST_ID,
value:c5::varchar as HOST_NAME,
to_date(value:c6::string, 'DD/MM/YYYY') as HOST_SINCE,
value:c7::boolean as HOST_IS_SUPERHOST,
value:c8::varchar as HOST_NEIGHBOURHOOD,
value:c9::varchar as LISTING_NEIGHBOURHOOD,
value:c10::varchar as PROPERTY_TYPE,
value:c11::varchar as ROOM_TYPE,
value:c12::int as ACCOMMODATES,
value:c13::double as PRICE,
value:c14::boolean as HAS_AVAILABILITY,
value:c15::int as AVAILABILITY_30,
value:c16::double as NUMBER_OF_REVIEWS,
value:c17::int as REVIEW_SCORES_RATING,
value:c18::int as REVIEW_SCORES_ACCURACY,
value:c19::int as REVIEW_SCORES_CLEANLINESS,
value:c20::int as REVIEW_SCORES_CHECKIN,
value:c21::int as REVIEW_SCORES_COMMUNICATION,
value:c22::int as REVIEW_SCORES_VALUE,
replace(split_part(split_part(metadata$filename, '/', 3), '.',1),'_','-') ::varchar as month_year
from raw.listing;


"""





query_create_dim_host = f"""

use bde_at3_1;


create or replace table warehouse.dimension_host as 
select 
    hash(HOST_NAME, HOST_SINCE, HOST_NEIGHBOURHOOD) host_hash_key,
    HOST_NAME,
    HOST_SINCE,
    HOST_NEIGHBOURHOOD
from(
    select 
        HOST_NAME,
        HOST_SINCE,
        HOST_NEIGHBOURHOOD,
        row_number() over( partition by 
            HOST_NAME,
            HOST_SINCE,
          HOST_NEIGHBOURHOOD
         order by host_name) row_num
    from staging.listings
)
where row_num = 1;

   
"""

query_create_dim_review =f"""

use bde_at3_1;


create or replace table warehouse.dimension_review as 
select 
    hash(NUMBER_OF_REVIEWS, REVIEW_SCORES_ACCURACY, REVIEW_SCORES_CLEANLINESS, REVIEW_SCORES_CHECKIN, REVIEW_SCORES_COMMUNICATION, REVIEW_SCORES_VALUE) review_hash_key,
    NUMBER_OF_REVIEWS,
    REVIEW_SCORES_ACCURACY,
    REVIEW_SCORES_CLEANLINESS,
    REVIEW_SCORES_CHECKIN,
    REVIEW_SCORES_COMMUNICATION,
    REVIEW_SCORES_VALUE

from(
    select 
        NUMBER_OF_REVIEWS,
        REVIEW_SCORES_ACCURACY,
        REVIEW_SCORES_CLEANLINESS,
        REVIEW_SCORES_CHECKIN,
        REVIEW_SCORES_COMMUNICATION,
        REVIEW_SCORES_VALUE,
        row_number() over( partition by 
            NUMBER_OF_REVIEWS,
            REVIEW_SCORES_ACCURACY,
            REVIEW_SCORES_CLEANLINESS,
            REVIEW_SCORES_CHECKIN,
            REVIEW_SCORES_COMMUNICATION,
            REVIEW_SCORES_VALUE
        order by number_of_reviews) row_num
    from staging.listings
)
where row_num = 1;


"""


query_create_dim_property = f"""

use bde_at3_1;

create or replace table warehouse.dimension_property as 
select 
    hash(PROPERTY_TYPE, ROOM_TYPE, ACCOMMODATES ) property_hash_key,
    PROPERTY_TYPE,
    ROOM_TYPE,
    ACCOMMODATES 
from(
    select 
        PROPERTY_TYPE,
        ROOM_TYPE,
        ACCOMMODATES, 
        row_number() over(partition by 
            PROPERTY_TYPE,
            ROOM_TYPE,
            ACCOMMODATES 
         order by PROPERTY_TYPE) row_num
    from staging.listings
)
where row_num = 1;

"""

query_create_fact_table =f"""

use bde_at3_1;

create or replace table warehouse.fact_listing as
select

LISTING_ID,
SCRAPE_ID,
SCRAPED_DATE,
HOST_ID,
LISTING_NEIGHBOURHOOD,
lga1.lga_code LISTING_LGA_CODE,
PRICE,
HAS_AVAILABILITY,
AVAILABILITY_30,
REVIEW_SCORES_RATING,
HOST_IS_SUPERHOST,
to_date(MONTH_YEAR,'MM-YYYY') month_year,
hash(PROPERTY_TYPE, ROOM_TYPE, ACCOMMODATES ) property_hash_key,
hash(HOST_NAME, HOST_SINCE, HOST_NEIGHBOURHOOD) host_hash_key,
hash(NUMBER_OF_REVIEWS, REVIEW_SCORES_ACCURACY, REVIEW_SCORES_CLEANLINESS, REVIEW_SCORES_CHECKIN, REVIEW_SCORES_COMMUNICATION, REVIEW_SCORES_VALUE) review_hash_key

from staging.listings l
left join warehouse.dimension_lga_suburb lga1
on lower(l.listing_neighbourhood) = lower(lga1.lga_name);

"""


query_add_primary_keys = f"""

use bde_at3_1;

alter table warehouse.dimension_host
add primary key(host_hash_key);

alter table warehouse.dimension_property
add primary key(property_hash_key);

alter table warehouse.dimension_review
add primary key(review_hash_key);

alter table warehouse.fact_listing
add primary key(listing_ID);


"""

query_add_foreign_keys = f"""

use bde_at3_1;

alter table warehouse.fact_listing
add constraint fk_fact_prop FOREIGN KEY (property_hash_key) REFERENCES warehouse.dimension_property(property_hash_key);

alter table warehouse.fact_listing
add constraint fk_fact_host FOREIGN KEY (host_hash_key) REFERENCES warehouse.dimension_host(host_hash_key);

alter table warehouse.fact_listing
add constraint fk_fact_review FOREIGN KEY (review_hash_key) REFERENCES warehouse.dimension_review(review_hash_key);

alter table warehouse.fact_listing
add constraint fk_fact_cg1 FOREIGN KEY (LISTING_LGA_CODE) REFERENCES warehouse.dimension_census_go1(code);

alter table warehouse.fact_listing
add constraint fk_fact_cg2 FOREIGN KEY (LISTING_LGA_CODE) REFERENCES warehouse.dimension_census_go2(code);

alter table warehouse.fact_listing
add constraint fk_fact_lga FOREIGN KEY (LISTING_LGA_CODE) REFERENCES warehouse.dimension_lga_suburb(lga_code);


"""





query_create_datamart_dm_listing_neighbourhood=f"""

use bde_at3_1;

create or replace view warehouse.listing_neigh_1 as
select 
listing_neighbourhood,
month_year,
count(CASE WHEN has_availability THEN 1 END)/count(listing_id)*100 Active_listing_rate,
count(distinct(host_id)) distinct_host,
count(CASE WHEN host_is_superhost THEN 1 END)/count(listing_id)*100 Superhost_rate,
(30*count(listing_id))-sum(availability_30) total_number_of_stays
from warehouse.fact_listing f
group by listing_neighbourhood, month_year;


create or replace view warehouse.listing_neigh_2 as
select 
listing_neighbourhood,
month_year,
min(price) Minimum_Price,
max(price) Maximum_Price,
median(price) Median_price,
avg(price) average_price,
avg(review_scores_rating) avg_review_scores_ratings,
avg((30-availability_30)*price) avg_estimated_revenue
from warehouse.fact_listing f
where has_availability = TRUE
group by listing_neighbourhood, month_year
order by listing_neighbourhood, month_year;


create or replace view warehouse.listing_neigh_4 as
select
listing_neighbourhood,
month_year,
round(100*(count(listing_id) - lag(count(listing_id),1) over ( partition by listing_neighbourhood  order by month_year))/ lag(count(listing_id),1) over ( partition by listing_neighbourhood  order by month_year),2) percentage_change_for_active_listing
from warehouse.fact_listing f
where has_availability = TRUE
group by listing_neighbourhood, month_year
order by listing_neighbourhood,month_year;

create or replace view warehouse.listing_neigh_5 as
select
listing_neighbourhood,
month_year,
round(100*(count(listing_id) - lag(count(listing_id),1) over ( partition by listing_neighbourhood  order by month_year))/ lag(count(listing_id),1) over (  partition by listing_neighbourhood order by month_year),2) percentage_change_for_inactive_listing
from warehouse.fact_listing f
where has_availability = FALSE
group by listing_neighbourhood, month_year
order by listing_neighbourhood,month_year;



create or replace view datamart.dm_listing_neighbourhood as
select 
l1.listing_neighbourhood,
l1.month_year,
l2.maximum_price,
l2.minimum_price,
l2.median_price,
l2.average_price,
l1.distinct_host,
l1.superhost_rate,
l2.avg_review_scores_ratings,
l4.percentage_change_for_active_listing,
l5.percentage_change_for_inactive_listing,
l1.total_number_of_stays,
l2.avg_estimated_revenue
from warehouse.listing_neigh_1 l1
full outer join warehouse.listing_neigh_2 l2 on l1.listing_neighbourhood = l2.listing_neighbourhood and l1.month_year = l2.month_year
full outer join warehouse.listing_neigh_4 l4 on l1.listing_neighbourhood = l4.listing_neighbourhood and l1.month_year = l4.month_year
full outer join warehouse.listing_neigh_5 l5 on l1.listing_neighbourhood = l5.listing_neighbourhood and l1.month_year = l5.month_year

order by l1.month_year, l1.listing_neighbourhood;

"""


query_create_datamart_dm_property_type = f"""

use bde_at3_1;

create or replace view warehouse.property_temp_1 as
select 
property_type,
room_type,
accommodates,
month_year,
count(CASE WHEN has_availability THEN 1 END)/count(listing_id)*100 Active_listing_rate,
count(distinct(host_id)) distinct_host,
count(CASE WHEN host_is_superhost THEN 1 END)/count(listing_id)*100 Superhost_rate,
(30*count(listing_id))-sum(availability_30) total_number_of_stays
from warehouse.fact_listing fl
inner join warehouse.dimension_property dp
on fl.property_hash_key = dp.property_hash_key
group by property_type, room_type, accommodates, month_year
order by month_year;

create or replace view warehouse.property_temp_2 as
select 
property_type,
room_type,
accommodates,
month_year,
min(price) Minimum_Price_active,
max(price) Maximum_Price_active,
median(price) Median_price_active,
avg(price) average_price_active,
avg(review_scores_rating) avg_review_scores_ratings,
avg((30-availability_30)*price) avg_estimated_revenue
from warehouse.fact_listing fl
inner join warehouse.dimension_property dp
on fl.property_hash_key = dp.property_hash_key
where has_availability = TRUE
group by property_type, room_type, accommodates, month_year
order by month_year;



create or replace view warehouse.property_temp_3 as
select
property_type, room_type, accommodates, month_year,
round(100*(count(listing_id) - lag(count(listing_id),1) over ( partition by property_type, room_type, accommodates order by month_year))/ lag(count(listing_id),1) over (partition by property_type, room_type, accommodates order by month_year),2) percentage_change_for_active_listing
from warehouse.fact_listing fl
inner join warehouse.dimension_property dp
on fl.property_hash_key = dp.property_hash_key
where has_availability = TRUE
group by property_type, room_type, accommodates, month_year
order by property_type, room_type, accommodates, month_year;


create or replace view warehouse.property_temp_4 as
select
property_type, room_type, accommodates, month_year,
round(100*(count(listing_id) - lag(count(listing_id),1) over ( partition by property_type, room_type, accommodates order by month_year))/ lag(count(listing_id),1) over (partition by property_type, room_type, accommodates order by month_year),2) percentage_change_for_inactive_listing
from warehouse.fact_listing fl
inner join warehouse.dimension_property dp
on fl.property_hash_key = dp.property_hash_key
where has_availability = FALSE
group by property_type, room_type, accommodates, month_year
order by property_type, room_type, accommodates, month_year;


create or replace view datamart.dm_property_type as
select 
p1.property_type,
p1.room_type,
p1.accommodates,
p1.month_year,
p1.active_listing_rate,
p2.minimum_price_active,
p2.maximum_price_active,
p2.median_price_active,
p2.average_price_active,
p1.distinct_host,
p1.superhost_rate,
p2.avg_review_scores_ratings,
p3.percentage_change_for_active_listing,
p4.percentage_change_for_inactive_listing,
p1.total_number_of_stays,
p2.avg_estimated_revenue

from warehouse.property_temp_1 p1
full outer join warehouse.property_temp_2 p2 on p1.property_type = p2.property_type and p1.room_type = p2.room_type and p1.accommodates = p2.accommodates
full outer join warehouse.property_temp_3 p3 on p1.property_type = p3.property_type and p1.room_type = p3.room_type and p1.accommodates = p3.accommodates
full outer join warehouse.property_temp_4 p4 on p1.property_type = p4.property_type and p1.room_type = p4.room_type and p1.accommodates = p4.accommodates
order by p1.property_type, p1.room_type, p1.accommodates, p1.month_year;



"""

query_create_datamart_dm_host_neighbourhood = f"""

use bde_at3_1;

create or replace view dm_host_neighbourhood as 
select 
lga_name as host_neighbourhood_lga,
count(distinct(host_id)) number_of_distinct_host,
avg((30-availability_30)*price) avg_estimated_revenue,
avg((30-availability_30)*price)/count(distinct(host_id)) avg_estimated_revenue_per_host
from warehouse.fact_listing fl
left join warehouse.dimension_host h
on fl.host_hash_key = h.host_hash_key
left join warehouse.dimension_lga_suburb lga
on lower(lga.suburb_name) = lower(h.host_neighbourhood)
group by 1, month_year
order by month_year, 1;


"""




#########################################################
#
#   DAG Operator Setup
#
#########################################################



setup = SnowflakeOperator(
    task_id='query_setup',
    sql=query_setup,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

create_listing = SnowflakeOperator(
    task_id='query_create_listing',
    sql= query_create_listing,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

create_dim_review = SnowflakeOperator(
    task_id='query_create_dim_review',
    sql= query_create_dim_review,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

create_dim_property = SnowflakeOperator(
    task_id='query_create_dim_property',
    sql= query_create_dim_property,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

create_dim_host = SnowflakeOperator(
    task_id='query_create_dim_host',
    sql= query_create_dim_host,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

create_fact_table = SnowflakeOperator(
    task_id='query_create_fact_table',
    sql= query_create_fact_table,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

add_primary_keys = SnowflakeOperator(
    task_id='query_add_primary_keys',
    sql= query_add_primary_keys,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

add_foreign_keys = SnowflakeOperator(
    task_id='query_add_foreign_keys',
    sql= query_add_foreign_keys,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)



create_datamart_dm_listing_neighbourhood = SnowflakeOperator(
    task_id='query_create_datamart_dm_listing_neighbourhood',
    sql= query_create_datamart_dm_listing_neighbourhood,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

create_datamart_dm_property_type = SnowflakeOperator(
    task_id='query_create_datamart_dm_property_type',
    sql= query_create_datamart_dm_property_type,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

create_datamart_dm_host_neighbourhood = SnowflakeOperator(
    task_id='query_create_datamart_dm_host_neighbourhood',
    sql= query_create_datamart_dm_host_neighbourhood,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)





setup >> create_listing
create_listing >> create_dim_review
create_listing >> create_dim_property
create_listing >> create_dim_host
create_dim_review >> create_fact_table
create_dim_property >> create_fact_table
create_dim_host >> create_fact_table
create_fact_table >> add_primary_keys
add_primary_keys >> add_foreign_keys
add_foreign_keys >> create_datamart_dm_host_neighbourhood
add_foreign_keys >> create_datamart_dm_listing_neighbourhood
add_foreign_keys >> create_datamart_dm_property_type



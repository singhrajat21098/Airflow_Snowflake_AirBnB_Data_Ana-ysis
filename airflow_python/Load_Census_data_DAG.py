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
    'start_date': datetime.now() + timedelta(minutes=5),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='load_census_data_dag',
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


query_create_dim_lga_suburb = f"""


use bde_at3_1;

create or replace external table raw.nsw_lga_code
with location = @stage_gcp/NSW_LGA
file_format = file_format_csv
pattern = '.*CODE[.]csv';



create or replace external table raw.nsw_lga_suburb
with location = @stage_gcp/NSW_LGA
file_format = file_format_csv
pattern = '.*SUBURB[.]csv';


create or replace table staging.nsw_lga_code as 
select 
    value:c1::varchar as lga_code,
    value:c2::varchar as lga_name
from raw.nsw_lga_code;


create or replace table staging.nsw_lga_suburb as 
select 
    value:c1::varchar as lga_name,
    value:c2::varchar as suburb_name
from raw.nsw_lga_suburb;


create or replace table warehouse.dimension_lga_suburb as
select 
l_code.lga_code,
l_code.lga_name,
l_suburb.suburb_name
from staging.nsw_lga_code l_code
full join staging.nsw_lga_suburb l_suburb 
on lower(l_code.lga_name) = lower(l_suburb.lga_name);
   
"""

query_create_dim_census_go1 = f"""


use bde_at3_1;

create or replace external table raw.census_GO1
with location = @stage_gcp/Census_LGA
file_format = file_format_csv
pattern = '.*G01.*.[.]csv';

create or replace table staging.census_GO1 as
select
value:c1::varchar as lga_code,
value:c2::int as Tot_P_M,
value:c3::int as Tot_P_F,
value:c4::int as Tot_P_P,
value:c5::int as Age_0_4_yr_M,
value:c6::int as Age_0_4_yr_F,
value:c7::int as Age_0_4_yr_P,
value:c8::int as Age_5_14_yr_M,
value:c9::int as Age_5_14_yr_F,
value:c10::int as Age_5_14_yr_P,
value:c11::int as Age_15_19_yr_M,
value:c12::int as Age_15_19_yr_F,
value:c13::int as Age_15_19_yr_P,
value:c14::int as Age_20_24_yr_M,
value:c15::int as Age_20_24_yr_F,
value:c16::int as Age_20_24_yr_P,
value:c17::int as Age_25_34_yr_M,
value:c18::int as Age_25_34_yr_F,
value:c19::int as Age_25_34_yr_P,
value:c20::int as Age_35_44_yr_M,
value:c21::int as Age_35_44_yr_F,
value:c22::int as Age_35_44_yr_P,
value:c23::int as Age_45_54_yr_M,
value:c24::int as Age_45_54_yr_F,
value:c25::int as Age_45_54_yr_P,
value:c26::int as Age_55_64_yr_M,
value:c27::int as Age_55_64_yr_F,
value:c28::int as Age_55_64_yr_P,
value:c29::int as Age_65_74_yr_M,
value:c30::int as Age_65_74_yr_F,
value:c31::int as Age_65_74_yr_P,
value:c32::int as Age_75_84_yr_M,
value:c33::int as Age_75_84_yr_F,
value:c34::int as Age_75_84_yr_P,
value:c35::int as Age_85ov_M,
value:c36::int as Age_85ov_F,
value:c37::int as Age_85ov_P,
value:c38::int as Counted_Census_Night_home_M,
value:c39::int as Counted_Census_Night_home_F,
value:c40::int as Counted_Census_Night_home_P,
value:c41::int as Count_Census_Nt_Ewhere_Aust_M,
value:c42::int as Count_Census_Nt_Ewhere_Aust_F,
value:c43::int as Count_Census_Nt_Ewhere_Aust_P,
value:c44::int as Indigenous_psns_Aboriginal_M,
value:c45::int as Indigenous_psns_Aboriginal_F,
value:c46::int as Indigenous_psns_Aboriginal_P,
value:c47::int as Indig_psns_Torres_Strait_Is_M,
value:c48::int as Indig_psns_Torres_Strait_Is_F,
value:c49::int as Indig_psns_Torres_Strait_Is_P,
value:c50::int as Indig_Bth_Abor_Torres_St_Is_M,
value:c51::int as Indig_Bth_Abor_Torres_St_Is_F,
value:c52::int as Indig_Bth_Abor_Torres_St_Is_P,
value:c53::int as Indigenous_P_Tot_M,
value:c54::int as Indigenous_P_Tot_F,
value:c55::int as Indigenous_P_Tot_P,
value:c56::int as Birthplace_Australia_M,
value:c57::int as Birthplace_Australia_F,
value:c58::int as Birthplace_Australia_P,
value:c59::int as Birthplace_Elsewhere_M,
value:c60::int as Birthplace_Elsewhere_F,
value:c61::int as Birthplace_Elsewhere_P,
value:c62::int as Lang_spoken_home_Eng_only_M,
value:c63::int as Lang_spoken_home_Eng_only_F,
value:c64::int as Lang_spoken_home_Eng_only_P,
value:c65::int as Lang_spoken_home_Oth_Lang_M,
value:c66::int as Lang_spoken_home_Oth_Lang_F,
value:c67::int as Lang_spoken_home_Oth_Lang_P,
value:c68::int as Australian_citizen_M,
value:c69::int as Australian_citizen_F,
value:c70::int as Australian_citizen_P,
value:c71::int as Age_psns_att_educ_inst_0_4_M,
value:c72::int as Age_psns_att_educ_inst_0_4_F,
value:c73::int as Age_psns_att_educ_inst_0_4_P,
value:c74::int as Age_psns_att_educ_inst_5_14_M,
value:c75::int as Age_psns_att_educ_inst_5_14_F,
value:c76::int as Age_psns_att_educ_inst_5_14_P,
value:c77::int as Age_psns_att_edu_inst_15_19_M,
value:c78::int as Age_psns_att_edu_inst_15_19_F,
value:c79::int as Age_psns_att_edu_inst_15_19_P,
value:c80::int as Age_psns_att_edu_inst_20_24_M,
value:c81::int as Age_psns_att_edu_inst_20_24_F,
value:c82::int as Age_psns_att_edu_inst_20_24_P,
value:c83::int as Age_psns_att_edu_inst_25_ov_M,
value:c84::int as Age_psns_att_edu_inst_25_ov_F,
value:c85::int as Age_psns_att_edu_inst_25_ov_P,
value:c86::int as High_yr_schl_comp_Yr_12_eq_M,
value:c87::int as High_yr_schl_comp_Yr_12_eq_F,
value:c88::int as High_yr_schl_comp_Yr_12_eq_P,
value:c89::int as High_yr_schl_comp_Yr_11_eq_M,
value:c90::int as High_yr_schl_comp_Yr_11_eq_F,
value:c91::int as High_yr_schl_comp_Yr_11_eq_P,
value:c92::int as High_yr_schl_comp_Yr_10_eq_M,
value:c93::int as High_yr_schl_comp_Yr_10_eq_F,
value:c94::int as High_yr_schl_comp_Yr_10_eq_P,
value:c95::int as High_yr_schl_comp_Yr_9_eq_M,
value:c96::int as High_yr_schl_comp_Yr_9_eq_F,
value:c97::int as High_yr_schl_comp_Yr_9_eq_P,
value:c98::int as High_yr_schl_comp_Yr_8_belw_M,
value:c99::int as High_yr_schl_comp_Yr_8_belw_F,
value:c100::int as High_yr_schl_comp_Yr_8_belw_P,
value:c101::int as High_yr_schl_comp_D_n_g_sch_M,
value:c102::int as High_yr_schl_comp_D_n_g_sch_F,
value:c103::int as High_yr_schl_comp_D_n_g_sch_P,
value:c104::int as Count_psns_occ_priv_dwgs_M,
value:c105::int as Count_psns_occ_priv_dwgs_F,
value:c106::int as Count_psns_occ_priv_dwgs_P,
value:c107::int as Count_Persons_other_dwgs_M,
value:c108::int as Count_Persons_other_dwgs_F,
value:c109::int as Count_Persons_other_dwgs_P

from raw.census_go1;

create or replace table warehouse.dimension_census_go1 as
select RIGHT(lga_code, LEN(lga_code) - 3) code , * from staging.census_go1;

 


"""


query_create_dim_census_go2 = f"""

use bde_at3_1;

create or replace external table raw.census_GO2
with location = @stage_gcp/Census_LGA
file_format = file_format_csv
pattern = '.*G02.*.[.]csv';

create or replace table staging.census_go2 as
select 
value:c1::varchar as lga_code,
value:c2::int as Median_age_persons,
value:c3::int as Median_mortgage_repay_monthly,
value:c4::int as Median_tot_prsnl_inc_weekly,
value:c5::int as Median_rent_weekly,
value:c6::int as Median_tot_fam_inc_weekly,
value:c7::int as Average_num_psns_per_bedroom,
value:c8::int as Median_tot_hhd_inc_weekly,
value:c9::int as Average_household_size
from raw.census_go2;

create or replace table warehouse.dimension_census_go2 as
select RIGHT(lga_code, LEN(lga_code) - 3) code ,* from staging.census_go2;


"""



query_add_primary_keys =f"""

use bde_at3_1;

alter table warehouse.dimension_census_go1
add primary key(code);

alter table warehouse.dimension_census_go2
add primary key(code);

alter table warehouse.dimension_lga_suburb
add primary key(lga_code);

"""


#########################################################
#
#   DAG Operator Setup
#
#########################################################






create_dim_lga_suburb = SnowflakeOperator(
    task_id='query_create_dim_lga_suburb',
    sql= query_create_dim_lga_suburb,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

create_dim_census_go1 = SnowflakeOperator(
    task_id='query_create_dim_census_go1',
    sql= query_create_dim_census_go1,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

create_dim_census_go2 = SnowflakeOperator(
    task_id='query_create_dim_census_go2',
    sql= query_create_dim_census_go2,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)

add_primary_keys = SnowflakeOperator(
    task_id='query_add_primary_keys',
    sql= query_add_primary_keys,
    snowflake_conn_id=snowflake_conn_id,
    dag=dag
)





create_dim_lga_suburb >> add_primary_keys
create_dim_census_go1 >> add_primary_keys
create_dim_census_go2 >> add_primary_keys


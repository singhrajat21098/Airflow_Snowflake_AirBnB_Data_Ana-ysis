// creating a new database
create or replace database bde_at3;

// Use the database
use bde_at3;

// Creating a new schema
create or replace schema raw;


use bde_at3.raw;

// Creating a new Storage Integration 
CREATE STORAGE INTEGRATION GCP
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = GCS
ENABLED = TRUE
STORAGE_ALLOWED_LOCATIONS = ('gcs://australia-southeast1-bde-at-b3605914-bucket/data');

// Copy STORAGE_GCP_SERVICE_ACCOUNT from the result of this command. 
DESCRIBE INTEGRATION GCP;

// Stage_gcp
create or replace stage stage_gcp
storage_integration = GCP
url='gcs://australia-southeast1-bde-at-b3605914-bucket/data'
;

// Creating a new file_format
create or replace file format file_format_csv 
type = 'CSV' 
field_delimiter = ',' 
skip_header = 1
NULL_IF = ('\\N', 'NULL', 'NUL', '')
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
;

// Listing Files in Stage
list @stage_gcp;


// Create a new raw external table
create or replace external table raw.nsw_lga_code
with location = @stage_gcp/NSW_LGA
file_format = file_format_csv
pattern = '.*CODE[.]csv';


// Create a new raw external table
create or replace external table raw.nsw_lga_suburb
with location = @stage_gcp/NSW_LGA
file_format = file_format_csv
pattern = '.*SUBURB[.]csv';

// Create a new raw external table
create or replace external table raw.census_GO1
with location = @stage_gcp/Census_LGA
file_format = file_format_csv
pattern = '.*G01.*.[.]csv';

// Create a new raw external table
create or replace external table raw.census_GO2
with location = @stage_gcp/Census_LGA
file_format = file_format_csv
pattern = '.*G02.*.[.]csv';

// Create a new raw external table
create or replace external table raw.listing
with location = @stage_gcp/listings
file_format = file_format_csv
pattern = '.*[.]csv';



USE WAREHOUSE AIRBNB_WAREHOUSE;



// STAGING 

create or replace schema staging;

// Creating New Table in Stage Layer by adding datatype

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

// Writing the entire thing would take a lot of time, so excel was used
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


// HOST_SINCE IS CONVERTED TO DATE and month_year added from file name
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


// Let's make a star schema in data warehouse

create schema warehouse;

// hash function is used to key create the primary key


// Create a new Dimension
create or replace table warehouse.dimension_lga_suburb as
select 
l_code.lga_code,
l_code.lga_name,
l_suburb.suburb_name
from staging.nsw_lga_code l_code
full join staging.nsw_lga_suburb l_suburb 
on lower(l_code.lga_name) = lower(l_suburb.lga_name);

// create a new dimension
create or replace table warehouse.dimension_census_go2 as
select RIGHT(lga_code, LEN(lga_code) - 3) code ,* from staging.census_go2;

// create a new dimension
create or replace table warehouse.dimension_census_go1 as
select RIGHT(lga_code, LEN(lga_code) - 3) code , * from staging.census_go1;


// create a new dimension
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


// Adding primary and secondary keys to the existing tables

alter table warehouse.dimension_census_go1
add primary key(code);

alter table warehouse.dimension_census_go2
add primary key(code);

alter table warehouse.dimension_host
add primary key(host_hash_key);

alter table warehouse.dimension_property
add primary key(property_hash_key);

alter table warehouse.dimension_review
add primary key(review_hash_key);

alter table warehouse.dimension_lga_suburb
add primary key(lga_code);

alter table warehouse.fact_listing
add primary key(listing_ID);

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




//  DATA MART

create schema datamart;


/*dm_listing_neighbourhood: For each listing_neighbourhood and month_year, the
following properties were calculated:
● Active listings rate
● Minimum, maximum, median and average price for active listings
● Number of distinct hosts
● Superhost rate
● Average of review_scores_rating for active listings
● Percentage change for active listings
● Percentage change for inactive listings
● Total Number of stays
● Average Estimated revenue per active listings
*/


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





/*Per “property_type”, “room_type” ,“accommodates” and “month/year”:
Active listings rate
Minimum, maximum, median and average price for active listings
Number of distinct hosts
Superhost rate 
Average of review_scores_rating for active listings
Percentage change for active listings
Percentage change for inactive listings
Total Number of stays
Average Estimated revenue per active listings
*/



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


/*Per “host_neighbourhood_lga” which is “host_neighbourhood” transformed to an LGA (e.g host_neighbourhood = 'Bondi' then you need to create host_neighbourhood_lga = 'Waverley')  and “month/year”:
Number of distinct host
Estimated Revenue
Estimated Revenue per host (distinct)*/


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
order by month_year, 1
;


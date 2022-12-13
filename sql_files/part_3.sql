// Business Questions


/*
What are the main differences from a population point of view (i.g. higher population of under 30s) between the best performing “listing_neighbourhood” and the worst (in terms of estimated revenue per active listings) over the last 12 months? 

What will be the best type of listing (property type, room type and accommodates for) for the top 5 “listing_neighbourhood” (in terms of estimated revenue per active listing) to have the highest number of stays?

Do hosts with multiple listings are more inclined to have their listings in the same LGA as where they live?

For hosts with a unique listing, does their estimated revenue over the last 12 months can cover the annualised median mortgage repayment of their listing’s “listing_neighbourhood”?
*/




/*
What are the main differences from a population point of view (i.g. higher population of under 30s) between the best performing “listing_neighbourhood” and the worst (in terms of estimated revenue per active listings) over the last 12 months? 
*/


select max(month_year) from warehouse.fact_listing;

select listing_neighbourhood , avg((30-availability_30)*price) Estimated_Revenue
from warehouse.fact_listing
where month_year > dateadd(year, -1, '2021-04-01')
group by listing_neighbourhood
order by 2 desc;

// We can see that Mosman is the best performing listing Neighbourhood by a huge margin, whereas Fairfield generates less revenue


select dls.suburb_name, * from warehouse.dimension_census_go1 dcg
inner join warehouse.dimension_lga_suburb dls
on 'LGA'||dls.lga_code = dcg.lga_code
where lower(dls.suburb_name) in ('mosman', 'fairfield');


// It is evident from the data that fairfield has almost 9 times people than Mosman, but the distribution is not evenly distributed. Under the age of 30s, we can see a 10:1 ratio between population of the fairfield and mosman. but as the population age increases, the ratio decreases to 6:1. Indicating that mosman has more people in their later stages of life hence they are richer and the revenue is higher.




/*
What will be the best type of listing (property type, room type and accommodates for) for the top 5 “listing_neighbourhood” (in terms of estimated revenue per active listing) to have the highest number of stays?
*/

// Some set of listing (property type, room type and accommodates for) has same number of maximum stays all of them are counted in the list. For more specific questions could be answered.

select * from (select 
listing_neighbourhood,
property_type,
room_type,
accommodates
revenue,
revenue/accommodates revenue_per_person,
rank() over(partition by  listing_neighbourhood order by revenue/accommodates desc) frk
from (
    select 
    listing_neighbourhood,
    property_type,
    room_type,
    accommodates,
    max(30-availability_30) highest_stay,
    avg((30-availability_30)*price) revenue,
    rank() over(partition by  listing_neighbourhood order by max(30-availability_30) desc) rk

    from warehouse.fact_listing fl inner join warehouse.dimension_property dp
    on fl.property_hash_key = dp.property_hash_key
    where listing_neighbourhood in (select top 5 listing_neighbourhood
        from warehouse.fact_listing
    group by 1
    order by avg((30-availability_30)*price) desc)
    group by listing_neighbourhood, property_type, room_type, accommodates
    order by listing_neighbourhood, property_type, room_type, accommodates

)
where rk = 1)
where frk = 1;



select 
    listing_neighbourhood,
    property_type,
    room_type,
    accommodates,
    max(30-availability_30) highest_stay,
    rank() over(partition by  listing_neighbourhood order by max(30-availability_30) desc) rk

    from warehouse.fact_listing fl inner join warehouse.dimension_property dp
    on fl.property_hash_key = dp.property_hash_key
    where listing_neighbourhood in (select top 5 listing_neighbourhood
        from warehouse.fact_listing
    group by 1
    order by avg((30-availability_30)*price) desc);


select * from datamart.dm_listing_neighbourhood;

select * from warehouse.fact_listing fl inner join warehouse.dimension_property dp 
on fl.property_hash_key = dp.property_hash_key
where listing_neighbourhood in (select top 5 listing_neighbourhood
from warehouse.fact_listing
group by listing_neighbourhood
order by avg((30-availability_30)*price) desc);




// Do hosts with multiple listings are more inclined to have their listings in the same LGA as where they live?

select 'All Distinct Host' Condition,count(distinct host_id) value from warehouse.fact_listing
union
select 'Distinct Host with more than 1 listing',count(*) from (select host_id from warehouse.fact_listing
group by host_id
having count(listing_id) > 1)
union
select 'Distinct Host with more than 2 listing',count(*) from (select host_id from warehouse.fact_listing
group by host_id
having count(listing_id) > 2)
union
select 'Distinct Host with more than 3 listing',count(*) from (select host_id from warehouse.fact_listing
group by host_id
having count(listing_id) > 3)
union
select 'Number of Listing Neighbourhood when host has more than 1 listing',count(*) from (
select 
host_id,
listing_neighbourhood,
count(listing_id)
from warehouse.fact_listing
group by host_id, listing_neighbourhood
having count(listing_id) > 1
order by host_id, listing_neighbourhood, 3 desc
);


// Yes


// For hosts with a unique listing, does their estimated revenue over the last 12 months can cover the annualised median mortgage repayment of their listing’s “listing_neighbourhood”?


select max(month_year) from warehouse.fact_listing;

// Host with Unique Listing
select host_id
from warehouse.fact_listing
group by host_id
having count(listing_id) = 1;

select host_id, listing_neighbourhood
from warehouse.fact_listing fl 
full outer join warehouse.dimension_lga_suburb dls on lower(fl.listing_neighbourhood) = lower(dls.suburb_name)
full outer join warehouse.dimension_census_go2 dcg on dcg.lga_code = 'LGA'||dls.lga_code
where fl.host_id in (
    select host_id
    from warehouse.fact_listing
    group by host_id
    having count(listing_id) =1
);

select 
fl.host_id, fl.listing_neighbourhood, revenue, 
iff(12*avg(dcg.median_mortgage_repay_monthly) < revenue, TRUE, FALSE) Revenue_greater_than_mortgage 
from
(select 
host_id,
listing_neighbourhood,
sum((30-availability_30)*price) revenue
from warehouse.fact_listing
where host_id in (select host_id from warehouse.fact_listing group by host_id having count(listing_id) =1) and month_year >= dateadd(year,-1,'2022-04-01')
group by host_id, listing_neighbourhood) fl
left join warehouse.dimension_lga_suburb dls on lower(fl.listing_neighbourhood) = lower(dls.lga_name)
left join warehouse.dimension_census_go2 dcg on dcg.lga_code = 'LGA'||dls.lga_code
group by fl.host_id, fl.listing_neighbourhood, fl.revenue 
order by fl.host_id;

// The Answer is No.

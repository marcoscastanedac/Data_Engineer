select count(*) 
from car_rental_analytics cra;
--- 4905 total de registros

select *
from car_rental_analytics cra;

--- a)
select count(*)
from car_rental_analytics cra
where fueltype = 'hybrid' or fueltype = 'electric'
and rating >= 4;
--- 771

--- b)
select cra.state_name,
sum(cra.rentertripstaken) as renter
from car_rental_analytics cra 
group by cra.state_name
order by renter asc
limit 5;

--- c)
select 
cra.model,
cra.make,
count(cra.rentertripstaken) as renter
from car_rental_analytics cra 
group by cra.model,cra.make
order by renter desc
limit 10;

--- d)
select 
cra.`year`,
count(cra.rentertripstaken) as renter
from car_rental_analytics cra 
where cra.year BETWEEN (2010) and (2015)
group by cra.`year` ;

--- e)
select cra.city ,
count(cra.rentertripstaken) as renter
from car_rental_analytics cra 
where cra.fueltype = 'hybrid' or cra.fueltype = 'electric'
group by cra.city
order by renter desc
limit 5;

--- f)
select 
cra.fueltype,
round(avg(cra.reviewcount),2) as review 
from car_rental_analytics cra 
GROUP BY cra.fueltype
order by review desc;

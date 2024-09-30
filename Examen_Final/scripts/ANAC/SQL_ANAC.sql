select count(*) from airport_details ;
--- 693
select count(*) from flights ;
--- 549822 total de registros
--- 481891 con el filtro

--- 6
select count(*) 
from flights 
where fecha between DATE('2021-12-01') and DATE('2022-01-31');
--- 57,915

--- 7
select sum(pasajeros) 
from flights 
where aerolinea_nombre  = 'AEROLINEAS ARGENTINAS SA'
and fecha between DATE('2021-01-01') and DATE('2022-06-30');
--- 7,483,736


--- 8
select 
f.fecha,
f.horautc as hora,
f.aeropuerto as codigo_aeropuerto_salida,
ad_salida.ref  as ciudad_de_salida,
f.origen_destino as codigo_aeropuerto_arribo,
ad_arribo.ref as ciudad_de_arribo,
f.pasajeros as cantidad_pasajeros
from flights f
inner join airport_details ad_salida on (f.aeropuerto = ad_salida.aeropuerto)
inner join airport_details ad_arribo on (f.origen_destino = ad_arribo.aeropuerto)
where f.fecha between DATE('2022-01-01') and DATE('2022-06-30')
order by f.fecha desc;


--- 9 
SELECT  
f.aerolinea_nombre,
sum(pasajeros) as pasajeros
FROM airport_details ad 
inner join flights f on (ad.aeropuerto = f.aeropuerto)
where f.fecha between DATE('2021-01-01') and DATE('2022-06-30')
and f.aerolinea_nombre <> '0' 
group by f.aerolinea_nombre
order by pasajeros desc
limit 10;


--- 10 
SELECT  
f.aeronave,
count(f.pasajeros) as total_vuelos
FROM flights f
inner join airport_details ad on (f.aeropuerto = ad.aeropuerto)
where f.fecha between DATE('2021-01-01') and DATE('2022-06-30')
and (ad.provincia = 'CIUDAD AUTONOMA DE BUENOS AIRES' or ad.provincia = 'BUENOS AIRES')  
and f.aeronave <> '0'
group by f.aeronave
order by total_vuelos desc
limit 10;
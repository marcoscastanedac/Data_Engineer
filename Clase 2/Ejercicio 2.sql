
--- 1. Obtener el promedio de precios por cada categoría de producto. La cláusula
--- OVER(PARTITION BY CategoryID) específica que se debe calcular el promedio de
--- precios por cada valor único de CategoryID en la tabla

select c.category_name, p.product_name, p.unit_price ,
avg(p.unit_price) over (partition by c.category_id) as price_by_category 
from products p   
inner join categories c on p.category_id = p.category_id ;

--- 2. Obtener el promedio de venta de cada cliente

select avg(od.unit_price * od.quantity) over (partition by od.order_id) as avg_order_amount,
o.order_id,o.customer_id, e.employee_id,o.order_date ,o.required_date ,o.shipped_date
from customers c 
inner join orders o on c.customer_id = o.customer_id 
inner join order_details od on o.order_id = od.order_id 
inner join employees e on o.employee_id = e.employee_id 
order by 3;

--- 3.Obtener el promedio de cantidad de productos vendidos por categoría (product_name,
--- quantity_per_unit, unit_price, quantity, avgquantity) y ordenarlo por nombre de la
--- categoría y nombre del producto 

select p.product_name, c2.category_name,p.quantity_per_unit ,p.unit_price,od.quantity ,
avg(od.quantity) over (partition by c2.category_id) as avg_order_amount
from customers c 
inner join orders o on c.customer_id = o.customer_id 
inner join order_details od on o.order_id = od.order_id
inner join products p on od.product_id  = p.product_id
inner join categories c2 on p.category_id = c2.category_id 
order by c2.category_name ,p.product_name;

--- 4. Selecciona el ID del cliente, la fecha de la orden y la fecha más antigua de la
--- orden para cada cliente de la tabla 'Orders'.

select c.customer_id,o.order_date,
min(o.order_date) over (partition by c.customer_id) as earliestorderdate
from customers c 
inner join orders o on c.customer_id = o.customer_id 

--- 5.Seleccione el id de producto, el nombre de producto, el precio unitario, el id de
--- categoría y el precio unitario máximo para cada categoría de la tabla Products.

select p.product_id,p.product_name,p.unit_price,c.category_id,
max(p.unit_price) over (partition by p.category_id) as maxunitprice
from products p   
inner join categories c on p.category_id = p.category_id  ;


--- 6.Obtener el ranking de los productos más vendidos

select distinct
row_number() over (order by sum(od.quantity) desc) as ranking,
p.product_name,
sum(od.quantity) as totalquantity
from products p
inner join order_details od on p.product_id = od.product_id
group by p.product_name
order by ranking;

--- 7.Asignar numeros de fila para cada cliente, ordenados por customer_id

select 
row_number () over (order by c.customer_id) as rownumber,
c.customer_id,c.company_name,c.contact_name,c.contact_title,c.address 
from customers c ;

--- 8.Obtener el ranking de los empleados más jóvenes () ranking, nombre y apellido del empleado, fecha de nacimiento)

select 
row_number () over (order by e.birth_date desc ) as ranking,
e.first_name ||' '|| e.last_name as employename,
e.birth_date 
from employees e;

--- 9.Obtener la suma de venta de cada cliente

select 
sum (od.unit_price) over (partition by o.customer_id) as sumsales,
o.order_id,o.customer_id,o.employee_id,o.order_date,o.required_date 
from  orders o  
inner join order_details od on o.order_id = od.order_id;

--- 10.Obtener la suma total de ventas por categoría de producto

select c.category_name ,p.product_name,p.unit_price,od.quantity,
sum (od.unit_price) over (partition by c.category_name order by p.product_name ) as totalsales
from products p  
inner join categories c on p.category_id = c.category_id 
inner join order_details od on p.product_id = od.product_id ;

--- 11.Calcular la suma total de gastos de envío por país de destino, luego ordenarlo por país y por orden de manera ascendente

select o.ship_country as country ,o.order_id ,o.shipped_date ,o.freight,
sum (o.freight) over (partition by o.ship_country) as totalsales
from orders o  
order by o.ship_country asc, o.order_id asc;


select 
    o.ship_country as country,
    o.order_id,
    o.shipped_date,
    o.freight,
    sum(o.freight) over (partition by o.ship_country) as total_freight_by_country
from orders o
where o.shipped_date is not null
order by o.ship_country asc, o.order_id asc;

--- 12.Ranking de ventas por cliente

select c.customer_id, c.company_name ,
sum (od.unit_price * od.quantity) as totalsales,
rank () over (order by sum (od.unit_price * od.quantity)  desc) as Rank
from customers c 
inner join orders o on c.customer_id = o.customer_id 
inner join order_details od on o.order_id = od.order_id 
group by c.customer_id,c.company_name
order by Rank;

--- 13.Ranking de empleados por fecha de contratacion

select e.employee_id,e.first_name ,e.last_name ,e.hire_date ,
rank () over (order by e.hire_date) as Rank
from employees e ;

--- 14.Ranking de productos por precio unitario

select p.product_id , p.product_name ,p.unit_price ,
rank () over (order by p.unit_price desc)
from products p ;


--- 15.Mostrar por cada producto de una orden, la cantidad vendida y la cantidad vendida del producto previo.

select o.order_id,p.product_id ,od.quantity ,
lag (od.quantity) over (order by o.order_id ) as prevquaintity
from orders o 
inner join order_details od on o.order_id = od.order_id
inner join products p on p.product_id = od.product_id ;

--- 16.Obtener un listado de ordenes mostrando el id de la orden, fecha de orden, id del cliente y última fecha de orden.

select o.order_id,o.order_date,o.customer_id,
lag (o.order_date) over (partition by o.customer_id order by o.order_date) as lastorder
from customers c 
inner join orders o on c.customer_id = o.customer_id ;


--- 17.Obtener un listado de productos que contengan: id de producto, nombre del producto,
--- precio unitario, precio del producto anterior, diferencia entre el precio del producto y
--- precio del producto anterior.

select p.product_id, p.product_name ,p.unit_price ,
lag (p.unit_price) over (order by p.product_id )  as lastunitprice,
p.unit_price - lag (p.unit_price) over (order by p.product_id ) as pricedifference
from products p ;

--- 18.Obtener un listado que muestra el precio de un producto junto con el precio del producto siguiente:

select p.product_name ,p.unit_price ,
lead (p.unit_price) over (order by p.product_id)
from products p ;


--- 19.Obtener un listado que muestra el total de ventas por categoría de producto junto con el total de ventas de la categoría siguiente

select c.category_name ,
sum (p.unit_price * od.quantity) as totalsales,
lead(sum(p.unit_price * od.quantity)) over (order by c.category_name ) as nexttotalsales
from products p 
inner join categories c on p.category_id = c.category_id 
inner join order_details od on p.product_id = od.product_id 
group by c.category_name ;







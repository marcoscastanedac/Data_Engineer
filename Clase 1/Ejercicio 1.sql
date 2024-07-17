--- BDE
--- Autor: Marcos Jovani Castañeda Castañon
--- Ejercicio SQL Clase 1
--- Date: 15/07/2024

-- 1.Obtener una lista de todas las categorías distintas: 
select distinct category_name 
from categories;

-- 2.Obtener una lista de todas las regiones distintas para los clientes
select distinct region 
from customers;

-- 3.Obtener una lista de todos los títulos de contacto distintos
select distinct contact_title 
from customers ;

-- 4.Obtener una lista de todos los clientes, ordenados por país
select *
from customers
order by country;

-- 5.Obtener una lista de todos los pedidos, ordenados por id del empleado y fecha del pedido
select * 
from orders
order by order_id, order_date; 

-- 6.Insertar un nuevo cliente en la tabla Customers
INSERT INTO customers
(customer_id, company_name, contact_name, contact_title, address, city, region, postal_code, country, phone, fax)
VALUES('MJCC', 'FIFA', 'Marcos Castañeda', 'IT', 'Zapotlan 22', 'CDMX', 'Coyoacan', '04369', 'Mexico', '5555555500', NULL);

-- 7.Insertar una nueva región en la tabla Región
INSERT INTO region
(region_id, region_description)
VALUES(5, 'Northeast');

-- 8.Obtener todos los clientes de la tabla Customers donde el campo Región es NULL
select *
from customers 
where region is null;

-- 9.Obtener Product_Name y Unit_Price de la tabla Products, y si Unit_Price es NULL, use el precio estándar de $10 en su lugar:
select product_name, 
case when unit_price is null
then 10
else unit_price end 
from products;

-- validacion
select product_name, 
case when unit_price = 10
then 100
else unit_price end 
from products;

-- 10.Obtener el nombre de la empresa, el nombre del contacto y la fecha del pedido de todos los pedidos
select c.company_name, c.contact_name, o.order_date 
from customers c 
left join orders o on c.customer_id = o.customer_id;
--- 833, 3 resgitros null

select c.company_name, c.contact_name, o.order_date 
from customers c 
inner join orders o on c.customer_id = o.customer_id;
--- 830 registros

--- 11.Obtener la identificación del pedido, el nombre del producto y el descuento de todos los detalles del pedido y productos
select o.order_id , p.product_name , od.discount 
from orders o 
inner join order_details od on o.order_id = od.order_id 
inner join products p on od.product_id = p.product_id 

--- 12.Obtener el identificador del cliente, el nombre de la compañía, el identificador y la fecha de la orden de todas las órdenes y aquellos clientes que hagan match
select c.customer_id , c.company_name , o.order_id , o.order_date 
from customers c 
inner join orders o on c.customer_id  = o.customer_id; 

--- 13.Obtener el identificador del empleados, apellido, identificador de territorio y descripción del territorio de todos los empleados y aquellos que hagan match en territorios
select e.employee_id , e.last_name , et.territory_id, t.territory_description 
from employees e 
left join employee_territories et on e.employee_id = et.employee_id
inner join territories t on et.territory_id  = t.territory_id ;

--- 14.Obtener el identificador de la orden y el nombre de la empresa de todos las órdenes y aquellos clientes que hagan match
select o.order_id , c.company_name 
from customers c 
inner join orders o on c.customer_id = o.customer_id 

--- 15.Obtener el identificador de la orden, y el nombre de la compañía de todas las órdenes y aquellos clientes que hagan match
select o.order_id , c.company_name 
from customers c 
right join orders o on c.customer_id = o.customer_id 

--- 16.Obtener el nombre de la compañía, y la fecha de la orden de todas las órdenes y aquellos transportistas que hagan match. Solamente para aquellas ordenes del año 1996
select  s.company_name , o.order_date 
from customers c 
left join orders o on c.customer_id  = o.customer_id 
inner join shippers s on o.ship_via  = s.shipper_id 
where o.order_date between TO_DATE('1996-01-01', 'YYYY-MM-DD') and TO_DATE('1996-12-31', 'YYYY-MM-DD');

--- 17.Obtener nombre y apellido del empleados y el identificador de territorio, de todos los empleados y aquellos que hagan match o no de employee_territories:
select e.first_name , e.last_name , et.territory_id, t.territory_description 
from employees e 
full outer join employee_territories et on e.employee_id = et.employee_id
full outer join territories t on et.territory_id  = t.territory_id ;

--- 18.Obtener el identificador de la orden, precio unitario, cantidad y total de todas las órdenes y aquellas órdenes detalles que hagan match o no:
--- Nota, no esta muy claro el total de todas las ordenes para ese cliente o ese order_id, o el precio total de la cantidad por el precio

select od.order_id , od.unit_price , od.quantity , (od.unit_price * od.quantity) AS total
from orders o 
full outer join order_details od on o.order_id = od.order_id 
--where o.order_id  = 10248
group by od.order_id , od.unit_price , od.quantity , od.unit_price ;

--- 19.Obtener la lista de todos los nombres de los clientes y los nombres de los proveedores
select c.contact_name 
from customers c 
union all
select s.contact_name
from suppliers s ;

--- 20.Obtener la lista de los nombres de todos los empleados y los nombres de los gerentes de departamento
select e.first_name 
from employees e 
union all
select s.contact_name
from suppliers s  
where s.contact_title like '%Manager%'

--- 21.Obtener los productos del stock que han sido vendidos
select distinct p.product_name , p.product_id 
from order_details od 
inner join products p on od.product_id = p.product_id
order by p.product_id ;

--- 22.Obtener los clientes que han realizado un pedido con destino a Argentina
select distinct c.company_name 
from customers c 
left join orders o on c.customer_id  = o.customer_id 
where o.ship_country  = 'Argentina';

--- 23.Obtener el nombre de los productos que nunca han sido pedidos por clientes de Francia:
select distinct p.product_name 
from customers c 
inner join orders o on c.customer_id  = o.customer_id 
inner join order_details od on o.order_id = od.order_id 
inner join products p on od.product_id  = p.product_id 
where c.country <> 'France';

--- 24.Obtener la cantidad de productos vendidos por identificador de orden

select distinct od.order_id , sum(od.quantity )
from customers c 
inner join orders o on c.customer_id  = o.customer_id 
inner join order_details od on o.order_id = od.order_id 
inner join products p on od.product_id  = p.product_id 
group by od.order_id
order by 1;

--- 25.Obtener el promedio de productos en stock por producto

select distinct p.product_name , avg(p.units_in_stock )
from order_details od 
inner join products p on od.product_id  = p.product_id 
group by p.product_name
order by 1;

--- 26.Cantidad de productos en stock por producto, donde haya más de 100 productos en stock
select distinct p.product_name , p.units_in_stock
from order_details od 
inner join products p on od.product_id  = p.product_id 
where p.units_in_stock > 100;

-- Cantidad de productos en cada orden, donde haya mas de 100 productos en sock
SELECT p.product_name, SUM(od.quantity) AS total_solicitados,p.units_in_stock as in_stock
FROM order_details od
INNER JOIN products p ON od.product_id = p.product_id
GROUP BY p.product_name,p.units_in_stock
HAVING SUM(od.quantity) > 100;

--- 27.Obtener el promedio de pedidos por cada compañía y solo mostrar aquellas con un promedio de pedidos superior a 10
SELECT c.company_name , avg(od.order_id) 
FROM customers c 
inner join orders o on c.customer_id  = o.customer_id 
inner join order_details od on o.order_id = od.order_id 
GROUP BY c.company_name
HAVING avg(od.order_id) > 10;


--- 28.Obtener el nombre del producto y su categoría, pero muestre "Discontinued" en lugar del nombre de la categoría si el producto ha sido descontinuado
SELECT p.product_name ,
	case when p.discontinued = 1 then 'Discontinued'
	else c.category_name
	end product_category
from products p
inner join categories c on p.category_id  = c.category_id ;

--- 29.Obtener el nombre del empleado y su título, pero muestre "Gerente de Ventas" en lugar del título si el empleado es un gerente de ventas (Sales Manager):
select e.first_name , e.last_name ,
	case when e.title = 'Sales Manager' then 'Gerente de Ventas'
	else e.title
	end job_title
from employees e ;




















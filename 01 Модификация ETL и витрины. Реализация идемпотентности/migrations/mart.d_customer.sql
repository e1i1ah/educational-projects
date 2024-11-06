
with update_orders as (
  select uol.customer_id as uo_customer_id, uol.city_id as uo_city_id
  from (select customer_id, first_name, last_name, max(city_id) as city_id
       from staging.user_order_log
       group by customer_id, first_name, last_name) uol join mart.d_customer dc on uol.customer_id = dc.customer_id 
  where uol.city_id != dc.city_id
)

update mart.d_customer dc
set city_id = uo_city_id
from update_orders uo
where customer_id = uo_customer_id;


with insert_orders as (
  select uol.customer_id, uol.first_name, uol.last_name, uol.city_id
  from (select customer_id, first_name, last_name, max(city_id) as city_id
       from staging.user_order_log
       group by customer_id, first_name, last_name) uol left join mart.d_customer dc on uol.customer_id = dc.customer_id 
  where dc.customer_id is null
)

insert into mart.d_customer (customer_id, first_name, last_name, city_id)
select customer_id, first_name, last_name, city_id 
from insert_orders;

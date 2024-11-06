with ranked_date_update_orders as (
  select city_id, city_name, dense_rank() over(partition by city_id order by date_time desc) as rnk
  from staging.user_order_log
), 
update_orders as (
  select uol.city_id as uo_city_id, uol.city_name as uo_city_name
  from (select city_id, city_name 
       from ranked_date_update_orders
       where rnk = 1
       group by city_id, city_name) uol join mart.d_city dc on dc.city_id = uol.city_id
  where dc.city_name != uol.city_name
)

update mart.d_city dc
set city_name = uo_city_name 
from update_orders uo
where city_id = uo_city_id;



with ranked_date_insert_orders as (
  select city_id, city_name, dense_rank() over(partition by city_id order by date_time desc) as rnk
  from staging.user_order_log
), 
insert_orders as (
  select uol.city_id, uol.city_name
  from (select city_id, city_name
       from ranked_date_insert_orders
       where rnk = 1
       group by city_id, city_name) uol left join mart.d_city dc on dc.city_id = uol.city_id
  where dc.city_id is null
)

insert into mart.d_city (city_id, city_name)
select city_id, city_name 
from insert_orders;

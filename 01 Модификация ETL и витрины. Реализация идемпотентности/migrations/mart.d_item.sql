with ranked_date_update_orders as (
    select item_id, item_name, dense_rank() over(partition by item_id order by date_time desc) as rnk
    from staging.user_order_log
), 
update_orders as (
  select uol.item_id as uo_item_id, uol.item_name as uo_item_name
  from (select item_id, item_name
        from ranked_date_update_orders
        where rnk = 1
        group by item_id, item_name) uol join mart.d_item di on di.item_id = uol.item_id
  where di.item_name != uol.item_name
)

update mart.d_item di
set item_name = uo_item_name
from update_orders uo
where item_id = uo_item_id;


with ranked_date_insert_orders as (
    select item_id, item_name, dense_rank() over(partition by item_id order by date_time desc) as rnk
    from staging.user_order_log
), 
insert_orders as (
  select uol.item_id, uol.item_name
  from (select item_id, item_name
       from ranked_date_insert_orders
       where rnk = 1
       group by item_id, item_name) uol left join mart.d_item di on di.item_id = uol.item_id
  where di.item_id is null
)

insert into mart.d_item (item_id, item_name)
select item_id, item_name
from insert_orders;

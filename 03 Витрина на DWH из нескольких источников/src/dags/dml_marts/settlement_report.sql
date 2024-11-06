insert into cdm.dm_settlement_report (restaurant_id, 
									  restaurant_name, 
									  settlement_date, 
									  orders_count, 
									  orders_total_sum, 
									  orders_bonus_payment_sum, 
									  orders_bonus_granted_sum, 
									  order_processing_fee, 
									  restaurant_reward_sum)
select rest.restaurant_id as restaurant_id, 
	   rest.restaurant_name as restaurant_name, 
	   ts.date as settlement_date,
	   count(distinct ord.id) as orders_count,
	   sum(fct.total_sum) as orders_total_sum,
	   sum(fct.bonus_payment) as orders_bonus_payment_sum,
	   sum(fct.bonus_grant) as orders_bonus_granted_sum,
	   0.25 * sum(fct.total_sum) as order_processing_fee,
	   sum(fct.total_sum) * 0.75 - sum(fct.bonus_payment) as restaurant_reward_sum
from dds.fct_product_sales as fct 
	 join dds.dm_products as prod on fct.product_id = prod.id
	 join dds.dm_orders as ord on fct.order_id = ord.id
	 join dds.dm_restaurants as rest on prod.restaurant_id = rest.id and ord.restaurant_id = rest.id
	 join dds.dm_timestamps as ts on ord.timestamp_id = ts.id
	 join dds.dm_users as us on ord.user_id = us.id
where ord.order_status = 'CLOSED'
group by rest.restaurant_id, rest.restaurant_name, ts.date  
on conflict (restaurant_id, settlement_date)
do update set orders_count = excluded.orders_count,
			  orders_total_sum = excluded.orders_total_sum,
			  orders_bonus_payment_sum = excluded.orders_bonus_payment_sum,
			  orders_bonus_granted_sum = excluded.orders_bonus_granted_sum,
			  order_processing_fee = excluded.order_processing_fee,
			  restaurant_reward_sum = excluded.restaurant_reward_sum
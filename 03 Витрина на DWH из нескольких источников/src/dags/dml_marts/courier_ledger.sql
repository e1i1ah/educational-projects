with orders_total_sum as (
	select dmo.id as order_id,
	       dmc.id as courier_id,
	       dmo.timestamp_id,
		   sum(total_sum) as order_total_sum
	from dds.dm_orders dmo
		 join dds.fct_product_sales fps on dmo.id = fps.order_id
		 join dds.dm_delivery dmd on dmo.id = dmd.order_id
		 join dds.dm_couriers dmc on dmc.id = dmd.courier_id
	where dmo.order_status = 'CLOSED'	
	group by dmo.id, dmc.id, dmo.timestamp_id
),

courier_avg_year_month as (
	select dmc.id as courier_id, 
		   dmc.courier_name,
		   dmt.year, 
		   dmt.month,
		   avg(dmd.rate) as avg_rate
	from dds.dm_couriers dmc 
		 join dds.dm_delivery dmd on dmc.id = dmd.courier_id
		 join dds.dm_orders dmo on dmd.order_id = dmo.id
		 join dds.dm_timestamps dmt on dmo.timestamp_id = dmt.id
	group by dmc.id, dmc.courier_name, dmt.year, dmt.month	 
),

result_for_mart as (
	select ots.order_id,
		   cr.courier_id,
		   cr.courier_name,
		   cr.year,
		   cr.month,
		   cr.avg_rate,
		   ots.order_total_sum,
		   case when cr.avg_rate < 4 then 
	   		    (case when (0.05 * ots.order_total_sum) < 100 then 100 else (0.05 * ots.order_total_sum) end) 
	   			when 4 <= cr.avg_rate and cr.avg_rate < 4.5 then 
	   			(case when (0.07 * ots.order_total_sum) < 150 then 150 else (0.07 * ots.order_total_sum) end)
	   			when 4.5 <= cr.avg_rate and cr.avg_rate < 4.9 then 
	   			(case when (0.08 * ots.order_total_sum) < 175 then 175 else (0.08 * ots.order_total_sum) end)
	   			when 4.9 <= cr.avg_rate then 
	   			(case when (0.1 * ots.order_total_sum) < 200 then 200 else (0.1 * ots.order_total_sum) end)
	   			end as courier_order_sum
	from orders_total_sum ots 
		 join dds.dm_timestamps dmt on ots.timestamp_id = dmt.id
		 join courier_avg_year_month cr on ots.courier_id = cr.courier_id and dmt.year = cr.year and dmt.month = cr.month	
)

insert into cdm.dm_courier_ledger (courier_id,
								   courier_name,
								   settlement_year,
								   settlement_month,
								   orders_count,
								   orders_total_sum,
								   rate_avg,
								   order_processing_fee,
								   courier_order_sum,
								   courier_tips_sum,
								   courier_reward_sum)
select rfm.courier_id,
	   rfm.courier_name,
	   rfm.year,
	   rfm.month,
	   count(rfm.order_id),
	   sum(rfm.order_total_sum),
	   avg(dmd.rate),
	   sum(rfm.order_total_sum) * 0.25,
	   sum(rfm.courier_order_sum),
	   sum(dmd.tip_sum),
	   sum(rfm.courier_order_sum) + (sum(dmd.tip_sum) * 0.95)
	   
	   
from result_for_mart rfm
	 join dds.dm_delivery dmd on rfm.order_id = dmd.order_id 
group by rfm.courier_id, rfm.courier_name, rfm.year, rfm.month
on conflict (courier_id, settlement_year, settlement_month)
do update set orders_count = excluded.orders_count,
			  orders_total_sum = excluded.orders_total_sum,
			  rate_avg = excluded.rate_avg,
			  order_processing_fee = excluded.order_processing_fee,
			  courier_order_sum = excluded.courier_order_sum,
			  courier_tips_sum = excluded.courier_tips_sum,
			  courier_reward_sum = excluded.courier_reward_sum
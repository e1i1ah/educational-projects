insert into mart.f_customer_retention (item_id, period_id, new_customers_count, returning_customers_count, refunded_customer_count, period_name, new_customers_revenue, returning_customers_revenue, customers_refunded)
select item_id,
	   week_of_year,
       sum(case when orders_count = 1 then 1 else 0 end) as new_customers_count,
       sum(case when orders_count > 1 then 1 else 0 end) as returning_customers_count,
       sum(case when sum_refunded > 0 then 1 else 0 end) as refunded_customer_count,
       'weekly' as period_name,
       sum(case when orders_count = 1 then payment_amount else 0 end) as new_customers_revenue,
       sum(case when orders_count > 1 then payment_amount else 0 end) as returning_customers_revenue,
       sum(sum_refunded) as customers_refunded
from (select item_id, customer_id, week_of_year, count(*) as orders_count, sum(case when status = 'refunded' then 1 else 0 end) as sum_refunded, sum(payment_amount) as payment_amount
	  from mart.d_calendar dc join mart.f_sales mfs on dc.date_id = mfs.date_id
	  group by item_id, customer_id, week_of_year) t1
group by item_id, week_of_year


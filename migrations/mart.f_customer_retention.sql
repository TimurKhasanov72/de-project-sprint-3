WITH 
t1 AS (
	SELECT 
		item_id,
		customer_id,
		COUNT(uniq_id) as orders_cnt,
		SUM(payment_amount) as payment_amount,
		COUNT(CASE WHEN status = 'refunded' THEN 1 END) as refunded_cnt
	FROM 
		staging.user_order_log uol
	WHERE  uol.date_time::date between '{{ds}}'::date - INTERVAL '7 days' and '{{ds}}'
	GROUP BY item_id , customer_id  
)
insert into mart.f_customer_retention (
	item_id, 
	period_name,
	period_id,
	new_customers_count,
	returning_customers_count,
	refunded_customer_count,
	new_customers_revenue,
	returning_customers_revenue,
	customers_refunded
)
SELECT -- запрос в разрезе item_id, customer_id
	  item_id,
  	  'weekly' as period_name,
  	  DATE_PART('week', DATE '{{ds}}'::date - INTERVAL '7 days') as period_id,
	  SUM(CASE WHEN orders_cnt = 1 THEN 1 ELSE 0 END) as new_customers_count,
	  SUM(CASE WHEN orders_cnt > 1 THEN 1 ELSE 0 END) as returning_customers_count,
  	  SUM(CASE WHEN refunded_cnt > 0 THEN 1 ELSE 0 END) as refunded_customer_count,
	  SUM(CASE WHEN orders_cnt = 1 THEN payment_amount ELSE 0 END) as new_customers_revenue,
	  SUM(CASE WHEN orders_cnt > 1 THEN payment_amount ELSE 0 END) as returning_customers_revenue,
	  SUM(refunded_cnt) as customers_refunded	
FROM t1
GROUP BY t1.item_id
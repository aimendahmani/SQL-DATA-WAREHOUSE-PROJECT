select * from gold.fact_sales sl
left join gold.dim_customer cst 
on sl.customer_sk = cst.customer_sk
left join gold.dim_product prd 
on sl.product_sk = prd.product_sk
where cst.customer_sk is null or prd.product_sk is null
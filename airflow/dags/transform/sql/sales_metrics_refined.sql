select t1.nm_city,
	t2.nm_description,
	count(distinct t1.id_transaction) as vl_sales,
	count(distinct t1.id_customer) as vl_active_customers,
	sum(t2.vl_price) as vl_revenue,
	'{reference_date}' dt_created_at 
from postgres_scan('host=postgres_hungry user={postgres_user} password={postgres_password} port=5432 dbname=outhhungry_trusted', 'public', 'transactions') as t1
left join postgres_scan('host=postgres_hungry user={postgres_user} password={postgres_user} port=5432 dbname=outhhungry_trusted', 'public', 'products') as t2 
on t1.id_product = t2.id_product 
group by t1.nm_city,
	t2.nm_description
order by t2.nm_description,
	vl_sales;
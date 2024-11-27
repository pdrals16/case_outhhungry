INSERT INTO sales_metrics (nm_city, nm_description, vl_sales, vl_active_customers, vl_revenue, dt_created_at)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (nm_city, nm_description, dt_created_at) DO UPDATE
SET
    vl_sales = EXCLUDED.vl_sales,
    vl_active_customers = EXCLUDED.vl_active_customers,
    vl_revenue = EXCLUDED.vl_revenue;
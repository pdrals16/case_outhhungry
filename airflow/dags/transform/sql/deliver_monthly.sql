SELECT * 
FROM {table} 
WHERE dt_created_at >= '{reference_date}' 
AND dt_created_at < '{reference_date}'::timestamp + interval '1 month'
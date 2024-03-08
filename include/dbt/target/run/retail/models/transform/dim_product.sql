
  
    

    create or replace table `gcp-retail-data-eng-proj-no3`.`retail`.`dim_product`
      
    
    

    OPTIONS()
    as (
      -- dim_product.sql
-- StockCode isn't unique, a product with the same id can have different and prices
-- Create the dimension table
SELECT DISTINCT
    to_hex(md5(cast(coalesce(cast(StockCode as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(Description as string), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(UnitPrice as string), '_dbt_utils_surrogate_key_null_') as string))) as product_id,
		StockCode AS stock_code,
    Description AS description,
    UnitPrice AS price
FROM `gcp-retail-data-eng-proj-no3`.`retail`.`raw_invoices`
WHERE StockCode IS NOT NULL
AND UnitPrice > 0
    );
  
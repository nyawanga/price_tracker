WITH jumia AS (
    SELECT * FROM {{ source("price_tracker", "jumia") }}
), 
stg_brands AS (
    SELECT 
        DISTINCT trim(brand) AS brand, md5(trim(brand)) AS brand_hash
    FROM jumia
    WHERE item_url IS NOT NULL
    AND created_at >= (SELECT MAX(DATE(created_at)) FROM jumia) 
)
SELECT * FROM stg_brands

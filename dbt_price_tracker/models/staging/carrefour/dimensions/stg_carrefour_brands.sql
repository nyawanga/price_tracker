WITH carrefour AS (
    SELECT * FROM {{ source("price_tracker", "carrefour") }}
), 
stg_brands AS (
    SELECT 
        DISTINCT trim(brand) AS brand, md5(trim(brand)) AS brand_hash
    FROM carrefour
    WHERE item_url IS NOT NULL
    AND created_at >= (SELECT MAX(DATE(created_at)) FROM carrefour) 
)
SELECT * FROM stg_brands

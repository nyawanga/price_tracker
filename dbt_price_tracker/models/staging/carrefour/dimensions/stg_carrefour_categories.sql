
WITH carrefour AS (
    SELECT * FROM {{ source("price_tracker", "carrefour") }}
),
categories AS (
    SELECT
        DISTINCT trim(category_hierarchy) AS category_hierarchy, md5(trim(category_hierarchy)) AS category_hash

    FROM carrefour
    WHERE item_url IS NOT NULL
        AND created_at >= (SELECT DATE(MAX(created_at)) FROM carrefour)
),
stg_categories AS (
    SELECT
        category_hash,
        category_hierarchy AS category,
        SPLIT_PART(category_hierarchy, '/', 1) AS category_1,
        SPLIT_PART(category_hierarchy, '/', 2) AS category_2,
        SPLIT_PART(category_hierarchy, '/', 3) AS category_3,
        SPLIT_PART(category_hierarchy, '/', 4) AS category_4,
        SPLIT_PART(category_hierarchy, '/', 5) AS category_5


    FROM categories
)

SELECT * FROM stg_categories

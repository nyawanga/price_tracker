
WITH jumia AS (
    SELECT * FROM {{ source("price_tracker", "jumia") }}
),
categories AS (
    SELECT
        DISTINCT trim(category) AS category, md5(trim(category)) AS category_hash

    FROM jumia
    WHERE item_url IS NOT NULL
        AND created_at >= (SELECT DATE(MAX(created_at)) FROM jumia)
),
stg_categories AS (
    SELECT
        category_hash,
        category,
        SPLIT_PART(category, '/', 1) AS category_1,
        SPLIT_PART(category, '/', 2) AS category_2,
        SPLIT_PART(category, '/', 3) AS category_3,
        SPLIT_PART(category, '/', 4) AS category_4,
        SPLIT_PART(category, '/', 5) AS category_5


    FROM categories
)

SELECT * FROM stg_categories

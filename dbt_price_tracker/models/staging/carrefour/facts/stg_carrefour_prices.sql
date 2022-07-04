WITH carrefour AS (
    SELECT * FROM {{ source("price_tracker", "carrefour") }}
), 
stg_prices AS (
    SELECT 
        extract(epoch from created_at) AS date_id,
        md5(lower(trim(item_url))) as url_hash,
        lower(trim(item_url)) AS item_url,
        price,
        md5(trim(brand)) AS brand_hash,
        md5(trim(category)) AS category_hash,
        created_at

    FROM carrefour
    WHERE item_url IS NOT NULL
    AND created_at >= (SELECT MAX(DATE(created_at)) FROM carrefour) 
    AND price IS NOT NULL
), _intermediate AS (
    SELECT 
        stg_prices.*,
        {{ dbt_utils.surrogate_key(['date_id', 'item_url'])}} AS dateid_url_hash
    FROM stg_prices
), final AS (
    SELECT 
        _intermediate.*, 
        ROW_NUMBER() OVER(PARTITION BY dateid_url_hash ORDER BY created_at DESC) AS entry_order 
    FROM _intermediate
)
SELECT * FROM final WHERE entry_order = 1

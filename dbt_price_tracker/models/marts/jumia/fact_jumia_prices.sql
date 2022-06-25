{{
    config(
        materialized='incremental',
        unique_key='dateid_url_hash'
    )
}}


with stg_jumia_prices AS (
    SELECT * FROM {{ ref("stg_jumia_prices")}}
),
dim_brands AS (
    SELECT * FROM {{ ref("dim_brands") }}
),
dim_categories AS (
    SELECT * FROM {{ ref("dim_categories")}}
),
intermediate_jumia_prices AS(
    SELECT
        *
    FROM stg_jumia_prices
    JOIN dim_brands USING(brand_hash)
    JOIN dim_categories USING(category_hash)
),
fact_jumia_prices AS (
    SELECT 
        date_id,
        url_hash,
        price,
        brand_hash,
        category_hash,
        dateid_url_hash

    FROM intermediate_jumia_prices
    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    WHERE dateid_url_hash NOT IN (SELECT dateid_url_hash FROM {{ this }}) 

    {% endif %}
)
SELECT * FROM fact_jumia_prices

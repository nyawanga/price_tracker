{{
    config(
        materialized='incremental',
        unique_key='brand_hash'
    )
}}


WITH stg_brands AS (
    SELECT * FROM {{ ref("stg_brands") }}
), 
dim_brands AS (
    SELECT 
        brand_hash, brand
    FROM stg_brands
    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    where brand_hash  NOT IN (select brand_hash from {{ this }})

    {% endif %}
)
SELECT * FROM dim_brands

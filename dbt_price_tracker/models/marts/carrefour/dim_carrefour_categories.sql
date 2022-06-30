{{
    config(
        materialized='incremental',
        unique_key='category_hash'
    )
}}

WITH stg_categories AS(
    SELECT * FROM {{ ref("stg_categories") }}
),
categories AS (
    SELECT
        category_hash,
        category,
        category_1,
        category_2,
        category_3,
        category_4,
        category_5

    FROM stg_categories
    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
    where category_hash  NOT IN (select category_hash from {{ this }})

    {% endif %}
)

SELECT * FROM categories

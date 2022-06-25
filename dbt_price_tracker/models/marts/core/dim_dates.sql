{{
  config(
    sort_key='date_id',
    dist_key='date_id',
    on_schema_change='sync_all_columns'
  )
}}


WITH all_dates AS (
    SELECT * FROM {{ ref('all_dates') }}
),

dim_dates AS (
    SELECT
        EXTRACT(EPOCH FROM date_day ) AS date_id,
        DATE(date_day) AS date_value,
        EXTRACT(YEAR FROM date_day) AS year_no,
        --   EXTRACT(isoyear FROM date_day ) AS year,
        EXTRACT(QUARTER FROM date_day) AS qtr_no,
        CONCAT('QTR', EXTRACT(QUARTER FROM date_day)) AS qtr_name,
        EXTRACT(MONTH FROM date_day ) AS month_no,
        TO_CHAR(date_day, 'Mon') AS month_name,
        TO_CHAR(date_day, 'Mon yyyy') AS month_year,
        EXTRACT( WEEK FROM date_day ) AS week_no,
        EXTRACT( DAY FROM date_day ) AS day_of_year,
        -- sunday AS 1  SARTUDAY AS 7 
        EXTRACT( DOW FROM date_day ) + 1 AS dow_no,
        TO_CHAR(date_day, 'DY') AS day_name,
        -- DATE(
        --     DATEADD('D', -DATEPART(dow, DATE(date_day)), DATE(date_day))
        -- ) AS week_start,
        --    EXTRACT( ISODOW FROM TIMESTAMP date_day ) AS isodo,
        -- EXTRACT( doy FROM date_day ) AS doy,
        --     year_of_week(date_day) AS year_of_curr_week,
        CASE
            WHEN EXTRACT(DOW FROM date_day) IN (0, 6) THEN 1 ELSE 0
        END AS is_weekend

    FROM all_dates
)

SELECT * FROM dim_dates

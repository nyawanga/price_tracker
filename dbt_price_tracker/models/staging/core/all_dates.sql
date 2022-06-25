{{ config(
    materialized="view"
)}}


{{
    dbt_utils.date_spine(
        datepart="day",
        start_date="date('2020-01-01')",
        end_date="date('2024-12-01')"
    )
}}

{#{{
    dbt_utils.date_spine(
        datepart="day",
        start_date="CAST('2010-01-01' AS TIMESTAMP)",
        end_date="CAST('2025-12-01' AS TIMESTAMP)"
    )
}}#}

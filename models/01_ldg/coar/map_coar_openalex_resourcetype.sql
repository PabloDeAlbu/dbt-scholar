WITH base as (
    SELECT 
        type::text,
        label::text,
        label_es::text,
        coar_uri::text,
        {{ dbt_date.today() }} as dv_load_datetime
    FROM {{ref('seed_coar_openalex')}}
),
ghost_record as (
    SELECT
        '!UNKNOWN'::text as type,
        '!UNKNOWN'::text as label,
        '!UNKNOWN'::text as label_es,
        '!UNKNOWN'::text as coar_uri,
        {{ dbt_date.today() }} as dv_load_datetime
)

SELECT * FROM base
UNION ALL
SELECT * FROM ghost_record

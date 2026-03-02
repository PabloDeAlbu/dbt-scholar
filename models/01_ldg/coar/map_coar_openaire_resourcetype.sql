WITH base as (
    SELECT 
        type::text,
        label_es::text,
        coar_uri::text,
        {{ dbt_date.today() }} as _load_datetime
    FROM {{ref('seed_coar_openaire')}}
),
ghost_record as (
    SELECT
        '!UNKNOWN'::text as type,
        '!UNKNOWN'::text as label_es,
        '!UNKNOWN'::text as coar_uri,
        {{ dbt_date.today() }} as _load_datetime
)

SELECT * FROM base
UNION ALL
SELECT * FROM ghost_record

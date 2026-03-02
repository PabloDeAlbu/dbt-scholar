WITH base as (
    SELECT 
        resourcetype_id as coar_uri,
        label,
        parent_label_1,
        parent_label_2,
        parent_label_3,
        label_es,
        {{ dbt_date.today() }} as _load_datetime
    FROM {{ref('seed_resourcetype_coar')}}
),
ghost_record as (
    SELECT
        '!UNKNOWN' as coar_uri,
        '!UNKNOWN' as label,
        '!UNKNOWN' as parent_label_1,
        '!UNKNOWN' as parent_label_2,
        '!UNKNOWN' as parent_label_3,
        '!UNKNOWN' as label_es,
        {{ dbt_date.today() }} as _load_datetime
)

SELECT * FROM base
UNION ALL
SELECT * FROM ghost_record

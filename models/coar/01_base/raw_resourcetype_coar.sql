WITH base as (
    SELECT 
        resourcetype_id as coar_uri,
        label,
        parent_label_1,
        parent_label_2,
        parent_label_3,
        label_es,
        {{ dbt_date.today() }} as load_datetime
    FROM {{ref('seed_resourcetype_coar')}}
)

SELECT * FROM base
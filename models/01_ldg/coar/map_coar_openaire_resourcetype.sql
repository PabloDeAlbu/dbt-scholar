WITH base as (
    SELECT 
        type::text,
        label_es::text,
        coar_uri::text,
        {{ dbt_date.today() }} as load_datetime
    FROM {{ref('seed_coar_openaire')}}
)

SELECT * FROM base
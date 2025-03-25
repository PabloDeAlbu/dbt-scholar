WITH base as (
    SELECT 
        type::varchar,
        label_es::varchar,
        coar_uri::varchar,
        {{ dbt_date.today() }} as load_datetime
    FROM {{ref('seed_coar_openaire')}}
)

SELECT * FROM base
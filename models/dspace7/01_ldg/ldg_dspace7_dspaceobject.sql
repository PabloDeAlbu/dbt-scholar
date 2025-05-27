WITH base AS (
    SELECT 
        uuid,
        {{ dbt_date.today() }} as load_datetime
    FROM {{ source('dspace7', 'dspaceobject') }}
)

SELECT * FROM base
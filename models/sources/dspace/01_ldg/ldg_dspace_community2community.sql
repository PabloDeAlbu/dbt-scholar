WITH base AS (
    SELECT 
        parent_comm_id,
        child_comm_id,
        {{ dbt_date.today() }} as load_datetime
    FROM {{ source('dspace', 'community2community') }}
)

SELECT * FROM base
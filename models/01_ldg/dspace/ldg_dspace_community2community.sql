WITH base AS (
    SELECT 
        parent_comm_id as parent_comm_uuid,
        child_comm_id as child_comm_uuid,
        {{ dbt_date.today() }} as load_datetime
    FROM {{ source('dspace', 'community2community') }}
)

SELECT * FROM base
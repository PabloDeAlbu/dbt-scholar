WITH base AS (
    SELECT 
        community_id,
        collection_id,
        {{ dbt_date.today() }} as load_datetime
    FROM {{ source('dspace7', 'community2collection') }}
)

SELECT * FROM base
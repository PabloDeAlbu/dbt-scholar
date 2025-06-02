WITH base AS (
    SELECT 
        community_id as community_uuid,
        collection_id as collection_uuid,
        {{ dbt_date.today() }} as load_datetime
    FROM {{ source('dspace', 'community2collection') }}
)

SELECT * FROM base
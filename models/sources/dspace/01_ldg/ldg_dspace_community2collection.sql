WITH base AS (
    SELECT 
        community_id,
        collection_id,
        {{ dbt_date.today() }} as load_datetime
    FROM {{ source('dspace', 'community2collection') }}
)

SELECT * FROM base
WITH base AS (
    SELECT 
        collection_id as collection_uuid,
        item_id as item_uuid,
        {{ dbt_date.today() }} as load_datetime
    FROM {{ source('dspace', 'collection2item') }}
)

SELECT * FROM base
WITH base AS (
    SELECT 
        collection_id,
        item_id,
        {{ dbt_date.today() }} as load_datetime
    FROM {{ source('dspace7', 'collection2item') }}
)

SELECT * FROM base
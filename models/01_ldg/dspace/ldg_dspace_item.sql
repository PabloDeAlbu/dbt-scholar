WITH base AS (
    SELECT 
        item_id,
        uuid as item_uuid,
        in_archive,
        withdrawn,
        last_modified,
        discoverable,
        submitter_id,
        owning_collection,
        {{ dbt_date.today() }} as load_datetime
    FROM {{ source('dspace', 'item') }}
)

SELECT * FROM base
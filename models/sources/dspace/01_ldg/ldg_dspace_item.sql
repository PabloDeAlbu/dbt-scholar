WITH base AS (
    SELECT 
        item_id,
        in_archive,
        withdrawn,
        last_modified,
        discoverable,
        uuid as item_uuid,
        submitter_id,
        owning_collection,
        {{ dbt_date.today() }} as load_datetime
    FROM {{ source('dspace', 'item') }}
)

SELECT * FROM base
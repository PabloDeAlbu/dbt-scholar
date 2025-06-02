WITH base AS (
    SELECT 
        collection_id,
        uuid AS collection_uuid,
        submitter,
        template_item_id,
        logo_bitstream_id,
        admin,
        {{ dbt_date.today() }} as load_datetime
    FROM {{ source('dspace', 'collection') }}
)

SELECT * FROM base
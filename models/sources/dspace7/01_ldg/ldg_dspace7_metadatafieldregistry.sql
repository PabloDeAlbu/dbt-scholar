WITH base AS (
    SELECT 
        metadata_field_id,
        metadata_schema_id,
        element,
        qualifier,
        scope_note,
        {{ dbt_date.today() }} as load_datetime
    FROM {{ source('dspace7', 'metadatafieldregistry') }}
)

SELECT * FROM base
WITH base AS (
    SELECT 
        metadata_schema_id,
        namespace,
        short_id,
        {{ dbt_date.today() }} as load_datetime
    FROM {{ source('dspace7', 'metadataschemaregistry') }}
)

SELECT * FROM base
WITH source AS (
  SELECT * FROM {{ source('dspacedb5', 'metadataschemaregistry') }}
),
renamed AS (
  SELECT
    metadata_schema_id::bigint AS metadata_schema_id,
    namespace::text AS namespace,
    short_id::text AS short_id
  FROM source
),
ghost_record AS (
  SELECT
    -1::bigint AS metadata_schema_id,
    '!UNKNOWN'::text AS namespace,
    '!UNKNOWN'::text AS short_id
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record

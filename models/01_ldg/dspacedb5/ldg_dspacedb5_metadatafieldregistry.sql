WITH source AS (
  SELECT * FROM {{ source('dspacedb5', 'metadatafieldregistry') }}
),
renamed AS (
  SELECT
    metadata_field_id::bigint AS metadata_field_id,
    metadata_schema_id::bigint AS metadata_schema_id,
    element::text AS element,
    qualifier::text AS qualifier,
    scope_note::text AS scope_note
  FROM source
),
ghost_record AS (
  SELECT
    -1::bigint AS metadata_field_id,
    -1::bigint AS metadata_schema_id,
    '!UNKNOWN'::text AS element,
    '!UNKNOWN'::text AS qualifier,
    '!UNKNOWN'::text AS scope_note
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record

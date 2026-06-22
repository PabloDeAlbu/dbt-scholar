WITH source AS (
  SELECT * FROM {{ source('dspacedb5', 'metadataschemaregistry') }}
),
context AS (
  SELECT * FROM {{ ref('ldg_dspacedb5__context') }}
),
renamed AS (
  SELECT
    metadata_schema_id::bigint AS metadata_schema_id,
    namespace::text AS namespace,
    short_id::text AS short_id,
    context.source_label::text AS _source_label,
    context.institution_ror::text AS _institution_ror,
    (context.institution_ror || '||' || context.source_label)::text AS _repository_scope,
    (context.institution_ror || '||' || context.source_label || '||' || metadata_schema_id::text)::text AS metadataschema_bk,
    context.extract_datetime::timestamp AS _extract_datetime,
    context.load_datetime::timestamp AS _load_datetime
  FROM source
  CROSS JOIN context
),
ghost_record AS (
  SELECT
    -1 AS metadata_schema_id,
    '!UNKNOWN' AS namespace,
    '!UNKNOWN' AS short_id,
    '!UNKNOWN' AS _source_label,
    '!UNKNOWN' AS _institution_ror,
    '!UNKNOWN||!UNKNOWN' AS _repository_scope,
    '!UNKNOWN||!UNKNOWN||-1' AS metadataschema_bk,
    '1900-01-01'::timestamp AS _extract_datetime,
    '1900-01-01'::timestamp AS _load_datetime
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record

WITH source AS (
  SELECT * FROM {{ source('dspacedb5', 'metadatafieldregistry') }}
),
context AS (
  SELECT * FROM {{ ref('ldg_dspacedb5__context') }}
),
renamed AS (
  SELECT
    metadata_field_id::bigint AS metadata_field_id,
    metadata_schema_id::bigint AS metadata_schema_id,
    element::text AS element,
    qualifier::text AS qualifier,
    scope_note::text AS scope_note,
    context.source_label::text AS _source_label,
    context.institution_ror::text AS _institution_ror,
    (context.institution_ror || '||' || context.source_label)::text AS _repository_scope,
    (context.institution_ror || '||' || context.source_label || '||' || metadata_field_id::text)::text AS metadatafield_bk,
    (context.institution_ror || '||' || context.source_label || '||' || metadata_schema_id::text)::text AS metadataschema_bk,
    context.extract_datetime::timestamp AS _extract_datetime,
    context.load_datetime::timestamp AS _load_datetime
  FROM source
  CROSS JOIN context
),
ghost_record AS (
  SELECT
    -1 AS metadata_field_id,
    -1 AS metadata_schema_id,
    '!UNKNOWN' AS element,
    '!UNKNOWN' AS qualifier,
    '!UNKNOWN' AS scope_note,
    '!UNKNOWN' AS _source_label,
    '!UNKNOWN' AS _institution_ror,
    '!UNKNOWN||!UNKNOWN' AS _repository_scope,
    '!UNKNOWN||!UNKNOWN||-1' AS metadatafield_bk,
    '!UNKNOWN||!UNKNOWN||-1' AS metadataschema_bk,
    '1900-01-01'::timestamp AS _extract_datetime,
    '1900-01-01'::timestamp AS _load_datetime
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record

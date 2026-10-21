WITH source AS (
  SELECT * FROM {{ source('dspacedb5', 'metadatavalue') }}
),
scope AS (
  SELECT source_label, institution_ror, base_url, extract_datetime, load_datetime
  FROM {{ ref('ldg_dspacedb5__scope') }}
),
renamed AS (
  SELECT
    metadata_value_id::integer AS metadata_value_id,
    resource_id::integer AS resource_id,
    metadata_field_id::integer AS metadata_field_id,
    text_value::text AS text_value,
    text_lang::text AS text_lang,
    place::integer AS place,
    authority::text AS authority,
    confidence::integer AS confidence,
    resource_type_id::integer AS resource_type_id
  FROM source
),
ghost_record AS (
  SELECT
    -1::integer AS metadata_value_id,
    -1::integer AS resource_id,
    -1::integer AS metadata_field_id,
    '!UNKNOWN'::text AS text_value,
    '!UNKNOWN'::text AS text_lang,
    -1::integer AS place,
    '!UNKNOWN'::text AS authority,
    -1::integer AS confidence,
    -1::integer AS resource_type_id
),
base AS (
  SELECT
    renamed.*
  FROM renamed
  UNION ALL
  SELECT
    ghost_record.*
  FROM ghost_record
),
final AS (
  SELECT
    base.*,
    CASE WHEN base.metadata_value_id = -1 THEN '!UNKNOWN' ELSE scope.source_label END AS source_label,
    CASE WHEN base.metadata_value_id = -1 THEN '!UNKNOWN' ELSE scope.institution_ror END AS institution_ror,
    CASE WHEN base.metadata_value_id = -1 THEN '!UNKNOWN' ELSE scope.base_url END AS base_url,
    CASE WHEN base.metadata_value_id = -1 THEN '1900-01-01'::timestamp ELSE scope.extract_datetime END AS extract_datetime,
    CASE WHEN base.metadata_value_id = -1 THEN '1900-01-01'::timestamp ELSE scope.load_datetime END AS load_datetime
  FROM base
  CROSS JOIN scope
)

SELECT * FROM final

WITH source AS (
  SELECT * FROM {{ source('dspacedb5', 'handle') }}
),
context AS (
  SELECT * FROM {{ ref('ldg_dspacedb5__context') }}
),
renamed AS (
  SELECT
    handle_id::bigint AS handle_id,
    handle::text AS handle,
    resource_type_id::integer AS resource_type_id,
    resource_id::text AS resource_id,
    context.source_label::text AS _source_label,
    context.institution_ror::text AS _institution_ror,
    context.extract_datetime::timestamp AS _extract_datetime,
    context.load_datetime::timestamp AS _load_datetime
  FROM source
  CROSS JOIN context
),
ghost_record AS (
  SELECT
    -1 AS handle_id,
    '!UNKNOWN' AS handle,
    -1 AS resource_type_id,
    '-1'::text AS resource_id,
    '!UNKNOWN' AS _source_label,
    '!UNKNOWN' AS _institution_ror,
    '1900-01-01'::timestamp AS _extract_datetime,
    '1900-01-01'::timestamp AS _load_datetime
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record

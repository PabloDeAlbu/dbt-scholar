WITH source AS (
  SELECT * FROM {{ source('dspacedb5', 'handle') }}
),
context AS (
  SELECT * FROM {{ ref('ldg_dspacedb5_context') }}
),
renamed AS (
  SELECT
    handle_id,
    handle,
    resource_type_id,
    resource_id,
    context.source_label AS _source_label,
    context.institution_ror AS _institution_ror,
    context.extract_datetime AS _extract_datetime,
    context.load_datetime AS _load_datetime
  FROM source
  CROSS JOIN context
),
ghost_record AS (
  SELECT
    -1 AS handle_id,
    '!UNKNOWN' AS handle,
    -1 AS resource_type_id,
    -1 AS resource_id,
    '!UNKNOWN' AS _source_label,
    '!UNKNOWN' AS _institution_ror,
    '1900-01-01'::timestamp AS _extract_datetime,
    '1900-01-01'::timestamp AS _load_datetime
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record

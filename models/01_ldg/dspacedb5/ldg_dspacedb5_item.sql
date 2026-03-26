WITH source AS (
  SELECT * FROM {{ source('dspacedb5', 'item') }}
),
context AS (
  SELECT * FROM {{ ref('ldg_dspacedb5_context') }}
),
renamed AS (
  SELECT
    item_id,
    submitter_id,
    in_archive,
    withdrawn,
    last_modified,
    owning_collection,
    discoverable,
    context.source_label AS _source_label,
    context.institution_ror AS _institution_ror,
    context.extract_datetime AS _extract_datetime,
    context.load_datetime AS _load_datetime
  FROM source
  CROSS JOIN context
),
ghost_record AS (
  SELECT
    -1 AS item_id,
    -1 AS submitter_id,
    false AS in_archive,
    false AS withdrawn,
    '1900-01-01'::timestamp AS last_modified,
    -1 AS owning_collection,
    false AS discoverable,
    '!UNKNOWN' AS _source_label,
    '!UNKNOWN' AS _institution_ror,
    '1900-01-01'::timestamp AS _extract_datetime,
    '1900-01-01'::timestamp AS _load_datetime
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record

WITH source AS (
  SELECT * FROM {{ source('dspacedb5', 'collection2item') }}
),
context AS (
  SELECT * FROM {{ ref('ldg_dspacedb5_context') }}
),
renamed AS (
  SELECT
    id AS collection_item_id,
    collection_id,
    item_id,
    context.source_label AS _source_label,
    context.institution_ror AS _institution_ror,
    context.extract_datetime AS _extract_datetime,
    context.load_datetime AS _load_datetime
  FROM source
  CROSS JOIN context
),
ghost_record AS (
  SELECT
    -1 AS collection_item_id,
    -1 AS collection_id,
    -1 AS item_id,
    '!UNKNOWN' AS _source_label,
    '!UNKNOWN' AS _institution_ror,
    '1900-01-01'::timestamp AS _extract_datetime,
    '1900-01-01'::timestamp AS _load_datetime
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record

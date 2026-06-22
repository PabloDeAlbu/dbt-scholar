WITH source AS (
  SELECT * FROM {{ source('dspacedb5', 'community2community') }}
),
context AS (
  SELECT * FROM {{ ref('ldg_dspacedb5__context') }}
),
renamed AS (
  SELECT
    id::bigint AS community_community_id,
    parent_comm_id::text AS parent_comm_id,
    child_comm_id::text AS child_comm_id,
    context.source_label::text AS _source_label,
    context.institution_ror::text AS _institution_ror,
    context.extract_datetime::timestamp AS _extract_datetime,
    context.load_datetime::timestamp AS _load_datetime
  FROM source
  CROSS JOIN context
),
ghost_record AS (
  SELECT
    -1 AS community_community_id,
    '-1'::text AS parent_comm_id,
    '-1'::text AS child_comm_id,
    '!UNKNOWN' AS _source_label,
    '!UNKNOWN' AS _institution_ror,
    '1900-01-01'::timestamp AS _extract_datetime,
    '1900-01-01'::timestamp AS _load_datetime
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record

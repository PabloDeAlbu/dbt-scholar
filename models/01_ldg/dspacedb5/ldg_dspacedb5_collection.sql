WITH source AS (
  SELECT * FROM {{ source('dspacedb5', 'collection') }}
),
context AS (
  SELECT * FROM {{ ref('ldg_dspacedb5__context') }}
),
renamed AS (
  SELECT
    collection_id::bigint AS collection_id,
    logo_bitstream_id::bigint AS logo_bitstream_id,
    template_item_id::text AS template_item_id,
    workflow_step_1::text AS workflow_step_1,
    workflow_step_2::text AS workflow_step_2,
    workflow_step_3::text AS workflow_step_3,
    submitter::text AS submitter,
    admin::text AS admin,
    context.source_label::text AS _source_label,
    context.institution_ror::text AS _institution_ror,
    context.extract_datetime::timestamp AS _extract_datetime,
    context.load_datetime::timestamp AS _load_datetime
  FROM source
  CROSS JOIN context
),
ghost_record AS (
  SELECT
    -1 AS collection_id,
    -1 AS logo_bitstream_id,
    '-1'::text AS template_item_id,
    '-1'::text AS workflow_step_1,
    '-1'::text AS workflow_step_2,
    '-1'::text AS workflow_step_3,
    '-1'::text AS submitter,
    '-1'::text AS admin,
    '!UNKNOWN' AS _source_label,
    '!UNKNOWN' AS _institution_ror,
    '1900-01-01'::timestamp AS _extract_datetime,
    '1900-01-01'::timestamp AS _load_datetime
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record

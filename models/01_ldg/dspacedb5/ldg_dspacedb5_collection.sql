WITH source AS (
  SELECT * FROM {{ source('dspacedb5', 'collection') }}
),
renamed AS (
  SELECT
    collection_id::bigint AS collection_id,
    logo_bitstream_id::bigint AS logo_bitstream_id,
    template_item_id::integer AS template_item_id,
    workflow_step_1::integer AS workflow_step_1,
    workflow_step_2::integer AS workflow_step_2,
    workflow_step_3::integer AS workflow_step_3,
    submitter::integer AS submitter,
    admin::integer AS admin
  FROM source
),
ghost_record AS (
  SELECT
    -1::bigint AS collection_id,
    -1::bigint AS logo_bitstream_id,
    -1::integer AS template_item_id,
    -1::integer AS workflow_step_1,
    -1::integer AS workflow_step_2,
    -1::integer AS workflow_step_3,
    -1::integer AS submitter,
    -1::integer AS admin
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record

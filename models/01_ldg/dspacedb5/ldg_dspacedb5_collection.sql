WITH source AS (
  SELECT * FROM {{ source('dspacedb5', 'collection') }}
),
scope AS (
  SELECT
    source_label,
    institution_ror,
    base_url,
    extract_datetime,
    load_datetime
  FROM {{ ref('ldg_dspacedb5__scope') }}
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
    scope.source_label,
    scope.institution_ror,
    scope.base_url,
    scope.extract_datetime,
    scope.load_datetime
  FROM base
  CROSS JOIN scope
)

SELECT * FROM final
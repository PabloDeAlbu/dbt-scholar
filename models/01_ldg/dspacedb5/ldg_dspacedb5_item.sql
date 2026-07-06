WITH source AS (
  SELECT * FROM {{ source('dspacedb5', 'item') }}
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
    item_id::integer AS item_id,
    submitter_id::integer AS submitter_id,
    in_archive::boolean AS in_archive,
    withdrawn::boolean AS withdrawn,
    last_modified::timestamp AS last_modified,
    owning_collection::integer AS owning_collection,
    discoverable::boolean AS discoverable
  FROM source
),
ghost_record AS (
  SELECT
    -1::integer AS item_id,
    -1::integer AS submitter_id,
    false::boolean AS in_archive,
    false::boolean AS withdrawn,
    '1900-01-01'::timestamp AS last_modified,
    -1::integer AS owning_collection,
    false::boolean AS discoverable
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

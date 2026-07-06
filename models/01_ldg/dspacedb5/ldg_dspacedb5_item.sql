WITH source AS (
  SELECT * FROM {{ source('dspacedb5', 'item') }}
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
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record

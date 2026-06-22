WITH source AS (
  SELECT * FROM {{ source('dspacedb5', 'item') }}
),
context AS (
  SELECT * FROM {{ ref('ldg_dspacedb5__context') }}
),
renamed AS (
  SELECT
    item_id::text AS item_id,
    submitter_id::text AS submitter_id,
    in_archive::boolean AS in_archive,
    withdrawn::boolean AS withdrawn,
    last_modified::timestamp AS last_modified,
    owning_collection::text AS owning_collection,
    discoverable::boolean AS discoverable,
    context.source_label::text AS _source_label,
    context.institution_ror::text AS _institution_ror,
    (context.institution_ror || '||' || context.source_label)::text AS _repository_scope,
    (context.institution_ror || '||' || context.source_label || '||' || item_id::text)::text AS item_bk,
    (context.institution_ror || '||' || context.source_label || '||' || owning_collection::text)::text AS owningcollection_bk,
    context.extract_datetime::timestamp AS _extract_datetime,
    context.load_datetime::timestamp AS _load_datetime
  FROM source
  CROSS JOIN context
),
ghost_record AS (
  SELECT
    '-1'::text AS item_id,
    '-1'::text AS submitter_id,
    false AS in_archive,
    false AS withdrawn,
    '1900-01-01'::timestamp AS last_modified,
    '-1'::text AS owning_collection,
    false AS discoverable,
    '!UNKNOWN' AS _source_label,
    '!UNKNOWN' AS _institution_ror,
    '!UNKNOWN||!UNKNOWN' AS _repository_scope,
    '!UNKNOWN||!UNKNOWN||-1' AS item_bk,
    '!UNKNOWN||!UNKNOWN||-1' AS owningcollection_bk,
    '1900-01-01'::timestamp AS _extract_datetime,
    '1900-01-01'::timestamp AS _load_datetime
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record

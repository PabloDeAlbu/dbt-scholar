WITH source AS (
    SELECT * FROM {{ source('dspacedb', 'item') }}
),
context AS (
    SELECT * FROM {{ ref('ldg_dspacedb_context') }}
),
renamed AS (
    SELECT
        item_id,
        uuid AS item_uuid,
        in_archive,
        withdrawn,
        last_modified,
        discoverable,
        submitter_id,
        owning_collection,
        context.base_url AS _base_url,
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
        '00000000-0000-0000-0000-000000000000'::uuid AS item_uuid,
        false AS in_archive,
        false AS withdrawn,
        '1900-01-01'::timestamp AS last_modified,
        false AS discoverable,
        '00000000-0000-0000-0000-000000000000'::uuid AS submitter_id,
        '00000000-0000-0000-0000-000000000000'::uuid AS owning_collection,
        '!UNKNOWN' AS _base_url,
        '!UNKNOWN' AS _source_label,
        '!UNKNOWN' AS _institution_ror,
        '1900-01-01'::timestamp AS _extract_datetime,
        '1900-01-01'::timestamp AS _load_datetime
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record

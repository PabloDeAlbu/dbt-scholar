WITH source AS (
    SELECT * FROM {{ source('dspacedb', 'community2collection') }}
),
context AS (
    SELECT * FROM {{ ref('ldg_dspacedb_context') }}
),
renamed AS (
    SELECT
        community_id AS community_uuid,
        collection_id AS collection_uuid,
        context.source_label AS _source_label,
        context.institution_ror AS _institution_ror,
        context.extract_datetime AS _extract_datetime,
        context.load_datetime AS _load_datetime
    FROM source
    CROSS JOIN context
),
ghost_record AS (
    SELECT
        '00000000-0000-0000-0000-000000000000'::uuid AS community_uuid,
        '00000000-0000-0000-0000-000000000000'::uuid AS collection_uuid,
        '!UNKNOWN' AS _source_label,
        '!UNKNOWN' AS _institution_ror,
        '1900-01-01'::timestamp AS _extract_datetime,
        '1900-01-01'::timestamp AS _load_datetime
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record

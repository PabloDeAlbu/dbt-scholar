WITH source AS (
    SELECT * FROM {{ source('dspacedb', 'community') }}
),
context AS (
    SELECT * FROM {{ ref('ldg_dspacedb_context') }}
),
renamed AS (
    SELECT
        community_id,
        uuid AS community_uuid,
        admin,
        logo_bitstream_id,
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
        -1 AS community_id,
        '00000000-0000-0000-0000-000000000000'::uuid AS community_uuid,
        '00000000-0000-0000-0000-000000000000'::uuid AS admin,
        '00000000-0000-0000-0000-000000000000'::uuid AS logo_bitstream_id,
        '!UNKNOWN' AS _base_url,
        '!UNKNOWN' AS _source_label,
        '!UNKNOWN' AS _institution_ror,
        '1900-01-01'::timestamp AS _extract_datetime,
        '1900-01-01'::timestamp AS _load_datetime
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record

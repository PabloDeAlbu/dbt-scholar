WITH source AS (
    SELECT * FROM {{ source('dspacedb', 'collection') }}
),
context AS (
    SELECT * FROM {{ ref('ldg_dspacedb_context') }}
),
renamed AS (
    SELECT
        collection_id,
        uuid AS collection_uuid,
        submitter,
        template_item_id,
        logo_bitstream_id,
        admin,
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
        -1 AS collection_id,
        '00000000-0000-0000-0000-000000000000'::uuid AS collection_uuid,
        '00000000-0000-0000-0000-000000000000'::uuid AS submitter,
        '00000000-0000-0000-0000-000000000000'::uuid AS template_item_id,
        '00000000-0000-0000-0000-000000000000'::uuid AS logo_bitstream_id,
        '00000000-0000-0000-0000-000000000000'::uuid AS admin,
        '!UNKNOWN' AS _base_url,
        '!UNKNOWN' AS _source_label,
        '!UNKNOWN' AS _institution_ror,
        '1900-01-01'::timestamp AS _extract_datetime,
        '1900-01-01'::timestamp AS _load_datetime
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record

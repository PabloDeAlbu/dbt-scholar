WITH source AS (
    SELECT * FROM {{ source('dspacedb', 'metadatavalue') }}
),
context AS (
    SELECT * FROM {{ ref('ldg_dspacedb_context') }}
),
renamed AS (
    SELECT
        metadata_value_id,
        metadata_field_id,
        text_value,
        text_lang,
        place,
        authority,
        confidence,
        dspace_object_id,
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
        -1 AS metadata_value_id,
        -1 AS metadata_field_id,
        '!UNKNOWN' AS text_value,
        '!UNKNOWN' AS text_lang,
        -1 AS place,
        '!UNKNOWN' AS authority,
        -1 AS confidence,
        '00000000-0000-0000-0000-000000000000'::uuid AS dspace_object_id,
        '!UNKNOWN' AS _base_url,
        '!UNKNOWN' AS _source_label,
        '!UNKNOWN' AS _institution_ror,
        '1900-01-01'::timestamp AS _extract_datetime,
        '1900-01-01'::timestamp AS _load_datetime
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record

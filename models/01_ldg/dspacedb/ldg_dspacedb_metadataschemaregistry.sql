WITH source AS (
    SELECT * FROM {{ source('dspacedb', 'metadataschemaregistry') }}
),
context AS (
    SELECT * FROM {{ ref('ldg_dspacedb_context') }}
),
renamed AS (
    SELECT
        metadata_schema_id,
        namespace,
        short_id,
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
        -1 AS metadata_schema_id,
        '!UNKNOWN' AS namespace,
        '!UNKNOWN' AS short_id,
        '!UNKNOWN' AS _base_url,
        '!UNKNOWN' AS _source_label,
        '!UNKNOWN' AS _institution_ror,
        '1900-01-01'::timestamp AS _extract_datetime,
        '1900-01-01'::timestamp AS _load_datetime
)

SELECT * FROM renamed
UNION ALL
SELECT * FROM ghost_record

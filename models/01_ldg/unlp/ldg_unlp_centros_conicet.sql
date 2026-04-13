{{ config(materialized = 'table') }}

WITH source AS (
    SELECT *
    FROM {{ ref('centros_conicet_unlp') }}
),

renamed AS (
    SELECT
        NULLIF(TRIM(set_spec), '')::text AS set_spec,
        NULLIF(TRIM(set_name), '')::text AS set_name,
        NULLIF(TRIM(sedici_collection_uri), '')::text AS sedici_collection_uri,
        NULLIF(TRIM(faculty), '')::text AS faculty,
        NULLIF(TRIM(manual_status), '')::text AS manual_status,
        NULLIF(TRIM(ingest_note), '')::text AS ingest_note,
        CASE
            WHEN NULLIF(TRIM(sedici_article_count), '') ~ '^[0-9]+$'
                THEN NULLIF(TRIM(sedici_article_count), '')::integer
        END AS sedici_article_count,
        CASE
            WHEN REPLACE(NULLIF(TRIM(pdf_share_pct), ''), ',', '.') ~ '^[0-9]+(\\.[0-9]+)?$'
                THEN REPLACE(NULLIF(TRIM(pdf_share_pct), ''), ',', '.')::numeric(8, 4)
        END AS pdf_share_pct
    FROM source
),

filtered AS (
    SELECT *
    FROM renamed
    WHERE set_spec LIKE 'com\_%' ESCAPE '\'
      AND sedici_collection_uri IS NOT NULL
),

final AS (
    SELECT
        set_spec,
        set_name,
        sedici_collection_uri,
        faculty,
        pdf_share_pct,
        sedici_article_count,
        manual_status,
        ingest_note
    FROM filtered
)

SELECT * FROM final

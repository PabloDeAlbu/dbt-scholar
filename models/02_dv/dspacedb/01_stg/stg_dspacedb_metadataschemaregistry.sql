{{ config(materialized='view') }}

WITH raw_source AS (
    SELECT
        metadata_schema_id,
        namespace,
        short_id,
        _base_url AS base_url,
        _source_label AS source_label,
        _institution_ror AS institution_ror,
        _extract_datetime AS extract_datetime,
        _load_datetime AS load_datetime,
        _institution_ror || '||' || _source_label || '||' || metadata_schema_id::text AS metadataschema_bk
    FROM {{ ref('ldg_dspacedb_metadataschemaregistry') }}
),
final AS (
    SELECT
        metadataschema_hk,
        metadataschema_bk,
        metadata_schema_id,
        namespace,
        short_id,
        base_url,
        source_label,
        institution_ror,
        extract_datetime,
        load_datetime,
        source_label AS source,
        extract_datetime AS effective_from,
        extract_datetime AS start_date,
        to_date('9999-12-31', 'YYYY-MM-DD') AS end_date
    FROM raw_source s0
    CROSS JOIN LATERAL (
        SELECT {{ automate_dv.hash(columns='metadataschema_bk', alias='metadataschema_hk') }}
    ) s1
)

SELECT * FROM final

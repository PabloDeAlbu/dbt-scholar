{{ config(materialized='view') }}

WITH raw_source AS (
    SELECT
        collection_id::text AS collection_id,
        collection_uuid::text AS collection_uuid,
        collection_bk,
        source_label,
        institution_ror,
        COALESCE(extract_datetime::timestamp, load_datetime::timestamp) AS extract_datetime,
        load_datetime::timestamp AS load_datetime
    FROM {{ ref('stg_dspacedb_collection') }}
),
final AS (
    SELECT
        collection_hk,
        collection_bk,
        collection_id,
        collection_uuid,
        source_label,
        institution_ror,
        extract_datetime,
        load_datetime,
        source,
        {{ automate_dv.hash(
            columns=['collection_hk', 'extract_datetime'],
            alias='extract_cdk'
        ) }},
        {{ automate_dv.hash(
            columns=['extract_datetime'],
            alias='collection_extract_hashdiff',
            is_hashdiff=true
        ) }}
    FROM (
        SELECT
            collection_id,
            collection_uuid,
            collection_bk,
            source_label,
            institution_ror,
            extract_datetime,
            load_datetime,
            source_label AS source
        FROM raw_source
    ) s0
    CROSS JOIN LATERAL (
        SELECT {{ automate_dv.hash(columns='collection_bk', alias='collection_hk') }}
    ) s1
)

SELECT * FROM final

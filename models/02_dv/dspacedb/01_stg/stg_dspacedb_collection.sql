{{ config(materialized='view') }}

WITH raw_source AS (
    SELECT
        collection_id,
        collection_uuid,
        submitter,
        template_item_id,
        logo_bitstream_id,
        admin,
        _source_label AS source_label,
        _institution_ror AS institution_ror,
        _extract_datetime AS extract_datetime,
        _load_datetime AS load_datetime,
        _institution_ror || '||' || _source_label || '||' || collection_uuid::text AS collection_bk
    FROM {{ ref('ldg_dspacedb_collection') }}
),
final AS (
    SELECT
        collection_hk,
        collection_hashdiff,
        collection_bk,
        collection_id,
        collection_uuid,
        submitter,
        template_item_id,
        logo_bitstream_id,
        admin,
        source_label,
        institution_ror,
        extract_datetime,
        load_datetime,
        source_label AS source,
        COALESCE(extract_datetime, load_datetime) AS effective_from,
        COALESCE(extract_datetime, load_datetime) AS start_date,
        to_date('9999-12-31', 'YYYY-MM-DD') AS end_date
    FROM raw_source s0
    CROSS JOIN LATERAL (
        SELECT
            {{ automate_dv.hash(columns='collection_bk', alias='collection_hk') }},
            {{ automate_dv.hash(
                columns=[
                    'collection_bk',
                    'submitter',
                    'template_item_id',
                    'logo_bitstream_id',
                    'admin'
                ],
                alias='collection_hashdiff',
                is_hashdiff=true
            ) }}
    ) s1
)

SELECT * FROM final

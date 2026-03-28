{{ config(materialized='view') }}

WITH raw_source AS (
    SELECT
        item_id,
        item_uuid,
        in_archive,
        withdrawn,
        last_modified,
        discoverable,
        submitter_id,
        owning_collection,
        _source_label AS source_label,
        _institution_ror AS institution_ror,
        _extract_datetime AS extract_datetime,
        _load_datetime AS load_datetime,
        _institution_ror || '||' || _source_label || '||' || item_uuid::text AS item_bk
    FROM {{ ref('ldg_dspacedb_item') }}
),
final AS (
    SELECT
        item_hk,
        item_hashdiff,
        item_bk,
        item_id,
        item_uuid,
        in_archive,
        withdrawn,
        last_modified,
        discoverable,
        submitter_id,
        owning_collection,
        source_label,
        institution_ror,
        extract_datetime,
        load_datetime,
        source_label AS source,
        COALESCE(last_modified, extract_datetime, load_datetime) AS effective_from,
        COALESCE(last_modified, extract_datetime, load_datetime) AS start_date,
        to_date('9999-12-31', 'YYYY-MM-DD') AS end_date
    FROM raw_source s0
    CROSS JOIN LATERAL (
        SELECT
            {{ automate_dv.hash(columns='item_bk', alias='item_hk') }},
            {{ automate_dv.hash(
                columns=[
                    'item_bk',
                    'submitter_id',
                    'in_archive',
                    'withdrawn',
                    'discoverable',
                    'last_modified',
                    'owning_collection'
                ],
                alias='item_hashdiff',
                is_hashdiff=true
            ) }}
    ) s1
)

SELECT * FROM final

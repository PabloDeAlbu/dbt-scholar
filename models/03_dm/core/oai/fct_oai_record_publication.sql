{{ config(materialized = 'table') }}

WITH latest_sat AS (
    SELECT *
    FROM {{ ref('latest_sat_oai_record') }}
),

latest_extract AS (
    SELECT *
    FROM {{ ref('latest_sat_oai_record__extract') }}
),

latest_extract_by_record AS (
    SELECT
        record_hk,
        repository_identifier,
        institution_ror,
        extract_datetime,
        load_datetime
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY record_hk
                ORDER BY
                    extract_datetime DESC NULLS LAST,
                    load_datetime DESC NULLS LAST,
                    repository_identifier,
                    institution_ror
            ) AS extract_rank
        FROM latest_extract
    ) ranked
    WHERE extract_rank = 1
),

type_agg AS (
    SELECT
        brg.record_hk,
        STRING_AGG(DISTINCT hub_t.dc_type, '|' ORDER BY hub_t.dc_type) AS dc_type,
        COUNT(DISTINCT hub_t.dc_type_hk) AS dc_type_count,
        (COUNT(DISTINCT hub_t.dc_type_hk) > 1) AS has_multiple_dc_type
    FROM {{ ref('brg_oai_record_type') }} brg
    INNER JOIN {{ ref('hub_oai_type') }} hub_t USING (dc_type_hk)
    GROUP BY brg.record_hk
),

base as (
    SELECT
        sat_r.record_id,
        sat_r.title,
        sat_r.date_issued,
        sat_r.record_hk,
        extract.repository_identifier,
        extract.institution_ror,
        extract.extract_datetime,
        extract.load_datetime,
        type_agg.dc_type,
        COALESCE(type_agg.dc_type_count, 0) AS dc_type_count,
        COALESCE(type_agg.has_multiple_dc_type, false) AS has_multiple_dc_type,
        coalesce(
            sat_r.date_issued >= DATE '1900-01-01'
            AND sat_r.date_issued < date_trunc('year', current_date) + interval '1 year',
            false
        ) AS valid_date_issued,
        coalesce(
            type_agg.dc_type_count > 0,
            false
        ) AS valid_dc_type
    FROM latest_sat sat_r
    LEFT JOIN latest_extract_by_record extract USING (record_hk)
    LEFT JOIN type_agg USING (record_hk)
    WHERE sat_r._context = 'request'
)

SELECT * FROM base

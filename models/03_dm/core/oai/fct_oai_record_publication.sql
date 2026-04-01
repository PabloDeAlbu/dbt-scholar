{{ config(materialized = 'table') }}

WITH latest_sat AS {{ latest_satellite(ref('sat_oai_record'), 'record_hk', order_column='_load_datetime') }},

latest_extract AS (
    SELECT
        record_hk,
        repository_identifier,
        institution_ror,
        extract_datetime,
        load_datetime
    FROM (
        SELECT
            record_hk,
            repository_identifier,
            institution_ror,
            extract_datetime,
            load_datetime,
            ROW_NUMBER() OVER (
                PARTITION BY record_hk, repository_identifier, institution_ror
                ORDER BY extract_datetime DESC, load_datetime DESC
            ) AS rn
        FROM {{ ref('sat_oai_record__extract') }}
    ) ranked
    WHERE rn = 1
),

base as (
    SELECT DISTINCT
        sat_r.record_id,
        sat_r.title,
        sat_r.date_issued,
        sat_r.record_hk,
        extract.repository_identifier,
        extract.institution_ror,
        extract.extract_datetime,
        extract.load_datetime,
        hub_t.dc_type,
        coalesce(
            sat_r.date_issued >= DATE '1900-01-01'
            AND sat_r.date_issued < date_trunc('year', current_date) + interval '1 year',
            false
        ) AS valid_date_issued,
        coalesce(
            hub_t.dc_type_hk IS NOT NULL,
            false
        ) AS valid_dc_type
    FROM latest_sat sat_r
    LEFT JOIN latest_extract extract USING (record_hk)
    LEFT JOIN {{ref('brg_oai_record_type')}} USING (record_hk)
    LEFT JOIN {{ref('hub_oai_type')}} hub_t USING (dc_type_hk)
    WHERE sat_r._context = 'request'
)

SELECT * FROM base

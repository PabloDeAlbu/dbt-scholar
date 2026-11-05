{{ config(materialized = 'table') }}

-- ¿Cuál es la última versión de los metadatos y el contexto de cada registro OAI?
WITH latest_sat AS (
    SELECT *
    FROM {{ ref('latest_sat_oai_record') }}
),

-- ¿Qué observaciones de extracción fueron registradas para cada registro OAI?
latest_extract AS (
    SELECT *
    FROM {{ ref('latest_sat_oai_record__extract') }}
),

-- ¿Cuál es el contexto de extracción más reciente de cada registro?
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

-- ¿Qué valores dc.type distintos tiene cada registro?
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

-- ¿Qué valores dc.right distintos tiene cada registro?
right_agg AS (
    SELECT
        brg.record_hk,
        STRING_AGG(DISTINCT brg.dc_right, '|' ORDER BY brg.dc_right) AS dc_right,
        COUNT(DISTINCT brg.dc_right_hk) AS dc_right_count,
        (COUNT(DISTINCT brg.dc_right_hk) > 1) AS has_multiple_dc_right
    FROM {{ ref('brg_oai_record_right') }} AS brg
    GROUP BY brg.record_hk
),

-- ¿Cómo se combinan el registro, su extracción más reciente, sus tipos y sus derechos?
record_publication AS (
    SELECT
        sat_r.record_id,
        sat_r.title,
        sat_r.date_issued,
        sat_r.record_hk,
        sat_r._context,
        extract.repository_identifier,
        extract.institution_ror,
        extract.extract_datetime,
        extract.load_datetime,
        type_agg.dc_type,
        COALESCE(type_agg.dc_type_count, 0) AS dc_type_count,
        COALESCE(type_agg.has_multiple_dc_type, false) AS has_multiple_dc_type,
        right_agg.dc_right,
        COALESCE(right_agg.dc_right_count, 0) AS dc_right_count,
        COALESCE(right_agg.has_multiple_dc_right, false) AS has_multiple_dc_right,
        coalesce(
            sat_r.date_issued >= DATE '1900-01-01'
            AND sat_r.date_issued < date_trunc('year', current_date) + interval '1 year',
            false
        ) AS valid_date_issued,
        coalesce(
            type_agg.dc_type_count > 0,
            false
        ) AS valid_dc_type,
        coalesce(
            right_agg.dc_right_count > 0,
            false
        ) AS valid_dc_right
    FROM latest_sat sat_r
    LEFT JOIN latest_extract_by_record extract USING (record_hk)
    LEFT JOIN type_agg USING (record_hk)
    LEFT JOIN right_agg USING (record_hk)
),

-- ¿Qué atributos expone fct_oai_record_publication?
final AS (
    SELECT
        record_id,
        title,
        date_issued,
        record_hk,
        _context,
        repository_identifier,
        institution_ror,
        extract_datetime,
        load_datetime,
        dc_type,
        dc_type_count,
        has_multiple_dc_type,
        dc_right,
        dc_right_count,
        has_multiple_dc_right,
        valid_date_issued,
        valid_dc_type,
        valid_dc_right
    FROM record_publication
)

SELECT *
FROM final

{{ config(materialized = 'table') }}

WITH base AS (
    SELECT
        record_hk,
        author,
        filliation
    FROM {{ ref('brg_conicet_record_fil') }}
),

classified AS (
    SELECT
        record_hk,
        author,
        filliation,
        (
            filliation ILIKE '%Universidad Nacional de La Plata%'
            OR filliation ILIKE '%Universidad Nacional de la Plata%'
            OR filliation ILIKE '%UNLP%'
        ) AS has_unlp_fil,
        (
            filliation ILIKE '%Comision de Investigaciones Cientificas%'
            OR filliation ILIKE '%Comisión de Investigaciones Científicas%'
            OR filliation ILIKE '%Provincia de Buenos Aires. Gobernación. Comisión de Investigaciones Científicas%'
            OR filliation ILIKE '% Provincia de Buenos Aires. Gobernación. Comisión de Investigaciones Científicas%'
            OR filliation ILIKE '% Comisión de Investigaciones Científicas%'
            OR filliation ILIKE '% CIC %'
            OR filliation ILIKE 'CIC;%'
            OR filliation ILIKE '%; CIC;%'
            OR filliation ILIKE 'CIC'
        ) AS has_cic_fil
    FROM base
),

final AS (
    SELECT
        record_hk,
        COUNT(*) AS fil_count,
        COUNT(DISTINCT author) AS fil_author_count,
        TRUE AS has_fil,
        BOOL_OR(has_unlp_fil) AS has_unlp_fil,
        COUNT(*) FILTER (WHERE has_unlp_fil) AS unlp_fil_count,
        COUNT(DISTINCT author) FILTER (WHERE has_unlp_fil) AS unlp_fil_author_count,
        BOOL_OR(has_cic_fil) AS has_cic_fil,
        COUNT(*) FILTER (WHERE has_cic_fil) AS cic_fil_count,
        COUNT(DISTINCT author) FILTER (WHERE has_cic_fil) AS cic_fil_author_count
    FROM classified
    GROUP BY record_hk
)

SELECT * FROM final

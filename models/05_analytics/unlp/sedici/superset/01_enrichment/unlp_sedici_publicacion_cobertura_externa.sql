{{ config(materialized='view') }}

SELECT
    item_id,
    CASE
        WHEN has_openalex_match THEN 'Sí'
        ELSE 'No'
    END AS en_openalex,
    CASE
        WHEN has_unique_openalex_match THEN 'Unívoca'
        WHEN has_openalex_match THEN 'Ambigua'
        ELSE 'No encontrada'
    END AS correspondencia_openalex,
    CASE
        WHEN has_openaire_match THEN 'Sí'
        ELSE 'No'
    END AS en_openaire,
    CASE
        WHEN has_unique_openaire_match THEN 'Unívoca'
        WHEN has_openaire_match THEN 'Ambigua'
        ELSE 'No encontrada'
    END AS correspondencia_openaire,
    CASE external_pair_status
        WHEN 'confirmed_unique' THEN 'Confirmada de forma unívoca'
        WHEN 'confirmed_ambiguous' THEN 'Confirmada con ambigüedad'
        WHEN 'not_confirmed_by_doi' THEN 'No confirmada por DOI'
        ELSE 'No aplica'
    END AS confirmacion_entre_fuentes_externas
FROM {{ ref('fct_unlp_sedicidb_publication_external_coverage') }}

{{ config(materialized='table') }}

-- ¿Qué publicaciones se consideran?
WITH published_item AS (
    SELECT
        item_id,
        handle
    FROM {{ ref('unlp_sedici_publicacion_base') }}
),

-- ¿Qué metadatos se consideran para buscar DOI?
metadata_observation AS (
    SELECT
        item.item_id,
        item.handle,
        metadata.metadata_value_id,
        metadata.metadatafield_fullname,
        metadata.text_value,
        REPLACE(
            LOWER(metadata.text_value),
            '%2f',
            '/'
        ) AS normalized_text
    FROM published_item AS item
    INNER JOIN {{ ref('int_unlp_sedicidb_item_metadatavalue') }} AS metadata
        USING (item_id)
    WHERE metadata.metadatafield_fullname IN (
        'dc.identifier.uri',
        'sedici.identifier.doi',
        'sedici.identifier.other'
    )
      AND metadata.text_value IS NOT NULL
),

-- ¿Cómo se prepara el texto para buscar un DOI?
candidate_source AS (
    SELECT
        *,
        CASE
            WHEN normalized_text ~ 'https?://(dx[.])?doi[.]org/'
                THEN REGEXP_REPLACE(normalized_text, '[?#].*$', '')
            ELSE normalized_text
        END AS doi_source
    FROM metadata_observation
),

-- ¿Qué DOI puede extraerse de cada observación?
doi_candidate AS (
    SELECT
        *,
        REGEXP_REPLACE(
            SUBSTRING(
                doi_source
                FROM '(10[.][0-9]{4,9}/[^[:space:]<>|";]+)'
            ),
            '[.),;:]+$',
            ''
        ) AS doi
    FROM candidate_source
),

-- ¿Qué DOI cumplen las reglas de aceptación?
accepted_observation AS (
    SELECT *
    FROM doi_candidate
    WHERE doi IS NOT NULL
      AND doi !~ '[%?#]'
      AND (
          metadatafield_fullname IN (
              'sedici.identifier.doi',
              'sedici.identifier.other'
          )
          OR (
              metadatafield_fullname = 'dc.identifier.uri'
              AND doi = '10.35537/' || handle
          )
      )
),

-- ¿Qué DOI se asocian con cada publicación visible de SEDICI?
final AS (
    SELECT
        item_id,
        handle,
        doi,
        STRING_AGG(
            DISTINCT metadatafield_fullname,
            '|'
            ORDER BY metadatafield_fullname
        ) AS metadatafields,
        ARRAY_AGG(
            DISTINCT metadata_value_id
            ORDER BY metadata_value_id
        ) AS metadata_value_ids,
        COUNT(DISTINCT metadata_value_id)::int AS observation_count,
        BOOL_OR(metadatafield_fullname = 'sedici.identifier.doi') AS has_explicit_doi,
        BOOL_OR(
            metadatafield_fullname = 'sedici.identifier.other'
        ) AS has_sedici_identifier_other,
        BOOL_OR(metadatafield_fullname = 'dc.identifier.uri') AS has_institutional_doi_uri
    FROM accepted_observation
    GROUP BY
        item_id,
        handle,
        doi
)

SELECT *
FROM final

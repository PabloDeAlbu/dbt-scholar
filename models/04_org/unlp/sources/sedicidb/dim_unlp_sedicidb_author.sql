{{ config(
    materialized='table',
    indexes=[
        {'columns': ['authority'], 'unique': true}
    ],
    post_hook=[
        "analyze {{ this }}"
    ]
) }}

-- ¿Qué nombres se observan para cada autoridad de autor?
WITH authority_name AS (
    SELECT
        authority,
        author_name,
        COUNT(*)::bigint AS name_count
    FROM {{ ref('brg_unlp_sedicidb_item_author') }}
    WHERE authority IS NOT NULL
    GROUP BY authority, author_name
),

-- ¿Qué nombre se prioriza para representar cada autoridad?
preferred_name AS (
    SELECT
        authority,
        author_name AS preferred_name,
        ROW_NUMBER() OVER (
            PARTITION BY authority
            ORDER BY name_count DESC, author_name
        ) AS name_rank
    FROM authority_name
),

-- ¿Qué nombres distintos están asociados con cada autoridad?
authority_names AS (
    SELECT
        authority,
        STRING_AGG(author_name, '|' ORDER BY author_name) AS names
    FROM authority_name
    GROUP BY authority
),

-- ¿Qué autoridades de autor existen y cómo se representan de forma legible?
final AS (
    SELECT
        names.authority,
        preferred.preferred_name,
        names.names
    FROM authority_names AS names
    INNER JOIN preferred_name AS preferred
        ON preferred.authority = names.authority
       AND preferred.name_rank = 1
)

SELECT *
FROM final

{{ config(
    materialized='table',
    indexes=[
        {'columns': ['metadata_value_id'], 'unique': true},
        {'columns': ['item_id']},
        {'columns': ['authority']},
        {'columns': ['sedici_community_id', 'publication_year']}
    ],
    post_hook=["analyze {{ this }}"]
) }}

-- ¿Qué autorías personales corresponden a publicaciones de revistas?
WITH publication_author AS (
    SELECT
        publication.item_id,
        author.metadata_value_id,
        author.author_name,
        author.author_name_raw,
        author.author_place,
        author.authority,
        author.has_authority_control,
        publication.sedici_community_id,
        publication.sedici_community_title,
        publication.publication_date,
        publication.publication_year
    FROM {{ ref('fct_unlp_sedici_revista_publicacion') }} AS publication
    INNER JOIN {{ ref('brg_unlp_sedicidb_item_author') }} AS author
        USING (item_id)
    WHERE author.author_role = 'creator_person'
      AND author.author_type = 'person'
),

-- ¿Qué publicaciones de revistas se relacionan con qué autorías declaradas?
final AS (
    SELECT *
    FROM publication_author
)

SELECT *
FROM final

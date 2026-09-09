{{ config(
    materialized='table',
    indexes=[
        {'columns': ['item_id', 'sedicidb_author_id'], 'unique': true},
        {'columns': ['item_id']},
        {'columns': ['sedicidb_author_id']},
        {'columns': ['sedici_community_id', 'publication_year']}
    ],
    post_hook=["analyze {{ this }}"]
) }}

WITH author_observation AS (
    SELECT
        item.item_id,
        item.sedici_community_id,
        item.sedici_community_title,
        item.publication_date,
        item.publication_year,
        bridge.author_id AS sedicidb_author_id,
        bridge.author_place,
        bridge.metadata_value_id,
        ROW_NUMBER() OVER (
            PARTITION BY item.item_id, bridge.author_id
            ORDER BY
                bridge.author_place NULLS LAST,
                bridge.metadata_value_id DESC
        ) AS observation_rank
    FROM {{ ref('fct_unlp_sedici_revista_publicacion') }} AS item
    INNER JOIN {{ ref('brg_unlp_sedicidb_item_author') }} AS bridge
        USING (item_id)
    WHERE bridge.author_role = 'creator_person'
      AND bridge.author_type = 'person'
),

author_prepared AS (
    SELECT
        observation.*,
        author.author_name_preferred AS author_name,
        author.author_name_normalized,
        author.authority,
        author.has_authority_control
    FROM author_observation AS observation
    INNER JOIN {{ ref('dim_unlp_sedicidb_author') }} AS author
        ON author.author_id = observation.sedicidb_author_id
    WHERE observation.observation_rank = 1
)

SELECT
    author.item_id,
    author.sedicidb_author_id,
    author.author_name,
    author.author_name_normalized,
    author.author_place,
    author.metadata_value_id,
    author.authority,
    author.has_authority_control,
    author.sedici_community_id,
    author.sedici_community_title,
    author.publication_date,
    author.publication_year
FROM author_prepared AS author

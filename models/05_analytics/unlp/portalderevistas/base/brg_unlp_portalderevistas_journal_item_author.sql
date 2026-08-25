{{ config(
    materialized='table',
    indexes=[
        {'columns': ['item_id', 'author_id'], 'unique': true},
        {'columns': ['item_id']},
        {'columns': ['author_id']},
        {'columns': ['journal_id', 'publication_year']}
    ],
    post_hook=["analyze {{ this }}"]
) }}

WITH author_observation AS (
    SELECT
        item.item_id,
        item.journal_id,
        item.journal_title,
        item.publication_date,
        item.publication_year,
        item.is_article,
        bridge.author_id,
        bridge.author_place,
        bridge.metadata_value_id,
        bridge.authority_uri,
        bridge.authority_host,
        bridge.authority_path,
        bridge.has_authority_control,
        ROW_NUMBER() OVER (
            PARTITION BY item.item_id, bridge.author_id
            ORDER BY
                bridge.author_place NULLS LAST,
                bridge.metadata_value_id DESC
        ) AS observation_rank
    FROM {{ ref('fct_unlp_portalderevistas_journal_item') }} AS item
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
        CASE
            WHEN observation.authority_host = 'voc.sedici.unlp.edu.ar'
             AND observation.authority_path ~ '^node/[0-9]+/?$'
                THEN SUBSTRING(
                    observation.authority_path
                    FROM 'node/([0-9]+)'
                )::bigint
        END AS authority_voc_person_node_id
    FROM author_observation AS observation
    INNER JOIN {{ ref('dim_unlp_sedicidb_author') }} AS author
        USING (author_id)
    WHERE observation.observation_rank = 1
)

SELECT
    author.item_id,
    author.author_id,
    author.author_name,
    author.author_name_normalized,
    author.author_place,
    author.metadata_value_id,
    author.authority_uri,
    author.has_authority_control,
    person.person_node_id AS voc_person_node_id,
    person.person_node_id IS NOT NULL AS has_voc_match,
    author.journal_id,
    author.journal_title,
    author.publication_date,
    author.publication_year,
    author.is_article
FROM author_prepared AS author
LEFT JOIN {{ ref('dim_vocsedici_persona') }} AS person
    ON person.person_node_id = author.authority_voc_person_node_id

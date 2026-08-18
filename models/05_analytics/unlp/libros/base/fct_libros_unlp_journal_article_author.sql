{{ config(
    indexes=[
        {'columns': ['item_id', 'author_id'], 'unique': true},
        {'columns': ['journal_id', 'publication_year']},
        {'columns': ['affiliation_status']},
        {'columns': ['voc_person_node_id']}
    ],
    post_hook=["analyze {{ this }}"]
) }}

WITH article AS (
    SELECT
        base.item_id,
        base.in_archive,
        base.withdrawn,
        base.discoverable,
        base.handle,
        base.dc_identifier_uri AS article_url,
        base.dc_title AS article_title,
        base.dc_type,
        base.sedici_subtype,
        base.dc_date_issued AS publication_date,
        base.publication_year,
        journal.journal_id,
        journal.journal_title,
        base.owning_root_community_title,
        base.owning_community_path_ids,
        base.owning_community_path_titles
    FROM {{ ref('fct_unlp_sedicidb_item_publication') }} AS base
    INNER JOIN {{ ref('dim_libros_unlp_journal') }} AS journal
        ON journal.journal_id = NULLIF(
            SPLIT_PART(base.owning_community_path_ids, ' > ', 2),
            ''
        )::bigint
    WHERE base.in_archive IS TRUE
      AND base.withdrawn IS FALSE
      AND base.discoverable IS TRUE
      AND base.handle IS NOT NULL
      AND base.owning_root_community_title = 'Revistas'
      AND base.dc_type IN ('Articulo', 'Artículo')
      AND base.dc_date_issued IS NOT NULL
      AND base.publication_year IS NOT NULL
),

author_observation AS (
    SELECT
        article.*,
        bridge.author_id,
        bridge.author_place,
        bridge.metadata_value_id,
        bridge.authority_uri,
        bridge.authority_host,
        bridge.authority_path,
        bridge.has_authority_control,
        ROW_NUMBER() OVER (
            PARTITION BY article.item_id, bridge.author_id
            ORDER BY bridge.author_place NULLS LAST, bridge.metadata_value_id
        ) AS observation_rank
    FROM article
    INNER JOIN {{ ref('brg_unlp_sedicidb_item_author') }} AS bridge
        USING (item_id)
    WHERE bridge.author_role = 'creator_person'
      AND bridge.author_type = 'person'
),

author_prepared AS (
    SELECT
        observation.*,
        author.author_name_preferred AS author_name,
        CASE
            WHEN observation.authority_host = 'voc.sedici.unlp.edu.ar'
             AND observation.authority_path ~ '^node/[0-9]+/?$'
                THEN SUBSTRING(observation.authority_path FROM 'node/([0-9]+)')::bigint
        END AS authority_voc_person_node_id
    FROM author_observation AS observation
    INNER JOIN {{ ref('dim_unlp_sedicidb_author') }} AS author
        USING (author_id)
    WHERE observation.observation_rank = 1
),

author_voc AS (
    SELECT
        author.*,
        person.person_node_id AS voc_person_node_id,
        person.person_node_id IS NOT NULL AS has_voc_match
    FROM author_prepared AS author
    LEFT JOIN {{ ref('dim_vocsedici_persona') }} AS person
        ON person.person_node_id = author.authority_voc_person_node_id
),

affiliation_stats AS (
    SELECT
        author.item_id,
        author.author_id,
        COUNT(DISTINCT affiliation.institution_node_id) FILTER (
            WHERE institution.pertenece_arbol_unlp IS TRUE
        )::integer AS unlp_institution_count,
        COUNT(DISTINCT affiliation.institution_node_id) FILTER (
            WHERE institution.pertenece_arbol_unlp IS FALSE
        )::integer AS external_institution_count
    FROM author_voc AS author
    LEFT JOIN {{ ref('fct_vocsedici_persona_afiliacion') }} AS affiliation
        ON affiliation.person_node_id = author.voc_person_node_id
       AND affiliation.afiliacion_esta_publicada IS TRUE
       AND affiliation.person_esta_publicada IS TRUE
       AND affiliation.institution_esta_publicada IS TRUE
       AND (affiliation.fecha_inicio IS NULL OR affiliation.fecha_inicio <= author.publication_date)
       AND (affiliation.fecha_fin IS NULL OR affiliation.fecha_fin >= author.publication_date)
    LEFT JOIN {{ ref('dim_vocsedici_institucion') }} AS institution
        USING (institution_node_id)
    GROUP BY author.item_id, author.author_id
),

final AS (
    SELECT
        author.item_id,
        author.author_id,
        author.author_name,
        author.author_place,
        author.handle,
        author.article_url,
        author.article_title,
        author.dc_type,
        author.sedici_subtype,
        author.publication_date,
        author.publication_year,
        author.journal_id,
        author.journal_title,
        author.owning_root_community_title,
        author.owning_community_path_ids,
        author.owning_community_path_titles,
        author.in_archive,
        author.withdrawn,
        author.discoverable,
        author.authority_uri,
        author.has_authority_control,
        author.voc_person_node_id,
        author.has_voc_match,
        CASE
            WHEN affiliation.unlp_institution_count > 0 THEN 'unlp'
            WHEN affiliation.external_institution_count > 0 THEN 'external'
            ELSE 'unknown'
        END::text AS affiliation_status,
        CASE
            WHEN affiliation.unlp_institution_count > 0 THEN 'voc_unlp_affiliation'
            WHEN affiliation.external_institution_count > 0 THEN 'voc_external_affiliation'
            WHEN NOT author.has_voc_match THEN 'no_voc_match'
            ELSE 'voc_without_dated_affiliation'
        END::text AS affiliation_evidence,
        affiliation.unlp_institution_count,
        affiliation.external_institution_count
    FROM author_voc AS author
    INNER JOIN affiliation_stats AS affiliation
        USING (item_id, author_id)
)

SELECT *
FROM final

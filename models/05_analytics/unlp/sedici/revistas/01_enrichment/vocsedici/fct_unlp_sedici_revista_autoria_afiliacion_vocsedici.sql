{{ config(
    indexes=[
        {'columns': ['item_id', 'sedicidb_author_id'], 'unique': true},
        {'columns': ['sedici_community_id', 'publication_year']},
        {'columns': ['affiliation_status']},
        {'columns': ['voc_person_node_id']}
    ],
    post_hook=["analyze {{ this }}"]
) }}

WITH revista_publication AS (
    SELECT
        base.item_id,
        base.in_archive,
        base.withdrawn,
        base.discoverable,
        base.handle,
        base.item_url AS publication_url,
        base.item_title AS publication_title,
        base.dc_type,
        base.sedici_subtype,
        base.publication_date,
        base.publication_year,
        base.sedici_community_id,
        base.sedici_community_title,
        base.owning_root_community_title,
        base.owning_community_path_ids,
        base.owning_community_path_titles
    FROM {{ ref('fct_unlp_sedici_revista_publicacion') }} AS base
),

authorship AS (
    SELECT
        revista_publication.*,
        author.sedicidb_author_id,
        author.author_name,
        author.author_place,
        author.authority,
        author.has_authority_control,
        identity.author_identity_basis,
        identity.voc_person_node_id,
        identity.has_voc_match,
        identity.voc_person_url
    FROM revista_publication
    INNER JOIN {{ ref('brg_unlp_sedici_revista_publicacion_autor') }} AS author
        USING (item_id)
    LEFT JOIN {{ ref('dim_unlp_author') }} AS identity
        ON identity.author_id = author.sedicidb_author_id
),

affiliation_stats AS (
    SELECT
        author.item_id,
        author.sedicidb_author_id,
        COUNT(DISTINCT affiliation.institution_node_id) FILTER (
            WHERE affiliation.is_unlp_affiliation IS TRUE
        )::integer AS unlp_institution_count,
        COUNT(DISTINCT affiliation.institution_node_id) FILTER (
            WHERE affiliation.is_unlp_affiliation IS FALSE
        )::integer AS external_institution_count
    FROM authorship AS author
    LEFT JOIN {{ ref('fct_unlp_author_affiliation') }} AS affiliation
        ON affiliation.author_id = author.sedicidb_author_id
       AND (affiliation.valid_from IS NULL OR affiliation.valid_from <= author.publication_date)
       AND (affiliation.valid_to IS NULL OR affiliation.valid_to >= author.publication_date)
    GROUP BY author.item_id, author.sedicidb_author_id
),

final AS (
    SELECT
        author.item_id,
        author.sedicidb_author_id,
        author.author_name,
        author.author_place,
        author.handle,
        author.publication_url,
        author.publication_title,
        author.dc_type,
        author.sedici_subtype,
        author.publication_date,
        author.publication_year,
        author.sedici_community_id,
        author.sedici_community_title,
        author.owning_root_community_title,
        author.owning_community_path_ids,
        author.owning_community_path_titles,
        author.in_archive,
        author.withdrawn,
        author.discoverable,
        author.authority,
        author.has_authority_control,
        author.author_identity_basis,
        author.voc_person_node_id,
        author.has_voc_match,
        author.voc_person_url,
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
    FROM authorship AS author
    INNER JOIN affiliation_stats AS affiliation
        USING (item_id, sedicidb_author_id)
)

SELECT *
FROM final

{{ config(
    materialized='table',
    indexes=[
        {'columns': ['item_id'], 'unique': true},
        {'columns': ['journal_id', 'publication_year']},
        {'columns': ['sedici_subtype']},
        {'columns': ['is_article']}
    ],
    post_hook=["analyze {{ this }}"]
) }}

SELECT
    item.item_id,
    item.handle,
    item.dc_identifier_uri AS item_url,
    item.dc_title AS item_title,
    item.dc_type,
    item.sedici_subtype,
    item.dc_date_issued AS publication_date,
    item.publication_year,
    journal.journal_id,
    journal.journal_title,
    journal.is_closed,
    item.owning_root_community_title,
    item.owning_community_path_ids,
    item.owning_community_path_titles,
    item.in_archive,
    item.withdrawn,
    item.discoverable,
    item.sedici_subtype IN (
        'Articulo',
        'Comunicacion',
        'Contribucion a revista',
        'Revision'
    ) AS is_article
FROM {{ ref('fct_unlp_sedicidb_item_publication') }} AS item
INNER JOIN {{ ref('dim_unlp_portalderevistas_journal') }} AS journal
    ON journal.journal_id = NULLIF(
        SPLIT_PART(item.owning_community_path_ids, ' > ', 2),
        ''
    )::bigint
WHERE item.in_archive IS TRUE
  AND item.withdrawn IS FALSE
  AND item.discoverable IS TRUE
  AND item.handle IS NOT NULL
  AND item.owning_root_community_title = 'Revistas'

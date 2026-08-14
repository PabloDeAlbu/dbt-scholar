WITH manual_duplicate AS (
    SELECT
        '10915/52442'::text AS sedici_id,
        '42205'::text AS koha_id,
        'Verificación manual: mismo libro con diferencia entre artes visuales y audiovisuales'::text AS resolution_note
),

dedup_result AS (
    SELECT
        BTRIM(id_document1) AS sedici_id,
        NULLIF(BTRIM(id_document2), 'None') AS koha_candidate_id,
        NULLIF(BTRIM(rules), 'None') AS dedup_rules,
        BTRIM(similarity) AS dedup_similarity
    FROM {{ ref('seed_issue_14365_02_dedup_book_sedici_koha_result') }}
),

candidate AS (
    SELECT
        publication.*,
        result.dedup_similarity,
        result.dedup_rules,
        manual.koha_id AS manually_matched_koha_id,
        manual.resolution_note AS manual_resolution_note
    FROM {{ ref('unlp_sedici_02_dedup_book_sedici_koha_input_1') }} AS publication
    INNER JOIN dedup_result AS result
        ON result.sedici_id = publication.id
    LEFT JOIN manual_duplicate AS manual
        ON manual.sedici_id = publication.id
    WHERE result.dedup_similarity = 'NO_DUPLICATE'
      AND manual.sedici_id IS NULL
)

SELECT
    id AS sedici_id,
    'https://sedici.unlp.edu.ar/handle/' || id AS sedici_url,
    title,
    subtitle,
    author,
    date,
    isbn,
    doi,
    issn,
    subject,
    description,
    owning_community_path_titles,
    owning_collection_title,
    mods_origin_info_place,
    dc_publisher,
    matches_fda_path,
    matches_fda_origin,
    matches_fda_publisher,
    fda_evidence,
    dedup_similarity,
    dedup_rules
FROM candidate

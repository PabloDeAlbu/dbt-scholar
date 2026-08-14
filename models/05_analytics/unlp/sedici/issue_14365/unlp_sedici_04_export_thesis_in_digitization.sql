WITH digitization_status AS (
    SELECT
        "INVENTARIO"::text AS inventory_id,
        MAX("valido a la fecha"::text) AS valid_as_of,
        STRING_AGG(
            DISTINCT NULLIF(BTRIM("Enviado al SEDICI"::text), ''),
            '|'
        ) AS sent_to_sedici,
        STRING_AGG(
            DISTINCT NULLIF(BTRIM("Devuelto por SEDICI"::text), ''),
            '|'
        ) AS returned_by_sedici,
        STRING_AGG(
            DISTINCT NULLIF(BTRIM("preparadas para envio"::text), ''),
            '|'
        ) AS prepared_for_delivery,
        STRING_AGG(
            DISTINCT NULLIF(BTRIM("Observaciones"::text), ''),
            '|'
        ) AS digitization_observations,
        STRING_AGG(
            DISTINCT NULLIF(BTRIM("comentarios"::text), ''),
            '|'
        ) AS digitization_comments
    FROM {{ ref('seed_issue_14365_04_thesis_fda_digitization') }}
    GROUP BY "INVENTARIO"::text
)

SELECT
    match.koha_id,
    'https://artes.bibliotecas.unlp.edu.ar/cgi-bin/koha/opac-detail.pl?biblionumber='
        || match.koha_id AS koha_url,
    match.canonical_koha_id,
    match.koha_id = match.canonical_koha_id AS is_canonical_koha_record,
    koha.title AS koha_title,
    koha.author AS koha_author,
    koha.date AS koha_date,
    match.inventory_id,
    digitization.title AS digitization_title,
    digitization.author AS digitization_author,
    digitization.date AS digitization_date,
    match.match_status,
    match.match_source,
    status.valid_as_of,
    status.sent_to_sedici,
    status.returned_by_sedici,
    status.prepared_for_delivery,
    status.digitization_observations,
    status.digitization_comments
FROM {{ ref('unlp_sedici_04_int_thesis_digitization_match') }} AS match
INNER JOIN {{ ref('unlp_sedici_04_dedup_thesis_digitization_input_1') }} AS koha
    ON koha.id = match.koha_id
INNER JOIN {{ ref('unlp_sedici_04_dedup_thesis_digitization_input_2') }} AS digitization
    ON digitization.id = match.inventory_id
LEFT JOIN digitization_status AS status
    USING (inventory_id)

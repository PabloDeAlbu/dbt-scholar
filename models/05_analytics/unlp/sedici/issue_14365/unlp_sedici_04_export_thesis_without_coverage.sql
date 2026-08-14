SELECT
    koha.id AS koha_id,
    'https://artes.bibliotecas.unlp.edu.ar/cgi-bin/koha/opac-detail.pl?biblionumber='
        || koha.id AS koha_url,
    koha.title,
    koha.author,
    koha.date,
    koha.type,
    koha.subtitle,
    koha.doi,
    koha.isbn,
    koha.issn,
    koha.description
FROM {{ ref('unlp_sedici_04_dedup_thesis_digitization_input_1') }} AS koha
LEFT JOIN {{ ref('unlp_sedici_04_int_thesis_digitization_match') }} AS match
    ON match.koha_id = koha.id
WHERE match.koha_id IS NULL

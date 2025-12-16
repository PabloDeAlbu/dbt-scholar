WITH dc_relation_doi_agg AS (
    SELECT 
        record_hk,
        -- Tomamos uno como "principal" para mostrar por defecto
        MIN(identifier_value) as primary_doi,
        
        -- Concatenamos todos los encontrados separados por " || "
        STRING_AGG(identifier_value, ' || ') as all_dois_concatenated,
        
        -- Contamos cuántos hay para generar los flags
        COUNT(*) as count_doi
    FROM {{ ref('brg_oai_record_relation') }} 
    WHERE relation_type = 'altIdentifier' AND identifier_type = 'doi'
    GROUP BY record_hk
)

SELECT * FROM dc_relation_doi_agg

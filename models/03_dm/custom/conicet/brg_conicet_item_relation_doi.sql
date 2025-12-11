WITH dc_relation_doi_agg AS (
    SELECT 
        record_hk,
        -- Tomamos uno como "principal" para mostrar por defecto
        MIN(REPLACE(dc_relation,'info:eu-repo/semantics/altIdentifier/doi/', '')) as primary_doi,
        
        -- Concatenamos todos los encontrados separados por " || "
        STRING_AGG(REPLACE(dc_relation,'info:eu-repo/semantics/altIdentifier/doi/', ''), ' || ') as all_dois_concatenated,
        
        -- Contamos cuántos hay para generar los flags
        COUNT(*) as count_doi
    FROM {{ ref('brg_oai_record_relation') }} 
    WHERE dc_relation LIKE 'info:eu-repo/semantics/altIdentifier/doi/%'
    GROUP BY record_hk
)

SELECT * FROM dc_relation_doi_agg

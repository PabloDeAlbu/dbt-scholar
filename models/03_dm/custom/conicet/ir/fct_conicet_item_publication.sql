{{ config(materialized = 'table') }}

WITH 
base as (
    SELECT DISTINCT
        fct.record_id,
        fct.title,
        fct.date_issued,
        dim_type.label as coar_type,
        dim_type.coar_uri,
        dim_right.coar_label as coar_right,
        fct.record_hk
    FROM {{ref('fct_oai_record_publication')}} fct
    INNER JOIN {{ref('brg_oai_record_type')}} USING (record_hk)
    INNER JOIN {{ref('dim_conicet_item_type')}} dim_type USING (dc_type_hk)
    INNER JOIN {{ref('brg_oai_record_right')}} USING (record_hk)
    INNER JOIN {{ref('dim_conicet_item_accessright')}} dim_right USING (dc_right_hk)
),

-- 4. Ensamblado Final con Lógica de Negocio
final AS (
    SELECT 
        base.*,
        -- Handle (Si es null, queda null)
        uri.dc_identifier_uri,
        
        -- DOI Principal
        doi.primary_doi as dc_relation_doi,
        
        -- Lógica de DOIs múltiples y auditoría
        COALESCE(doi.all_dois_concatenated, doi.primary_doi) as doi_audit_string,
        
        -- Flags Booleanos (True/False)
        CASE WHEN doi.count_doi > 0 THEN true ELSE false END as has_doi,
        CASE WHEN doi.count_doi > 1 THEN true ELSE false END as has_multiple_doi,        
        
        -- Número exacto para duplicados
        COALESCE(doi.count_doi, 0) as doi_count_check,

        fos.fos_label_level_1 as subject_area,      -- Nombre amigable para BI
        fos.fos_label_level_2 as subject_subarea,   -- Nombre amigable para BI
        fos.fos_code_level_1,
        
        -- Auditoría de FOS (Opcional)
        COALESCE(fos.has_multiple_fos_assignments, false) as mult_fos_flag

    FROM base
    INNER JOIN {{ref('brg_conicet_item_identifier_uri')}} uri USING (record_hk)
    LEFT JOIN {{ref('brg_conicet_item_relation_doi')}} doi USING (record_hk)
    
    -- AQUÍ AGREGAMOS EL FOS APLANADO
    LEFT JOIN {{ref('brg_conicet_item_fos_flattened')}} fos USING (record_hk)
)

SELECT * FROM final
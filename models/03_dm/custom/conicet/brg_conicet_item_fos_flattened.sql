{{ config(materialized = 'table') }}

WITH 
-- 1. Traemos la relación cruda (Paper <-> Subject)
raw_rel AS (
    SELECT record_hk, dc_subject_hk
    FROM {{ ref('brg_oai_record_subject') }}
),

-- 2. Cruzamos con nuestra Dimensión FOS limpia (que usa tu Seed)
joined AS (
    SELECT 
        r.record_hk,
        d.label_level_1, -- Ej: "Ciencias Agrícolas"
        d.label_level_2, -- Ej: "Agricultura"
        d.code_level_1,
        d.code_level_2
    FROM raw_rel r
    INNER JOIN {{ ref('dim_conicet_item_subject_fos') }} d USING (dc_subject_hk)
),

-- 3. APLANADO DEFENSIVO (La clave del éxito)
-- Agrupamos por record_hk para garantizar 1 fila por paper.
grouped AS (
    SELECT 
        record_hk,
        -- Si hay más de una, MAX() toma la última alfabéticamente. 
        -- Es una regla de negocio arbitraria pero segura para evitar duplicados.
        MAX(label_level_1) as fos_label_level_1,
        MAX(label_level_2) as fos_label_level_2,
        MAX(code_level_1) as fos_code_level_1,
        MAX(code_level_2) as fos_code_level_2,
        
        -- Flag de auditoría: ¿Perdimos datos al aplanar?
        (COUNT(*) > 1) as has_multiple_fos_assignments
        
    FROM joined
    GROUP BY record_hk
)

SELECT * FROM grouped
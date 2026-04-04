{{ config(materialized = 'table') }}

WITH 
-- 1. Base: La relación Autor-Paper (Un autor aparece tantas veces como papers tenga)
creators_base AS (
    SELECT 
        author,
        filliation,
        institution_count,
        record_hk
    FROM {{ ref('brg_conicet_record_fil') }}
    WHERE author IS NOT NULL AND author != ''
),

-- 2. Contexto: Datos del paper para poder filtrar luego
papers_context AS (
    SELECT 
        record_hk,
        record_id,
        title,
        publication_date,
        subject_area,      -- FOS Nivel 1
        subject_subarea,   -- FOS Nivel 2
        publication_type,
        has_doi
    FROM {{ ref('fct_conicet_publication') }}
)

-- 3. Tabla Final: Centrada en la actividad del Autor
SELECT 
    -- Dimensiones de Autor
    upper(replace(c.author, '.', '')) as author,
    c.filliation,
    c.institution_count,

    -- Dimensiones de Tiempo y Contexto (para filtros)
    p.record_id,
    p.title,
    p.publication_date as date_issued,
    p.subject_area,
    p.subject_subarea,
    p.publication_type as coar_type,
    
    -- Métricas y Keys
    c.record_hk, -- Se usa para hacer COUNT(DISTINCT record_hk)
    
    1 as activity_count

FROM creators_base c
INNER JOIN papers_context p USING (record_hk)

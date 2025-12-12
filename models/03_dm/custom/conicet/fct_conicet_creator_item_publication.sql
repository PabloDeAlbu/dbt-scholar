{{ config(materialized = 'table') }}

WITH 
-- 1. Base: La relación Autor-Paper (Un autor aparece tantas veces como papers tenga)
creators_base AS (
    SELECT 
        dc_creator, -- Tu protagonista
        record_hk
    FROM {{ ref('brg_oai_record_creator') }}
    -- Opcional: Filtra nulos o nombres vacíos si ensucian el ranking
    WHERE dc_creator IS NOT NULL AND dc_creator != ''
),

-- 2. Contexto: Datos del paper para poder filtrar luego
papers_context AS (
    SELECT 
        record_hk,
        date_issued,
        subject_area,      -- FOS Nivel 1
        subject_subarea,   -- FOS Nivel 2
        coar_type,
        has_doi
    FROM {{ ref('fct_conicet_item_publication') }}
)

-- 3. Tabla Final: Centrada en la actividad del Autor
SELECT 
    -- Dimensiones de Autor
    c.dc_creator,

    -- Dimensiones de Tiempo y Contexto (para filtros)
    p.date_issued,
    p.subject_area,
    p.subject_subarea,
    p.coar_type,
    
    -- Métricas y Keys
    c.record_hk, -- Se usa para hacer COUNT(DISTINCT record_hk)
    
    1 as activity_count

FROM creators_base c
INNER JOIN papers_context p USING (record_hk)
{{ config(materialized='table') }}

with openalex_authors as (
    select
        author_id,
        display_name,
        works_count,
        cited_by_count,
        replace(orcid, 'https://orcid.org/','') as orcid,
        author_hk
    from {{ ref('dim_openalex_author') }}
),

openaire_authors as (
    select
        orcid,
        openaire_full_name,
        openaire_name,
        openaire_surname,
        orcid_hk
    from {{ ref('dim_openaire_author') }}
),

-- Unimos por ORCID conservando autores que estén en una u otra fuente
joined as (
    select
        coalesce(openalex.orcid, openaire.orcid) as orcid,
        openalex.author_id as openalex_author_id,
        openalex.display_name as openalex_display_name,
        openalex.works_count as openalex_works_count,
        openalex.cited_by_count as openalex_cited_by_count,
        openalex.author_hk as openalex_author_hk,
        openaire.full_name as openaire_full_name,
        openaire.name as openaire_name,
        openaire.surname as openaire_surname,
        (openalex.author_id is not null) as in_openalex,
        (openaire.orcid is not null) as in_openaire
    from openalex_authors openalex
    full outer join openaire_authors openaire
      on openalex.orcid = openaire.orcid
),

final as (
    select
        orcid,
        -- nombre canónico preferido (ajustá prioridad a gusto)
        coalesce(openaire_full_name, openalex_display_name) as display_name,

        -- métricas/campos de OA (útiles para análisis)
        openalex_author_id,
        openalex_author_hk,
        openalex_works_count,
        openalex_cited_by_count,

        -- flags de cobertura
        in_openalex,
        in_openaire,

        -- calidad de identificación
        (orcid is not null) as has_orcid,
        case
            when orcid is not null then 'HIGH'
            else 'LOW'
        end as id_confidence
    from joined
)

select * from final

{{ config(materialized = 'table') }}

WITH base AS (
    SELECT 
        hub_author.author_id,
        hub_orcid.orcid,
        sat_author.full_name,
        sat_author.name,
        sat_author.surname,
        hub_author.author_hk,
        hub_orcid.orcid_hk
    FROM {{ref('hub_openaire_author')}} hub_author
    INNER JOIN {{ref('sat_openaire_author')}} sat_author ON
        sat_author.author_hk =  hub_author.author_hk    
    INNER JOIN {{ref('link_openaire_author_orcid')}} link_author_orcid ON 
        link_author_orcid.author_hk =  sat_author.author_hk
    INNER JOIN {{ref('hub_openaire_orcid')}} hub_orcid ON 
        hub_orcid.orcid_hk =  link_author_orcid.orcid_hk
),

-- Selecciona un único registro por ORCID limpio, 
-- eligiendo la variante de nombre con orden alfabético más bajo.
-- Esto permite deduplicar autores que comparten el mismo ORCID 
-- pero tienen diferencias en la forma en que se registró su nombre.

openaire_authors AS (
    select *
    from (
        select 
            orcid,
            full_name,
            name,
            surname,
            row_number() over (
                partition by orcid
                order by full_name
            ) as rn,
            author_hk,
            orcid_hk
        from base
        where orcid is not null
    ) sub
    where rn = 1
),

final as (
    SELECT 
        orcid,
        full_name,
        name,
        surname,
        orcid_hk
    FROM openaire_authors
)

SELECT * 
FROM final

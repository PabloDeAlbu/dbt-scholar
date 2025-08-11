
-- Selecciona un único registro por ORCID limpio, 
-- eligiendo la variante de nombre con orden alfabético más bajo.
-- Esto permite deduplicar autores que comparten el mismo ORCID 
-- pero tienen diferencias en la forma en que se registró su nombre.

WITH openaire_authors AS (
    select *
    from (
        select 
            replace(orcid, 'https://orcid.org/','') as orcid,
            full_name,
            name  as openaire_name,
            surname as openaire_surname,
            row_number() over (
                partition by replace(orcid, 'https://orcid.org/','')
                order by full_name
            ) as rn,
            author_hk
        from {{ ref('dim_openaire_author') }}
        where orcid is not null
    ) sub
    where rn = 1
),

final as (
    SELECT 
        orcid,
        full_name,
        openaire_name,
        openaire_surname,
        orcid_hk
    FROM openaire_authors
    INNER JOIN {{ref('link_openaire_author_orcid')}} link_author_orcid ON 
        link_author_orcid.author_hk =  openaire_authors.author_hk
)

SELECT * 
FROM final

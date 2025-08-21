WITH base AS (
    SELECT
        work_author_hk,
        work_hk,
        author_hk
    FROM {{ref('brg_openalex_publication_author')}}
)

SELECT * FROM base
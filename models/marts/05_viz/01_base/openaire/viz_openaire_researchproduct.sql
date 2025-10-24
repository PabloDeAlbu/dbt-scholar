WITH base AS (
    SELECT 
        researchproduct_id,

        publication_date,
        embargo_end_date,
        type,
        main_title,
        language_code,
        language_label,
        publisher,

        best_access_right,
        best_access_right_uri,
        publicly_funded,
        is_green,
        is_in_diamond_journal,
        
        downloads,
        views,
        
        citation_class,
        citation_count,
        impulse,
        impulse_class,
        influence,
        influence_class,
        popularity,
        popularity_class

        has_publication_date

    FROM {{ref('fct_openaire_researchproduct_publication')}}
)

SELECT * FROM base

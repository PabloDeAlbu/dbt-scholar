WITH autor AS (
    SELECT *
    FROM {{ ref('dim_unlp_googlescholar_author') }}
),

final AS (
    SELECT
        scholar_user_id,
        author_name AS autor,
        profile_url AS url_perfil_scholar,
        affiliation AS afiliacion_declarada,
        verified_email AS email_verificado,
        verified_email_domain AS dominio_email_verificado,
        has_unlp_verified_email AS tiene_email_verificado_unlp,
        mentions_unlp_affiliation AS menciona_unlp_en_afiliacion,
        is_unlp_profile AS tiene_evidencia_unlp,
        cited_by_count AS cantidad_citas,
        interests AS intereses,
        interest_count AS cantidad_intereses,
        load_datetime AS fecha_carga
    FROM autor
)

SELECT *
FROM final

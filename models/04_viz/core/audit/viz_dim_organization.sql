with base as (
    SELECT* from {{ref('dim_organization')}}
),

final as (
    SELECT *
    FROM base
    WHERE 
        organization_ror = 'https://ror.org/02s7sax82' OR -- CIC ROR
        organization_ror = 'https://ror.org/03cqe8w59' OR -- CONICET ROR
        organization_ror = 'https://ror.org/01tjs6929' -- UNLP ROR
)

SELECT * FROM final

with source as (
    select * from {{ source('openaire', 'researchproduct_sources') }}
),
renamed as (
    select 
        id::text as researchproduct_id,
        sources::text,
        load_datetime::timestamp
    from source
)

select * from renamed

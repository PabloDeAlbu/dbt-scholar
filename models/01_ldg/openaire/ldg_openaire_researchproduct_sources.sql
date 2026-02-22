with source as (
    select * from {{ source('openaire', 'researchproduct_sources') }}
),
renamed as (
    select 
        id::text as researchproduct_id,
        sources::text,
        dv_load_datetime::timestamp
    from source
),
ghost_record as (
    select
        '!UNKNOWN'::text as researchproduct_id,
        '!UNKNOWN'::text as sources,
        {{ dbt_date.today() }} as dv_load_datetime
)

select * from renamed
union all
select * from ghost_record

with source as (
    select 
        * 
    from {{ source('dspace', 'metadataschemaregistry') }}
),
renamed as (
    select 
        metadata_schema_id,
        namespace,
        short_id,
        {{ dbt_date.today() }} as load_datetime
    from source
),
ghost_record as (
    select
        -1 as metadata_schema_id,
        '!UNKNOWN' as namespace,
        '!UNKNOWN' as short_id,
        {{ dbt_date.today() }} as load_datetime
)

select * from renamed
union all
select * from ghost_record

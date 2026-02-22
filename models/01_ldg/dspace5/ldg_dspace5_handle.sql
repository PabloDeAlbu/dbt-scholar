with source as (
  select * from {{ source('dspace5', 'handle') }}
),
renamed as (
  select
    handle_id,
    handle,
    resource_type_id,
    resource_id,
    {{ dbt_date.today() }} as dv_load_datetime
  from source
),
ghost_record as (
  select
    -1 as handle_id,
    '!UNKNOWN' as handle,
    -1 as resource_type_id,
    -1 as resource_id,
    {{ dbt_date.today() }} as dv_load_datetime
)

select * from renamed
union all
select * from ghost_record

with source as (
  select * from {{ source('dspace5', 'item2bundle') }}
),
renamed as (
  select
    id,
    item_id,
    bundle_id,
    {{ dbt_date.today() }} as _load_datetime
  from source
),
ghost_record as (
  select
    -1 as id,
    -1 as item_id,
    -1 as bundle_id,
    {{ dbt_date.today() }} as _load_datetime
)
select * from renamed
union all
select * from ghost_record
    

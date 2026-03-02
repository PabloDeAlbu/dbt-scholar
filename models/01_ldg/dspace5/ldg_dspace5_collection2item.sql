with source as (
  select * from {{ source('dspace5', 'collection2item') }}
),
renamed as (
  select
    id as collection_item_id,
    collection_id,
    item_id,
    {{ dbt_date.today() }} as _load_datetime
  from source
),
ghost_record as (
  select
    -1 as collection_item_id,
    -1 as collection_id,
    -1 as item_id,
    {{ dbt_date.today() }} as _load_datetime
)
select * from renamed
union all
select * from ghost_record

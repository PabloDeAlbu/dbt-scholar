with source as (
  select * from {{ source('dspace5', 'collection2item') }}
),
renamed as (
  select
    id as collection_item_id,
    collection_id,
    item_id,
    {{ dbt_date.today() }} as load_datetime
  from source
)
select * from renamed

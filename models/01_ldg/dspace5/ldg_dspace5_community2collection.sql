with source as (
  select * from {{ source('dspace5', 'community2collection') }}
),

renamed as (
  select
    id as community_collection_id,
    community_id,
    collection_id,
    {{ dbt_date.today() }} as load_datetime

  from source
)

select * from renamed

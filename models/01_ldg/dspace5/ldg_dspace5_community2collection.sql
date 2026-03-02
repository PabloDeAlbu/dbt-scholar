with source as (
  select * from {{ source('dspace5', 'community2collection') }}
),
renamed as (
  select
    id as community_collection_id,
    community_id,
    collection_id,
    {{ dbt_date.today() }} as _load_datetime
  from source
),
ghost_record as (
  select
    -1 as community_collection_id,
    -1 as community_id,
    -1 as collection_id,
    {{ dbt_date.today() }} as _load_datetime
)

select * from renamed
union all
select * from ghost_record

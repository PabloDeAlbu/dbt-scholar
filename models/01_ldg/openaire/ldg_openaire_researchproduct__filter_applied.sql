with source as (
  select * from {{ source('openaire', 'researchproduct') }}
),
final as (
  select
    id::text as researchproduct_id,
    _filter_param::text as _filter_param,
    _filter_value::text as _filter_value,
    _load_datetime::timestamp as _load_datetime
  from source
)

select * from final

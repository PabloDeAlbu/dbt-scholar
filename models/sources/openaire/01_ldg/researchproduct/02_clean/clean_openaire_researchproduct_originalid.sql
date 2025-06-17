with base as (
  select 
    researchproduct_id,
    original_id,
    load_datetime
  from {{ ref('rel_openaire_researchproduct_originalid')}}
)

select * from base

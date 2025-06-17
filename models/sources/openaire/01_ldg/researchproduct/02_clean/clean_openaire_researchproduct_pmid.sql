with base as (
  select
    researchproduct_id,
    scheme,
    value as pmid,
    load_datetime,
    case
      when not (value ~* '^\d{7,8}$') then 'formato de pmid invalido'
      else 'ok'
    end as valid_reason
  from {{ ref('rel_openaire_researchproduct_pids')}}
  where scheme = 'pmid'
)

select * from base

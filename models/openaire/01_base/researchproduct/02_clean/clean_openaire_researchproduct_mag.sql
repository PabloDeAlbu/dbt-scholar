with base as (
  select
    researchproduct_id,
    scheme,
    value as mag,
    load_datetime,
    case
      when not (value ~* '^\d{5,}$') then 'formato mag invalido'
      else 'ok'
    end as valid_reason
  from {{ ref('rel_openaire_researchproduct_pids')}}
  where scheme = 'mag_id'
)

select * from base

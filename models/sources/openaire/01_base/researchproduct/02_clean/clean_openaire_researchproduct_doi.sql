with base as (
  select
    researchproduct_id,
    scheme,
    value as doi,
    load_datetime,
    case
      when not (value ~* '^10\.\d{4,9}/[-._;()/:a-zA-Z0-9]+$') then 'formato doi invalido'
      else 'ok'
    end as valid_reason
  from {{ ref('rel_openaire_researchproduct_pids')}}
  where scheme = 'doi'
  and value ~* '^10\.\d{4,9}/[-._;()/:a-zA-Z0-9]+$'  -- Solo mantiene DOIs válidos
)

select * from base

with base as (
  select 
    researchproduct_id,
    scheme,
    value as handle,
    load_datetime,
    case
      when not (value ~* '^\d{4,5}/\w+$') then 'formato handle invalido'
      else 'ok'
    end as valid_reason
  from {{ ref('rel_openaire_researchproduct_pids')}}
  where scheme = 'handle' 
  -- handles con prefijo 123456789, aparecen como urls completas.
  -- and value like 'http%'
)

select * from base

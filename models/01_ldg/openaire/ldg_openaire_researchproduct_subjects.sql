with source as (
  select * from {{ source('openaire', 'researchproduct_subjects') }}
),
renamed as (
  select
    id::text as researchproduct_id,
    provenance::text as provenance,
    {{ adapter.quote("subject.scheme") }}::text as subject_scheme,
    {{ adapter.quote("subject.value") }}::text as subject_value,
    dv_load_datetime::timestamp
  from source
),
ghost_record as (
  select
    '!UNKNOWN'::text as researchproduct_id,
    '!UNKNOWN'::text as provenance,
    '!UNKNOWN'::text as subject_scheme,
    '!UNKNOWN'::text as subject_value,
    {{ dbt_date.today() }} as dv_load_datetime
)

select * from renamed
where subject_scheme != 'keyword'
union all
select * from ghost_record
    

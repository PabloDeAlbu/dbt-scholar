with source as (
        select * from {{ source('openaire', 'researchproduct_subjects') }}
  ),
  renamed as (
      select
        id::text as researchproduct_id,
        provenance::text as provenance,
        {{ adapter.quote("subject.scheme") }}::text as subject_scheme,
        {{ adapter.quote("subject.value") }}::text as subject_value,
        load_datetime::timestamp
      from source
  )
  select * from renamed
    
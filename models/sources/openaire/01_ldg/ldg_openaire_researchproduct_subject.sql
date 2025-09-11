with source as (
        select * from {{ source('openaire', 'researchproduct_subjects') }}
  ),
  renamed as (
      select
        id::text,
        provenance::text as provenance,
        {{ adapter.quote("subject.scheme") }}::text as subject_scheme,
        {{ adapter.quote("subject.value") }}::text as subject_value,
        {{ dbt_date.convert_timezone("load_datetime") }} as load_datetime
      from source
  )
  select * from renamed
    
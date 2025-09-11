with source as (
    select
        id as work_id,
        source_id,
        source_display_name,
        source_is_core,
        source_type,
        source_host_organization,
        source_host_organization_name,
        is_accepted,
        is_oa,
        is_published,
        landing_page_url,
        license,
        license_id,
        pdf_url,
        version,
        source_is_in_doaj,
        source_is_oa,
        source_issn_l,
        load_datetime
    from {{ source('openalex', 'map_work_location') }}
),

casted as (
    select
        work_id::text,
        source_id::text,
        source_display_name::text,
        source_type::text,
        source_host_organization::text,
        source_host_organization_name::text,
        source_is_core::text,
        source_issn_l::text,
        landing_page_url::text,
        license::text,
        license_id::text,
        pdf_url::text,
        version::text,
        is_accepted::boolean,
        is_oa::boolean,
        is_published::boolean,
        source_is_in_doaj::boolean,
        source_is_oa::boolean,
        load_datetime::timestamp
    from source
)

select * from casted
  
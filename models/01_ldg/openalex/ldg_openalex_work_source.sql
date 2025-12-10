with source as (
    select
        id::text as work_id,
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
    from {{ source('openalex', 'map_work_location') }}
)

select * from source

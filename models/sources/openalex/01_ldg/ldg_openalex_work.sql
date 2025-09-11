with source as (
  select
    {{ adapter.quote("id") }} as work_id,
    {{ adapter.quote("title") }},
    {{ adapter.quote("display_name") }},
    {{ adapter.quote("publication_year") }},
    {{ adapter.quote("publication_date") }},
    {{ adapter.quote("language") }},
    {{ adapter.quote("type") }},
    {{ adapter.quote("type_crossref") }},
    {{ adapter.quote("countries_distinct_count") }},
    {{ adapter.quote("institutions_distinct_count") }},
    {{ adapter.quote("fwci") }},
    {{ adapter.quote("has_fulltext") }},
    {{ adapter.quote("fulltext_origin") }},
    {{ adapter.quote("cited_by_count") }},
    {{ adapter.quote("is_retracted") }},
    {{ adapter.quote("is_paratext") }},
    {{ adapter.quote("locations_count") }},
    {{ adapter.quote("referenced_works_count") }},
    {{ adapter.quote("cited_by_api_url") }},
    {{ adapter.quote("updated_date") }},
    {{ adapter.quote("created_date") }},
    {{ adapter.quote("doi") }},
    {{ adapter.quote("mag") }},
    {{ adapter.quote("openalex") }},
    {{ adapter.quote("pmcid") }},
    {{ adapter.quote("pmid") }},
    {{ adapter.quote("primary_location.is_accepted") }} as location_is_accepted,
    {{ adapter.quote("primary_location.is_oa") }} as location_is_oa,
    {{ adapter.quote("primary_location.is_published") }} as location_is_published,
    {{ adapter.quote("primary_location.landing_page_url") }} as location_landing_page_url,
    {{ adapter.quote("primary_location.license") }} as location_license,
    {{ adapter.quote("primary_location.license_id") }} as location_license_id,
    {{ adapter.quote("primary_location.pdf_url") }} as location_pdf_url,
    {{ adapter.quote("primary_location.version") }} as location_version,
    {{ adapter.quote("primary_location.source") }} as location_source,
    {{ adapter.quote("primary_location.source.id") }} as location_source_id,
    {{ adapter.quote("primary_location.source.type") }} as location_source_type,
    {{ adapter.quote("primary_location.source.display_name") }} as location_source_display_name,
    {{ adapter.quote("primary_location.source.host_organization") }} as location_source_host_organization,
    {{ adapter.quote("primary_location.source.host_organization_name") }} as location_source_host_organization_name,
    {{ adapter.quote("primary_location.source.is_core") }} as location_source_is_core,
    {{ adapter.quote("primary_location.source.is_in_doaj") }} as location_source_is_in_doaj,
    {{ adapter.quote("primary_location.source.is_indexed_in_scopus") }} as location_source_is_indexed_in_scopus,
    {{ adapter.quote("primary_location.source.is_oa") }} as location_source_is_oa,
    {{ adapter.quote("primary_location.source.issn_l") }} as location_source_issn_l,
    {{ adapter.quote("any_repository_has_fulltext") }},
    {{ adapter.quote("is_oa") }},
    {{ adapter.quote("oa_status") }},
    {{ adapter.quote("oa_url") }},
    {{ adapter.quote("apc_list.currency") }} as apc_list_currency,
    {{ adapter.quote("apc_list.value") }} as apc_list_value,
    {{ adapter.quote("apc_list.value_usd") }} as apc_list_value_usd,
    {{ adapter.quote("apc_paid.currency") }} as apc_paid_currency,
    {{ adapter.quote("apc_paid.value") }} as apc_paid_value,
    {{ adapter.quote("apc_paid.value_usd") }} as apc_paid_value_usd,
    {{ adapter.quote("citation_normalized_percentile.is_in_top_10_percent") }} as citation_normalized_percentile_is_in_top_10_percent,
    {{ adapter.quote("citation_normalized_percentile.is_in_top_1_percent") }} as citation_normalized_percentile_is_in_top_1_percent,
    {{ adapter.quote("citation_normalized_percentile.value") }} as citation_normalized_percentile_value,
    {{ adapter.quote("cited_by_percentile_year.max") }} as cited_by_percentile_year_max,
    {{ adapter.quote("cited_by_percentile_year.min") }} as cited_by_percentile_year_min,
    {{ adapter.quote("biblio.first_page") }} as biblio_first_page,
    {{ adapter.quote("biblio.issue") }} as biblio_issue,
    {{ adapter.quote("biblio.last_page") }} as biblio_last_page,
    {{ adapter.quote("biblio.volume") }} as biblio_volume,
    {{ adapter.quote("primary_topic.display_name") }} as topic_display_name,
    {{ adapter.quote("primary_topic.id") }} as topic_id,
    {{ adapter.quote("primary_topic.score") }} as primary_topic_score,
    {{ adapter.quote("primary_topic.domain.display_name") }} as topic_domain_display_name,
    {{ adapter.quote("primary_topic.domain.id") }} as topic_domain_id,
    {{ adapter.quote("primary_topic.field.display_name") }} as topic_field_display_name,
    {{ adapter.quote("primary_topic.field.id") }} as topic_field_id,
    {{ adapter.quote("primary_topic.subfield.display_name") }} as topic_subfield_display_name,
    {{ adapter.quote("primary_topic.subfield.id") }} as topic_subfield_id,
    {{ adapter.quote("best_oa_location.is_accepted") }} as best_oa_location_is_accepted,
    {{ adapter.quote("best_oa_location.is_oa") }} as best_oa_location_is_oa,
    {{ adapter.quote("best_oa_location.is_published") }} as best_oa_location_is_published,
    {{ adapter.quote("best_oa_location.landing_page_url") }} as best_oa_location_landing_page_url,
    {{ adapter.quote("best_oa_location.license") }} as best_oa_location_license,
    {{ adapter.quote("best_oa_location.license_id") }} as best_oa_location_license_id,
    {{ adapter.quote("best_oa_location.pdf_url") }} as best_oa_location_pdf_url,
    {{ adapter.quote("best_oa_location.version") }} as best_oa_location_version,
    {{ adapter.quote("best_oa_location.source") }} as best_oa_location_source,
    {{ adapter.quote("best_oa_location.source.id") }} as best_oa_location_source_id,
    {{ adapter.quote("best_oa_location.source.display_name") }} as best_oa_location_source_display_name,
    {{ adapter.quote("best_oa_location.source.host_organization") }} as best_oa_location_source_host_organization,
    {{ adapter.quote("best_oa_location.source.host_organization_name") }} as best_oa_location_source_host_organization_name,
    {{ adapter.quote("best_oa_location.source.is_core") }} as best_oa_location_source_is_core,
    {{ adapter.quote("best_oa_location.source.is_in_doaj") }} as best_oa_location_source_is_in_doaj,
    {{ adapter.quote("best_oa_location.source.is_indexed_in_scopus") }} as best_oa_location_source_is_indexed_in_scopus,
    {{ adapter.quote("best_oa_location.source.is_oa") }} as best_oa_location_source_is_oa,
    {{ adapter.quote("best_oa_location.source.issn_l") }} as best_oa_location_source_issn_l,
    {{ adapter.quote("best_oa_location.source.type") }} as best_oa_location_source_type,
    {{ adapter.quote("load_datetime") }}
  from {{ source('openalex', 'work') }}
),

casted as (
  select
    -- work
    ---- varchar
    work_id::text,
    title::text,
    display_name::text,
    language::text,
    type::text,
    type_crossref::text,
    fulltext_origin::text,
    cited_by_api_url::text,
    doi::text,
    mag::text,
    openalex::text,
    pmcid::text,
    pmid::text,
    ---- bool
    has_fulltext::boolean,
    is_retracted::boolean,
    is_paratext::boolean,
    ---- int
    institutions_distinct_count::int,
    fwci::int,
    cited_by_count::int,
    locations_count::int,
    referenced_works_count::int,
    countries_distinct_count::int,
    publication_year::int,

    ---- date
    publication_date::timestamp,
    updated_date::timestamp,
    created_date::timestamp,
    load_datetime::timestamp,

    -- openaeccess
    ---- varchar
    oa_status::text,
    oa_url::text,
    ---- bool
    any_repository_has_fulltext::boolean,
    is_oa::boolean,
    ---- int
    ---- date

    -- apc_list
    ---- varchar
    apc_list_currency::text,
    ---- bool
    ---- int
    apc_list_value::int,
    apc_list_value_usd::int,
    ---- date

    -- apc_paid
    ---- varchar
    apc_paid_currency::text,
    ---- bool
    ---- int
    apc_paid_value::int,
    apc_paid_value_usd::int,
    ---- date

    -- citation_normalized_percentile
    ---- varchar
    ---- bool
    citation_normalized_percentile_is_in_top_10_percent::boolean,
    citation_normalized_percentile_is_in_top_1_percent::boolean,
    ---- int
    citation_normalized_percentile_value::int,
    cited_by_percentile_year_max::int,
    cited_by_percentile_year_min::int,
    ---- date

    -- biblio
    ---- varchar
    biblio_first_page::text,
    biblio_issue::text,
    biblio_last_page::text,
    biblio_volume::text,
    ---- bool
    ---- int
    ---- date

    -- Location object. https://docs.openalex.org/api-entities/works/work-object/location-object

    -- primary location
    ---- varchar
    location_source::text,
    location_source_id::text,
    location_source_type::text,
    location_source_display_name::text,
    location_source_host_organization::text,
    location_source_host_organization_name::text,
    location_landing_page_url::text,
    location_license::text,
    location_license_id::text,
    location_pdf_url::text,
    location_version::text,
    location_source_issn_l::text,
    ---- bool
    location_is_accepted::boolean,
    location_is_oa::boolean,
    location_is_published::boolean,
    location_source_is_core::boolean,
    location_source_is_in_doaj::boolean,
    location_source_is_indexed_in_scopus::boolean,
    location_source_is_oa::boolean,
    ---- int
    ---- date

    -- best_oa_location
    ---- varchar
    best_oa_location_source::text,
    best_oa_location_source_id::text,
    best_oa_location_source_display_name::text,
    best_oa_location_source_type::text,
    best_oa_location_source_host_organization::text,
    best_oa_location_source_host_organization_name::text,
    best_oa_location_landing_page_url::text,
    best_oa_location_license::text,
    best_oa_location_license_id::text,
    best_oa_location_pdf_url::text,
    best_oa_location_version::text,
    best_oa_location_source_issn_l::text,
    ---- bool
    best_oa_location_is_accepted::boolean,
    best_oa_location_is_oa::boolean,
    best_oa_location_is_published::boolean,
    best_oa_location_source_is_core::boolean,
    best_oa_location_source_is_in_doaj::boolean,
    best_oa_location_source_is_indexed_in_scopus::boolean,
    best_oa_location_source_is_oa::boolean,

    -- topic
    ---- varchar
    topic_display_name::text,
    topic_id::text,
    topic_domain_display_name::text,
    topic_domain_id::text,
    topic_field_display_name::text,
    topic_field_id::text,
    topic_subfield_display_name::text,
    topic_subfield_id::text,
    ---- bool
    ---- int
    primary_topic_score::int
    ---- date

  from source
)

select * from casted
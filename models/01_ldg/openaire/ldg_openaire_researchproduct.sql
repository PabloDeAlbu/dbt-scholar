with source as (
  select * from {{ source('openaire', 'researchproduct') }}
),
renamed as (
  select
    id::text as researchproduct_id,
    {{ adapter.quote("openAccessColor") }}::text as open_access_color,
    {{ adapter.quote("publiclyFunded") }}::text as publicly_funded,
    type::text as type,
    {{ adapter.quote("mainTitle") }}::text as main_title,
    {{ adapter.quote("publicationDate") }}::timestamp as publication_date,
    {{ adapter.quote("publisher") }}::text as publisher,
    {{ adapter.quote("embargoEndDate") }}::timestamp as embargo_end_date,
    {{ adapter.quote("isGreen") }}::boolean as is_green,
    {{ adapter.quote("isInDiamondJournal") }}::boolean as is_in_diamond_journal,
    language_code::text as language_code,
    language_label::text as language_label,
    {{ adapter.quote("bestAccessRight_label") }}::text as best_access_right,
    {{ adapter.quote("bestAccessRight_scheme") }}::text as best_access_right_uri,
    {{ adapter.quote("citationImpact.citationClass") }}::text as citation_class,
    {{ adapter.quote("citationImpact.citationCount") }}::int as citation_count,
    {{ adapter.quote("citationImpact.impulse") }}::float as impulse,
    {{ adapter.quote("citationImpact.impulseClass") }}::text as impulse_class,
    {{ adapter.quote("citationImpact.influence") }}::float as influence,
    {{ adapter.quote("citationImpact.influenceClass") }}::text as influence_class,
    {{ adapter.quote("citationImpact.popularity") }}::float as popularity,
    {{ adapter.quote("citationImpact.popularityClass") }}::text as popularity_class,
    {{ adapter.quote("usageCounts.downloads") }}::int as downloads,
    {{ adapter.quote("usageCounts.views") }}::int as views,
    load_datetime::timestamp
  from source
),
ghost_record as (
  select
    '!UNKNOWN'::text as researchproduct_id,
    '!UNKNOWN'::text as open_access_color,
    '!UNKNOWN'::text as publicly_funded,
    '!UNKNOWN'::text as type,
    '!UNKNOWN'::text as main_title,
    '1900-01-01'::timestamp as publication_date,
    '!UNKNOWN'::text as publisher,
    '1900-01-01'::timestamp as embargo_end_date,
    false as is_green,
    false as is_in_diamond_journal,
    '!UNKNOWN'::text as language_code,
    '!UNKNOWN'::text as language_label,
    '!UNKNOWN'::text as best_access_right,
    '!UNKNOWN'::text as best_access_right_uri,
    '!UNKNOWN'::text as citation_class,
    -1::int as citation_count,
    -1.0::float as impulse,
    '!UNKNOWN'::text as impulse_class,
    -1.0::float as influence,
    '!UNKNOWN'::text as influence_class,
    -1.0::float as popularity,
    '!UNKNOWN'::text as popularity_class,
    -1::int as downloads,
    -1::int as views,
    {{ dbt_date.today() }} as load_datetime
)

select * from renamed
union all
select * from ghost_record

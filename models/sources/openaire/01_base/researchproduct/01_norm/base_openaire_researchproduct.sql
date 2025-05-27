with source as (
        select * from {{ source('openaire', 'researchproduct') }}
  ),
  renamed as (
      select
        {{ adapter.quote("id") }}::varchar as researchproduct_id,
        {{ adapter.quote("openAccessColor") }}::varchar  as open_access_color,
        {{ adapter.quote("publiclyFunded") }}::varchar  as publicly_funded,
        {{ adapter.quote("type") }}::varchar  as type,
        {{ adapter.quote("mainTitle") }}::varchar  as main_title,
        {{ dbt_date.convert_timezone(adapter.quote("publicationDate")) }} as publication_date,
        {{ adapter.quote("publisher") }}::varchar  as publisher,
        {{ dbt_date.convert_timezone(adapter.quote("embargoEndDate")) }} as embargo_end_date,
        {{ adapter.quote("isGreen") }}::boolean as is_green,
        {{ adapter.quote("isInDiamondJournal") }}::boolean as is_in_diamond_journal,
        {{ adapter.quote("language_code") }}::varchar  as language_code,
        {{ adapter.quote("language_label") }}::varchar  as language_label,
        {{ adapter.quote("bestAccessRight_label") }}::varchar  as best_access_right,
        {{ adapter.quote("bestAccessRight_scheme") }}::varchar  as best_access_right_uri,
        {{ adapter.quote("citationImpact.citationClass") }}::varchar  as citation_class,
        {{ adapter.quote("citationImpact.citationCount") }}::int  as citation_count,        
        {{ adapter.quote("citationImpact.impulse") }}::float as impulse,
        {{ adapter.quote("citationImpact.impulseClass") }}::varchar  as impulse_class,
        {{ adapter.quote("citationImpact.influence") }}::float as influence,
        {{ adapter.quote("citationImpact.influenceClass") }}::varchar  as influence_class,
        {{ adapter.quote("citationImpact.popularity") }}::float as popularity,
        {{ adapter.quote("citationImpact.popularityClass") }}::varchar  as popularity_class,
        {{ adapter.quote("usageCounts.downloads") }}::int  as downloads,
        {{ adapter.quote("usageCounts.views") }}::int  as views,
        {{ dbt_date.convert_timezone("load_datetime") }} as load_datetime
      from source
  )
  select * from renamed

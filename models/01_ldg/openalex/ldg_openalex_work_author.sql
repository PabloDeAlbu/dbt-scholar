select
    work_id,
    author_id,
    author_display_name,
    author_orcid,
    author_position,
    COALESCE(
        NULLIF(author_id, '!UNKNOWN'),
        NULLIF(author_orcid, '!UNKNOWN'),
        NULLIF(author_display_name, '!UNKNOWN'),
        '!UNKNOWN'
    ) as work_author_identity,
    _load_datetime
from {{ ref('ldg_openalex_work_authorship') }}
  

select
    work_id,
    authorship_id,
    count(*) as duplicate_count
from {{ ref('brg_openalex_work_authorship') }}
group by 1, 2
having count(*) > 1

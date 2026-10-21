SELECT
    "record_id",
    "extract_datetime"
FROM {{ source('oai', 'records') }}
WHERE "record_id" IS NOT NULL
GROUP BY "record_id", "extract_datetime"
HAVING COUNT(DISTINCT "_repository_identifier") <> 1
    OR COUNT(DISTINCT "_institution_ror") <> 1
    OR COUNT(DISTINCT "_source_key") <> 1
    OR COUNT(DISTINCT "_context") <> 1

{{ config(materialized='table') }}

SELECT *
FROM {{ ref('fct_dspacedb5_item_publication') }}
WHERE institution_ror = 'https://ror.org/01tjs6929'

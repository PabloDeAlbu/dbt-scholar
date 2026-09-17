{{ config(materialized='table') }}

SELECT *
FROM {{ ref('brg_dspacedb5_item_community') }}
WHERE institution_ror = 'https://ror.org/01tjs6929'

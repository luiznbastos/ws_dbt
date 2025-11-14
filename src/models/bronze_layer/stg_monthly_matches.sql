{{ config(materialized='view') }}

select *
from {{ source('whoscored_db','monthly_matches') }}


{{
  config(
    materialized='incremental',
    unique_key='match_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns'  )
}}

with main_metadata as
(
select * from {{ref('stg_main_metadata')}}
),

leagues as
(
select * from {{ref('dim_leagues')}}
)

select m.*
, l.league_name
, l.tier 
from main_metadata m 
join leagues l on m.league_id = l.league_id


{% if is_incremental() %}
  -- Only process new trips based on pickup datetime
  where m.start_datetime > (select max(start_datetime) from {{ this }})
{% endif %}
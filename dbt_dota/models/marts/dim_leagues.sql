select 
distinct league_id
, league_name
, tier
from {{ref('stg_leagues')}}
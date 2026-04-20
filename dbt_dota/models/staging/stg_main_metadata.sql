select 

cast(version as integer) as version,

cast(match_id as integer) as match_id,

cast(leagueid as integer) as league_id,

cast(start_date_time as timestamp) as start_datetime,

cast(start_date_time as date ) as start_date,

cast(duration as integer) as duration,

cast(first_blood_time as integer) as first_blood_time,

cast(radiant_win as STRING) as radiant_win,

cast(radiant_score as integer) as radiant_score,

cast(dire_score as integer) as dire_score


 from {{ source('raw', 'main_metadata') }}
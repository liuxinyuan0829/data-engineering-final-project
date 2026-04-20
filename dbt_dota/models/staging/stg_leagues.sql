select 

cast(leagueid as integer) as league_id,

cast(leaguename as STRING) as league_name,

cast(tier as STRING) as tier


from {{ source('raw', 'leagues') }}

where tier is not null
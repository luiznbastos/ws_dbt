{{ config(materialized='view') }}

with fe as (
  select * from {{ ref('fct_events') }}
),

dm as (
  select match_id, home_team_id, away_team_id from {{ ref('dim_matches') }}
),

-- Helper CTE to compute bins and thirds
base as (
  select
    fe.match_id,
    fe.event_id,
    fe.team_id,
    fe.player_id,
    fe.minute,
    fe.second,
    fe.main_type,
    fe.outcome_type,
    fe.possessing_team_id,
    fe.possession_chain,
    fe.x,
    fe.y,
    fe.end_x,
    fe.end_y,

    -- Clamp coordinates to [0,100]
    LEAST(GREATEST(fe.x, 0), 100)  as x_clamped,
    LEAST(GREATEST(fe.y, 0), 100)  as y_clamped,
    LEAST(GREATEST(fe.end_x, 0), 100) as end_x_clamped,
    LEAST(GREATEST(fe.end_y, 0), 100) as end_y_clamped,

    dm.home_team_id,
    dm.away_team_id
  from fe
  left join dm on dm.match_id = fe.match_id
),

zoned as (
  select
    *,
    -- Bin sizes (6x5 grid)
    100.0/6.0 as x_bin_size,
    100.0/5.0 as y_bin_size,

    -- Start bins (1..6, 1..5)
    LEAST( FLOOR(x_clamped / (100.0/6.0))::int + 1, 6) as start_bin_x,
    LEAST( FLOOR(y_clamped / (100.0/5.0))::int + 1, 5) as start_bin_y,

    -- End bins (for passes/dribbles)
    case when end_x is not null then LEAST( FLOOR(end_x_clamped / (100.0/6.0))::int + 1, 6) end as end_bin_x,
    case when end_y is not null then LEAST( FLOOR(end_y_clamped / (100.0/5.0))::int + 1, 5) end as end_bin_y,

    -- Zone ids
    'Z' || LEAST( FLOOR(x_clamped / (100.0/6.0))::int + 1, 6)::varchar || '_' || LEAST( FLOOR(y_clamped / (100.0/5.0))::int + 1, 5)::varchar as start_zone_id,
    case when end_x is not null and end_y is not null
         then 'Z' || LEAST( FLOOR(end_x_clamped / (100.0/6.0))::int + 1, 6)::varchar || '_' || LEAST( FLOOR(end_y_clamped / (100.0/5.0))::int + 1, 5)::varchar end as end_zone_id,

    -- Global thirds from absolute x
    case
      when x_clamped < (100.0/3.0) then 'defense'
      when x_clamped < (200.0/3.0) then 'midfield'
      else 'attack'
    end as global_third,

    -- Team-relative thirds (assumes home attacks left->right, away right->left)
    case when team_id = home_team_id then x_clamped else (100.0 - x_clamped) end as x_team_relative,
    case when team_id = home_team_id then end_x_clamped else (100.0 - end_x_clamped) end as end_x_team_relative,

    case
      when (case when team_id = home_team_id then x_clamped else (100.0 - x_clamped) end) < (100.0/3.0) then 'defense'
      when (case when team_id = home_team_id then x_clamped else (100.0 - x_clamped) end) < (200.0/3.0) then 'midfield'
      else 'attack'
    end as team_third,

    case
      when end_x is null then null
      when (case when team_id = home_team_id then end_x_clamped else (100.0 - end_x_clamped) end) < (100.0/3.0) then 'defense'
      when (case when team_id = home_team_id then end_x_clamped else (100.0 - end_x_clamped) end) < (200.0/3.0) then 'midfield'
      else 'attack'
    end as team_end_third

  from base
)

select
  match_id,
  event_id,
  team_id,
  player_id,
  minute,
  second,
  main_type,
  outcome_type,
  possessing_team_id,
  possession_chain,

  x,
  y,
  end_x,
  end_y,

  start_bin_x,
  start_bin_y,
  start_zone_id,

  end_bin_x,
  end_bin_y,
  end_zone_id,

  global_third,
  team_third,
  team_end_third
from zoned

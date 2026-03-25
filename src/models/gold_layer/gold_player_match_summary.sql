{{ config(materialized='table') }}

with fe as (
  select * from {{ ref('fct_events') }}
)

select
  fe.match_id,
  fe.team_id,
  fe.player_id,

  -- In-possession
  sum(case when fe.is_touch then 1 else 0 end) as touches,
  sum(case when fe.is_pass then 1 else 0 end) as passes_attempted,
  sum(case when fe.is_pass and fe.outcome_type ilike 'successful' then 1 else 0 end) as passes_completed,
  1.0 * sum(case when fe.is_pass and fe.outcome_type ilike 'successful' then 1 else 0 end) / nullif(sum(case when fe.is_pass then 1 else 0 end), 0) as pass_accuracy,
  sum(case when fe.is_pass and fe.outcome_type ilike 'successful' and (fe.end_x - fe.x) >= 25 then 1 else 0 end) as progressive_passes,
  sum(case when fe.is_dribble then 1 else 0 end) as dribbles_attempted,
  sum(case when fe.is_dribble and fe.outcome_type ilike 'successful' then 1 else 0 end) as dribbles_completed,

  -- Shooting
  sum(case when fe.is_shot then 1 else 0 end) as shots_total,
  sum(case when fe.main_type ilike 'goal' then 1 else 0 end) as goals,
  sum(case when fe.is_save then 1 else 0 end) + sum(case when fe.main_type ilike 'goal' then 1 else 0 end) as shots_on_target,
  sum(case when fe.is_touch and fe.x >= 83 then 1 else 0 end) as box_touches,

  -- Defensive
  sum(case when fe.is_tackle then 1 else 0 end) as tackles,
  sum(case when fe.is_interception then 1 else 0 end) as interceptions,
  sum(case when fe.is_block then 1 else 0 end) as blocks,
  sum(case when fe.is_clearance then 1 else 0 end) as clearances,

  -- Aerials
  sum(case when fe.is_aerial then 1 else 0 end) as aerials_attempted,
  sum(case when fe.is_aerial and fe.outcome_type ilike 'successful' then 1 else 0 end) as aerials_won,

  -- Discipline and transitions
  sum(case when fe.is_foul then 1 else 0 end) as fouls_committed,
  sum(case when fe.is_loss_possession then 1 else 0 end) as turnovers,
  sum(case when fe.is_error then 1 else 0 end) as errors

from fe
group by 1,2,3


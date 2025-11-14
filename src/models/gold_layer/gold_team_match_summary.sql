{{ config(materialized='table') }}

with fe as (
  select * from {{ ref('fct_events') }}
),

chain_sizes as (
  select
    match_id,
    possessing_team_id as team_id,
    possession_chain,
    count(*) as events_in_chain
  from fe
  group by 1,2,3
),

chain_summary as (
  select
    match_id,
    team_id,
    avg(events_in_chain) as avg_chain_length
  from chain_sizes
  group by 1,2
),

team_base as (
  select
    fe.match_id,
    fe.team_id,

    -- Possession / territory
    count(distinct case when fe.possessing_team_id = fe.team_id then fe.possession_chain end) as possessions_total,
    max(cs2.avg_chain_length) as avg_chain_length,
    sum(case when fe.is_touch then 1 else 0 end) as touches,
    1.0 * sum(case when fe.is_touch and fe.x > 50 then 1 else 0 end) / nullif(sum(case when fe.is_touch then 1 else 0 end), 0) as field_tilt,
    sum(case when fe.is_pass and fe.outcome_type ilike 'successful' and fe.x < 66 and fe.end_x >= 66 then 1 else 0 end) as final_third_entries,
    sum(case when fe.outcome_type ilike 'successful' and (fe.main_type ilike '%TakeOn%' or fe.is_pass) and fe.x < 83 and fe.end_x >= 83 then 1 else 0 end) as box_entries,

    -- Passing
    sum(case when fe.is_pass then 1 else 0 end) as passes_attempted,
    sum(case when fe.is_pass and fe.outcome_type ilike 'successful' then 1 else 0 end) as passes_completed,
    1.0 * sum(case when fe.is_pass and fe.outcome_type ilike 'successful' then 1 else 0 end) / nullif(sum(case when fe.is_pass then 1 else 0 end), 0) as pass_accuracy,
    sum(case when fe.is_pass and fe.outcome_type ilike 'successful' and (fe.end_x - fe.x) >= 25 then 1 else 0 end) as progressive_passes,
    sum(case when fe.is_pass and fe.outcome_type ilike 'successful' and abs(fe.end_y - fe.y) >= 40 then 1 else 0 end) as switches,

    -- Attack
    sum(case when fe.is_shot then 1 else 0 end) as shots_total,
    sum(case when fe.main_type ilike 'goal' then 1 else 0 end) as goals,
    sum(case when fe.is_save then 1 else 0 end) + sum(case when fe.main_type ilike 'goal' then 1 else 0 end) as shots_on_target,
    sum(case when fe.is_offside then 1 else 0 end) as offsides,

    -- Defense
    sum(case when fe.is_tackle then 1 else 0 end) as tackles,
    sum(case when fe.is_interception then 1 else 0 end) as interceptions,
    sum(case when fe.is_block then 1 else 0 end) as blocks,
    sum(case when fe.is_clearance then 1 else 0 end) as clearances,
    sum(case when fe.is_aerial and fe.outcome_type ilike 'successful' then 1 else 0 end) as aerials_won,
    sum(case when fe.is_aerial then 1 else 0 end) as aerials_attempted,

    -- Goalkeeping
    sum(case when fe.is_save then 1 else 0 end) as saves,
    sum(case when fe.is_claim then 1 else 0 end) as claims,
    sum(case when fe.is_punch then 1 else 0 end) as punches,

    -- Discipline / transitions
    sum(case when fe.is_foul then 1 else 0 end) as fouls_committed,
    sum(case when fe.is_loss_possession then 1 else 0 end) as turnovers,
    sum(case when fe.is_error then 1 else 0 end) as errors

  from fe
  left join chain_summary cs2
    on cs2.match_id = fe.match_id
   and cs2.team_id = fe.team_id
  group by 1,2
)

select * from team_base

-- models/gold/gold_match_events_analysis.sql

SELECT
    -- Event facts
    fe.*,

    -- Match context
    dm.match_date,
    dm.competition_name,
    dm.season_name,
    dm.home_team_id,
    dm.away_team_id,
    dm.home_score,
    dm.away_score,

    -- Player context
    dp.primary_position,
    dp.preferred_foot,

    -- Team context
    dt.team_name,
    dt.team_country

FROM {{ ref('fct_events') }} AS fe
LEFT JOIN {{ ref('dim_matches') }} AS dm ON fe.match_id = dm.match_id
LEFT JOIN {{ ref('dim_players') }} AS dp ON fe.player_id = dp.player_id
LEFT JOIN {{ ref('dim_teams') }} AS dt ON fe.team_id = dt.team_id 
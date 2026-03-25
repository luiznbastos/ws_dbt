WITH matches AS (
    SELECT DISTINCT
        mm.id AS match_id,
        mm.hometeamid AS home_team_id,
        mm.awayteamid AS away_team_id,
        mm.homescore AS home_score,
        mm.awayscore AS away_score,
        CAST(mm.starttimeutc AS TIMESTAMP) AS match_date
    FROM {{ ref('stg_monthly_matches') }} mm
    WHERE mm.id IS NOT NULL
        AND mm.hometeamid IS NOT NULL
        AND mm.awayteamid IS NOT NULL
),

season_info AS (
    SELECT
        sm.match_id,
        REPLACE(s.tournament_prefix, '/', '') AS competition_name,
        s.season_prefix AS season_name
    FROM {{ source('whoscored_db', 'season_matches') }} sm
    INNER JOIN {{ source('whoscored_db', 'seasons') }} s
        ON sm.season_id = s.id
)

SELECT
    m.match_id,
    m.home_team_id,
    m.away_team_id,
    m.home_score,
    m.away_score,
    m.match_date,
    si.competition_name,
    si.season_name
FROM matches m
LEFT JOIN season_info si ON m.match_id = si.match_id


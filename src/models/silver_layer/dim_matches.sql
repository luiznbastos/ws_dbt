SELECT DISTINCT
    mm.id as match_id,
    mm.hometeamid as home_team_id,
    mm.awayteamid as away_team_id
FROM {{ ref('stg_monthly_matches') }} mm
WHERE mm.id IS NOT NULL
    AND mm.hometeamid IS NOT NULL
    AND mm.awayteamid IS NOT NULL


WITH team_names_from_matches AS (
    SELECT DISTINCT
        hometeamid AS team_id,
        hometeamname AS team_name,
        hometeamcountrycode AS team_country
    FROM {{ ref('stg_monthly_matches') }}
    WHERE hometeamid IS NOT NULL AND hometeamname IS NOT NULL
    
    UNION
    
    SELECT DISTINCT
        awayteamid AS team_id,
        awayteamname AS team_name,
        awayteamcountrycode AS team_country
    FROM {{ ref('stg_monthly_matches') }}
    WHERE awayteamid IS NOT NULL AND awayteamname IS NOT NULL
)

SELECT 
    team_id,
    MAX(team_name) AS team_name,
    MAX(team_country) AS team_country   
FROM team_names_from_matches
GROUP BY team_id
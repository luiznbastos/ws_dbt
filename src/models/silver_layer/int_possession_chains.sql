WITH event_context AS (
    -- Step 1: Get the context of the previous and next event for each row
    SELECT
        *,
        LAG(team_id, 1) OVER (PARTITION BY match_id ORDER BY minute, second, event_id) AS prev_team_id,
        LEAD(team_id, 1) OVER (PARTITION BY match_id ORDER BY minute, second, event_id) AS next_team_id
    FROM {{ ref('stg_event_main') }}
),

possession_team_corrected AS (
    -- Step 2: Determine the "true" possessing team, correcting for single-touch turnovers
    SELECT
        *,
        CASE
            -- If the previous and next teams are the same, but the current team is different,
            -- it's a single touch. Attribute possession to the surrounding team.
            WHEN prev_team_id IS NOT NULL AND next_team_id IS NOT NULL
                 AND prev_team_id = next_team_id AND team_id != prev_team_id
            THEN prev_team_id
            ELSE team_id
        END AS possessing_team_id
    FROM event_context
),

chain_break_rules AS (
    -- Step 3: Identify the start of a new chain based on the corrected possession team
    SELECT
        *,
        LAG(possessing_team_id, 1) OVER (PARTITION BY match_id ORDER BY minute, second, event_id) AS previous_possessing_team,
        LAG(type, 1) OVER (PARTITION BY match_id ORDER BY minute, second, event_id) AS previous_event_type,
        LAG(outcome_type, 1) OVER (PARTITION BY match_id ORDER BY minute, second, event_id) AS previous_outcome_type
    FROM possession_team_corrected
),

chain_starts AS (
    SELECT
        *,
        CASE
            -- A new chain starts if it's the first event of the match
            WHEN previous_possessing_team IS NULL THEN 1
            
            -- Or if the "corrected" possessing team ID changes
            WHEN possessing_team_id != previous_possessing_team THEN 1
            
            -- Or due to other rules like fouls or the ball going out of play
            WHEN previous_event_type ILIKE '%foul%' THEN 1
            WHEN previous_event_type ILIKE 'pass' AND previous_outcome_type ILIKE 'unsuccessful' THEN 1
            
            ELSE 0
        END AS is_chain_start
    FROM chain_break_rules
),

chains AS (
    -- Step 4: Use a running sum on the 'is_chain_start' flag to assign a unique ID to each chain
    SELECT
        *,
        SUM(is_chain_start) OVER (
            PARTITION BY match_id 
            ORDER BY minute, second, event_id 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS possession_chain
    FROM chain_starts
)

-- Final selection of the data with the new, more accurate possession_chain ID
SELECT
    *
FROM chains 
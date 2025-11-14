    WITH stg_main as (
        select * from stg_event_main
        -- Where match_id = 1913924
    ),

    event_context AS (
        -- Step 1: Get the context of the previous and next event for each row
        SELECT
            *,
            LAG(team_id, 1) OVER (PARTITION BY match_id ORDER BY minute, second, event_id) AS prev_team_id,
            LEAD(team_id, 1) OVER (PARTITION BY match_id ORDER BY minute, second, event_id) AS next_team_id,
            LAG(type, 1) OVER (PARTITION BY match_id ORDER BY minute, second, event_id) AS previous_event_type,
            LAG(outcome_type, 1) OVER (PARTITION BY match_id ORDER BY minute, second, event_id) AS previous_outcome_type
        FROM stg_main
    ),

    possession_team_corrected AS (
        -- Step 2: Determine the "true" possessing team, correcting for single-touch turnovers
        SELECT
            *,
            CASE
                WHEN prev_team_id IS NOT NULL AND next_team_id IS NOT NULL
                    AND prev_team_id = next_team_id AND team_id != prev_team_id
                THEN prev_team_id
                ELSE team_id
            END AS possessing_team_id
        FROM event_context
    ),

    chain_starts AS (
        -- Step 3: Identify the start of a new chain
        SELECT
            *,
            CASE
                WHEN LAG(possessing_team_id, 1) OVER (PARTITION BY match_id ORDER BY minute, second, event_id) IS NULL THEN 1
                WHEN possessing_team_id != LAG(possessing_team_id, 1) OVER (PARTITION BY match_id ORDER BY minute, second, event_id) THEN 1
                WHEN previous_event_type ILIKE '%foul%' THEN 1
                WHEN previous_event_type ILIKE 'pass' AND previous_outcome_type ILIKE 'unsuccessful' THEN 1
                ELSE 0
            END AS is_chain_start
        FROM possession_team_corrected
    ),

    base_events AS (
        -- Step 4: Use a running sum to assign a unique ID to each chain
        SELECT
            *,
            SUM(is_chain_start) OVER (
                PARTITION BY match_id
                ORDER BY minute, second, event_id
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS possession_chain
        FROM chain_starts
    ),

    event_qualifiers AS (
        SELECT
            match_id,
            event_id,
            team_id,
            id,
            qualifier_type
        FROM stg_event_qualifiers
        where match_id in (select match_id from stg_main)
    ),

    satisfied_events AS (
        SELECT
            match_id,
            event_id,
            team_id,
            id,
            satisfied_types
        FROM stg_satisfied_event
        where match_id in (select match_id from stg_main)
    ),

    joined_data AS (
        SELECT
            be.*,
            eq.qualifier_type,
            se.satisfied_types
        FROM base_events be
        LEFT JOIN event_qualifiers eq 
            ON be.match_id = eq.match_id
            AND be.event_id = eq.event_id
            AND be.team_id = eq.team_id
        LEFT JOIN satisfied_events se 
            ON be.match_id = se.match_id
            AND be.event_id = se.event_id
            AND be.team_id = se.team_id
    )

SELECT
    match_id,
    possession_chain,
    possessing_team_id,
    event_id,
    team_id,
    player_id,
    id,
    type as main_type,
    outcome_type,
    player_name,
    minute,
    second,
    x,
    y,
    end_x,
    end_y,
    qualifier_type,
    satisfied_types,
    CASE WHEN type ILIKE 'save' AND (
            JSON_SERIALIZE(qualifier_type) LIKE '%KeeperSaveInTheBox%'
            OR JSON_SERIALIZE(qualifier_type) LIKE '%KeeperSaveObox%'
            OR JSON_SERIALIZE(qualifier_type) LIKE '%ParriedSafe%'
            AND JSON_SERIALIZE(qualifier_type) LIKE '%Hands%'
        ) THEN True ELSE False END AS is_save,
    CASE WHEN type ILIKE '%error%' THEN True ELSE False END AS is_error,
    CASE WHEN type ILIKE '%claim%' THEN True ELSE False END AS is_claim,
    CASE WHEN type ILIKE '%punch%' THEN True ELSE False END AS is_punch,
    CASE WHEN type ILIKE '%dispossessed%'
        OR JSON_SERIALIZE(satisfied_types) LIKE '%turnover%'
        THEN True ELSE False END AS is_loss_possession,
    CASE WHEN JSON_SERIALIZE(satisfied_types) LIKE '%touches%' THEN True ELSE False END AS is_touch,
    CASE WHEN outcome_type ILIKE 'successful'
        AND (
            type ILIKE '%aerial%' OR
            (type ILIKE '%foul%' AND JSON_SERIALIZE(qualifier_type) LIKE '%AerialFoul%')
        ) THEN True ELSE False END AS is_aerial,
    CASE WHEN type ILIKE '%foul%' AND outcome_type ILIKE 'unsuccessful' THEN True ELSE False END AS is_foul,
    CASE WHEN type ILIKE '%OffsideGiven%' THEN True ELSE False END AS is_offside,
    CASE WHEN (type ILIKE '%block%' OR type ILIKE 'save' OR type ILIKE 'clearance')
        AND NOT (type ILIKE 'save'
            AND (JSON_SERIALIZE(qualifier_type) LIKE '%KeeperSaveInTheBox%'
                OR JSON_SERIALIZE(satisfied_types) LIKE '%KeeperSaveInTheBox%'
                OR JSON_SERIALIZE(qualifier_type) LIKE '%KeeperSaveObox%'
                OR JSON_SERIALIZE(satisfied_types) LIKE '%KeeperSaveObox%'
                OR JSON_SERIALIZE(qualifier_type) LIKE '%Hands%'
            )
        )
        AND end_x IS NULL
        AND end_y IS NULL 
        THEN True ELSE False END AS is_block,
    CASE WHEN (type ILIKE '%clearance%' OR type ILIKE '%punch%')
        AND NOT (end_x IS NULL AND end_y IS NULL) THEN True ELSE False END AS is_clearance,
    CASE WHEN type ILIKE '%Interception%' THEN True ELSE False END AS is_interception,
    CASE WHEN type ILIKE '%Tackle%' OR type ILIKE '%Challenge%' THEN True ELSE False END AS is_tackle,
    CASE WHEN type ILIKE '%TakeOn%' THEN True ELSE False END AS is_dribble,
    CASE WHEN type ILIKE '%shot%' OR type ILIKE 'goal' THEN True ELSE False END AS is_shot,
    CASE WHEN type ILIKE 'pass'
        AND NOT (
            JSON_SERIALIZE(qualifier_type) LIKE '%KeeperThrow%'
            OR JSON_SERIALIZE(qualifier_type) LIKE '%Cross%'
            OR JSON_SERIALIZE(qualifier_type) LIKE '%ThrowIn%'
        ) THEN True ELSE False END AS is_pass
FROM joined_data
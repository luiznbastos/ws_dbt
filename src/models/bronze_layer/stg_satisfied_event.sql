WITH satisfied_event_array AS (
SELECT
    "eventId", 
    "teamId", 
    "matchId", 
    event_main.id AS id,
    event_main.type AS main_type, 
    event_main.outcome_type,
    split_to_array(TRIM(BOTH '{}' FROM satisfied_event_types."satisfiedEventsTypes"), ',') AS event_type_array
FROM {{ ref('stg_event_main')}} AS event_main
LEFT JOIN {{source('whoscored_db', 'satisfied_event_types')}} AS satisfied_event_types
ON event_main.match_id = satisfied_event_types."matchId"
AND event_main.event_id = satisfied_event_types."eventId"
AND event_main.team_id = satisfied_event_types."teamId"
AND event_main.id = satisfied_event_types."id"
),
satisfied_event_preparation AS (
SELECT
    "eventId" as event_id, 
    "teamId" as team_id, 
    "matchId" as match_id, 
    id as id,
    main_type, 
    CAST(event_type_value AS bigint) AS event_type_id,
    outcome_type
FROM satisfied_event_array AS sea,
sea.event_type_array AS event_type_value
WHERE event_type_value IS NOT NULL AND event_type_value <> ''
)
SELECT
    event_id, 
    team_id, 
    match_id, 
    id,
    main_type, 
    SPLIT_TO_ARRAY(LISTAGG(event_type, ',') WITHIN GROUP (ORDER BY event_type), ',') AS satisfied_types
FROM satisfied_event_preparation
LEFT JOIN {{source('whoscored_db', 'event_types')}} AS event_types
ON satisfied_event_preparation.event_type_id = event_types.event_type_id
GROUP BY main_type, team_id, event_id, match_id, id

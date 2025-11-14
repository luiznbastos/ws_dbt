-- models/silver_layer/dim_players.sql

SELECT DISTINCT
    player_id,
    player_name,
    -- These are placeholder values. In a real project, you'd have richer data.
    'Unknown' AS nationality,
    NULL AS date_of_birth,
    NULL AS height_cm,
    NULL AS weight_kg,
    'Unknown' AS primary_position,
    'Unknown' AS preferred_foot
FROM {{ ref('stg_event_main') }}
WHERE player_id IS NOT NULL AND player_name IS NOT NULL 
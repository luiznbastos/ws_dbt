 WITH satisfied_event_array AS (
    SELECT
    eventId, teamId, matchId, event_main.type AS main_type, outcomeType,
    FROM_JSON(JSON(satisfiedEventsTypes), '["VARCHAR"]') AS satisfiedEventsTypes
    FROM event_main
    LEFT JOIN satisfied_event_types
    USING (matchId, eventId, teamId)
    ),
    satisfied_event_preparation AS (
    SELECT
    eventId, teamId, matchId, main_type, unnest AS event_type_id, outcomeType
    FROM satisfied_event_array,
    UNNEST (satisfiedEventsTypes) AS set
    ),
    satisfied_event AS (
    SELECT
    eventId, teamId, matchId, main_type, array_agg(event_type) AS satisfied_types
    FROM satisfied_event_preparation
    LEFT JOIN event_types
    USING (event_type_id)
    GROUP BY main_type, teamId, eventId, matchId
    ),
    qualifiers AS (
    SELECT
    matchId,
    eventId,
    teamId,
    array_agg(eq.type) AS qualifier_type
    FROM event_qualifiers AS eq
    LEFT JOIN event_main AS em
    USING (matchId, eventId, teamId)
    GROUP BY matchId, eventId, teamId
    )
SELECT
    em.matchId,
    em.teamId,
    COUNT(*)
FROM event_main AS em
LEFT JOIN qualifiers AS q
USING (matchId, eventId, teamId)
LEFT JOIN satisfied_event AS se
USING (matchId, eventId, teamId)
WHERE em.matchId = 1821529
AND 'touches' = ANY(se.satisfied_types)
GROUP BY em.matchId, em.teamId
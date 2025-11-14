SELECT
"matchId" as match_id,
"eventId" as event_id,
"teamId" as team_id,
em.id as id,
SPLIT_TO_ARRAY(LISTAGG(eq.type, ',') WITHIN GROUP (ORDER BY eq.type), ',') AS qualifier_type
FROM {{ source('whoscored_db', 'event_qualifiers')}} AS eq
LEFT JOIN {{ ref('stg_event_main')}} AS em
ON eq."matchId" = em.match_id
AND eq."eventId" = em.event_id
AND eq."teamId" = em.team_id
AND eq."id" = em.id
GROUP BY "matchId", "eventId", "teamId", em.id
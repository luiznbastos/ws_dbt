SELECT
teamId,
COUNT(*),
FROM event_main
WHERE matchId = 1821538
AND type ILIKE '%punch%'
GROUP BY teamId
ORDER BY teamId
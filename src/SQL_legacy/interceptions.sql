SELECT
teamId,
COUNT(*),
FROM event_main
WHERE matchId = 1821549
AND type ILIKE '%Interception%'
GROUP BY teamId
ORDER BY teamId
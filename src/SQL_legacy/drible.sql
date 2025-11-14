SELECT
teamId,
COUNT(*),
FROM event_main
WHERE matchId = 1821529
AND type ILIKE '%TakeOn%'
GROUP BY teamId
ORDER BY teamId
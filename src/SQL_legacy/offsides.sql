SELECT
teamId,
COUNT(*),
FROM event_main
WHERE matchId = 1821529
AND type ILIKE '%OffsideGiven%'
GROUP BY teamId
ORDER BY teamId
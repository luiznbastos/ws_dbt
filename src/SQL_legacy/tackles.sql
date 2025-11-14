SELECT
teamId,
COUNT(*),
FROM event_main
WHERE matchId = 1821549
AND (type ILIKE '%Tackle%'
OR type ILIKE '%Challenge%')
GROUP BY teamId
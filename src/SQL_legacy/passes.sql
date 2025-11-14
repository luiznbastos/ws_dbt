WITH
    removed_pass AS (
        SELECT DISTINCT
            matchId,
            eventId,
            teamId,
            eq.type,
            eq.value
        FROM
            event_qualifiers AS eq
            LEFT JOIN event_main AS em USING (matchId, eventId, teamId)
        WHERE
            eq.type IN ('KeeperThrow', 'Cross', 'ThrowIn')
    )
SELECT
    teamId,
    COUNT(*),
    SUM(IF (removed_pass.eventId IS NULL, 1, 0))
FROM
    event_main
    FULL OUTER JOIN removed_pass USING (matchId, eventId, teamId)
WHERE
    matchId = 1821460
    AND event_main.type ILIKE 'pass'
GROUP BY
    teamId
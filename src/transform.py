import pandas as pd
import os
import sqlalchemy
import duckdb
import time

engine_path = os.environ.get("ENGINE_PATH")
con = duckdb.connect(database=engine_path)

df = pd.read_sql("SELECT * FROM event_main", con)
df_barca = pd.read_sql("SELECT * FROM event_main WHERE matchId = 1821470", con)
# df_events_treated = pd.read_sql(
#     """
#     SELECT
#     *,
#     CASE
#         WHEN type IN ('MissedShots', 'SavedShot', 'ShotOnPost', 'Goal') THEN 'Shot'
#         ELSE type
#     END AS grouped_type
#     FROM event_types
#     """,
#     con,
# )


# def calculate_shots():
# resultados = [
#     {"value": evento["type"]["value"], "displayName": evento["type"]["displayName"]}
#     for evento in df_barca["matchCentreData"]["events"]
#     if evento["type"]["value"] == 2
# ]
# missedshots
# savedshot


# SELECT
# teamId, type, COUNT(type)
# FROM event_main
# WHERE event_main.matchId = 1821470
# AND outcomeType ILIKE 'successful'
# AND type ILIKE '%pass%'
# GROUP BY type, teamId


test_df = pd.read_sql(
    """
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
            em.eventId,
            em.teamId,
            em.type AS main_type,
            em.outcomeType,
            em.playerName,
            em.minute,
            em.second,
            em.x,
            em.y,
            em.endX,
            em.endY,
            q.qualifier_type,
            se.satisfied_types
        FROM event_main AS em
        LEFT JOIN qualifiers AS q 
        USING (matchId, eventId, teamId)
        LEFT JOIN satisfied_event AS se
        USING (matchId, eventId, teamId)
        WHERE em.matchId = 1821538
        AND minute = 46
        ORDER BY minute, second
    """,
    con,
)
Stop = True


# Conclusões: Cross nao entra
# FreeKick entra
# Corner nao entra, exceto se for escanteio curto
# Throughl ball entra
# Throw in nao entra
# Key passes entra se nao for um corner que resultou em cabeçada
# Keeper throws nao entra

# Tabela OFICIAL de passes

# WITH removed_pass AS (
# SELECT DISTINCT matchId, eventId, teamId, eq.type, eq.value
# FROM event_qualifiers AS eq
# LEFT JOIN event_main AS em
# USING (matchId, eventId, teamId)
# WHERE eq.type IN (
# 'KeeperThrow',
# 'Cross',
# 'ThrowIn'
# )
# )
# SELECT
# teamId,
# COUNT(*),
# SUM(IF(removed_pass.eventId IS NULL, 1,0))
# FROM event_main
# FULL OUTER JOIN removed_pass
# USING (matchId, eventId, teamId)
# WHERE matchId = 1821460
# AND event_main.type ILIKE 'pass'
# GROUP BY teamId

# Tabela oficial de chutes

# SELECT
# teamId,
# COUNT(*),
# FROM event_main
# WHERE matchId = 1821549
# AND (type ILIKE '%shot%'
# OR type ILIKE 'goal')
# GROUP BY teamId
# ORDER BY teamId


# Tabela oficial de dribles

# SELECT
# teamId,
# COUNT(*),
# FROM event_main
# WHERE matchId = 1821529
# AND type ILIKE '%TakeOn%'
# GROUP BY teamId
# ORDER BY teamId

# Tabela oficial de tackles attempted

# SELECT
# teamId,
# COUNT(*),
# FROM event_main
# WHERE matchId = 1821549
# AND (type ILIKE '%Tackle%'
# OR type ILIKE '%Challenge%')
# GROUP BY teamId

# Tabela oficial de Interceptions

# SELECT
# teamId,
# COUNT(*),
# FROM event_main
# WHERE matchId = 1821549
# AND type ILIKE '%Interception%'
# GROUP BY teamId
# ORDER BY teamId

# Tabela oficial de Clearances

# Se nao for um "clearance effective" ou "clearance total" nao conta
# Se o endX e endY for = 0, nao conta
# Punches também conta como clearance

# SELECT
# teamId,
# COUNT(*),
# FROM event_main
# WHERE matchId = 1821538
# AND (type ILIKE '%Clearance%'
# OR type ILIKE '%punch%')
# AND NOT (endX IS NULL AND endY IS NULL)
# GROUP BY teamId
# ORDER BY teamId

# Tabela oficial de Blocks

# Clearance pode ser um block
# Saves que nao sao de goleiros sao blocks

#     WITH satisfied_event_array AS (
#     SELECT
#     eventId, teamId, matchId, event_main.type AS main_type, outcomeType,
#     FROM_JSON(JSON(satisfiedEventsTypes), '["VARCHAR"]') AS satisfiedEventsTypes
#     FROM event_main
#     LEFT JOIN satisfied_event_types
#     USING (matchId, eventId, teamId)
#     ),
#     satisfied_event_preparation AS (
#     SELECT
#     eventId, teamId, matchId, main_type, unnest AS event_type_id, outcomeType
#     FROM satisfied_event_array,
#     UNNEST (satisfiedEventsTypes) AS set
#     ),
#     satisfied_event AS (
#     SELECT
#     eventId, teamId, matchId, main_type, array_agg(event_type) AS satisfied_types
#     FROM satisfied_event_preparation
#     LEFT JOIN event_types
#     USING (event_type_id)
#     GROUP BY main_type, teamId, eventId, matchId
#     ),
#     qualifiers AS (
#     SELECT
#     matchId,
#     eventId,
#     teamId,
#     array_agg(eq.type) AS qualifier_type
#     FROM event_qualifiers AS eq
#     LEFT JOIN event_main AS em
#     USING (matchId, eventId, teamId)
#     GROUP BY matchId, eventId, teamId
#     )
# SELECT
#     em.matchId,
#     em.teamId,
#     COUNT(*)
# FROM event_main AS em
# LEFT JOIN qualifiers AS q
# USING (matchId, eventId, teamId)
# LEFT JOIN satisfied_event AS se
# USING (matchId, eventId, teamId)
# WHERE em.matchId = 1821529
# AND (em.type ILIKE '%block%'
# OR em.type ILIKE 'save'
# OR em.type ILIKE 'clearance')
# AND NOT (em.type ILIKE 'save'
#           AND (
# 'KeeperSaveInTheBox' = ANY(q.qualifier_type)
# OR 'KeeperSaveInTheBox' = ANY(se.satisfied_types)
# OR 'KeeperSaveObox' = ANY(q.qualifier_type)
# OR 'KeeperSaveObox' = ANY(se.satisfied_types)
# OR 'Hands' = ANY(q.qualifier_type)
#   )
#  )
# AND em.endX IS NULL
# AND em.endY IS NULL
# GROUP BY em.matchId, em.teamId


# Tabela oficial de offsides

# SELECT
# teamId,
# COUNT(*),
# FROM event_main
# WHERE matchId = 1821529
# AND type ILIKE '%OffsideGiven%'
# GROUP BY teamId
# ORDER BY teamId


# Tabela oficial de fouls

# Apenas as fouls que sao unsuccessful contam

#  WITH satisfied_event_array AS (
#     SELECT
#     eventId, teamId, matchId, event_main.type AS main_type, outcomeType,
#     FROM_JSON(JSON(satisfiedEventsTypes), '["VARCHAR"]') AS satisfiedEventsTypes
#     FROM event_main
#     LEFT JOIN satisfied_event_types
#     USING (matchId, eventId, teamId)
#     ),
#     satisfied_event_preparation AS (
#     SELECT
#     eventId, teamId, matchId, main_type, unnest AS event_type_id, outcomeType
#     FROM satisfied_event_array,
#     UNNEST (satisfiedEventsTypes) AS set
#     ),
#     satisfied_event AS (
#     SELECT
#     eventId, teamId, matchId, main_type, array_agg(event_type) AS satisfied_types
#     FROM satisfied_event_preparation
#     LEFT JOIN event_types
#     USING (event_type_id)
#     GROUP BY main_type, teamId, eventId, matchId
#     ),
#     qualifiers AS (
#     SELECT
#     matchId,
#     eventId,
#     teamId,
#     array_agg(eq.type) AS qualifier_type
#     FROM event_qualifiers AS eq
#     LEFT JOIN event_main AS em
#     USING (matchId, eventId, teamId)
#     GROUP BY matchId, eventId, teamId
#     )
# SELECT
#     em.matchId,
#     em.teamId,
#     COUNT(*)
# FROM event_main AS em
# LEFT JOIN qualifiers AS q
# USING (matchId, eventId, teamId)
# LEFT JOIN satisfied_event AS se
# USING (matchId, eventId, teamId)
# WHERE em.matchId = 1821529
# AND (em.type ILIKE '%foul%')
# AND em.outcomeType ILIKE 'unsuccessful'
# GROUP BY em.matchId, em.teamId

# Tabela oficial de aerials duels

# Aerials sucessful contam
# Aerials Fouls contam

#  WITH satisfied_event_array AS (
#     SELECT
#     eventId, teamId, matchId, event_main.type AS main_type, outcomeType,
#     FROM_JSON(JSON(satisfiedEventsTypes), '["VARCHAR"]') AS satisfiedEventsTypes
#     FROM event_main
#     LEFT JOIN satisfied_event_types
#     USING (matchId, eventId, teamId)
#     ),
#     satisfied_event_preparation AS (
#     SELECT
#     eventId, teamId, matchId, main_type, unnest AS event_type_id, outcomeType
#     FROM satisfied_event_array,
#     UNNEST (satisfiedEventsTypes) AS set
#     ),
#     satisfied_event AS (
#     SELECT
#     eventId, teamId, matchId, main_type, array_agg(event_type) AS satisfied_types
#     FROM satisfied_event_preparation
#     LEFT JOIN event_types
#     USING (event_type_id)
#     GROUP BY main_type, teamId, eventId, matchId
#     ),
#     qualifiers AS (
#     SELECT
#     matchId,
#     eventId,
#     teamId,
#     array_agg(eq.type) AS qualifier_type
#     FROM event_qualifiers AS eq
#     LEFT JOIN event_main AS em
#     USING (matchId, eventId, teamId)
#     GROUP BY matchId, eventId, teamId
#     )
# SELECT
#     em.matchId,
#     em.teamId,
#     COUNT(*)
# FROM event_main AS em
# LEFT JOIN qualifiers AS q
# USING (matchId, eventId, teamId)
# LEFT JOIN satisfied_event AS se
# USING (matchId, eventId, teamId)
# WHERE em.matchId = 1821538
# AND em.outcomeType ILIKE 'successful'
# AND (
#     (em.type ILIKE '%aerial%') OR
#     (em.type ILIKE '%foul%' AND 'AerialFoul' = ANY(q.qualifier_type))
# )
# GROUP BY em.matchId, em.teamId

# Tabela oficial de touches

#  WITH satisfied_event_array AS (
#     SELECT
#     eventId, teamId, matchId, event_main.type AS main_type, outcomeType,
#     FROM_JSON(JSON(satisfiedEventsTypes), '["VARCHAR"]') AS satisfiedEventsTypes
#     FROM event_main
#     LEFT JOIN satisfied_event_types
#     USING (matchId, eventId, teamId)
#     ),
#     satisfied_event_preparation AS (
#     SELECT
#     eventId, teamId, matchId, main_type, unnest AS event_type_id, outcomeType
#     FROM satisfied_event_array,
#     UNNEST (satisfiedEventsTypes) AS set
#     ),
#     satisfied_event AS (
#     SELECT
#     eventId, teamId, matchId, main_type, array_agg(event_type) AS satisfied_types
#     FROM satisfied_event_preparation
#     LEFT JOIN event_types
#     USING (event_type_id)
#     GROUP BY main_type, teamId, eventId, matchId
#     ),
#     qualifiers AS (
#     SELECT
#     matchId,
#     eventId,
#     teamId,
#     array_agg(eq.type) AS qualifier_type
#     FROM event_qualifiers AS eq
#     LEFT JOIN event_main AS em
#     USING (matchId, eventId, teamId)
#     GROUP BY matchId, eventId, teamId
#     )
# SELECT
#     em.matchId,
#     em.teamId,
#     COUNT(*)
# FROM event_main AS em
# LEFT JOIN qualifiers AS q
# USING (matchId, eventId, teamId)
# LEFT JOIN satisfied_event AS se
# USING (matchId, eventId, teamId)
# WHERE em.matchId = 1821529
# AND 'touches' = ANY(se.satisfied_types)
# GROUP BY em.matchId, em.teamId


# Tabela oficial de Loss of possession

#  WITH satisfied_event_array AS (
#     SELECT
#     eventId, teamId, matchId, event_main.type AS main_type, outcomeType,
#     FROM_JSON(JSON(satisfiedEventsTypes), '["VARCHAR"]') AS satisfiedEventsTypes
#     FROM event_main
#     LEFT JOIN satisfied_event_types
#     USING (matchId, eventId, teamId)
#     ),
#     satisfied_event_preparation AS (
#     SELECT
#     eventId, teamId, matchId, main_type, unnest AS event_type_id, outcomeType
#     FROM satisfied_event_array,
#     UNNEST (satisfiedEventsTypes) AS set
#     ),
#     satisfied_event AS (
#     SELECT
#     eventId, teamId, matchId, main_type, array_agg(event_type) AS satisfied_types
#     FROM satisfied_event_preparation
#     LEFT JOIN event_types
#     USING (event_type_id)
#     GROUP BY main_type, teamId, eventId, matchId
#     ),
#     qualifiers AS (
#     SELECT
#     matchId,
#     eventId,
#     teamId,
#     array_agg(eq.type) AS qualifier_type
#     FROM event_qualifiers AS eq
#     LEFT JOIN event_main AS em
#     USING (matchId, eventId, teamId)
#     GROUP BY matchId, eventId, teamId
#     )
# SELECT
#     em.matchId,
#     em.teamId,
#     COUNT(*)
# FROM event_main AS em
# LEFT JOIN qualifiers AS q
# USING (matchId, eventId, teamId)
# LEFT JOIN satisfied_event AS se
# USING (matchId, eventId, teamId)
# WHERE em.matchId = 1821538
# AND (em.type ILIKE '%dispossessed%'
# OR 'turnover' = ANY(se.satisfied_types))
# GROUP BY em.matchId, em.teamId


# Tabela oficial de errors

# SELECT
# teamId,
# COUNT(*),
# FROM event_main
# WHERE matchId = 1821538
# AND type ILIKE '%error%'
# GROUP BY teamId
# ORDER BY teamId


# Tabela oficial de saves

#  WITH satisfied_event_array AS (
#     SELECT
#     eventId, teamId, matchId, event_main.type AS main_type, outcomeType,
#     FROM_JSON(JSON(satisfiedEventsTypes), '["VARCHAR"]') AS satisfiedEventsTypes
#     FROM event_main
#     LEFT JOIN satisfied_event_types
#     USING (matchId, eventId, teamId)
#     ),
#     satisfied_event_preparation AS (
#     SELECT
#     eventId, teamId, matchId, main_type, unnest AS event_type_id, outcomeType
#     FROM satisfied_event_array,
#     UNNEST (satisfiedEventsTypes) AS set
#     ),
#     satisfied_event AS (
#     SELECT
#     eventId, teamId, matchId, main_type, array_agg(event_type) AS satisfied_types
#     FROM satisfied_event_preparation
#     LEFT JOIN event_types
#     USING (event_type_id)
#     GROUP BY main_type, teamId, eventId, matchId
#     ),
#     qualifiers AS (
#     SELECT
#     matchId,
#     eventId,
#     teamId,
#     array_agg(eq.type) AS qualifier_type
#     FROM event_qualifiers AS eq
#     LEFT JOIN event_main AS em
#     USING (matchId, eventId, teamId)
#     GROUP BY matchId, eventId, teamId
#     )
# SELECT
#     em.matchId,
#     em.teamId,
#     COUNT(*)
# FROM event_main AS em
# LEFT JOIN qualifiers AS q
# USING (matchId, eventId, teamId)
# LEFT JOIN satisfied_event AS se
# USING (matchId, eventId, teamId)
# WHERE em.matchId = 1821538
# AND (em.type ILIKE 'save'
#     AND (
#         'KeeperSaveInTheBox' = ANY(q.qualifier_type)
#         OR 'KeeperSaveObox' = ANY(q.qualifier_type)
#         OR 'ParriedSafe' = ANY(q.qualifier_type)
#         AND 'Hands' = ANY(q.qualifier_type)
#  )
# )
# GROUP BY em.matchId, em.teamId


# Tabela oficial de claims

# SELECT
# teamId,
# COUNT(*),
# FROM event_main
# WHERE matchId = 1821538
# AND type ILIKE '%claim%'
# GROUP BY teamId
# ORDER BY teamId


# Tabela oficial de punches

# SELECT
# teamId,
# COUNT(*),
# FROM event_main
# WHERE matchId = 1821538
# AND type ILIKE '%punch%'
# GROUP BY teamId
# ORDER BY teamId

start_time = time.time()

query = pd.read_sql(
    """
    WITH satisfied_event_array AS (
        SELECT
            eventId, teamId, matchId, event_main.type AS main_type, outcomeType,
            FROM_JSON(JSON(satisfiedEventsTypes), '["VARCHAR"]') AS satisfiedEventsTypes
        FROM event_main
        LEFT JOIN satisfied_event_types USING (matchId, eventId, teamId)
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
        LEFT JOIN event_types USING (event_type_id)
        GROUP BY main_type, teamId, eventId, matchId
    ),
    qualifiers AS (
        SELECT
            matchId,
            eventId,
            teamId,
            array_agg(eq.type) AS qualifier_type
        FROM event_qualifiers AS eq
        LEFT JOIN event_main AS em USING (matchId, eventId, teamId)
        GROUP BY matchId, eventId, teamId
    )
    SELECT
        em.matchId,
        em.eventId,
        em.teamId,
        em.type AS main_type,
        em.outcomeType,
        em.playerName,
        em.minute,
        em.second,
        em.x,
        em.y,
        em.endX,
        em.endY,
        q.qualifier_type,
        se.satisfied_types,
        IF(
            em.type ILIKE 'save' AND (
                'KeeperSaveInTheBox' = ANY(q.qualifier_type)
                OR 'KeeperSaveObox' = ANY(q.qualifier_type)
                OR 'ParriedSafe' = ANY(q.qualifier_type)
                AND 'Hands' = ANY(q.qualifier_type)
            ), 
            True, 
            False
        ) AS is_save,
        IF(em.type ILIKE '%error%', True, False) AS is_error,
        IF(em.type ILIKE '%claim%', True, False) AS is_claim,
        IF(em.type ILIKE '%punch%', True, False) AS is_punch,
        IF(
            em.type ILIKE '%dispossessed%' 
            OR 'turnover' = ANY(se.satisfied_types),
            True, False
        ) AS is_loss_possession,
        IF('touches' = ANY(se.satisfied_types), True, False) AS is_touch,
        IF(em.outcomeType ILIKE 'successful'
            AND (
                em.type ILIKE '%aerial%' OR
                (em.type ILIKE '%foul%' AND 'AerialFoul' = ANY(q.qualifier_type))
        ), True, False) AS is_aerial,
        IF(em.type ILIKE '%foul%'AND em.outcomeType ILIKE 'unsuccessful', True, False) AS is_foul,
        IF(em.type ILIKE '%OffsideGiven%', True, False) AS is_offside,
        IF((em.type ILIKE '%block%' OR em.type ILIKE 'save' OR em.type ILIKE 'clearance')
            AND NOT (em.type ILIKE 'save'
                AND ('KeeperSaveInTheBox' = ANY(q.qualifier_type)
                    OR 'KeeperSaveInTheBox' = ANY(se.satisfied_types)
                    OR 'KeeperSaveObox' = ANY(q.qualifier_type)
                    OR 'KeeperSaveObox' = ANY(se.satisfied_types)
                    OR 'Hands' = ANY(q.qualifier_type)
                )
            )
            AND em.endX IS NULL
            AND em.endY IS NULL, 
        True, False) AS is_block,
        IF((em.type ILIKE '%clearance%' OR em.type ILIKE '%punch%')
        AND NOT (endX IS NULL AND endY IS NULL), True, False) AS is_clearance,
        IF(em.type ILIKE '%Interception%', True, False) AS is_interception,
        IF(em.type ILIKE '%Tackle%' OR em.type ILIKE '%Challenge%', True, False) AS is_tackle,
        IF(em.type ILIKE '%TakeOn%', True, False) AS is_dribble,
        IF(em.type ILIKE '%shot%' OR em.type ILIKE 'goal', True, False) AS is_shot,
        IF(em.type ILIKE 'pass'
            AND NOT (
                'KeeperThrow' = ANY(q.qualifier_type)
                OR 'Cross' = ANY(q.qualifier_type)
                OR 'ThrowIn' = ANY(q.qualifier_type)
            ), True, False) AS is_pass
    FROM event_main AS em
    LEFT JOIN qualifiers AS q USING (matchId, eventId, teamId)
    LEFT JOIN satisfied_event AS se USING (matchId, eventId, teamId)
""",
    con,
)
end_time = time.time()
execution_time = end_time - start_time

print(f"Query execution time: {execution_time} seconds")

query_filtered = query[
    (query["matchId"] == 1821549)
    # & (query["minute"] == 46)
    # & (query["second"] == 0)
    & (query["is_pass"] == True)
]
query_df = query[query["matchId"] == 1821549]
query_df = (
    query_df.groupby(["teamId", "matchId", "eventId"])[
        [
            "is_shot",
            "is_pass",
            "is_dribble",
            "is_tackle",
            "is_interception",
            "is_clearance",
            "is_block",
            "is_offside",
            "is_foul",
            "is_aerial",
            "is_touch",
            "is_loss_possession",
            "is_error",
            "is_save",
            "is_claim",
            "is_punch",
        ]
    ]
    .agg("sum")
    .sum()
)

number_df = query_df.reset_index()

number_df.to_sql("main_query", con=con, index=False, if_exists="replace")

Stop = True

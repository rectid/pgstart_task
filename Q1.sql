WITH
    -- 1) Разбиваем теги вопросов с postgresql в формате |t1|t2|…|
    qt AS (
        SELECT
            q.id             AS question_id,
            q.creationdate   AS question_date,
            regexp_split_to_table(
                    trim(both '|' FROM q.tags),
                    '\|'
            )                AS tag
        FROM dba_stackexchange_com.posts AS q
        WHERE q.posttypeid = 1
          AND q.tags LIKE '%|postgresql|%'
    ),

    -- 2) Оставляем только «ко‑теги» (все, кроме postgresql)
    co AS (
        SELECT
            question_id,
            question_date,
            tag              AS co_tag
        FROM qt
        WHERE tag <> 'postgresql'
    ),

    -- 3) Считаем, сколько раз каждый ко‑тег встречается вместе с postgresql
    tag_counts AS (
        SELECT
            co_tag,
            COUNT(DISTINCT question_id) AS pair_count
        FROM co
        GROUP BY co_tag
        ORDER BY pair_count DESC
    ),

    -- 4) Собираем ответы на эти вопросы
    answers AS (
        SELECT
            a.parentid         AS question_id,
            a.creationdate     AS answer_date,
            a.owneruserid      AS answerer_id
        FROM dba_stackexchange_com.posts AS a
        WHERE a.posttypeid = 2
          AND a.parentid IN (SELECT question_id FROM co)
    ),

    -- 5) Считаем метрики по каждой из топ‑10 пар
    metrics AS (
        SELECT
            tc.co_tag,
            tc.pair_count,
            ROUND(
                    AVG(EXTRACT(EPOCH FROM (ans.answer_date - co.question_date)) / 3600.0)
                , 2)                 AS avg_response_hours,
            ROUND(AVG(u.reputation), 2) AS avg_answerer_reputation
        FROM tag_counts AS tc
                 JOIN co
                      ON co.co_tag = tc.co_tag
                 LEFT JOIN answers AS ans
                           ON ans.question_id = co.question_id
                 LEFT JOIN dba_stackexchange_com.users AS u
                           ON u.id = ans.answerer_id
        GROUP BY tc.co_tag, tc.pair_count
    )

SELECT
    'postgresql'           AS tag1,
    co_tag                 AS tag2,
    pair_count,
    avg_response_hours,
    avg_answerer_reputation
FROM metrics
ORDER BY pair_count DESC;

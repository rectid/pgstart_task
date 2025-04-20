SELECT
    q.id                   AS question_id,
    q.title                AS question_title,
    q.tags                 AS question_tags,
    a.id                   AS answer_id,
    a.score                AS answer_score,
    a.creationdate         AS answer_date,
    u.id                   AS answerer_id,
    u.displayname          AS answerer_name,
    u.reputation           AS answerer_reputation
FROM dba_stackexchange_com.posts AS q
         JOIN dba_stackexchange_com.posts AS a
              ON q.acceptedanswerid = a.id
         JOIN dba_stackexchange_com.users AS u
              ON a.owneruserid = u.id
WHERE q.posttypeid = 1                      -- только вопросы
  AND a.posttypeid = 2                      -- только ответы
  AND q.tags LIKE '%|postgresql|%'          -- тег postgresql
  AND a.score < 0                           -- отрицательный рейтинг
ORDER BY a.score ASC                        -- сначала самые «парадоксальные»

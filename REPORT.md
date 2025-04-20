# REPORT

## Цель работы
Разработать инструмент импорта и анализа данных из XML-дампов (StackExchange) в PostgreSQL.

## Используемые технологии
- Язык: Go
- База данных: PostgreSQL
- Формат данных: XML + SQL
- Конфигурация: YAML

## Этапы обработки данных

### 1. Парсинг SQL-схемы
- Определение таблиц, колонок и связей (FK).
- Осуществляется парсинг DDL-файла `schema.sql` (путь задается в конфиге).

### 2. Анализ доступных XML-дампов
- Определяется, какие таблицы и поля реально присутствуют в дампах.
- Анализ XML-файлов из директории с дампами.
- Поддерживаются как `dba.stackexchange.com/*.xml`, так и `dba.meta.stackexchange.com/*.xml`.

### 3. Генерация отфильтрированной схемы
- Создается SQL-файл `filtered_schema.sql`, содержащий:
  - создание схемы,
  - удаление старых таблиц,
  - создание таблиц только с существующими в дампах колонками,
  - восстановление внешних ключей.
- Путь к выходной схеме задается в конфигурации.

### 4. Применение схемы к PostgreSQL
- Выполняется SQL-файл на указанной БД.
- Все операции идут под транзакционным управлением.
- Пример запуска из командной строки:
```sh
psql -h localhost -U postgres -d postgres -f output/filtered_schema.sql
```

### 5. Импорт данных из XML
- Данные загружаются в PostgreSQL с помощью батчевой вставки.
- Используется мультипоточность (по числу CPU).
- Триггеры временно отключаются, затем восстанавливаются.

## Скрипты
- `Q1.sql` - код к заданию Q1
- `Q2.sql` - код к заданию Q2
- `filtered_schema.sql` — итоговая DDL-схема
- `schema.sql` — полная DDL-схема
- Go-код импорта данных — в `main.go`, `database/`, `parser/`, `config/`

## Оптимизации

### Реализованные подходы:
- Импорт только реально существующих данных (исключение «пустых» таблиц).
- Использование батчевых вставок с подготовленными выражениями.
- Импорт в несколько потоков (по числу логических CPU).
- Отключение/включение триггеров для ускорения импорта.

### Идеи для дальнейшего улучшения:
- Добавление `COPY FROM STDIN` для ещё более быстрого импорта.
- Использование индексирования и партиционирования для больших таблиц.
- Загрузка дампов напрямую через `COPY xml_data` из заранее преобразованных CSV.
- Использование временных таблиц и `INSERT ... SELECT` для предварительной валидации.

## Вопросы и принятые решения
- **Нужно ли использовать FOREIGN KEY?** — принято решение оставить FK, но добавлять их после загрузки данных. В схему они включаются, но оборачиваются в `DROP IF EXISTS + ADD`, чтобы избежать конфликтов при частичной загрузке.
- **Если колонка есть в DDL, но нет в XML?** — она игнорируется при генерации `filtered_schema.sql`.
- **Как обрабатываются типы данных?** — осуществляется маппинг MSSQL → PostgreSQL с учетом длины и nullable-ограничений.

## Конфигурационный файл
Пример `config.yaml`:
```yaml
database:
  host: "localhost"
  port: 5432
  user: "postgres"
  password: "yourpassword"
  dbname: "postgres"
  sslmode: "disable"

paths:
  xml_dumps_dir: "/path/to/xml/dumps"
  output_sql_file: "filtered_schema.sql"
  schema_sql_file: "schema.sql"
```

## SQL скрипты выполненные в работе

#### Скрипт для задания Q1:
```sql
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

```

#### Скрипт для задания Q2:
```sql
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

```

#### Схема для `dba.stackexchange.com/*.xml`:
```sql
-- filtered schema
CREATE SCHEMA IF NOT EXISTS dba_stackexchange_com;

-- Drop existing tables (if any) in reverse dependency order
DROP TABLE IF EXISTS dba_stackexchange_com.Votes CASCADE;
DROP TABLE IF EXISTS dba_stackexchange_com.Users CASCADE;
DROP TABLE IF EXISTS dba_stackexchange_com.Tags CASCADE;
DROP TABLE IF EXISTS dba_stackexchange_com.Posts CASCADE;
DROP TABLE IF EXISTS dba_stackexchange_com.PostLinks CASCADE;
DROP TABLE IF EXISTS dba_stackexchange_com.PostHistory CASCADE;
DROP TABLE IF EXISTS dba_stackexchange_com.Comments CASCADE;
DROP TABLE IF EXISTS dba_stackexchange_com.Badges CASCADE;

CREATE TABLE IF NOT EXISTS dba_stackexchange_com.Badges (
    Id INTEGER PRIMARY KEY,
    UserId INTEGER,
    Name VARCHAR(50),
    Date TIMESTAMP,
    Class SMALLINT,
    TagBased BOOLEAN
);

CREATE TABLE IF NOT EXISTS dba_stackexchange_com.Comments (
    Id INTEGER PRIMARY KEY,
    PostId INTEGER,
    Score INTEGER,
    Text TEXT,
    CreationDate TIMESTAMP,
    UserDisplayName VARCHAR(30),
    UserId INTEGER
);

CREATE TABLE IF NOT EXISTS dba_stackexchange_com.PostHistory (
    Id INTEGER PRIMARY KEY,
    PostHistoryTypeId SMALLINT,
    PostId INTEGER,
    RevisionGUID UUID,
    CreationDate TIMESTAMP,
    UserId INTEGER,
    UserDisplayName VARCHAR(40),
    Comment TEXT,
    Text TEXT
);

CREATE TABLE IF NOT EXISTS dba_stackexchange_com.PostLinks (
    Id INTEGER PRIMARY KEY,
    CreationDate TIMESTAMP,
    PostId INTEGER,
    RelatedPostId INTEGER,
    LinkTypeId SMALLINT
);

CREATE TABLE IF NOT EXISTS dba_stackexchange_com.Posts (
    Id INTEGER PRIMARY KEY,
    PostTypeId SMALLINT,
    AcceptedAnswerId INTEGER,
    ParentId INTEGER,
    CreationDate TIMESTAMP,
    Score INTEGER,
    ViewCount INTEGER,
    Body TEXT,
    OwnerUserId INTEGER,
    OwnerDisplayName VARCHAR(40),
    LastEditorUserId INTEGER,
    LastEditorDisplayName VARCHAR(40),
    LastEditDate TIMESTAMP,
    LastActivityDate TIMESTAMP,
    Title VARCHAR(250),
    Tags VARCHAR(250),
    AnswerCount INTEGER,
    CommentCount INTEGER,
    FavoriteCount INTEGER,
    ClosedDate TIMESTAMP,
    CommunityOwnedDate TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dba_stackexchange_com.Tags (
    Id INTEGER PRIMARY KEY,
    TagName VARCHAR(35),
    Count INTEGER,
    ExcerptPostId INTEGER,
    WikiPostId INTEGER
);

CREATE TABLE IF NOT EXISTS dba_stackexchange_com.Users (
    Id INTEGER PRIMARY KEY,
    Reputation INTEGER,
    CreationDate TIMESTAMP,
    DisplayName VARCHAR(40),
    LastAccessDate TIMESTAMP,
    WebsiteUrl VARCHAR(200),
    Location VARCHAR(100),
    AboutMe TEXT,
    Views INTEGER,
    UpVotes INTEGER,
    DownVotes INTEGER,
    AccountId INTEGER
);

CREATE TABLE IF NOT EXISTS dba_stackexchange_com.Votes (
    Id INTEGER PRIMARY KEY,
    PostId INTEGER,
    VoteTypeId SMALLINT,
    UserId INTEGER,
    CreationDate TIMESTAMP,
    BountyAmount INTEGER
);

-- Add foreign key constraints
ALTER TABLE dba_stackexchange_com.PostHistory DROP CONSTRAINT IF EXISTS Fk_PostHistory_Posts;
ALTER TABLE dba_stackexchange_com.PostHistory ADD CONSTRAINT Fk_PostHistory_Posts FOREIGN KEY (PostId) REFERENCES dba_stackexchange_com.Posts(Id);
ALTER TABLE dba_stackexchange_com.PostHistory DROP CONSTRAINT IF EXISTS Fk_PostHistory_Users;
ALTER TABLE dba_stackexchange_com.PostHistory ADD CONSTRAINT Fk_PostHistory_Users FOREIGN KEY (UserId) REFERENCES dba_stackexchange_com.Users(Id);
ALTER TABLE dba_stackexchange_com.Votes DROP CONSTRAINT IF EXISTS Fk_Votes_Posts;
ALTER TABLE dba_stackexchange_com.Votes ADD CONSTRAINT Fk_Votes_Posts FOREIGN KEY (PostId) REFERENCES dba_stackexchange_com.Posts(Id);
ALTER TABLE dba_stackexchange_com.Votes DROP CONSTRAINT IF EXISTS Fk_Votes_Users;
ALTER TABLE dba_stackexchange_com.Votes ADD CONSTRAINT Fk_Votes_Users FOREIGN KEY (UserId) REFERENCES dba_stackexchange_com.Users(Id);
ALTER TABLE dba_stackexchange_com.Badges DROP CONSTRAINT IF EXISTS Fk_Badges_Users;
ALTER TABLE dba_stackexchange_com.Badges ADD CONSTRAINT Fk_Badges_Users FOREIGN KEY (UserId) REFERENCES dba_stackexchange_com.Users(Id);
ALTER TABLE dba_stackexchange_com.Comments DROP CONSTRAINT IF EXISTS Fk_Comments_Posts;
ALTER TABLE dba_stackexchange_com.Comments ADD CONSTRAINT Fk_Comments_Posts FOREIGN KEY (PostId) REFERENCES dba_stackexchange_com.Posts(Id);
ALTER TABLE dba_stackexchange_com.Comments DROP CONSTRAINT IF EXISTS Fk_Comments_Users;
ALTER TABLE dba_stackexchange_com.Comments ADD CONSTRAINT Fk_Comments_Users FOREIGN KEY (UserId) REFERENCES dba_stackexchange_com.Users(Id);
ALTER TABLE dba_stackexchange_com.PostLinks DROP CONSTRAINT IF EXISTS Fk_PostLinks_Posts;
ALTER TABLE dba_stackexchange_com.PostLinks ADD CONSTRAINT Fk_PostLinks_Posts FOREIGN KEY (PostId) REFERENCES dba_stackexchange_com.Posts(Id);
ALTER TABLE dba_stackexchange_com.Posts DROP CONSTRAINT IF EXISTS Fk_Posts_Users;
ALTER TABLE dba_stackexchange_com.Posts ADD CONSTRAINT Fk_Posts_Users FOREIGN KEY (OwnerUserId) REFERENCES dba_stackexchange_com.Users(Id);
ALTER TABLE dba_stackexchange_com.Tags DROP CONSTRAINT IF EXISTS Fk_Tags_Posts;
ALTER TABLE dba_stackexchange_com.Tags ADD CONSTRAINT Fk_Tags_Posts FOREIGN KEY (ExcerptPostId) REFERENCES dba_stackexchange_com.Posts(Id);
ALTER TABLE dba_stackexchange_com.Posts DROP CONSTRAINT IF EXISTS Fk_Posts_Posts;
ALTER TABLE dba_stackexchange_com.Posts ADD CONSTRAINT Fk_Posts_Posts FOREIGN KEY (AcceptedAnswerId) REFERENCES dba_stackexchange_com.Posts(Id);

```

#### Схема для `dba.meta.stackexchange.com/*.xml`:
```sql
-- filtered schema
CREATE SCHEMA IF NOT EXISTS dba_meta_stackexchange_com;

-- Drop existing tables (if any) in reverse dependency order
DROP TABLE IF EXISTS dba_meta_stackexchange_com.Votes CASCADE;
DROP TABLE IF EXISTS dba_meta_stackexchange_com.Users CASCADE;
DROP TABLE IF EXISTS dba_meta_stackexchange_com.Tags CASCADE;
DROP TABLE IF EXISTS dba_meta_stackexchange_com.Posts CASCADE;
DROP TABLE IF EXISTS dba_meta_stackexchange_com.PostLinks CASCADE;
DROP TABLE IF EXISTS dba_meta_stackexchange_com.PostHistory CASCADE;
DROP TABLE IF EXISTS dba_meta_stackexchange_com.Comments CASCADE;
DROP TABLE IF EXISTS dba_meta_stackexchange_com.Badges CASCADE;

CREATE TABLE IF NOT EXISTS dba_meta_stackexchange_com.Badges (
    Id INTEGER PRIMARY KEY,
    UserId INTEGER,
    Name VARCHAR(50),
    Date TIMESTAMP,
    Class SMALLINT,
    TagBased BOOLEAN
);

CREATE TABLE IF NOT EXISTS dba_meta_stackexchange_com.Comments (
    Id INTEGER PRIMARY KEY,
    PostId INTEGER,
    Score INTEGER,
    Text TEXT,
    CreationDate TIMESTAMP,
    UserDisplayName VARCHAR(30),
    UserId INTEGER
);

CREATE TABLE IF NOT EXISTS dba_meta_stackexchange_com.PostHistory (
    Id INTEGER PRIMARY KEY,
    PostHistoryTypeId SMALLINT,
    PostId INTEGER,
    RevisionGUID UUID,
    CreationDate TIMESTAMP,
    UserId INTEGER,
    UserDisplayName VARCHAR(40),
    Comment TEXT,
    Text TEXT
);

CREATE TABLE IF NOT EXISTS dba_meta_stackexchange_com.PostLinks (
    Id INTEGER PRIMARY KEY,
    CreationDate TIMESTAMP,
    PostId INTEGER,
    RelatedPostId INTEGER,
    LinkTypeId SMALLINT
);

CREATE TABLE IF NOT EXISTS dba_meta_stackexchange_com.Posts (
    Id INTEGER PRIMARY KEY,
    PostTypeId SMALLINT,
    AcceptedAnswerId INTEGER,
    ParentId INTEGER,
    CreationDate TIMESTAMP,
    Score INTEGER,
    ViewCount INTEGER,
    Body TEXT,
    OwnerUserId INTEGER,
    OwnerDisplayName VARCHAR(40),
    LastEditorUserId INTEGER,
    LastEditorDisplayName VARCHAR(40),
    LastEditDate TIMESTAMP,
    LastActivityDate TIMESTAMP,
    Title VARCHAR(250),
    Tags VARCHAR(250),
    AnswerCount INTEGER,
    CommentCount INTEGER,
    FavoriteCount INTEGER,
    ClosedDate TIMESTAMP,
    CommunityOwnedDate TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dba_meta_stackexchange_com.Tags (
    Id INTEGER PRIMARY KEY,
    TagName VARCHAR(35),
    Count INTEGER,
    ExcerptPostId INTEGER,
    WikiPostId INTEGER
);

CREATE TABLE IF NOT EXISTS dba_meta_stackexchange_com.Users (
    Id INTEGER PRIMARY KEY,
    Reputation INTEGER,
    CreationDate TIMESTAMP,
    DisplayName VARCHAR(40),
    LastAccessDate TIMESTAMP,
    WebsiteUrl VARCHAR(200),
    Location VARCHAR(100),
    AboutMe TEXT,
    Views INTEGER,
    UpVotes INTEGER,
    DownVotes INTEGER,
    AccountId INTEGER
);

CREATE TABLE IF NOT EXISTS dba_meta_stackexchange_com.Votes (
    Id INTEGER PRIMARY KEY,
    PostId INTEGER,
    VoteTypeId SMALLINT,
    CreationDate TIMESTAMP
);

-- Add foreign key constraints
ALTER TABLE dba_meta_stackexchange_com.PostHistory DROP CONSTRAINT IF EXISTS Fk_PostHistory_Posts;
ALTER TABLE dba_meta_stackexchange_com.PostHistory ADD CONSTRAINT Fk_PostHistory_Posts FOREIGN KEY (PostId) REFERENCES dba_meta_stackexchange_com.Posts(Id);
ALTER TABLE dba_meta_stackexchange_com.PostHistory DROP CONSTRAINT IF EXISTS Fk_PostHistory_Users;
ALTER TABLE dba_meta_stackexchange_com.PostHistory ADD CONSTRAINT Fk_PostHistory_Users FOREIGN KEY (UserId) REFERENCES dba_meta_stackexchange_com.Users(Id);
ALTER TABLE dba_meta_stackexchange_com.Votes DROP CONSTRAINT IF EXISTS Fk_Votes_Posts;
ALTER TABLE dba_meta_stackexchange_com.Votes ADD CONSTRAINT Fk_Votes_Posts FOREIGN KEY (PostId) REFERENCES dba_meta_stackexchange_com.Posts(Id);
ALTER TABLE dba_meta_stackexchange_com.Badges DROP CONSTRAINT IF EXISTS Fk_Badges_Users;
ALTER TABLE dba_meta_stackexchange_com.Badges ADD CONSTRAINT Fk_Badges_Users FOREIGN KEY (UserId) REFERENCES dba_meta_stackexchange_com.Users(Id);
ALTER TABLE dba_meta_stackexchange_com.Comments DROP CONSTRAINT IF EXISTS Fk_Comments_Posts;
ALTER TABLE dba_meta_stackexchange_com.Comments ADD CONSTRAINT Fk_Comments_Posts FOREIGN KEY (PostId) REFERENCES dba_meta_stackexchange_com.Posts(Id);
ALTER TABLE dba_meta_stackexchange_com.Comments DROP CONSTRAINT IF EXISTS Fk_Comments_Users;
ALTER TABLE dba_meta_stackexchange_com.Comments ADD CONSTRAINT Fk_Comments_Users FOREIGN KEY (UserId) REFERENCES dba_meta_stackexchange_com.Users(Id);
ALTER TABLE dba_meta_stackexchange_com.PostLinks DROP CONSTRAINT IF EXISTS Fk_PostLinks_Posts;
ALTER TABLE dba_meta_stackexchange_com.PostLinks ADD CONSTRAINT Fk_PostLinks_Posts FOREIGN KEY (PostId) REFERENCES dba_meta_stackexchange_com.Posts(Id);
ALTER TABLE dba_meta_stackexchange_com.Posts DROP CONSTRAINT IF EXISTS Fk_Posts_Users;
ALTER TABLE dba_meta_stackexchange_com.Posts ADD CONSTRAINT Fk_Posts_Users FOREIGN KEY (OwnerUserId) REFERENCES dba_meta_stackexchange_com.Users(Id);
ALTER TABLE dba_meta_stackexchange_com.Tags DROP CONSTRAINT IF EXISTS Fk_Tags_Posts;
ALTER TABLE dba_meta_stackexchange_com.Tags ADD CONSTRAINT Fk_Tags_Posts FOREIGN KEY (ExcerptPostId) REFERENCES dba_meta_stackexchange_com.Posts(Id);
ALTER TABLE dba_meta_stackexchange_com.Posts DROP CONSTRAINT IF EXISTS Fk_Posts_Posts;
ALTER TABLE dba_meta_stackexchange_com.Posts ADD CONSTRAINT Fk_Posts_Posts FOREIGN KEY (AcceptedAnswerId) REFERENCES dba_meta_stackexchange_com.Posts(Id);

```

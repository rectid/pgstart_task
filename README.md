# README

## Как запустить

### 1. Установите зависимости:
```bash
go mod tidy
```

### 2. Настройте конфигурацию
Создайте файл `config.yaml` в корне:
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
  output_sql_file: "../filtered_schema.sql"
  schema_file: "../schema.sql"
```

### 3. Подготовьте входные файлы
- `schema.sql` — файл с полной SQL-схемой StackExchange (формат DDL).
- `dba.stackexchange.com/*.xml` или `dba.meta.stackexchange.com/*.xml` — XML-дампы.

### 4. Запустите загрузку
```bash
go run main.go
```

После выполнения:
- в БД будет создана схема с названием `dba_stackexchange_com`
- данные будут импортированы в таблицы

## Что делает код?
Читайте `REPORT.md`!

## Где что лежит
| Путь | Назначение |
|------|------------|
| `main.go` | Главная точка входа |
| `config/` | Загрузка YAML конфигурации |
| `parser/` | Парсинг SQL схемы и связей |
| `xmlparser/` | Сканирование XML-дампов |
| `database/` | Создание схемы, импорт данных |
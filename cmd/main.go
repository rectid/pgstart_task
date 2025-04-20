package main

import (
	"database/sql"
	"log"
	"path/filepath"
	"strings"

	"pgstart_task/config"
	"pgstart_task/database"
	"pgstart_task/parser"
	"pgstart_task/xmlparser"

	_ "github.com/lib/pq"
)

func main() {
	// Загрузка конфигурации
	cfgPath := "../config.yaml"
	cfg, err := config.LoadConfig(cfgPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// 1) Парсим DDL
	tables, err := parser.ParseSQLSchema(cfg.Paths.SchemaFile)
	if err != nil {
		log.Fatal(err)
	}
	relations, err := parser.ParseRelations(cfg.Paths.SchemaFile)
	if err != nil {
		log.Fatal(err)
	}

	// 2) Анализируем XML-дампы
	dumpTables := xmlparser.AnalyzeXMLDumps(cfg.Paths.XMLDumpsDir)

	// 3) Генерируем конечный скрипт
	schemaName := filepath.Base(cfg.Paths.XMLDumpsDir)
	schemaName = strings.ReplaceAll(schemaName, ".", "_")
	if err := database.GenerateFilteredSchema(tables, relations, dumpTables, cfg.Paths.OutputSQLFile, schemaName); err != nil {
		log.Fatal(err)
	}

	// 4) Применяем схему
	db, err := sql.Open("postgres", cfg.Database.GetConnectionString())
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := database.ExecuteSQLFile(db, cfg.Paths.OutputSQLFile); err != nil {
		log.Fatal(err)
	}
	log.Println("Schema created.")

	// 5) Импорт данных из дампов
	if err := database.ImportDataToPostgres(db, tables, dumpTables, cfg.Paths.XMLDumpsDir, schemaName); err != nil {
		log.Fatal(err)
	}
	log.Println("Data import completed.")
}

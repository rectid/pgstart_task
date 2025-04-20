package database

import (
	"database/sql"
	"encoding/xml"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
)

// ImportDataToPostgres importDataToPostgres импортирует данные из XML в PostgreSQL
func ImportDataToPostgres(db *sql.DB, tables []Table, dumps map[string]*TableInfo, dumpDir, schemaName string) error {
	// Определяем список реальных таблиц для импорта
	var jobs []Table
	for _, t := range tables {
		if _, ok := dumps[t.Name]; ok {
			jobs = append(jobs, t)
		}
	}

	// 1) Отключаем триггеры (FK) на этих таблицах
	log.Println("Disabling triggers (FK) on all tables")
	for _, t := range jobs {
		if _, err := db.Exec(
			fmt.Sprintf("ALTER TABLE %s.%s DISABLE TRIGGER ALL;", schemaName, t.Name),
		); err != nil {
			return fmt.Errorf("disable triggers %s: %w", t.Name, err)
		}
	}

	// 2) Параллельный импорт
	maxWorkers := runtime.NumCPU()
	var wg sync.WaitGroup
	jobsCh := make(chan Table)

	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range jobsCh {
				ti := dumps[t.Name]
				if err := importSingleTable(db, t, ti, dumpDir, schemaName); err != nil {
					log.Printf("Error importing table %s: %v", t.Name, err)
				}
			}
		}()
	}
	for _, t := range jobs {
		jobsCh <- t
	}
	close(jobsCh)
	wg.Wait()

	// 3) Включаем триггеры обратно
	log.Println("Re-enabling triggers (FK) on all tables")
	for _, t := range jobs {
		if _, err := db.Exec(
			fmt.Sprintf("ALTER TABLE %s.%s ENABLE TRIGGER ALL;", schemaName, t.Name),
		); err != nil {
			return fmt.Errorf("enable triggers %s: %w", t.Name, err)
		}
	}

	return nil
}

// importSingleTable импортирует одну таблицу из XML с использованием batch insert
func importSingleTable(db *sql.DB, table Table, ti *TableInfo, dumpDir, schemaName string) error {
	log.Printf("Start batch import for table %s", table.Name)

	// 1. Определяем колонки для экспорта
	var cols []string
	for _, c := range table.Columns {
		if _, ok := ti.Columns[c.Name]; ok {
			cols = append(cols, c.Name)
		}
	}
	if len(cols) == 0 {
		log.Printf("Skipping %s: no matching columns", table.Name)
		return nil
	}

	// 2. Подготавливаем SQL запрос для batch insert
	batchSize := 1000
	var placeholders []string
	var values []interface{}

	for i := 1; i <= batchSize; i++ {
		var ph []string
		for j := range cols {
			ph = append(ph, fmt.Sprintf("$%d", (i-1)*len(cols)+j+1))
		}
		placeholders = append(placeholders, "("+strings.Join(ph, ",")+")")
	}

	query := fmt.Sprintf(
		"INSERT INTO %s.%s (%s) VALUES %s",
		schemaName, table.Name,
		strings.Join(cols, ", "),
		strings.Join(placeholders, ", "),
	)

	// 3. Открываем XML файл
	xmlPath := filepath.Join(dumpDir, table.Name+".xml")
	f, err := os.Open(xmlPath)
	if err != nil {
		return err
	}
	defer f.Close()

	dec := xml.NewDecoder(f)
	var rowCount int
	var currentBatchSize int

	// 4. Начинаем транзакцию
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction failed: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare(query)
	if err != nil {
		return fmt.Errorf("prepare statement failed: %w", err)
	}
	defer stmt.Close()

	// 5. Обрабатываем XML и собираем батчи
	for {
		tok, err := dec.Token()
		if err != nil {
			break
		}

		if se, ok := tok.(xml.StartElement); ok && se.Name.Local == "row" {
			var row struct {
				Attrs []xml.Attr `xml:",any,attr"`
			}
			if dec.DecodeElement(&row, &se) != nil {
				continue
			}

			for _, col := range cols {
				found := false
				for _, a := range row.Attrs {
					if a.Name.Local == col {
						values = append(values, a.Value)
						found = true
						break
					}
				}
				if !found {
					values = append(values, nil)
				}
			}

			currentBatchSize++
			rowCount++

			if currentBatchSize >= batchSize {
				if err := executeBatch(stmt, values, currentBatchSize, len(cols)); err != nil {
					return fmt.Errorf("batch insert failed: %w", err)
				}
				values = values[:0]
				currentBatchSize = 0
			}
		}
	}

	// 6. Выполняем оставшиеся записи
	if currentBatchSize > 0 {
		placeholders = placeholders[:currentBatchSize]
		query = fmt.Sprintf(
			"INSERT INTO %s.%s (%s) VALUES %s",
			schemaName, table.Name,
			strings.Join(cols, ", "),
			strings.Join(placeholders, ", "),
		)

		stmt, err := tx.Prepare(query)
		if err != nil {
			return fmt.Errorf("prepare final statement failed: %w", err)
		}

		if err := executeBatch(stmt, values, currentBatchSize, len(cols)); err != nil {
			return fmt.Errorf("final batch insert failed: %w", err)
		}
	}

	// 7. Финализируем транзакцию
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit failed: %w", err)
	}

	log.Printf("Finished import table %s: %d rows", table.Name, rowCount)
	return nil
}

// executeBatch выполняет пакетную вставку
func executeBatch(stmt *sql.Stmt, values []interface{}, batchSize, cols int) error {
	expectedArgs := batchSize * cols
	if len(values) < expectedArgs {
		for len(values) < expectedArgs {
			values = append(values, nil)
		}
	}

	if _, err := stmt.Exec(values...); err != nil {
		return err
	}
	return nil
}

// ExecuteSQLFile выполняет SQL скрипт
func ExecuteSQLFile(db *sql.DB, path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	_, err = db.Exec(string(b))
	return err
}

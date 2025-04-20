package database

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// GenerateFilteredSchema создает DDL с учетом только существующих в дампах таблиц и колонок
func GenerateFilteredSchema(tables []Table, relations []Relation, dumps map[string]*TableInfo, outFile, schemaName string) error {
	f, err := os.Create(outFile)
	if err != nil {
		return err
	}
	defer f.Close()

	fmt.Fprintf(f, "-- filtered schema\nCREATE SCHEMA IF NOT EXISTS %s;\n\n", schemaName)

	// DROP существующих таблиц
	fmt.Fprintln(f, "-- Drop existing tables (if any) in reverse dependency order")
	for i := len(tables) - 1; i >= 0; i-- {
		t := tables[i]
		if _, ok := dumps[t.Name]; ok {
			fmt.Fprintf(f, "DROP TABLE IF EXISTS %s.%s CASCADE;\n", schemaName, t.Name)
		}
	}
	fmt.Fprintln(f)

	// CREATE TABLE
	for _, t := range tables {
		ti, ok := dumps[t.Name]
		if !ok {
			continue
		}

		var cols []Column
		for _, c := range t.Columns {
			if _, found := ti.Columns[c.Name]; found {
				cols = append(cols, c)
			}
		}
		if len(cols) == 0 {
			continue
		}

		fmt.Fprintf(f, "CREATE TABLE IF NOT EXISTS %s.%s (\n", schemaName, t.Name)
		for i, c := range cols {
			comma := ""
			if i+1 < len(cols) {
				comma = ","
			}
			pg := toPgType(c.Type)
			pk := ""
			if strings.EqualFold(c.Name, "Id") {
				pk = " PRIMARY KEY"
			}
			fmt.Fprintf(f, "    %s %s%s%s\n", c.Name, pg, pk, comma)
		}
		fmt.Fprintln(f, ");\n")
	}

	// ALTER TABLE … FOREIGN KEY
	seen := map[string]struct{}{}
	fmt.Fprintln(f, "-- Add foreign key constraints")
	for _, r := range relations {
		tiSrc, ok1 := dumps[r.SourceTable]
		tiTgt, ok2 := dumps[r.TargetTable]
		if !ok1 || !ok2 {
			continue
		}
		if _, ok := tiSrc.Columns[r.SourceColumn]; !ok {
			continue
		}
		if _, ok := tiTgt.Columns[r.TargetColumn]; !ok {
			continue
		}
		if _, dup := seen[r.ConstraintName]; dup {
			continue
		}

		fmt.Fprintf(f, "ALTER TABLE %s.%s DROP CONSTRAINT IF EXISTS %s;\n",
			schemaName, r.SourceTable, r.ConstraintName)

		fmt.Fprintf(f,
			"ALTER TABLE %s.%s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s.%s(%s);\n",
			schemaName, r.SourceTable,
			r.ConstraintName,
			r.SourceColumn,
			schemaName, r.TargetTable, r.TargetColumn,
		)
		seen[r.ConstraintName] = struct{}{}
	}
	return nil
}

// toPgType конвертирует MSSQL тип в PostgreSQL
func toPgType(sqlType string) string {
	t := strings.ToLower(strings.TrimSpace(sqlType))

	var length int
	if m := regexp.MustCompile(`\((\d+)\)`).FindStringSubmatch(t); len(m) == 2 {
		if n, err := strconv.Atoi(m[1]); err == nil {
			length = n
		}
	}

	switch {
	case strings.HasPrefix(t, "int("), t == "int":
		return "INTEGER"
	case strings.HasPrefix(t, "smallint"):
		return "SMALLINT"
	case strings.HasPrefix(t, "tinyint"):
		return "SMALLINT"
	case strings.HasPrefix(t, "bigint"):
		return "BIGINT"
	case strings.HasPrefix(t, "bit"):
		return "BOOLEAN"
	case strings.HasPrefix(t, "datetime"), strings.HasPrefix(t, "timestamp"):
		return "TIMESTAMP"
	case strings.HasPrefix(t, "date"):
		return "DATE"
	case strings.HasPrefix(t, "uniqueidentifier"):
		return "UUID"
	case strings.HasPrefix(t, "decimal"), strings.HasPrefix(t, "numeric"):
		return strings.ToUpper(sqlType)
	case strings.HasPrefix(t, "text"):
		return "TEXT"
	case strings.HasPrefix(t, "nvarchar"), strings.HasPrefix(t, "varchar"), strings.HasPrefix(t, "char"):
		if length > 255 {
			return "TEXT"
		}
		if length > 0 {
			return fmt.Sprintf("VARCHAR(%d)", length)
		}
		return "VARCHAR"
	default:
		return strings.ToUpper(sqlType)
	}
}

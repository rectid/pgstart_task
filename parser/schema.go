package parser

import (
	"os"
	"regexp"
	"strings"

	"pgstart_task/database"
)

// ParseSQLSchema parseSQLSchema парсит CREATE TABLE из SQL файла
func ParseSQLSchema(path string) ([]database.Table, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	text := string(data)

	reTable := regexp.MustCompile(`(?is)CREATE TABLE\s+(\w+)\s*\((.*?)\)\s*;`)
	reCol := regexp.MustCompile(`(?i)^\s*([A-Za-z_]\w*)\s+([A-Za-z0-9()_,]+)`)

	var tables []database.Table
	for _, m := range reTable.FindAllStringSubmatch(text, -1) {
		name := m[1]
		block := m[2]
		t := database.Table{Name: name}

		for _, line := range strings.Split(block, "\n") {
			line = strings.TrimSpace(line)
			if line == "" ||
				strings.HasPrefix(strings.ToUpper(line), "PRIMARY KEY") ||
				strings.HasPrefix(strings.ToUpper(line), "CONSTRAINT") {
				continue
			}
			line = strings.TrimSuffix(line, ",")
			if caps := reCol.FindStringSubmatch(line); caps != nil {
				t.Columns = append(t.Columns, database.Column{
					Name: caps[1],
					Type: caps[2],
				})
			}
		}
		tables = append(tables, t)
	}
	return tables, nil
}

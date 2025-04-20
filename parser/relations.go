package parser

import (
	"os"
	"regexp"

	"pgstart_task/database"
)

// ParseRelations parseRelations парсит ALTER TABLE с внешними ключами
func ParseRelations(path string) ([]database.Relation, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	text := string(data)

	reRel := regexp.MustCompile(
		`(?i)ALTER TABLE\s+(\w+)\s+ADD CONSTRAINT\s+(\w+)\s+FOREIGN KEY\s*\(\s*(\w+)\s*\)\s+REFERENCES\s+(\w+)\s*\(\s*(\w+)\s*\)`,
	)

	var rels []database.Relation
	for _, m := range reRel.FindAllStringSubmatch(text, -1) {
		rels = append(rels, database.Relation{
			ConstraintName: m[2],
			SourceTable:    m[1],
			SourceColumn:   m[3],
			TargetTable:    m[4],
			TargetColumn:   m[5],
		})
	}
	return rels, nil
}

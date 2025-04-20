package database

// Column представляет колонку таблицы
type Column struct {
	Name string
	Type string
}

// Table представляет таблицу с колонками
type Table struct {
	Name    string
	Columns []Column // Порядок сохранён
}

// Relation представляет внешний ключ между таблицами
type Relation struct {
	ConstraintName string
	SourceTable    string
	SourceColumn   string
	TargetTable    string
	TargetColumn   string
}

// TableInfo содержит информацию о таблице из XML-дампа
type TableInfo struct {
	Name    string
	Columns map[string]string
}

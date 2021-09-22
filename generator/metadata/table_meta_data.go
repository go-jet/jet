package metadata

// Table metadata struct
type Table struct {
	Name    string
	Columns []Column
}

// MutableColumns returns list of mutable columns for table
func (t Table) MutableColumns() []Column {
	var ret []Column

	for _, column := range t.Columns {
		if column.IsPrimaryKey {
			continue
		}

		ret = append(ret, column)
	}

	return ret
}

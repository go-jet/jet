package metadata

// Table metadata struct
type Table struct {
	Name    string `sql:"primary_key"`
	Comment string
	Columns []Column
}

// MutableColumns returns list of mutable columns for table
func (t Table) MutableColumns() []Column {
	var ret []Column

	for _, column := range t.Columns {
		if column.IsPrimaryKey || column.IsGenerated {
			continue
		}

		ret = append(ret, column)
	}

	return ret
}

// DefaultColumns returns list of columns with default values set for table
func (t Table) DefaultColumns() []Column {
	var ret []Column

	for _, column := range t.Columns {
		if column.HasDefault {
			ret = append(ret, column)
		}
	}

	return ret
}

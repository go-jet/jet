package metadata

import "regexp"

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

// GoLangComment returns table comment without ascii control characters
func (t Table) GoLangComment() string {
	if t.Comment == "" {
		return ""
	}

	// remove ascii control characters from string
	return regexp.MustCompile(`[[:cntrl:]]+`).ReplaceAllString(t.Comment, "")
}

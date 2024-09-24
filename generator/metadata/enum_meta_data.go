package metadata

import "regexp"

// Enum metadata struct
type Enum struct {
	Name    string `sql:"primary_key"`
	Comment string
	Values  []string
}

// GoLangComment returns enum comment without ascii control characters
func (e Enum) GoLangComment() string {
	if e.Comment == "" {
		return ""
	}

	// remove ascii control characters from string
	return regexp.MustCompile(`[[:cntrl:]]+`).ReplaceAllString(e.Comment, "")
}

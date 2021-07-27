package metadata

// Enum metadata struct
type Enum struct {
	Name   string `sql:"primary_key"`
	Values []string
}

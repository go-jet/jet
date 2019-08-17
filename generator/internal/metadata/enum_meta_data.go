package metadata

// EnumMetaData struct
type EnumMetaData struct {
	EnumName string
	Values   []string
}

// Name returns enum name
func (e EnumMetaData) Name() string {
	return e.EnumName
}

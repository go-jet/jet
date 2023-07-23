package metadata

import (
	"regexp"
)

// Column struct
type Column struct {
	Name         string
	IsPrimaryKey bool
	IsNullable   bool
	IsGenerated  bool
	DataType     DataType
	Comment      string
}

// GoLangComment returns column comment without ascii control characters
func (c Column) GoLangComment() string {
	if c.Comment == "" {
		return ""
	}

	// remove ascii control characters from string
	return regexp.MustCompile(`[[:cntrl:]]+`).ReplaceAllString(c.Comment, "")
}

// DataTypeKind is database type kind(base, enum, user-defined, array)
type DataTypeKind string

// DataTypeKind possible values
const (
	BaseType        DataTypeKind = "base"
	EnumType        DataTypeKind = "enum"
	UserDefinedType DataTypeKind = "user-defined"
	ArrayType       DataTypeKind = "array"
)

// DataType contains information about column data type
type DataType struct {
	Name       string
	Kind       DataTypeKind
	IsUnsigned bool
}

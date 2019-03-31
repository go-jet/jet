package sqlbuilder

import "bytes"

type serializeOption int

const (
	ALIASED = iota
	FOR_PROJECTION
)

type Clause interface {
	SerializeSql(out *bytes.Buffer, options ...serializeOption) error
}

func contains(s []serializeOption, e serializeOption) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}

package sqlbuilder

import "bytes"

type Clause interface {
	SerializeSql(out *bytes.Buffer) error
}

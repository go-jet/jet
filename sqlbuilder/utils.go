package sqlbuilder

import "bytes"

func serializeExpressionList(expressions []Expression, buf *bytes.Buffer) error {
	for i, value := range expressions {
		if i > 0 {
			buf.WriteString(", ")
		}

		err := value.SerializeSql(buf)

		if err != nil {
			return err
		}
	}

	return nil
}

func serializeProjectionList(projections []Projection, buf *bytes.Buffer) error {
	for i, value := range projections {
		if i > 0 {
			buf.WriteString(", ")
		}

		err := value.SerializeForProjection(buf)

		if err != nil {
			return err
		}
	}

	return nil
}

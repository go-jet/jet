package sqlbuilder

import (
	"bytes"
	"errors"
	"strconv"
)

type clause interface {
	serialize(statement statementType, out *queryData) error
}

type queryData struct {
	buff bytes.Buffer
	args []interface{}
}

type statementType string

const (
	select_statement statementType = "SELECT"
	insert_statement statementType = "INSERT"
	update_statement statementType = "UPDATE"
	delete_statement statementType = "DELETE"
	set_statement    statementType = "SET"
	lock_statement   statementType = "LOCK"
)

func (q *queryData) writeProjection(statement statementType, projections []projection) error {
	return serializeProjectionList(statement, projections, q)
}

func (q *queryData) writeWhere(statement statementType, where expression) error {
	q.writeString(" WHERE ")
	return where.serialize(statement, q)
}

func (q *queryData) writeGroupBy(statement statementType, groupBy []groupByClause) error {
	q.writeString(" GROUP BY ")

	return serializeGroupByClauseList(statement, groupBy, q)
}

func (q *queryData) writeOrderBy(statement statementType, orderBy []orderByClause) error {
	q.writeString(" ORDER BY ")
	return serializeOrderByClauseList(statement, orderBy, q)
}

func (q *queryData) writeHaving(statement statementType, having expression) error {
	q.writeString(" HAVING ")
	return having.serialize(statement, q)
}

func (q *queryData) write(data []byte) {
	q.buff.Write(data)
}

func (q *queryData) writeString(str string) {
	q.buff.WriteString(str)
}

func (q *queryData) writeByte(b byte) {
	q.buff.WriteByte(b)
}

func (q *queryData) insertArgument(arg interface{}) {
	q.args = append(q.args, arg)
	argPlaceholder := "$" + strconv.Itoa(len(q.args))

	q.buff.WriteString(argPlaceholder)
}

func (q *queryData) reset() {
	q.buff.Reset()
	q.args = []interface{}{}
}

func argToString(value interface{}) (string, error) {
	switch bindVal := value.(type) {
	case bool:
		if bindVal {
			return "TRUE", nil
		} else {
			return "FALSE", nil
		}
	case int8:
		return strconv.FormatInt(int64(bindVal), 10), nil
	case int:
		return strconv.FormatInt(int64(bindVal), 10), nil
	case int16:
		return strconv.FormatInt(int64(bindVal), 10), nil
	case int32:
		return strconv.FormatInt(int64(bindVal), 10), nil
	case int64:
		return strconv.FormatInt(int64(bindVal), 10), nil

	case uint8:
		return strconv.FormatUint(uint64(bindVal), 10), nil
	case uint:
		return strconv.FormatUint(uint64(bindVal), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(bindVal), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(bindVal), 10), nil
	case uint64:
		return strconv.FormatUint(uint64(bindVal), 10), nil

	case float32:
		return strconv.FormatFloat(float64(bindVal), 'f', -1, 64), nil
	case float64:
		return strconv.FormatFloat(float64(bindVal), 'f', -1, 64), nil

	case string:
		return bindVal, nil
	case []byte:
		return string(bindVal), nil
		//TODO: implement
	//case time.Time:
	//	return bindVal.String())
	default:
		return "", errors.New("Unsupported literal type. ")
	}
}

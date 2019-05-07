package sqlbuilder

import (
	"bytes"
	"errors"
	"strconv"
)

type clause interface {
	serialize(out *queryData) error
}

type queryData struct {
	buff bytes.Buffer
	args []interface{}

	statementType int
	clauseType    int
}

const (
	select_statement = iota
	insert_statement
	update_statement
	delete_statement
	set_statement
)

const (
	projection_clause = iota
	where_clause
	order_by_clause
	group_by_clause
	having_clause
)

func (q *queryData) WriteProjection(projections []projection) error {
	q.clauseType = projection_clause
	return serializeProjectionList(projections, q)
}

func (q *queryData) WriteWhere(where expression) error {
	q.clauseType = where_clause
	q.WriteString(" WHERE ")
	return where.serialize(q)
}

func (q *queryData) WriteGroupBy(groupBy []groupByClause) error {
	q.clauseType = group_by_clause
	q.WriteString(" GROUP BY ")

	return serializeGroupByClauseList(groupBy, q)
}

func (q *queryData) WriteOrderBy(orderBy []orderByClause) error {
	q.clauseType = order_by_clause
	q.WriteString(" ORDER BY ")
	return serializeOrderByClauseList(orderBy, q)
}

func (q *queryData) WriteHaving(having expression) error {
	q.clauseType = having_clause
	q.WriteString(" HAVING ")
	return having.serialize(q)

}

func (q *queryData) Write(data []byte) {
	q.buff.Write(data)
}

func (q *queryData) WriteString(str string) {
	q.buff.WriteString(str)
}

func (q *queryData) WriteByte(b byte) {
	q.buff.WriteByte(b)
}

func (q *queryData) InsertArgument(arg interface{}) {
	q.args = append(q.args, arg)
	argPlaceholder := "$" + strconv.Itoa(len(q.args))

	q.buff.WriteString(argPlaceholder)
}

func (q *queryData) Reset() {
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

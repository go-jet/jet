package sqlbuilder

import (
	"bytes"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"strconv"
	"strings"
	"time"
)

type serializeOption int

const (
	NO_WRAP serializeOption = iota
)

type clause interface {
	serialize(statement statementType, out *queryData, options ...serializeOption) error
}

func contains(options []serializeOption, option serializeOption) bool {
	for _, opt := range options {
		if opt == option {
			return true
		}
	}

	return false
}

type queryData struct {
	buff bytes.Buffer
	args []interface{}

	lastChar byte
	ident    int
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

const defaultIdent = 5

func (q *queryData) increaseIdent() {
	q.ident += defaultIdent
}

func (q *queryData) decreaseIdent() {
	if q.ident < defaultIdent {
		q.ident = 0
	}

	q.ident -= defaultIdent
}

func (q *queryData) writeProjections(statement statementType, projections []projection) error {
	q.increaseIdent()
	err := serializeProjectionList(statement, projections, q)
	q.decreaseIdent()
	return err
}

func (q *queryData) writeFrom(statement statementType, table ReadableTable) error {
	q.newLine()
	q.writeString("FROM")

	q.increaseIdent()
	err := table.serialize(statement, q)
	q.decreaseIdent()

	return err
}

func (q *queryData) writeWhere(statement statementType, where Expression) error {
	q.newLine()
	q.writeString("WHERE")

	q.increaseIdent()
	err := where.serialize(statement, q, NO_WRAP)
	q.decreaseIdent()

	return err
}

func (q *queryData) writeGroupBy(statement statementType, groupBy []groupByClause) error {
	q.newLine()
	q.writeString("GROUP BY")

	q.increaseIdent()
	err := serializeGroupByClauseList(statement, groupBy, q)
	q.decreaseIdent()

	return err
}

func (q *queryData) writeOrderBy(statement statementType, orderBy []OrderByClause) error {
	q.newLine()
	q.writeString("ORDER BY")

	q.increaseIdent()
	err := serializeOrderByClauseList(statement, orderBy, q)
	q.decreaseIdent()

	return err
}

func (q *queryData) writeHaving(statement statementType, having Expression) error {
	q.newLine()
	q.writeString("HAVING")

	q.increaseIdent()
	err := having.serialize(statement, q, NO_WRAP)
	q.decreaseIdent()

	return err
}

func (q *queryData) newLine() {
	q.write([]byte{'\n'})
	q.write(bytes.Repeat([]byte{' '}, q.ident))
}

func (q *queryData) write(data []byte) {
	if len(data) == 0 {
		return
	}

	if !isPreSeparator(q.lastChar) && !isPostSeparator(data[0]) && q.buff.Len() > 0 {
		q.buff.WriteByte(' ')
	}

	q.buff.Write(data)
	q.lastChar = data[len(data)-1]
}

func isPreSeparator(b byte) bool {
	return b == ' ' || b == '.' || b == ',' || b == '(' || b == '\n' || b == ':'
}

func isPostSeparator(b byte) bool {
	return b == ' ' || b == '.' || b == ',' || b == ')' || b == '\n' || b == ':'
}

func (q *queryData) writeQuotedString(str string) {
	q.writeString(`"` + str + `"`)
}

func (q *queryData) writeString(str string) {
	q.write([]byte(str))
}

func (q *queryData) writeIdentifier(name string) {
	quoteWrap := name != strings.ToLower(name) || strings.Contains(name, ".")

	if quoteWrap {
		q.writeString(`"` + name + `"`)
	} else {
		q.writeString(name)
	}
}

func (q *queryData) writeByte(b byte) {
	q.write([]byte{b})
}

func (q *queryData) finalize() (string, []interface{}) {
	return q.buff.String() + ";\n", q.args
}

func (q *queryData) insertConstantArgument(arg interface{}) {
	q.writeString(ArgToString(arg))
}

func (q *queryData) insertPreparedArgument(arg interface{}) {
	q.args = append(q.args, arg)
	argPlaceholder := "$" + strconv.Itoa(len(q.args))

	q.writeString(argPlaceholder)
}

func (q *queryData) reset() {
	q.buff.Reset()
	q.args = []interface{}{}
}

func ArgToString(value interface{}) string {
	if isNil(value) {
		return "NULL"
	}

	switch bindVal := value.(type) {
	case bool:
		if bindVal {
			return "TRUE"
		} else {
			return "FALSE"
		}
	case int8:
		return strconv.FormatInt(int64(bindVal), 10)
	case int:
		return strconv.FormatInt(int64(bindVal), 10)
	case int16:
		return strconv.FormatInt(int64(bindVal), 10)
	case int32:
		return strconv.FormatInt(int64(bindVal), 10)
	case int64:
		return strconv.FormatInt(int64(bindVal), 10)

	case uint8:
		return strconv.FormatUint(uint64(bindVal), 10)
	case uint:
		return strconv.FormatUint(uint64(bindVal), 10)
	case uint16:
		return strconv.FormatUint(uint64(bindVal), 10)
	case uint32:
		return strconv.FormatUint(uint64(bindVal), 10)
	case uint64:
		return strconv.FormatUint(uint64(bindVal), 10)

	case float32:
		return strconv.FormatFloat(float64(bindVal), 'f', -1, 64)
	case float64:
		return strconv.FormatFloat(float64(bindVal), 'f', -1, 64)

	case string:
		return stringQuote(bindVal)
	case []byte:
		return stringQuote(string(bindVal))
	case uuid.UUID:
		return stringQuote(bindVal.String())
	case time.Time:
		return stringQuote(string(pq.FormatTimestamp(bindVal)))
	default:
		return "[Unknown type]"
	}
}

func stringQuote(value string) string {
	return `'` + strings.Replace(value, "'", "''", -1) + `'`
}

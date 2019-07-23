package jet

import (
	"bytes"
	"github.com/go-jet/jet/internal/utils"
	"github.com/google/uuid"
	"strconv"
	"strings"
	"time"
)

type serializeOption int

const (
	noWrap serializeOption = iota
)

type clause interface {
	serialize(statement statementType, out *sqlBuilder, options ...serializeOption) error
}

func contains(options []serializeOption, option serializeOption) bool {
	for _, opt := range options {
		if opt == option {
			return true
		}
	}

	return false
}

type sqlBuilder struct {
	buff bytes.Buffer
	args []interface{}

	lastChar byte
	ident    int
}

type statementType string

const (
	selectStatement statementType = "SELECT"
	insertStatement statementType = "INSERT"
	updateStatement statementType = "UPDATE"
	deleteStatement statementType = "DELETE"
	setStatement    statementType = "SET"
	lockStatement   statementType = "LOCK"
)

const defaultIdent = 5

func (q *sqlBuilder) increaseIdent() {
	q.ident += defaultIdent
}

func (q *sqlBuilder) decreaseIdent() {
	if q.ident < defaultIdent {
		q.ident = 0
	}

	q.ident -= defaultIdent
}

func (q *sqlBuilder) writeProjections(statement statementType, projections []projection) error {
	q.increaseIdent()
	err := serializeProjectionList(statement, projections, q)
	q.decreaseIdent()
	return err
}

func (q *sqlBuilder) writeFrom(statement statementType, table ReadableTable) error {
	q.newLine()
	q.writeString("FROM")

	q.increaseIdent()
	err := table.serialize(statement, q)
	q.decreaseIdent()

	return err
}

func (q *sqlBuilder) writeWhere(statement statementType, where Expression) error {
	q.newLine()
	q.writeString("WHERE")

	q.increaseIdent()
	err := where.serialize(statement, q, noWrap)
	q.decreaseIdent()

	return err
}

func (q *sqlBuilder) writeGroupBy(statement statementType, groupBy []groupByClause) error {
	q.newLine()
	q.writeString("GROUP BY")

	q.increaseIdent()
	err := serializeGroupByClauseList(statement, groupBy, q)
	q.decreaseIdent()

	return err
}

func (q *sqlBuilder) writeOrderBy(statement statementType, orderBy []orderByClause) error {
	q.newLine()
	q.writeString("ORDER BY")

	q.increaseIdent()
	err := serializeOrderByClauseList(statement, orderBy, q)
	q.decreaseIdent()

	return err
}

func (q *sqlBuilder) writeHaving(statement statementType, having Expression) error {
	q.newLine()
	q.writeString("HAVING")

	q.increaseIdent()
	err := having.serialize(statement, q, noWrap)
	q.decreaseIdent()

	return err
}

func (q *sqlBuilder) writeReturning(statement statementType, returning []projection) error {
	if len(returning) == 0 {
		return nil
	}

	q.newLine()
	q.writeString("RETURNING")
	q.increaseIdent()

	return q.writeProjections(statement, returning)
}

func (q *sqlBuilder) newLine() {
	q.write([]byte{'\n'})
	q.write(bytes.Repeat([]byte{' '}, q.ident))
}

func (q *sqlBuilder) write(data []byte) {
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

func (q *sqlBuilder) writeQuotedString(str string) {
	q.writeString(`"` + str + `"`)
}

func (q *sqlBuilder) writeString(str string) {
	q.write([]byte(str))
}

func (q *sqlBuilder) writeIdentifier(name string) {
	quoteWrap := name != strings.ToLower(name) || strings.ContainsAny(name, ". -")

	if quoteWrap {
		q.writeString(`"` + name + `"`)
	} else {
		q.writeString(name)
	}
}

func (q *sqlBuilder) writeByte(b byte) {
	q.write([]byte{b})
}

func (q *sqlBuilder) finalize() (string, []interface{}) {
	return q.buff.String() + ";\n", q.args
}

func (q *sqlBuilder) insertConstantArgument(arg interface{}) {
	q.writeString(argToString(arg))
}

func (q *sqlBuilder) insertParametrizedArgument(arg interface{}) {
	q.args = append(q.args, arg)
	argPlaceholder := "$" + strconv.Itoa(len(q.args))

	q.writeString(argPlaceholder)
}

func argToString(value interface{}) string {
	if utils.IsNil(value) {
		return "NULL"
	}

	switch bindVal := value.(type) {
	case bool:
		if bindVal {
			return "TRUE"
		}
		return "FALSE"
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
		return stringQuote(string(utils.FormatTimestamp(bindVal)))
	default:
		return "[Unsupported type]"
	}
}

func stringQuote(value string) string {
	return `'` + strings.Replace(value, "'", "''", -1) + `'`
}

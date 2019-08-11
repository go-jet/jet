package jet

import (
	"bytes"
	"github.com/go-jet/jet/internal/utils"
	"github.com/google/uuid"
	"strconv"
	"strings"
	"time"
)

type SqlBuilder struct {
	Dialect Dialect
	Buff    bytes.Buffer
	Args    []interface{}

	lastChar byte
	ident    int

	debug bool
}

const defaultIdent = 5

func (s *SqlBuilder) IncreaseIdent(ident ...int) {
	if len(ident) > 0 {
		s.ident += ident[0]
	} else {
		s.ident += defaultIdent
	}
}

func (s *SqlBuilder) DecreaseIdent(ident ...int) {
	toDecrease := defaultIdent

	if len(ident) > 0 {
		toDecrease = ident[0]
	}

	if s.ident < toDecrease {
		s.ident = 0
	}

	s.ident -= toDecrease
}

func (s *SqlBuilder) WriteProjections(statement StatementType, projections []Projection) error {
	s.IncreaseIdent()
	err := SerializeProjectionList(statement, projections, s)
	s.DecreaseIdent()
	return err
}

func (s *SqlBuilder) writeFrom(statement StatementType, table Serializer) error {
	s.NewLine()
	s.WriteString("FROM")

	s.IncreaseIdent()
	err := table.serialize(statement, s)
	s.DecreaseIdent()

	return err
}

func (s *SqlBuilder) writeWhere(statement StatementType, where Expression) error {
	s.NewLine()
	s.WriteString("WHERE")

	s.IncreaseIdent()
	err := where.serialize(statement, s, noWrap)
	s.DecreaseIdent()

	return err
}

func (s *SqlBuilder) writeGroupBy(statement StatementType, groupBy []GroupByClause) error {
	s.NewLine()
	s.WriteString("GROUP BY")

	s.IncreaseIdent()
	err := serializeGroupByClauseList(statement, groupBy, s)
	s.DecreaseIdent()

	return err
}

func (s *SqlBuilder) writeOrderBy(statement StatementType, orderBy []OrderByClause) error {
	s.NewLine()
	s.WriteString("ORDER BY")

	s.IncreaseIdent()
	err := serializeOrderByClauseList(statement, orderBy, s)
	s.DecreaseIdent()

	return err
}

func (s *SqlBuilder) writeHaving(statement StatementType, having Expression) error {
	s.NewLine()
	s.WriteString("HAVING")

	s.IncreaseIdent()
	err := having.serialize(statement, s, noWrap)
	s.DecreaseIdent()

	return err
}

func (s *SqlBuilder) WriteReturning(statement StatementType, returning []Projection) error {
	if len(returning) == 0 {
		return nil
	}

	s.NewLine()
	s.WriteString("RETURNING")
	s.IncreaseIdent()

	return s.WriteProjections(statement, returning)
}

func (s *SqlBuilder) NewLine() {
	s.write([]byte{'\n'})
	s.write(bytes.Repeat([]byte{' '}, s.ident))
}

func (s *SqlBuilder) write(data []byte) {
	if len(data) == 0 {
		return
	}

	if !isPreSeparator(s.lastChar) && !isPostSeparator(data[0]) && s.Buff.Len() > 0 {
		s.Buff.WriteByte(' ')
	}

	s.Buff.Write(data)
	s.lastChar = data[len(data)-1]
}

func isPreSeparator(b byte) bool {
	return b == ' ' || b == '.' || b == ',' || b == '(' || b == '\n' || b == ':'
}

func isPostSeparator(b byte) bool {
	return b == ' ' || b == '.' || b == ',' || b == ')' || b == '\n' || b == ':'
}

func (s *SqlBuilder) writeAlias(str string) {
	aliasQuoteChar := string(s.Dialect.AliasQuoteChar())
	s.WriteString(aliasQuoteChar + str + aliasQuoteChar)
}

func (s *SqlBuilder) WriteString(str string) {
	s.write([]byte(str))
}

func (s *SqlBuilder) writeIdentifier(name string, alwaysQuote ...bool) {
	quoteWrap := name != strings.ToLower(name) || strings.ContainsAny(name, ". -")

	if quoteWrap || len(alwaysQuote) > 0 {
		identQuoteChar := string(s.Dialect.IdentifierQuoteChar())
		s.WriteString(identQuoteChar + name + identQuoteChar)
	} else {
		s.WriteString(name)
	}
}

func (s *SqlBuilder) writeByte(b byte) {
	s.write([]byte{b})
}

func (s *SqlBuilder) finalize() (string, []interface{}) {
	return s.Buff.String() + ";\n", s.Args
}

func (s *SqlBuilder) insertConstantArgument(arg interface{}) {
	s.WriteString(argToString(arg))
}

func (s *SqlBuilder) insertParametrizedArgument(arg interface{}) {
	if s.debug {
		s.insertConstantArgument(arg)
		return
	}

	s.Args = append(s.Args, arg)
	argPlaceholder := s.Dialect.ArgumentPlaceholder()(len(s.Args))

	s.WriteString(argPlaceholder)
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

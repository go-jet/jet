package jet

import (
	"bytes"
	"fmt"
	"github.com/go-jet/jet/internal/3rdparty/pq"
	"github.com/go-jet/jet/internal/utils"
	"github.com/google/uuid"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// SQLBuilder generates output SQL
type SQLBuilder struct {
	Dialect Dialect
	Buff    bytes.Buffer
	Args    []interface{}

	lastChar byte
	ident    int

	Debug bool
}

const defaultIdent = 5

// IncreaseIdent adds ident or defaultIdent number of spaces to each new line
func (s *SQLBuilder) IncreaseIdent(ident ...int) {
	if len(ident) > 0 {
		s.ident += ident[0]
	} else {
		s.ident += defaultIdent
	}
}

// DecreaseIdent removes ident or defaultIdent number of spaces for each new line
func (s *SQLBuilder) DecreaseIdent(ident ...int) {
	toDecrease := defaultIdent

	if len(ident) > 0 {
		toDecrease = ident[0]
	}

	if s.ident < toDecrease {
		s.ident = 0
	}

	s.ident -= toDecrease
}

// WriteProjections func
func (s *SQLBuilder) WriteProjections(statement StatementType, projections []Projection) {
	s.IncreaseIdent()
	SerializeProjectionList(statement, projections, s)
	s.DecreaseIdent()
}

// NewLine adds new line to output SQL
func (s *SQLBuilder) NewLine() {
	s.write([]byte{'\n'})
	s.write(bytes.Repeat([]byte{' '}, s.ident))
}

func (s *SQLBuilder) write(data []byte) {
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

// WriteAlias is used to add alias to output SQL
func (s *SQLBuilder) WriteAlias(str string) {
	aliasQuoteChar := string(s.Dialect.AliasQuoteChar())
	s.WriteString(aliasQuoteChar + str + aliasQuoteChar)
}

// WriteString writes sting to output SQL
func (s *SQLBuilder) WriteString(str string) {
	s.write([]byte(str))
}

// WriteIdentifier adds identifier to output SQL
func (s *SQLBuilder) WriteIdentifier(name string, alwaysQuote ...bool) {
	if shouldQuoteIdentifier(name) || len(alwaysQuote) > 0 {
		identQuoteChar := string(s.Dialect.IdentifierQuoteChar())
		s.WriteString(identQuoteChar + name + identQuoteChar)
	} else {
		s.WriteString(name)
	}
}

// WriteByte writes byte to output SQL
func (s *SQLBuilder) WriteByte(b byte) {
	s.write([]byte{b})
}

func (s *SQLBuilder) finalize() (string, []interface{}) {
	return s.Buff.String() + ";\n", s.Args
}

func (s *SQLBuilder) insertConstantArgument(arg interface{}) {
	s.WriteString(argToString(arg))
}

func (s *SQLBuilder) insertParametrizedArgument(arg interface{}) {
	if s.Debug {
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
	case int:
		return strconv.FormatInt(int64(bindVal), 10)
	case int32:
		return strconv.FormatInt(int64(bindVal), 10)
	case int64:
		return strconv.FormatInt(bindVal, 10)

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
		panic(fmt.Sprintf("jet: %s type can not be used as SQL query parameter", reflect.TypeOf(value).String()))
	}
}

func shouldQuoteIdentifier(identifier string) bool {
	for _, c := range identifier {
		if unicode.IsNumber(c) || c == '_' {
			continue
		}
		if c > unicode.MaxASCII || !unicode.IsLetter(c) || unicode.IsUpper(c) {
			return true
		}
	}
	return false
}

func stringQuote(value string) string {
	return `'` + strings.Replace(value, "'", "''", -1) + `'`
}

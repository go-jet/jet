package jet

import (
	"bytes"
	"fmt"
	"github.com/go-jet/jet/v2/internal/3rdparty/pq"
	"github.com/go-jet/jet/v2/internal/utils"
	"github.com/google/uuid"
	"reflect"
	"sort"
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
	if s.shouldQuote(name, alwaysQuote...) {
		identQuoteChar := string(s.Dialect.IdentifierQuoteChar())
		s.WriteString(identQuoteChar + name + identQuoteChar)
	} else {
		s.WriteString(name)
	}
}

func (s *SQLBuilder) shouldQuote(name string, alwaysQuote ...bool) bool {
	return s.Dialect.IsReservedWord(name) || shouldQuoteIdentifier(name) || len(alwaysQuote) > 0
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

func (s *SQLBuilder) insertRawQuery(raw string, namedArg map[string]interface{}) {
	type namedArgumentPosition struct {
		Name     string
		Value    interface{}
		Position int
	}

	var namedArgumentPositions []namedArgumentPosition

	for namedArg, value := range namedArg {
		rawCopy := raw
		rawIndex := 0
		exists := false

		// one named argument can occur multiple times inside raw string
		for {
			index := strings.Index(rawCopy, namedArg)
			if index == -1 {
				break
			}

			exists = true
			namedArgumentPositions = append(namedArgumentPositions, namedArgumentPosition{
				Name:     namedArg,
				Value:    value,
				Position: rawIndex + index,
			})

			rawCopy = rawCopy[index+len(namedArg):]
			rawIndex += index + len(namedArg)
		}

		if !exists {
			panic("jet: named argument '" + namedArg + "' does not appear in raw query")
		}
	}

	sort.Slice(namedArgumentPositions, func(i, j int) bool {
		return namedArgumentPositions[i].Position < namedArgumentPositions[j].Position
	})

	for _, namedArgumentPos := range namedArgumentPositions {
		// if named argument does not exists in raw string do not add argument to the list of arguments
		// It can happen if the same argument occurs multiple times in postgres query.
		if !strings.Contains(raw, namedArgumentPos.Name) {
			continue
		}
		s.Args = append(s.Args, namedArgumentPos.Value)
		currentArgNum := len(s.Args)

		placeholder := s.Dialect.ArgumentPlaceholder()(currentArgNum)
		// if placeholder is not unique identifier ($1, $2, etc..), we will replace just one occurrence of the argument
		toReplace := -1 // all occurrences
		if placeholder == "?" {
			toReplace = 1 // just one occurrence
		}

		if s.Debug {
			placeholder = argToString(namedArgumentPos.Value)
		}

		raw = strings.Replace(raw, namedArgumentPos.Name, placeholder, toReplace)
	}

	s.WriteString(raw)
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
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return integerTypesToString(bindVal)

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
		if strBindValue, ok := bindVal.(toStringInterface); ok {
			return stringQuote(strBindValue.String())
		}
		panic(fmt.Sprintf("jet: %s type can not be used as SQL query parameter", reflect.TypeOf(value).String()))
	}
}

type toStringInterface interface {
	String() string
}

func integerTypesToString(value interface{}) string {
	switch bindVal := value.(type) {
	case int:
		return strconv.FormatInt(int64(bindVal), 10)
	case uint:
		return strconv.FormatUint(uint64(bindVal), 10)
	case int8:
		return strconv.FormatInt(int64(bindVal), 10)
	case uint8:
		return strconv.FormatUint(uint64(bindVal), 10)
	case int16:
		return strconv.FormatInt(int64(bindVal), 10)
	case uint16:
		return strconv.FormatUint(uint64(bindVal), 10)
	case int32:
		return strconv.FormatInt(int64(bindVal), 10)
	case uint32:
		return strconv.FormatUint(uint64(bindVal), 10)
	case int64:
		return strconv.FormatInt(bindVal, 10)
	case uint64:
		return strconv.FormatUint(bindVal, 10)
	}
	panic("jet: Unsupported integer type: " + reflect.TypeOf(value).String())
}

func shouldQuoteIdentifier(identifier string) bool {
	_, err := strconv.ParseInt(identifier, 10, 64)

	if err == nil { // if it is a number we should quote it
		return true
	}

	// check if contains non ascii characters
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

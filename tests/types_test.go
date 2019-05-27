package tests

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/sub0zero/go-sqlbuilder/tests/.test_files/dvd_rental/test_sample/model"
	. "github.com/sub0zero/go-sqlbuilder/tests/.test_files/dvd_rental/test_sample/table"
	"gotest.tools/assert"
	"testing"
)

func TestAllTypesSelect(t *testing.T) {

	dest := []model.AllTypes{}

	err := AllTypes.SELECT(AllTypes.AllColumns).Query(db, &dest)

	fmt.Println(err)
	assert.NilError(t, err)

	assert.Equal(t, len(dest), 2)

	assert.DeepEqual(t, dest[0], dest0)
	assert.DeepEqual(t, dest[1], dest1)
}

var dest0 = model.AllTypes{
	SmallintPtr:        int16Ptr(1),
	Smallint:           1,
	IntegerPtr:         int32Ptr(300),
	Integer:            300,
	BigintPtr:          int64Ptr(50000),
	Bigint:             5000,
	DecimalPtr:         float64Ptr(11.44),
	Decimal:            11.44,
	NumericPtr:         float64Ptr(55.77),
	Numeric:            55.77,
	RealPtr:            float32Ptr(99.1),
	Real:               99.1,
	DoublePrecisionPtr: float64Ptr(11111111.22),
	DoublePrecision:    11111111.22,
	Smallserial:        1,
	Serial:             1,
	Bigserial:          1,
	//MoneyPtr: nil,
	//Money:
	CharacterVaryingPtr:  stringPtr("ABBA"),
	CharacterVarying:     "ABBA",
	CharacterPtr:         stringPtr("JOHN                                                                            "),
	Character:            "JOHN                                                                            ",
	TextPtr:              stringPtr("Some text"),
	Text:                 "Some text",
	ByteaPtr:             []byte("bytea"),
	Bytea:                []byte("bytea"),
	TimestampzPtr:        timestampWithTimeZone("1999-01-08 13:05:06 +0100 CET", 0),
	Timestampz:           *timestampWithTimeZone("1999-01-08 13:05:06 +0100 CET", 0),
	TimestampPtr:         timestampWithoutTimeZone("1999-01-08 04:05:06", 0),
	Timestamp:            *timestampWithoutTimeZone("1999-01-08 04:05:06", 0),
	DatePtr:              timestampWithoutTimeZone("1999-01-08 00:00:00", 0),
	Date:                 *timestampWithoutTimeZone("1999-01-08 00:00:00", 0),
	TimezPtr:             timeWithTimeZone("04:05:06 -0800"),
	Timez:                *timeWithTimeZone("04:05:06 -0800"),
	TimePtr:              timeWithoutTimeZone("04:05:06"),
	Time:                 *timeWithoutTimeZone("04:05:06"),
	IntervalPtr:          stringPtr("3 days 04:05:06"),
	Interval:             "3 days 04:05:06",
	BooleanPtr:           boolPtr(true),
	Boolean:              false,
	PointPtr:             stringPtr("(2,3)"),
	BitPtr:               stringPtr("101"),
	Bit:                  "101",
	BitVaryingPtr:        stringPtr("101111"),
	BitVarying:           "101111",
	TsvectorPtr:          stringPtr("'supernova':1"),
	Tsvector:             "'supernova':1",
	UUIDPtr:              uuidPtr("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
	UUID:                 uuid.MustParse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
	XMLPtr:               stringPtr("<Sub>abc</Sub>"),
	XML:                  "<Sub>abc</Sub>",
	JSONPtr:              stringPtr(`{"a": 1, "b": 3}`),
	JSON:                 `{"a": 1, "b": 3}`,
	JsonbPtr:             stringPtr(`{"a": 1, "b": 3}`),
	Jsonb:                `{"a": 1, "b": 3}`,
	IntegerArrayPtr:      stringPtr("{1,2,3}"),
	IntegerArray:         "{1,2,3}",
	TextArrayPtr:         stringPtr("{breakfast,consulting}"),
	TextArray:            "{breakfast,consulting}",
	JsonbArray:           `{"{\"a\": 1, \"b\": 2}","{\"a\": 3, \"b\": 4}"}`,
	TextMultiDimArrayPtr: stringPtr("{{meeting,lunch},{training,presentation}}"),
	TextMultiDimArray:    "{{meeting,lunch},{training,presentation}}",
}

var dest1 = model.AllTypes{
	SmallintPtr:        nil,
	Smallint:           1,
	IntegerPtr:         nil,
	Integer:            300,
	BigintPtr:          nil,
	Bigint:             5000,
	DecimalPtr:         nil,
	Decimal:            11.44,
	NumericPtr:         nil,
	Numeric:            55.77,
	RealPtr:            nil,
	Real:               99.1,
	DoublePrecisionPtr: nil,
	DoublePrecision:    11111111.22,
	Smallserial:        2,
	Serial:             2,
	Bigserial:          2,
	//MoneyPtr: nil,
	//Money:
	CharacterVaryingPtr:  nil,
	CharacterVarying:     "ABBA",
	CharacterPtr:         nil,
	Character:            "JOHN                                                                            ",
	TextPtr:              nil,
	Text:                 "Some text",
	ByteaPtr:             nil,
	Bytea:                []byte("bytea"),
	TimestampzPtr:        nil,
	Timestampz:           *timestampWithTimeZone("1999-01-08 13:05:06 +0100 CET", 0),
	TimestampPtr:         nil,
	Timestamp:            *timestampWithoutTimeZone("1999-01-08 04:05:06", 0),
	DatePtr:              nil,
	Date:                 *timestampWithoutTimeZone("1999-01-08 00:00:00", 0),
	TimezPtr:             nil,
	Timez:                *timeWithTimeZone("04:05:06 -0800"),
	TimePtr:              nil,
	Time:                 *timeWithoutTimeZone("04:05:06"),
	IntervalPtr:          nil,
	Interval:             "3 days 04:05:06",
	BooleanPtr:           nil,
	Boolean:              false,
	PointPtr:             nil,
	BitPtr:               nil,
	Bit:                  "101",
	BitVaryingPtr:        nil,
	BitVarying:           "101111",
	TsvectorPtr:          nil,
	Tsvector:             "'supernova':1",
	UUIDPtr:              nil,
	UUID:                 uuid.MustParse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"),
	XMLPtr:               nil,
	XML:                  "<Sub>abc</Sub>",
	JSONPtr:              nil,
	JSON:                 `{"a": 1, "b": 3}`,
	JsonbPtr:             nil,
	Jsonb:                `{"a": 1, "b": 3}`,
	IntegerArrayPtr:      nil,
	IntegerArray:         "{1,2,3}",
	TextArrayPtr:         nil,
	TextArray:            "{breakfast,consulting}",
	JsonbArray:           `{"{\"a\": 1, \"b\": 2}","{\"a\": 3, \"b\": 4}"}`,
	TextMultiDimArrayPtr: nil,
	TextMultiDimArray:    "{{meeting,lunch},{training,presentation}}",
}

package postgres

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/test_sample/model"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"

	. "github.com/go-jet/jet/v2/postgres"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/test_sample/table"
)

type AllTypesJsonRawMessageResult struct {
	SmallIntPtr          *int16           `alias:"all_types.small_int_ptr"`
	SmallInt             int16            `alias:"all_types.small_int"`
	IntegerPtr           *int32           `alias:"all_types.integer_ptr"`
	Integer              int32            `alias:"all_types.integer"`
	BigIntPtr            *int64           `alias:"all_types.big_int_ptr"`
	BigInt               int64            `alias:"all_types.big_int"`
	DecimalPtr           *float64         `alias:"all_types.decimal_ptr"`
	Decimal              float64          `alias:"all_types.decimal"`
	NumericPtr           *float64         `alias:"all_types.numeric_ptr"`
	Numeric              float64          `alias:"all_types.numeric"`
	RealPtr              *float32         `alias:"all_types.real_ptr"`
	Real                 float32          `alias:"all_types.real"`
	DoublePrecisionPtr   *float64         `alias:"all_types.double_precision_ptr"`
	DoublePrecision      float64          `alias:"all_types.double_precision"`
	Smallserial          int64            `alias:"all_types.smallserial"`
	Serial               int64            `sql:"primary_key" alias:"all_types.serial"`
	Bigserial            int64            `alias:"all_types.bigserial"`
	VarCharPtr           *string          `alias:"all_types.var_char_ptr"`
	VarChar              string           `alias:"all_types.var_char"`
	CharPtr              *string          `alias:"all_types.char_ptr"`
	Char                 string           `alias:"all_types.char"`
	TextPtr              *string          `alias:"all_types.text_ptr"`
	Text                 string           `alias:"all_types.text"`
	ByteaPtr             *[]byte          `alias:"all_types.bytea_ptr"`
	Bytea                []byte           `alias:"all_types.bytea"`
	TimestampzPtr        *time.Time       `alias:"all_types.timestampz_ptr"`
	Timestampz           time.Time        `alias:"all_types.timestampz"`
	TimestampPtr         *time.Time       `alias:"all_types.timestamp_ptr"`
	Timestamp            time.Time        `alias:"all_types.timestamp"`
	DatePtr              *time.Time       `alias:"all_types.date_ptr"`
	Date                 time.Time        `alias:"all_types.date"`
	TimezPtr             *time.Time       `alias:"all_types.timez_ptr"`
	Timez                time.Time        `alias:"all_types.timez"`
	TimePtr              *time.Time       `alias:"all_types.time_ptr"`
	Time                 time.Time        `alias:"all_types.time"`
	IntervalPtr          *string          `alias:"all_types.interval_ptr"`
	Interval             string           `alias:"all_types.interval"`
	BooleanPtr           *bool            `alias:"all_types.boolean_ptr"`
	Boolean              bool             `alias:"all_types.boolean"`
	PointPtr             *string          `alias:"all_types.point_ptr"`
	BitPtr               *string          `alias:"all_types.bit_ptr"`
	Bit                  string           `alias:"all_types.bit"`
	BitVaryingPtr        *string          `alias:"all_types.bit_varying_ptr"`
	BitVarying           string           `alias:"all_types.bit_varying"`
	TsvectorPtr          *string          `alias:"all_types.tsvector_ptr"`
	Tsvector             string           `alias:"all_types.tsvector"`
	UUIDPtr              *uuid.UUID       `alias:"all_types.uuid_ptr"`
	UUID                 uuid.UUID        `alias:"all_types.uuid"`
	XMLPtr               *string          `alias:"all_types.xml_ptr"`
	XML                  string           `alias:"all_types.xml"`
	JSONPtr              *string          `alias:"all_types.json_ptr"`
	JSON                 string           `alias:"all_types.json"`
	JsonbPtr             *json.RawMessage `alias:"all_types.jsonb_ptr"`
	Jsonb                json.RawMessage  `alias:"all_types.jsonb"`
	IntegerArrayPtr      *pq.Int32Array   `alias:"all_types.integer_array_ptr"`
	IntegerArray         pq.Int32Array    `alias:"all_types.integer_array"`
	TextArrayPtr         *pq.StringArray  `alias:"all_types.text_array_ptr"`
	TextArray            pq.StringArray   `alias:"all_types.text_array"`
	JsonbArray           pq.StringArray   `alias:"all_types.jsonb_array"`
	TextMultiDimArrayPtr *string          `alias:"all_types.text_multi_dim_array_ptr"`
	TextMultiDimArray    string           `alias:"all_types.text_multi_dim_array"`
	MoodPtr              *model.Mood      `alias:"all_types.mood_ptr"`
	Mood                 model.Mood       `alias:"all_types.mood"`
}

func TestJsonRawMessage(t *testing.T) {
	var dest []AllTypesJsonRawMessageResult

	err := SELECT(AllTypes.AllColumns).
		FROM(AllTypes).
		LIMIT(2).
		QueryContext(ctx, db, &dest)

	require.NoError(t, err)

	for _, row := range dest {
		t.Logf("%+v", string(row.Jsonb))
	}
}

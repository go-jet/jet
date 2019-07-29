package mysql

import (
	"github.com/go-jet/jet/internal/testutils"
	"github.com/go-jet/jet/tests/.gentestdata/mysql/test_sample/model"
	. "github.com/go-jet/jet/tests/.gentestdata/mysql/test_sample/table"
	"gotest.tools/assert"
	"testing"
)

func TestAllTypes(t *testing.T) {

	dest := []model.AllTypes{}

	err := AllTypes.
		SELECT(AllTypes.AllColumns).
		LIMIT(2).
		Query(db, &dest)

	assert.NilError(t, err)

	//testutils.JsonPrint(dest)
	testutils.AssertJSON(t, dest, allTypesJson)
}

var allTypesJson = `
[
	{
		"TinyInt": -3,
		"UtinyInt": 3,
		"SmallInt": -14,
		"UsmallInt": 14,
		"MediumInt": -150,
		"UmediumInt": 150,
		"Int": -1600,
		"Uint": 1600,
		"BigInt": -17000,
		"UbigInt": 17000,
		"TinyIntPtr": -3,
		"UtinyIntPtr": 3,
		"SmallIntPtr": -14,
		"UsmallIntPtr": 14,
		"MediumIntPtr": -150,
		"UmediumIntPtr": 150,
		"IntPtr": -1600,
		"UintPtr": 1600,
		"BigIntPtr": -17000,
		"UbigIntPtr": 17000,
		"Decimal": 1.11,
		"DecimalPtr": 1.11,
		"Numeric": 2.22,
		"NumericPtr": 2.22,
		"Float": 3.33,
		"FloatPtr": 3.33,
		"Double": 4.44,
		"DoublePtr": 4.44,
		"Bit": "\u0000\u0003",
		"BitPtr": "\u0000\u0003",
		"Date": "2008-07-04T00:00:00Z",
		"DatePtr": "2008-07-04T00:00:00Z",
		"DateTime": "2011-12-18T13:17:17Z",
		"DateTimePtr": "2011-12-18T13:17:17Z",
		"Timestamp": "2007-12-31T23:00:01Z",
		"TimestampPtr": "2007-12-31T23:00:01Z",
		"Year": 2004,
		"YearPtr": 2004,
		"Char": "char",
		"CharPtr": "char",
		"Varchar": "varchar",
		"VarcharPtr": "varchar",
		"Binary": "YmluYXJ5AAAAAAAAAAAAAAAAAAA=",
		"BinaryPtr": "YmluYXJ5AAAAAAAAAAAAAAAAAAA=",
		"VarBinary": "dmFyYmluYXJ5",
		"VarBinaryPtr": "dmFyYmluYXJ5",
		"Blob": "YmxvYg==",
		"BlobPtr": "YmxvYg==",
		"Text": "text",
		"TextPtr": "text",
		"Enum": "value1",
		"EnumPtr": "value1",
		"Set": "s1",
		"SetPtr": "s2",
		"JSON": "{\"key1\": \"value1\", \"key2\": \"value2\"}",
		"JSONPtr": "{\"key1\": \"value1\", \"key2\": \"value2\"}"
	},
	{
		"TinyInt": -3,
		"UtinyInt": 3,
		"SmallInt": -14,
		"UsmallInt": 14,
		"MediumInt": -150,
		"UmediumInt": 150,
		"Int": -1600,
		"Uint": 1600,
		"BigInt": -17000,
		"UbigInt": 17000,
		"TinyIntPtr": null,
		"UtinyIntPtr": null,
		"SmallIntPtr": null,
		"UsmallIntPtr": null,
		"MediumIntPtr": null,
		"UmediumIntPtr": null,
		"IntPtr": null,
		"UintPtr": null,
		"BigIntPtr": null,
		"UbigIntPtr": null,
		"Decimal": 1.11,
		"DecimalPtr": null,
		"Numeric": 2.22,
		"NumericPtr": null,
		"Float": 3.33,
		"FloatPtr": null,
		"Double": 4.44,
		"DoublePtr": null,
		"Bit": "\u0000\u0003",
		"BitPtr": null,
		"Date": "2008-07-04T00:00:00Z",
		"DatePtr": null,
		"DateTime": "2011-12-18T13:17:17Z",
		"DateTimePtr": null,
		"Timestamp": "2007-12-31T23:00:01Z",
		"TimestampPtr": null,
		"Year": 2004,
		"YearPtr": null,
		"Char": "char",
		"CharPtr": null,
		"Varchar": "varchar",
		"VarcharPtr": null,
		"Binary": "YmluYXJ5AAAAAAAAAAAAAAAAAAA=",
		"BinaryPtr": null,
		"VarBinary": "dmFyYmluYXJ5",
		"VarBinaryPtr": null,
		"Blob": "YmxvYg==",
		"BlobPtr": null,
		"Text": "text",
		"TextPtr": null,
		"Enum": "value1",
		"EnumPtr": null,
		"Set": "s1",
		"SetPtr": null,
		"JSON": "{\"key1\": \"value1\", \"key2\": \"value2\"}",
		"JSONPtr": null
	}
]
`

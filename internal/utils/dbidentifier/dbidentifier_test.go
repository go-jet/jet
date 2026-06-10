package dbidentifier

import (
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

func TestToGoIdentifier(t *testing.T) {
	require.Equal(t, ToGoIdentifier(""), "")
	require.Equal(t, ToGoIdentifier("uuid"), "UUID")
	require.Equal(t, ToGoIdentifier("uuid_ptr"), "UUIDPtr")
	require.Equal(t, ToGoIdentifier("col1"), "Col1")
	require.Equal(t, ToGoIdentifier("PG-13"), "Pg13")
	require.Equal(t, ToGoIdentifier("13_pg"), "13Pg")

	require.Equal(t, ToGoIdentifier("mytable"), "Mytable")
	require.Equal(t, ToGoIdentifier("MYTABLE"), "Mytable")
	require.Equal(t, ToGoIdentifier("MyTaBlE"), "MyTaBlE")
	require.Equal(t, ToGoIdentifier("myTaBlE"), "MyTaBlE")

	require.Equal(t, ToGoIdentifier("my_table"), "MyTable")
	require.Equal(t, ToGoIdentifier("my_____table"), "MyTable")
	require.Equal(t, ToGoIdentifier("MY_TABLE"), "MyTable")
	require.Equal(t, ToGoIdentifier("My_Table"), "MyTable")
	require.Equal(t, ToGoIdentifier("My Table"), "MyTable")
	require.Equal(t, ToGoIdentifier("My-Table"), "MyTable")

	require.Equal(t, ToGoIdentifier("EN\bUM"), "Enum")         // control character
	require.Equal(t, ToGoIdentifier("EN\tUM"), "EnUm")         // space character
	require.Equal(t, ToGoIdentifier("S3:INIT"), "S3ColonInit") // replacement chars
	require.Equal(t, ToGoIdentifier("Entity-"), "Entity")
	require.Equal(t, ToGoIdentifier("Entity+"), "EntityPlus")
	require.Equal(t, ToGoIdentifier("="), "Equal")
	require.Equal(t, ToGoIdentifier("<="), "LessEqual")
	require.Equal(t, ToGoIdentifier(">="), "GreaterEqual")
	require.Equal(t, ToGoIdentifier("some#$%name"), "SomeNumberDollarPercentName")
	require.Equal(t, ToGoIdentifier(`An!"them`), "AnExclamationQuotationThem")
	require.Equal(t, ToGoIdentifier(`An(Um)`),
		"AnOpeningParenthesesUmClosingParentheses")
}

func TestNeedsCharReplacement(t *testing.T) {
	increase, needs := needsCharReplacement("some_name")
	require.False(t, needs)
	require.Zero(t, increase)

	increase, needs = needsCharReplacement("some  name")
	require.True(t, needs)
	require.Zero(t, increase)

	increase, needs = needsCharReplacement("some\bname")
	require.True(t, needs)
	require.Equal(t, increase, -1)

	increase, needs = needsCharReplacement("some#$%name")
	require.True(t, needs)
	require.Equal(t, increase, 22)
}

func TestToGoFileName(t *testing.T) {
	require.Equal(t, ToGoFileName("FileName"), "filename")
	require.Equal(t, ToGoFileName("File_Name"), "file_name")
	require.Equal(t, ToGoFileName("File___Name__"), "file___name__")
	require.Equal(t, ToGoFileName("File___Name__"), "file___name__")
	require.Equal(t, ToGoFileName("File\bName"), "filename")
	require.Equal(t, ToGoFileName("File\tName"), "file_name")
	require.Equal(t, ToGoFileName("File^^Name"), "file_caret__caret_name")
}

func TestGetStructFieldForColumn(t *testing.T) {
	value := reflect.ValueOf(struct {
		FooID            int
		Bar              int
		BazId            int `column:"baz_id"`
		CustomINITIALISM int `column:"custom_initialism"`
	}{1, 2, 3, 4})

	require.Equal(t, int64(1), GetStructFieldForColumn(value, "foo_id").Int())
	require.Equal(t, int64(2), GetStructFieldForColumn(value, "bar").Int())
	require.Equal(t, int64(3), GetStructFieldForColumn(value, "baz_id").Int())
	require.Equal(t, int64(4), GetStructFieldForColumn(value, "custom_initialism").Int())
	require.Panics(t, func() { GetStructFieldForColumn(value, "wrong_property") })
}

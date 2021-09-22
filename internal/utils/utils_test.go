package utils

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestToGoIdentifier(t *testing.T) {
	require.Equal(t, ToGoIdentifier(""), "")
	require.Equal(t, ToGoIdentifier("uuid"), "UUID")
	require.Equal(t, ToGoIdentifier("col1"), "Col1")
	require.Equal(t, ToGoIdentifier("PG-13"), "Pg13")
	require.Equal(t, ToGoIdentifier("13_pg"), "13Pg")

	require.Equal(t, ToGoIdentifier("mytable"), "Mytable")
	require.Equal(t, ToGoIdentifier("MYTABLE"), "Mytable")
	require.Equal(t, ToGoIdentifier("MyTaBlE"), "MyTaBlE")
	require.Equal(t, ToGoIdentifier("myTaBlE"), "MyTaBlE")

	require.Equal(t, ToGoIdentifier("my_table"), "MyTable")
	require.Equal(t, ToGoIdentifier("MY_TABLE"), "MyTable")
	require.Equal(t, ToGoIdentifier("My_Table"), "MyTable")
	require.Equal(t, ToGoIdentifier("My Table"), "MyTable")
	require.Equal(t, ToGoIdentifier("My-Table"), "MyTable")
}

func TestErrorCatchErr(t *testing.T) {
	var err error

	func() {
		defer ErrorCatch(&err)

		panic(fmt.Errorf("newError"))
	}()

	require.Error(t, err, "newError")
}

func TestErrorCatchNonErr(t *testing.T) {
	var err error

	func() {
		defer ErrorCatch(&err)

		panic(11)
	}()

	require.Error(t, err, "11")
}

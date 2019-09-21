package utils

import (
	"fmt"
	"gotest.tools/assert"
	"testing"
)

func TestToGoIdentifier(t *testing.T) {
	assert.Equal(t, ToGoIdentifier(""), "")
	assert.Equal(t, ToGoIdentifier("uuid"), "UUID")
	assert.Equal(t, ToGoIdentifier("col1"), "Col1")
	assert.Equal(t, ToGoIdentifier("PG-13"), "Pg13")
	assert.Equal(t, ToGoIdentifier("13_pg"), "13Pg")

	assert.Equal(t, ToGoIdentifier("mytable"), "Mytable")
	assert.Equal(t, ToGoIdentifier("MYTABLE"), "Mytable")
	assert.Equal(t, ToGoIdentifier("MyTaBlE"), "MyTaBlE")
	assert.Equal(t, ToGoIdentifier("myTaBlE"), "MyTaBlE")

	assert.Equal(t, ToGoIdentifier("my_table"), "MyTable")
	assert.Equal(t, ToGoIdentifier("MY_TABLE"), "MyTable")
	assert.Equal(t, ToGoIdentifier("My_Table"), "MyTable")
	assert.Equal(t, ToGoIdentifier("My Table"), "MyTable")
	assert.Equal(t, ToGoIdentifier("My-Table"), "MyTable")
}

func TestErrorCatchErr(t *testing.T) {
	var err error

	func() {
		defer ErrorCatch(&err)

		panic(fmt.Errorf("newError"))
	}()

	assert.Error(t, err, "newError")
}

func TestErrorCatchNonErr(t *testing.T) {
	var err error

	func() {
		defer ErrorCatch(&err)

		panic(11)
	}()

	assert.Error(t, err, "11")
}

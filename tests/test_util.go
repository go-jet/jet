package tests

import (
	"github.com/go-jet/jet/sqlbuilder"
	"github.com/go-jet/jet/tests/.test_files/dvd_rental/dvds/model"
	"github.com/google/uuid"
	"gotest.tools/assert"
	"strings"
	"testing"
	"time"
)

func assertQuery(t *testing.T, query sqlbuilder.Statement, expectedQuery string, expectedArgs ...interface{}) {
	_, args, err := query.Sql()
	assert.NilError(t, err)
	//assert.Equal(t, queryStr, expectedQuery)
	assert.DeepEqual(t, args, expectedArgs)

	debuqSql, err := query.DebugSql()
	assert.NilError(t, err)
	assert.Equal(t, debuqSql, expectedQuery)
}

func boolPtr(b bool) *bool {
	return &b
}

func int16Ptr(i int16) *int16 {
	return &i
}

func int32Ptr(i int32) *int32 {
	return &i
}

func int64Ptr(i int64) *int64 {
	return &i
}

func stringPtr(s string) *string {
	return &s
}

func byteArrayPtr(arr []byte) *[]byte {
	return &arr
}

func float32Ptr(f float32) *float32 {
	return &f
}
func float64Ptr(f float64) *float64 {
	return &f
}

func uuidPtr(u string) *uuid.UUID {
	uuid := uuid.MustParse(u)

	return &uuid
}

func timeWithoutTimeZone(t string) *time.Time {
	time, err := time.Parse("15:04:05", t)

	if err != nil {
		panic(err)
	}

	return &time
}

func timeWithTimeZone(t string) *time.Time {
	time, err := time.Parse("15:04:05 -0700", t)

	if err != nil {
		panic(err)
	}

	return &time
}

func timestampWithoutTimeZone(t string, precision int) *time.Time {

	precisionStr := ""

	if precision > 0 {
		precisionStr = "." + strings.Repeat("9", precision)
	}

	time, err := time.Parse("2006-01-02 15:04:05"+precisionStr+" +0000", t+" +0000")

	if err != nil {
		panic(err)
	}

	return &time
}

func timestampWithTimeZone(t string, precision int) *time.Time {

	precisionStr := ""

	if precision > 0 {
		precisionStr = "." + strings.Repeat("9", precision)
	}

	time, err := time.Parse("2006-01-02 15:04:05"+precisionStr+" -0700 MST", t)

	if err != nil {
		panic(err)
	}

	return &time
}

func M3(a, b, c interface{}) []interface{} {
	return []interface{}{a, b, c}
}

var customer0 = model.Customer{
	CustomerID: 1,
	StoreID:    1,
	FirstName:  "Mary",
	LastName:   "Smith",
	Email:      stringPtr("mary.smith@sakilacustomer.org"),
	AddressID:  5,
	Activebool: true,
	CreateDate: *timestampWithoutTimeZone("2006-02-14 00:00:00", 0),
	LastUpdate: timestampWithoutTimeZone("2013-05-26 14:49:45.738", 3),
	Active:     int32Ptr(1),
}

var customer1 = model.Customer{
	CustomerID: 2,
	StoreID:    1,
	FirstName:  "Patricia",
	LastName:   "Johnson",
	Email:      stringPtr("patricia.johnson@sakilacustomer.org"),
	AddressID:  6,
	Activebool: true,
	CreateDate: *timestampWithoutTimeZone("2006-02-14 00:00:00", 0),
	LastUpdate: timestampWithoutTimeZone("2013-05-26 14:49:45.738", 3),
	Active:     int32Ptr(1),
}

var lastCustomer = model.Customer{
	CustomerID: 599,
	StoreID:    2,
	FirstName:  "Austin",
	LastName:   "Cintron",
	Email:      stringPtr("austin.cintron@sakilacustomer.org"),
	AddressID:  605,
	Activebool: true,
	CreateDate: *timestampWithoutTimeZone("2006-02-14 00:00:00", 0),
	LastUpdate: timestampWithoutTimeZone("2013-05-26 14:49:45.738", 3),
	Active:     int32Ptr(1),
}

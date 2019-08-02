package postgres

import (
	"github.com/go-jet/jet"
	"github.com/go-jet/jet/internal/testutils"
	"github.com/go-jet/jet/tests/.gentestdata/jetdb/dvds/model"
	"github.com/google/uuid"
	"gotest.tools/assert"
	"testing"
)

func AssertExec(t *testing.T, stmt jet.Statement, rowsAffected int64) {
	res, err := stmt.Exec(db)

	assert.NilError(t, err)
	rows, err := res.RowsAffected()
	assert.NilError(t, err)
	assert.Equal(t, rows, rowsAffected)
}

func assertExecErr(t *testing.T, stmt jet.Statement, errorStr string) {
	_, err := stmt.Exec(db)

	assert.Error(t, err, errorStr)
}
func BoolPtr(b bool) *bool {
	return &b
}

func Int16Ptr(i int16) *int16 {
	return &i
}

func Int32Ptr(i int32) *int32 {
	return &i
}

func Int64Ptr(i int64) *int64 {
	return &i
}

func StringPtr(s string) *string {
	return &s
}

func ByteArrayPtr(arr []byte) *[]byte {
	return &arr
}

func Float32Ptr(f float32) *float32 {
	return &f
}
func Float64Ptr(f float64) *float64 {
	return &f
}

func UUIDPtr(u string) *uuid.UUID {
	newUUID := uuid.MustParse(u)

	return &newUUID
}

var customer0 = model.Customer{
	CustomerID: 1,
	StoreID:    1,
	FirstName:  "Mary",
	LastName:   "Smith",
	Email:      StringPtr("mary.smith@sakilacustomer.org"),
	AddressID:  5,
	Activebool: true,
	CreateDate: *testutils.TimestampWithoutTimeZone("2006-02-14 00:00:00", 0),
	LastUpdate: testutils.TimestampWithoutTimeZone("2013-05-26 14:49:45.738", 3),
	Active:     Int32Ptr(1),
}

var customer1 = model.Customer{
	CustomerID: 2,
	StoreID:    1,
	FirstName:  "Patricia",
	LastName:   "Johnson",
	Email:      StringPtr("patricia.johnson@sakilacustomer.org"),
	AddressID:  6,
	Activebool: true,
	CreateDate: *testutils.TimestampWithoutTimeZone("2006-02-14 00:00:00", 0),
	LastUpdate: testutils.TimestampWithoutTimeZone("2013-05-26 14:49:45.738", 3),
	Active:     Int32Ptr(1),
}

var lastCustomer = model.Customer{
	CustomerID: 599,
	StoreID:    2,
	FirstName:  "Austin",
	LastName:   "Cintron",
	Email:      StringPtr("austin.cintron@sakilacustomer.org"),
	AddressID:  605,
	Activebool: true,
	CreateDate: *testutils.TimestampWithoutTimeZone("2006-02-14 00:00:00", 0),
	LastUpdate: testutils.TimestampWithoutTimeZone("2013-05-26 14:49:45.738", 3),
	Active:     Int32Ptr(1),
}

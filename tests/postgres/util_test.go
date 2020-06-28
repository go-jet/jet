package postgres

import (
	"github.com/go-jet/jet/v2/internal/jet"
	"github.com/go-jet/jet/v2/internal/testutils"
	"github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/dvds/model"
	"github.com/stretchr/testify/require"
	"testing"
)

func AssertExec(t *testing.T, stmt jet.Statement, rowsAffected int64) {
	res, err := stmt.Exec(db)

	require.NoError(t, err)
	rows, err := res.RowsAffected()
	require.NoError(t, err)
	require.Equal(t, rows, rowsAffected)
}

var customer0 = model.Customer{
	CustomerID: 1,
	StoreID:    1,
	FirstName:  "Mary",
	LastName:   "Smith",
	Email:      testutils.StringPtr("mary.smith@sakilacustomer.org"),
	AddressID:  5,
	Activebool: true,
	CreateDate: *testutils.TimestampWithoutTimeZone("2006-02-14 00:00:00", 0),
	LastUpdate: testutils.TimestampWithoutTimeZone("2013-05-26 14:49:45.738", 3),
	Active:     testutils.Int32Ptr(1),
}

var customer1 = model.Customer{
	CustomerID: 2,
	StoreID:    1,
	FirstName:  "Patricia",
	LastName:   "Johnson",
	Email:      testutils.StringPtr("patricia.johnson@sakilacustomer.org"),
	AddressID:  6,
	Activebool: true,
	CreateDate: *testutils.TimestampWithoutTimeZone("2006-02-14 00:00:00", 0),
	LastUpdate: testutils.TimestampWithoutTimeZone("2013-05-26 14:49:45.738", 3),
	Active:     testutils.Int32Ptr(1),
}

var lastCustomer = model.Customer{
	CustomerID: 599,
	StoreID:    2,
	FirstName:  "Austin",
	LastName:   "Cintron",
	Email:      testutils.StringPtr("austin.cintron@sakilacustomer.org"),
	AddressID:  605,
	Activebool: true,
	CreateDate: *testutils.TimestampWithoutTimeZone("2006-02-14 00:00:00", 0),
	LastUpdate: testutils.TimestampWithoutTimeZone("2013-05-26 14:49:45.738", 3),
	Active:     testutils.Int32Ptr(1),
}

//
// Code generated by go-jet DO NOT EDIT.
//
// WARNING: Changes to this file may cause incorrect behavior
// and will be lost if the code is regenerated
//

package view

import (
	"github.com/go-jet/jet/v2/postgres"
)

var CustomerList = newCustomerListTable("dvds", "customer_list", "")

type customerListTable struct {
	postgres.Table

	//Columns
	ID      postgres.ColumnInteger
	Name    postgres.ColumnString
	Address postgres.ColumnString
	ZipCode postgres.ColumnString
	Phone   postgres.ColumnString
	City    postgres.ColumnString
	Country postgres.ColumnString
	Notes   postgres.ColumnString
	Sid     postgres.ColumnInteger

	AllColumns     postgres.ColumnList
	MutableColumns postgres.ColumnList
}

type CustomerListTable struct {
	customerListTable

	EXCLUDED customerListTable
}

// AS creates new CustomerListTable with assigned alias
func (a CustomerListTable) AS(alias string) *CustomerListTable {
	return newCustomerListTable(a.SchemaName(), a.TableName(), alias)
}

// Schema creates new CustomerListTable with assigned schema name
func (a CustomerListTable) FromSchema(schemaName string) *CustomerListTable {
	return newCustomerListTable(schemaName, a.TableName(), a.Alias())
}

func newCustomerListTable(schemaName, tableName, alias string) *CustomerListTable {
	return &CustomerListTable{
		customerListTable: newCustomerListTableImpl(schemaName, tableName, alias),
		EXCLUDED:          newCustomerListTableImpl("", "excluded", ""),
	}
}

func newCustomerListTableImpl(schemaName, tableName, alias string) customerListTable {
	var (
		IDColumn       = postgres.IntegerColumn("id")
		NameColumn     = postgres.StringColumn("name")
		AddressColumn  = postgres.StringColumn("address")
		ZipCodeColumn  = postgres.StringColumn("zip code")
		PhoneColumn    = postgres.StringColumn("phone")
		CityColumn     = postgres.StringColumn("city")
		CountryColumn  = postgres.StringColumn("country")
		NotesColumn    = postgres.StringColumn("notes")
		SidColumn      = postgres.IntegerColumn("sid")
		allColumns     = postgres.ColumnList{IDColumn, NameColumn, AddressColumn, ZipCodeColumn, PhoneColumn, CityColumn, CountryColumn, NotesColumn, SidColumn}
		mutableColumns = postgres.ColumnList{IDColumn, NameColumn, AddressColumn, ZipCodeColumn, PhoneColumn, CityColumn, CountryColumn, NotesColumn, SidColumn}
	)

	return customerListTable{
		Table: postgres.NewTable(schemaName, tableName, alias, allColumns...),

		//Columns
		ID:      IDColumn,
		Name:    NameColumn,
		Address: AddressColumn,
		ZipCode: ZipCodeColumn,
		Phone:   PhoneColumn,
		City:    CityColumn,
		Country: CountryColumn,
		Notes:   NotesColumn,
		Sid:     SidColumn,

		AllColumns:     allColumns,
		MutableColumns: mutableColumns,
	}
}

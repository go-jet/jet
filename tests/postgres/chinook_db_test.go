package postgres

import (
	"context"
	"github.com/go-jet/jet/v2/internal/testutils"
	. "github.com/go-jet/jet/v2/postgres"
	"github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/chinook/model"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/chinook/table"
	"github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/chinook2/table"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestSelect(t *testing.T) {
	stmt := SELECT(Album.AllColumns).
		FROM(Album).
		ORDER_BY(Album.AlbumId.ASC())

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT "Album"."AlbumId" AS "Album.AlbumId",
     "Album"."Title" AS "Album.Title",
     "Album"."ArtistId" AS "Album.ArtistId",
     "Album"."Type" AS "Album.Type"
FROM chinook."Album"
ORDER BY "Album"."AlbumId" ASC;
`)
	var dest []model.Album

	err := stmt.Query(db, &dest)

	require.NoError(t, err)
	require.Equal(t, len(dest), 347)
	testutils.AssertDeepEqual(t, dest[0], album1)
	testutils.AssertDeepEqual(t, dest[1], album2)
	testutils.AssertDeepEqual(t, dest[len(dest)-1], album347)
	requireLogged(t, stmt)
	requireQueryLogged(t, stmt, 347)
}

func TestComplex_AND_OR(t *testing.T) {
	stmt := SELECT(
		Artist.AllColumns,
		Album.AllColumns,
		Track.AllColumns,
	).FROM(
		Artist.
			LEFT_JOIN(Album, Artist.ArtistId.EQ(Album.ArtistId)).
			LEFT_JOIN(Track, Track.AlbumId.EQ(Album.AlbumId)),
	).WHERE(
		AND(
			Artist.ArtistId.BETWEEN(Int(5), Int(11)),
			Album.AlbumId.GT_EQ(Int(7)),
			Track.TrackId.GT(Int(74)),
			OR(
				Track.GenreId.EQ(Int(2)),
				Track.UnitPrice.GT(Float(1.01)),
			),
			Track.TrackId.LT(Int(125)),
		),
	).ORDER_BY(
		Artist.ArtistId,
		Album.AlbumId,
		Track.TrackId,
	)

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT "Artist"."ArtistId" AS "Artist.ArtistId",
     "Artist"."Name" AS "Artist.Name",
     "Album"."AlbumId" AS "Album.AlbumId",
     "Album"."Title" AS "Album.Title",
     "Album"."ArtistId" AS "Album.ArtistId",
     "Album"."Type" AS "Album.Type",
     "Track"."TrackId" AS "Track.TrackId",
     "Track"."Name" AS "Track.Name",
     "Track"."AlbumId" AS "Track.AlbumId",
     "Track"."MediaTypeId" AS "Track.MediaTypeId",
     "Track"."GenreId" AS "Track.GenreId",
     "Track"."Composer" AS "Track.Composer",
     "Track"."Milliseconds" AS "Track.Milliseconds",
     "Track"."Bytes" AS "Track.Bytes",
     "Track"."UnitPrice" AS "Track.UnitPrice"
FROM chinook."Artist"
     LEFT JOIN chinook."Album" ON ("Artist"."ArtistId" = "Album"."ArtistId")
     LEFT JOIN chinook."Track" ON ("Track"."AlbumId" = "Album"."AlbumId")
WHERE (
          ("Artist"."ArtistId" BETWEEN 5 AND 11)
              AND ("Album"."AlbumId" >= 7)
              AND ("Track"."TrackId" > 74)
              AND (
                      ("Track"."GenreId" = 2)
                          OR ("Track"."UnitPrice" > 1.01)
                  )
              AND ("Track"."TrackId" < 125)
      )
ORDER BY "Artist"."ArtistId", "Album"."AlbumId", "Track"."TrackId";
`)

	var dest []struct {
		model.Artist

		Albums []struct {
			model.Album

			Tracks []model.Track
		}
	}

	err := stmt.Query(db, &dest)
	require.NoError(t, err)

	testutils.AssertJSON(t, dest, `
[
	{
		"ArtistId": 6,
		"Name": "Ant�nio Carlos Jobim",
		"Albums": [
			{
				"AlbumId": 8,
				"Title": "Warner 25 Anos",
				"ArtistId": 6,
				"Type": "Studio",
				"Tracks": [
					{
						"TrackId": 75,
						"Name": "O Boto (B�to)",
						"AlbumId": 8,
						"MediaTypeId": 1,
						"GenreId": 2,
						"Composer": null,
						"Milliseconds": 366837,
						"Bytes": 12089673,
						"UnitPrice": 0.99
					},
					{
						"TrackId": 76,
						"Name": "Canta, Canta Mais",
						"AlbumId": 8,
						"MediaTypeId": 1,
						"GenreId": 2,
						"Composer": null,
						"Milliseconds": 271856,
						"Bytes": 8719426,
						"UnitPrice": 0.99
					}
				]
			}
		]
	},
	{
		"ArtistId": 10,
		"Name": "Billy Cobham",
		"Albums": [
			{
				"AlbumId": 13,
				"Title": "The Best Of Billy Cobham",
				"ArtistId": 10,
				"Type": "Studio",
				"Tracks": [
					{
						"TrackId": 123,
						"Name": "Quadrant",
						"AlbumId": 13,
						"MediaTypeId": 1,
						"GenreId": 2,
						"Composer": "Billy Cobham",
						"Milliseconds": 261851,
						"Bytes": 8538199,
						"UnitPrice": 0.99
					},
					{
						"TrackId": 124,
						"Name": "Snoopy's search-Red baron",
						"AlbumId": 13,
						"MediaTypeId": 1,
						"GenreId": 2,
						"Composer": "Billy Cobham",
						"Milliseconds": 456071,
						"Bytes": 15075616,
						"UnitPrice": 0.99
					}
				]
			}
		]
	}
]
`)
}

func TestJoinEverything(t *testing.T) {

	manager := Employee.AS("Manager")

	stmt := SELECT(
		Artist.AllColumns,
		Album.AllColumns,
		Track.AllColumns,
		Genre.AllColumns,
		MediaType.AllColumns,
		PlaylistTrack.AllColumns,
		Playlist.AllColumns,
		Invoice.AllColumns,
		Customer.AllColumns,
		Employee.AllColumns,
		manager.AllColumns,
	).FROM(
		Artist.
			LEFT_JOIN(Album, Artist.ArtistId.EQ(Album.ArtistId)).
			LEFT_JOIN(Track, Track.AlbumId.EQ(Album.AlbumId)).
			LEFT_JOIN(Genre, Genre.GenreId.EQ(Track.GenreId)).
			LEFT_JOIN(MediaType, MediaType.MediaTypeId.EQ(Track.MediaTypeId)).
			LEFT_JOIN(PlaylistTrack, PlaylistTrack.TrackId.EQ(Track.TrackId)).
			LEFT_JOIN(Playlist, Playlist.PlaylistId.EQ(PlaylistTrack.PlaylistId)).
			LEFT_JOIN(InvoiceLine, InvoiceLine.TrackId.EQ(Track.TrackId)).
			LEFT_JOIN(Invoice, Invoice.InvoiceId.EQ(InvoiceLine.InvoiceId)).
			LEFT_JOIN(Customer, Customer.CustomerId.EQ(Invoice.CustomerId)).
			LEFT_JOIN(Employee, Employee.EmployeeId.EQ(Customer.SupportRepId)).
			LEFT_JOIN(manager, manager.EmployeeId.EQ(Employee.ReportsTo)),
	).ORDER_BY(
		Artist.ArtistId, Album.AlbumId, Track.TrackId,
		Genre.GenreId, MediaType.MediaTypeId, Playlist.PlaylistId,
		Invoice.InvoiceId, Customer.CustomerId,
	)

	var dest []struct { //list of all artist
		model.Artist

		Albums []struct { // list of albums per artist
			model.Album

			Tracks []struct { // list of tracks per album
				model.Track

				Genre     model.Genre     // track genre
				MediaType model.MediaType // track media type

				Playlists []model.Playlist // list of playlist where track is used

				Invoices []struct { // list of invoices where track occurs
					model.Invoice

					Customer struct { // customer data for invoice
						model.Customer

						Employee *struct { // employee data for customer if exists
							model.Employee

							Manager *model.Employee `alias:"Manager"`
						}
					}
				}
			}
		}
	}

	testutils.AssertStatementSql(t, stmt, `
SELECT "Artist"."ArtistId" AS "Artist.ArtistId",
     "Artist"."Name" AS "Artist.Name",
     "Album"."AlbumId" AS "Album.AlbumId",
     "Album"."Title" AS "Album.Title",
     "Album"."ArtistId" AS "Album.ArtistId",
     "Album"."Type" AS "Album.Type",
     "Track"."TrackId" AS "Track.TrackId",
     "Track"."Name" AS "Track.Name",
     "Track"."AlbumId" AS "Track.AlbumId",
     "Track"."MediaTypeId" AS "Track.MediaTypeId",
     "Track"."GenreId" AS "Track.GenreId",
     "Track"."Composer" AS "Track.Composer",
     "Track"."Milliseconds" AS "Track.Milliseconds",
     "Track"."Bytes" AS "Track.Bytes",
     "Track"."UnitPrice" AS "Track.UnitPrice",
     "Genre"."GenreId" AS "Genre.GenreId",
     "Genre"."Name" AS "Genre.Name",
     "MediaType"."MediaTypeId" AS "MediaType.MediaTypeId",
     "MediaType"."Name" AS "MediaType.Name",
     "PlaylistTrack"."PlaylistId" AS "PlaylistTrack.PlaylistId",
     "PlaylistTrack"."TrackId" AS "PlaylistTrack.TrackId",
     "Playlist"."PlaylistId" AS "Playlist.PlaylistId",
     "Playlist"."Name" AS "Playlist.Name",
     "Invoice"."InvoiceId" AS "Invoice.InvoiceId",
     "Invoice"."CustomerId" AS "Invoice.CustomerId",
     "Invoice"."InvoiceDate" AS "Invoice.InvoiceDate",
     "Invoice"."BillingAddress" AS "Invoice.BillingAddress",
     "Invoice"."BillingCity" AS "Invoice.BillingCity",
     "Invoice"."BillingState" AS "Invoice.BillingState",
     "Invoice"."BillingCountry" AS "Invoice.BillingCountry",
     "Invoice"."BillingPostalCode" AS "Invoice.BillingPostalCode",
     "Invoice"."Total" AS "Invoice.Total",
     "Customer"."CustomerId" AS "Customer.CustomerId",
     "Customer"."FirstName" AS "Customer.FirstName",
     "Customer"."LastName" AS "Customer.LastName",
     "Customer"."Company" AS "Customer.Company",
     "Customer"."Address" AS "Customer.Address",
     "Customer"."City" AS "Customer.City",
     "Customer"."State" AS "Customer.State",
     "Customer"."Country" AS "Customer.Country",
     "Customer"."PostalCode" AS "Customer.PostalCode",
     "Customer"."Phone" AS "Customer.Phone",
     "Customer"."Fax" AS "Customer.Fax",
     "Customer"."Email" AS "Customer.Email",
     "Customer"."SupportRepId" AS "Customer.SupportRepId",
     "Employee"."EmployeeId" AS "Employee.EmployeeId",
     "Employee"."LastName" AS "Employee.LastName",
     "Employee"."FirstName" AS "Employee.FirstName",
     "Employee"."Title" AS "Employee.Title",
     "Employee"."ReportsTo" AS "Employee.ReportsTo",
     "Employee"."BirthDate" AS "Employee.BirthDate",
     "Employee"."HireDate" AS "Employee.HireDate",
     "Employee"."Address" AS "Employee.Address",
     "Employee"."City" AS "Employee.City",
     "Employee"."State" AS "Employee.State",
     "Employee"."Country" AS "Employee.Country",
     "Employee"."PostalCode" AS "Employee.PostalCode",
     "Employee"."Phone" AS "Employee.Phone",
     "Employee"."Fax" AS "Employee.Fax",
     "Employee"."Email" AS "Employee.Email",
     "Manager"."EmployeeId" AS "Manager.EmployeeId",
     "Manager"."LastName" AS "Manager.LastName",
     "Manager"."FirstName" AS "Manager.FirstName",
     "Manager"."Title" AS "Manager.Title",
     "Manager"."ReportsTo" AS "Manager.ReportsTo",
     "Manager"."BirthDate" AS "Manager.BirthDate",
     "Manager"."HireDate" AS "Manager.HireDate",
     "Manager"."Address" AS "Manager.Address",
     "Manager"."City" AS "Manager.City",
     "Manager"."State" AS "Manager.State",
     "Manager"."Country" AS "Manager.Country",
     "Manager"."PostalCode" AS "Manager.PostalCode",
     "Manager"."Phone" AS "Manager.Phone",
     "Manager"."Fax" AS "Manager.Fax",
     "Manager"."Email" AS "Manager.Email"
FROM chinook."Artist"
     LEFT JOIN chinook."Album" ON ("Artist"."ArtistId" = "Album"."ArtistId")
     LEFT JOIN chinook."Track" ON ("Track"."AlbumId" = "Album"."AlbumId")
     LEFT JOIN chinook."Genre" ON ("Genre"."GenreId" = "Track"."GenreId")
     LEFT JOIN chinook."MediaType" ON ("MediaType"."MediaTypeId" = "Track"."MediaTypeId")
     LEFT JOIN chinook."PlaylistTrack" ON ("PlaylistTrack"."TrackId" = "Track"."TrackId")
     LEFT JOIN chinook."Playlist" ON ("Playlist"."PlaylistId" = "PlaylistTrack"."PlaylistId")
     LEFT JOIN chinook."InvoiceLine" ON ("InvoiceLine"."TrackId" = "Track"."TrackId")
     LEFT JOIN chinook."Invoice" ON ("Invoice"."InvoiceId" = "InvoiceLine"."InvoiceId")
     LEFT JOIN chinook."Customer" ON ("Customer"."CustomerId" = "Invoice"."CustomerId")
     LEFT JOIN chinook."Employee" ON ("Employee"."EmployeeId" = "Customer"."SupportRepId")
     LEFT JOIN chinook."Employee" AS "Manager" ON ("Manager"."EmployeeId" = "Employee"."ReportsTo")
ORDER BY "Artist"."ArtistId", "Album"."AlbumId", "Track"."TrackId", "Genre"."GenreId", "MediaType"."MediaTypeId", "Playlist"."PlaylistId", "Invoice"."InvoiceId", "Customer"."CustomerId";
`)

	err := stmt.QueryContext(context.Background(), db, &dest)

	require.NoError(t, err)
	require.Equal(t, len(dest), 275)
	testutils.AssertJSONFile(t, dest, "./testdata/results/postgres/joined_everything.json")
	requireLogged(t, stmt)
	requireQueryLogged(t, stmt, 9423)
}

// default column aliases from sub-CTEs are bubbled up to the main query,
// cte name does not affect default column alias in main query
func TestSubQueryColumnAliasBubbling(t *testing.T) {
	subQuery1 := SELECT(
		Artist.AllColumns,
		String("custom_column_1").AS("custom_column_1"),
	).FROM(
		Artist,
	).ORDER_BY(
		Artist.ArtistId.ASC(),
	).AsTable("subQuery1")

	subQuery2 := SELECT(
		subQuery1.AllColumns(),
		String("custom_column_2").AS("custom_column_2"),
	).FROM(
		subQuery1,
	).AsTable("subQuery2")

	mainQuery := SELECT(
		subQuery2.AllColumns(),                 // columns will have the same alias as in the sub-query
		subQuery2.AllColumns().As("artist2.*"), // all column aliases will be changed to artist2.*
		subQuery2.AllColumns().Except(Artist.Name).As("artist3.*"),
		subQuery2.AllColumns().Except(
			Artist.MutableColumns,
			StringColumn("custom_column_1").From(subQuery2), // custom_column_1 appears with the same alias in subQuery2
			StringColumn("custom_column_2").From(subQuery2),
		).As("artist4.*"),
	).FROM(
		subQuery2,
	)

	// fmt.Println(mainQuery.Sql())

	testutils.AssertStatementSql(t, mainQuery, `
SELECT "subQuery2"."Artist.ArtistId" AS "Artist.ArtistId",
     "subQuery2"."Artist.Name" AS "Artist.Name",
     "subQuery2".custom_column_1 AS "custom_column_1",
     "subQuery2".custom_column_2 AS "custom_column_2",
     "subQuery2"."Artist.ArtistId" AS "artist2.ArtistId",
     "subQuery2"."Artist.Name" AS "artist2.Name",
     "subQuery2".custom_column_1 AS "artist2.custom_column_1",
     "subQuery2".custom_column_2 AS "artist2.custom_column_2",
     "subQuery2"."Artist.ArtistId" AS "artist3.ArtistId",
     "subQuery2".custom_column_1 AS "artist3.custom_column_1",
     "subQuery2".custom_column_2 AS "artist3.custom_column_2",
     "subQuery2"."Artist.ArtistId" AS "artist4.ArtistId"
FROM (
          SELECT "subQuery1"."Artist.ArtistId" AS "Artist.ArtistId",
               "subQuery1"."Artist.Name" AS "Artist.Name",
               "subQuery1".custom_column_1 AS "custom_column_1",
               $1::text AS "custom_column_2"
          FROM (
                    SELECT "Artist"."ArtistId" AS "Artist.ArtistId",
                         "Artist"."Name" AS "Artist.Name",
                         $2::text AS "custom_column_1"
                    FROM chinook."Artist"
                    ORDER BY "Artist"."ArtistId" ASC
               ) AS "subQuery1"
     ) AS "subQuery2";
`)
	var dest []struct {
		// subQuery2.AllColumns()
		Artist1 struct {
			model.Artist

			CustomColumn1 string
			CustomColumn2 string
		}

		// subQuery2.AllColumns().As("artist2.*")
		Artist2 struct {
			model.Artist `alias:"artist2.*"`

			CustomColumn1 string
			CustomColumn2 string
		} `alias:"artist2.*"`

		// subQuery2.AllColumns().Except(Artist.Name).As("artist3.*")
		Artist3 struct {
			model.Artist `alias:"artist3.*"`

			CustomColumn1 string
			CustomColumn2 string
		} `alias:"artist3.*"`

		// subQuery2.AllColumns().Except(...).As("artist4.*")
		Artist4 struct {
			model.Artist `alias:"artist4.*"`

			CustomColumn1 string
			CustomColumn2 string
		} `alias:"artist4.*"`
	}

	err := mainQuery.Query(db, &dest)
	require.NoError(t, err)

	// Artist1
	require.Len(t, dest, 275)
	require.Equal(t, dest[0].Artist1.Artist, model.Artist{
		ArtistId: 1,
		Name:     testutils.StringPtr("AC/DC"),
	})
	require.Equal(t, dest[0].Artist1.CustomColumn1, "custom_column_1")
	require.Equal(t, dest[0].Artist1.CustomColumn2, "custom_column_2")

	// Artist2
	require.Equal(t, testutils.ToJSON(dest[0].Artist1), testutils.ToJSON(dest[0].Artist2))

	// Artist3
	require.Equal(t, dest[0].Artist3.ArtistId, int32(1))
	require.Nil(t, dest[0].Artist3.Name)
	require.Equal(t, dest[0].Artist3.CustomColumn1, "custom_column_1")
	require.Equal(t, dest[0].Artist3.CustomColumn2, "custom_column_2")

	// Artist4
	require.Equal(t, dest[0].Artist3.Artist, dest[0].Artist4.Artist)
	require.Equal(t, dest[0].Artist4.CustomColumn1, "")
	require.Equal(t, dest[0].Artist4.CustomColumn2, "")
}

func TestUnAliasedNamesPanicError(t *testing.T) {
	subQuery1 := SELECT(
		Artist.AllColumns,
		Artist.Name.CONCAT(String("-musician")), //alias missing
	).FROM(
		Artist,
	).ORDER_BY(
		Artist.ArtistId.ASC(),
	).AsTable("subQuery1")

	require.Panics(t, func() {
		SELECT(
			subQuery1.AllColumns(), // panic, column not aliased
		).FROM(
			subQuery1,
		)
	}, "jet: can't export unaliased expression subQuery: subQuery1, expression: (\"Artist\".\"Name\" || '-musician')")
}

func TestProjectionListReAliasing(t *testing.T) {
	projectionList := ProjectionList{
		Track.GenreId,
		SUM(Track.Milliseconds).AS("duration"),
		MAX(Track.Milliseconds).AS("duration.max"),
	}

	stmt := SELECT(
		projectionList.As("genre_info"),
	).FROM(
		Track,
	).WHERE(
		Track.GenreId.LT(Int(5)),
	).GROUP_BY(
		Track.GenreId,
	).ORDER_BY(
		Track.GenreId,
	)

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT "Track"."GenreId" AS "genre_info.GenreId",
     SUM("Track"."Milliseconds") AS "genre_info.duration",
     MAX("Track"."Milliseconds") AS "genre_info.max"
FROM chinook."Track"
WHERE "Track"."GenreId" < 5
GROUP BY "Track"."GenreId"
ORDER BY "Track"."GenreId";
`)

	type GenreInfo struct {
		GenreID  string
		Duration int64
		Max      int64
	}

	var dest []GenreInfo

	err := stmt.Query(db, &dest)
	require.NoError(t, err)

	expectedSQL := `
[
	{
		"GenreID": "1",
		"Duration": 368231326,
		"Max": 1612329
	},
	{
		"GenreID": "2",
		"Duration": 37928199,
		"Max": 907520
	},
	{
		"GenreID": "3",
		"Duration": 115846292,
		"Max": 816509
	},
	{
		"GenreID": "4",
		"Duration": 77805478,
		"Max": 558602
	}
]
`
	testutils.AssertJSON(t, dest, expectedSQL)

	subQuery := stmt.AsTable("subQuery")

	mainStmt := SELECT(
		subQuery.AllColumns().As("genre_information.*"),
	).FROM(
		subQuery,
	)

	testutils.AssertDebugStatementSql(t, mainStmt, `
SELECT "subQuery"."genre_info.GenreId" AS "genre_information.GenreId",
     "subQuery"."genre_info.duration" AS "genre_information.duration",
     "subQuery"."genre_info.max" AS "genre_information.max"
FROM (
          SELECT "Track"."GenreId" AS "genre_info.GenreId",
               SUM("Track"."Milliseconds") AS "genre_info.duration",
               MAX("Track"."Milliseconds") AS "genre_info.max"
          FROM chinook."Track"
          WHERE "Track"."GenreId" < 5
          GROUP BY "Track"."GenreId"
          ORDER BY "Track"."GenreId"
     ) AS "subQuery";
`)

	type GenreInformation GenreInfo
	var newDest []GenreInformation

	err = mainStmt.Query(db, &newDest)
	require.NoError(t, err)
	testutils.AssertJSON(t, dest, expectedSQL)
}

func TestSelfJoin(t *testing.T) {
	var dest []struct {
		model.Employee

		Manager *model.Employee `alias:"Manager.*"`
	}

	manager := Employee.AS("Manager")

	stmt := SELECT(
		Employee.EmployeeId,
		Employee.FirstName,
		Employee.LastName,
		manager.EmployeeId,
		manager.FirstName,
		manager.LastName,
	).FROM(
		Employee.
			LEFT_JOIN(manager, Employee.ReportsTo.EQ(manager.EmployeeId)),
	).ORDER_BY(
		Employee.EmployeeId,
	)

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT "Employee"."EmployeeId" AS "Employee.EmployeeId",
     "Employee"."FirstName" AS "Employee.FirstName",
     "Employee"."LastName" AS "Employee.LastName",
     "Manager"."EmployeeId" AS "Manager.EmployeeId",
     "Manager"."FirstName" AS "Manager.FirstName",
     "Manager"."LastName" AS "Manager.LastName"
FROM chinook."Employee"
     LEFT JOIN chinook."Employee" AS "Manager" ON ("Employee"."ReportsTo" = "Manager"."EmployeeId")
ORDER BY "Employee"."EmployeeId";
`)

	err := stmt.Query(db, &dest)

	require.NoError(t, err)
	require.Equal(t, len(dest), 8)
	testutils.AssertJSON(t, dest[0:2], `
[
	{
		"EmployeeId": 1,
		"LastName": "Adams",
		"FirstName": "Andrew",
		"Title": null,
		"ReportsTo": null,
		"BirthDate": null,
		"HireDate": null,
		"Address": null,
		"City": null,
		"State": null,
		"Country": null,
		"PostalCode": null,
		"Phone": null,
		"Fax": null,
		"Email": null,
		"Manager": null
	},
	{
		"EmployeeId": 2,
		"LastName": "Edwards",
		"FirstName": "Nancy",
		"Title": null,
		"ReportsTo": null,
		"BirthDate": null,
		"HireDate": null,
		"Address": null,
		"City": null,
		"State": null,
		"Country": null,
		"PostalCode": null,
		"Phone": null,
		"Fax": null,
		"Email": null,
		"Manager": {
			"EmployeeId": 1,
			"LastName": "Adams",
			"FirstName": "Andrew",
			"Title": null,
			"ReportsTo": null,
			"BirthDate": null,
			"HireDate": null,
			"Address": null,
			"City": null,
			"State": null,
			"Country": null,
			"PostalCode": null,
			"Phone": null,
			"Fax": null,
			"Email": null
		}
	}
]
`)
}

func TestMultipleNestedAliasedSlices(t *testing.T) {
	manager := Employee.AS("manager")
	managerEmployees := Employee.AS("manager_employees")
	managerEmployeesEmployees := Employee.AS("manager_employees_employees")
	managerEmployeeEmployeesCustomers := Customer.AS("manager_employee_employee_customers")

	trimmedCustomersResultSet := managerEmployeeEmployeesCustomers.CustomerId.BETWEEN(Int(37), Int(42))

	stmt := SELECT(
		manager.AllColumns,
		managerEmployees.AllColumns,
		managerEmployeesEmployees.AllColumns,
		managerEmployeeEmployeesCustomers.AllColumns,
	).FROM(
		manager.
			LEFT_JOIN(managerEmployees, managerEmployees.ReportsTo.EQ(manager.EmployeeId)).
			LEFT_JOIN(managerEmployeesEmployees, managerEmployeesEmployees.ReportsTo.EQ(managerEmployees.EmployeeId)).
			LEFT_JOIN(managerEmployeeEmployeesCustomers,
				managerEmployeeEmployeesCustomers.SupportRepId.EQ(managerEmployeesEmployees.EmployeeId).
					AND(trimmedCustomersResultSet)),
	).WHERE(
		manager.ReportsTo.IS_NULL(),
	).ORDER_BY(
		manager.EmployeeId.ASC(),
		managerEmployees.EmployeeId.ASC(),
		managerEmployeesEmployees.EmployeeId.ASC(),
		managerEmployeeEmployeesCustomers.CustomerId.ASC(),
	)

	var dest []struct {
		model.Employee   `alias:"manager"`
		Employees        []model.Employee `alias:"manager_employees"`
		EmployeesCustom1 []struct {
			model.Employee   `alias:"manager_employees"`
			Employees1       []model.Employee `alias:"manager_employees_employees"`
			EmployeesCustom2 []struct {
				model.Employee `alias:"manager_employees_employees"`
				Customers2     []model.Customer `alias:"manager_employee_employee_customers"`
			}
		}
	}

	err := stmt.Query(db, &dest)
	require.NoError(t, err)
	testutils.AssertJSON(t, dest, testMultipleNestedAliasedSlices)
}

func TestUnionForQuotedNames(t *testing.T) {

	stmt := UNION_ALL(
		Album.SELECT(Album.AllColumns).WHERE(Album.AlbumId.EQ(Int(1))),
		Album.SELECT(Album.AllColumns).WHERE(Album.AlbumId.EQ(Int(2))),
	).ORDER_BY(
		Album.AlbumId,
	)

	//fmt.Println(stmt.DebugSql())
	testutils.AssertDebugStatementSql(t, stmt, `
(
     SELECT "Album"."AlbumId" AS "Album.AlbumId",
          "Album"."Title" AS "Album.Title",
          "Album"."ArtistId" AS "Album.ArtistId",
          "Album"."Type" AS "Album.Type"
     FROM chinook."Album"
     WHERE "Album"."AlbumId" = 1
)
UNION ALL
(
     SELECT "Album"."AlbumId" AS "Album.AlbumId",
          "Album"."Title" AS "Album.Title",
          "Album"."ArtistId" AS "Album.ArtistId",
          "Album"."Type" AS "Album.Type"
     FROM chinook."Album"
     WHERE "Album"."AlbumId" = 2
)
ORDER BY "Album.AlbumId";
`, int64(1), int64(2))

	dest := []model.Album{}

	err := stmt.Query(db, &dest)

	require.NoError(t, err)

	require.Equal(t, len(dest), 2)
	testutils.AssertDeepEqual(t, dest[0], album1)
	testutils.AssertDeepEqual(t, dest[1], album2)
}

func TestQueryWithContext(t *testing.T) {
	if sourceIsCockroachDB() && !isPgxDriver() {
		return // context cancellation doesn't work for pq driver
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	var dest []model.Album

	err := Album.
		CROSS_JOIN(Track).
		CROSS_JOIN(InvoiceLine).
		SELECT(Album.AllColumns, Track.AllColumns, InvoiceLine.AllColumns).
		QueryContext(ctx, db, &dest)

	require.Error(t, err, "context deadline exceeded")
}

func TestExecWithContext(t *testing.T) {
	if sourceIsCockroachDB() && !isPgxDriver() {
		return // context cancellation doesn't work for pq driver
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := Album.
		CROSS_JOIN(Track).
		CROSS_JOIN(InvoiceLine).
		SELECT(Album.AllColumns, Track.AllColumns, InvoiceLine.AllColumns).
		ExecContext(ctx, db)

	require.Error(t, err, "pq: canceling statement due to user request")
}

func TestSubQueriesForQuotedNames(t *testing.T) {
	first10Artist := SELECT(Artist.AllColumns).
		FROM(Artist).
		ORDER_BY(Artist.ArtistId).
		LIMIT(10).
		AsTable("first10Artist")

	artistID := Artist.ArtistId.From(first10Artist)

	first10Albums := SELECT(Album.AllColumns).
		FROM(Album).
		ORDER_BY(Album.AlbumId).
		LIMIT(10).
		AsTable("first10Albums")

	albumArtistID := Album.ArtistId.From(first10Albums)

	stmt := SELECT(
		first10Artist.AllColumns(),
		first10Albums.AllColumns(),
	).FROM(
		first10Artist.
			INNER_JOIN(first10Albums, artistID.EQ(albumArtistID)),
	).ORDER_BY(
		artistID,
	)

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT "first10Artist"."Artist.ArtistId" AS "Artist.ArtistId",
     "first10Artist"."Artist.Name" AS "Artist.Name",
     "first10Albums"."Album.AlbumId" AS "Album.AlbumId",
     "first10Albums"."Album.Title" AS "Album.Title",
     "first10Albums"."Album.ArtistId" AS "Album.ArtistId",
     "first10Albums"."Album.Type" AS "Album.Type"
FROM (
          SELECT "Artist"."ArtistId" AS "Artist.ArtistId",
               "Artist"."Name" AS "Artist.Name"
          FROM chinook."Artist"
          ORDER BY "Artist"."ArtistId"
          LIMIT 10
     ) AS "first10Artist"
     INNER JOIN (
          SELECT "Album"."AlbumId" AS "Album.AlbumId",
               "Album"."Title" AS "Album.Title",
               "Album"."ArtistId" AS "Album.ArtistId",
               "Album"."Type" AS "Album.Type"
          FROM chinook."Album"
          ORDER BY "Album"."AlbumId"
          LIMIT 10
     ) AS "first10Albums" ON ("first10Artist"."Artist.ArtistId" = "first10Albums"."Album.ArtistId")
ORDER BY "first10Artist"."Artist.ArtistId";
`, int64(10), int64(10))

	var dest []struct {
		model.Artist

		Album []model.Album
	}

	err := stmt.Query(db, &dest)
	require.NoError(t, err)
	require.Len(t, dest, 8)
}

func TestMultiTenantDifferentSchema(t *testing.T) {

	Artist2 := Artist.FromSchema("chinook2")
	Album2 := Album.FromSchema("chinook2")

	first10Artist := SELECT(Artist2.AllColumns).
		FROM(Artist2).
		ORDER_BY(Artist2.ArtistId).
		LIMIT(10).
		AsTable("first10Artist")

	artistID := Artist2.ArtistId.From(first10Artist)

	first10Albums := SELECT(Album2.AllColumns).
		FROM(Album2).
		ORDER_BY(Album2.AlbumId).
		LIMIT(10).
		AsTable("first10Albums")

	albumArtistID := Album2.ArtistId.From(first10Albums)

	stmt := SELECT(
		first10Artist.AllColumns(),
		first10Albums.AllColumns(),
	).FROM(
		first10Artist.
			INNER_JOIN(first10Albums, artistID.EQ(albumArtistID)),
	).ORDER_BY(
		artistID,
	)

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT "first10Artist"."Artist.ArtistId" AS "Artist.ArtistId",
     "first10Artist"."Artist.Name" AS "Artist.Name",
     "first10Albums"."Album.AlbumId" AS "Album.AlbumId",
     "first10Albums"."Album.Title" AS "Album.Title",
     "first10Albums"."Album.ArtistId" AS "Album.ArtistId",
     "first10Albums"."Album.Type" AS "Album.Type"
FROM (
          SELECT "Artist"."ArtistId" AS "Artist.ArtistId",
               "Artist"."Name" AS "Artist.Name"
          FROM chinook2."Artist"
          ORDER BY "Artist"."ArtistId"
          LIMIT 10
     ) AS "first10Artist"
     INNER JOIN (
          SELECT "Album"."AlbumId" AS "Album.AlbumId",
               "Album"."Title" AS "Album.Title",
               "Album"."ArtistId" AS "Album.ArtistId",
               "Album"."Type" AS "Album.Type"
          FROM chinook2."Album"
          ORDER BY "Album"."AlbumId"
          LIMIT 10
     ) AS "first10Albums" ON ("first10Artist"."Artist.ArtistId" = "first10Albums"."Album.ArtistId")
ORDER BY "first10Artist"."Artist.ArtistId";
`)

	var dest []struct {
		model.Artist

		Album []model.Album
	}

	err := stmt.Query(db, &dest)
	require.NoError(t, err)

	require.Len(t, dest, 2)
	require.Equal(t, *dest[0].Artist.Name, "Apocalyptica")
	require.Len(t, dest[0].Album, 1)
	require.Equal(t, dest[0].Album[0].Title, "Plays Metallica By Four Cellos")
}

func TestUseSchema(t *testing.T) {
	UseSchema("chinook2")
	defer UseSchema("chinook")

	stmt := SELECT(
		Artist.AllColumns,
	).FROM(
		Artist,
	).WHERE(Artist.ArtistId.EQ(Int(11)))

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT "Artist"."ArtistId" AS "Artist.ArtistId",
     "Artist"."Name" AS "Artist.Name"
FROM chinook2."Artist"
WHERE "Artist"."ArtistId" = 11;
`)

	var artist model.Artist

	err := stmt.Query(db, &artist)
	require.NoError(t, err)

	testutils.AssertJSON(t, artist, `
{
	"ArtistId": 11,
	"Name": "Black Label Society"
}
`)
}

func TestMultiTenantSameSchemaDifferentTablePrefix(t *testing.T) {

	var selectAlbumsFrom = func(tenant string) SelectStatement {
		Album := table.Album.WithPrefix(tenant)

		return SELECT(
			Album.AlbumId,
			Album.Title,
			Album.ArtistId,
		).FROM(
			Album,
		).ORDER_BY(
			Album.AlbumId.ASC(),
		).LIMIT(3)
	}

	t.Run("tenant1", func(t *testing.T) {
		stmt := selectAlbumsFrom("tenant1.")

		testutils.AssertStatementSql(t, stmt, `
SELECT "Album"."AlbumId" AS "Album.AlbumId",
     "Album"."Title" AS "Album.Title",
     "Album"."ArtistId" AS "Album.ArtistId"
FROM chinook2."tenant1.Album" AS "Album"
ORDER BY "Album"."AlbumId" ASC
LIMIT $1;
`)

		var albums []model.Album
		err := stmt.Query(db, &albums)
		require.NoError(t, err)

		testutils.AssertJSON(t, albums, `
[
	{
		"AlbumId": 80,
		"Title": "In Your Honor [Disc 2]",
		"ArtistId": 84,
		"Type": ""
	},
	{
		"AlbumId": 81,
		"Title": "One By One",
		"ArtistId": 84,
		"Type": ""
	},
	{
		"AlbumId": 82,
		"Title": "The Colour And The Shape",
		"ArtistId": 84,
		"Type": ""
	}
]
`)
	})

	t.Run("tenant2", func(t *testing.T) {
		stmt := selectAlbumsFrom("tenant2.")

		testutils.AssertStatementSql(t, stmt, `
SELECT "Album"."AlbumId" AS "Album.AlbumId",
     "Album"."Title" AS "Album.Title",
     "Album"."ArtistId" AS "Album.ArtistId"
FROM chinook2."tenant2.Album" AS "Album"
ORDER BY "Album"."AlbumId" ASC
LIMIT $1;
`)

		var albums []model.Album
		err := stmt.Query(db, &albums)
		require.NoError(t, err)
		testutils.AssertJSON(t, albums, `
[
	{
		"AlbumId": 152,
		"Title": "Master Of Puppets",
		"ArtistId": 50,
		"Type": ""
	},
	{
		"AlbumId": 153,
		"Title": "ReLoad",
		"ArtistId": 50,
		"Type": ""
	},
	{
		"AlbumId": 154,
		"Title": "Ride The Lightning",
		"ArtistId": 50,
		"Type": ""
	}
]
`)
	})
}

func TestMultiTenantSameSchemaDifferentTableSuffix(t *testing.T) {

	var selectAlbumsFrom = func(tenant string) SelectStatement {
		Album := table.Album.WithSuffix(tenant)

		return SELECT(
			Album.AlbumId,
			Album.Title,
			Album.ArtistId,
		).FROM(
			Album,
		).ORDER_BY(
			Album.AlbumId.ASC(),
		).LIMIT(3)
	}

	t.Run("tenant1", func(t *testing.T) {
		stmt := selectAlbumsFrom(".tenant1")

		testutils.AssertStatementSql(t, stmt, `
SELECT "Album"."AlbumId" AS "Album.AlbumId",
     "Album"."Title" AS "Album.Title",
     "Album"."ArtistId" AS "Album.ArtistId"
FROM chinook2."Album.tenant1" AS "Album"
ORDER BY "Album"."AlbumId" ASC
LIMIT $1;
`)

		var albums []model.Album
		err := stmt.Query(db, &albums)
		require.NoError(t, err)
		testutils.AssertJSON(t, albums, `
[
	{
		"AlbumId": 80,
		"Title": "In Your Honor [Disc 2]",
		"ArtistId": 84,
		"Type": ""
	},
	{
		"AlbumId": 81,
		"Title": "One By One",
		"ArtistId": 84,
		"Type": ""
	},
	{
		"AlbumId": 82,
		"Title": "The Colour And The Shape",
		"ArtistId": 84,
		"Type": ""
	}
]
`)
	})

	t.Run("tenant2", func(t *testing.T) {
		stmt := selectAlbumsFrom(".tenant2")

		testutils.AssertStatementSql(t, stmt, `
SELECT "Album"."AlbumId" AS "Album.AlbumId",
     "Album"."Title" AS "Album.Title",
     "Album"."ArtistId" AS "Album.ArtistId"
FROM chinook2."Album.tenant2" AS "Album"
ORDER BY "Album"."AlbumId" ASC
LIMIT $1;
`)

		var albums []model.Album
		err := stmt.Query(db, &albums)
		require.NoError(t, err)
		testutils.AssertJSON(t, albums, `
[
	{
		"AlbumId": 152,
		"Title": "Master Of Puppets",
		"ArtistId": 50,
		"Type": ""
	},
	{
		"AlbumId": 153,
		"Title": "ReLoad",
		"ArtistId": 50,
		"Type": ""
	},
	{
		"AlbumId": 154,
		"Title": "Ride The Lightning",
		"ArtistId": 50,
		"Type": ""
	}
]
`)
	})
}

var album1 = model.Album{
	AlbumId:  1,
	Title:    "For Those About To Rock We Salute You",
	ArtistId: 1,
	Type:     model.AlbumType_Studio,
}

var album2 = model.Album{
	AlbumId:  2,
	Title:    "Balls to the Wall",
	ArtistId: 2,
	Type:     model.AlbumType_Studio,
}

var album347 = model.Album{
	AlbumId:  347,
	Title:    "Koyaanisqatsi (Soundtrack from the Motion Picture)",
	ArtistId: 275,
	Type:     model.AlbumType_Studio,
}

func TestAggregateFunc(t *testing.T) {
	skipForCockroachDB(t)

	stmt := SELECT(
		PERCENTILE_DISC(Float(0.1)).WITHIN_GROUP_ORDER_BY(Invoice.InvoiceId).AS("percentile_disc_1"),
		PERCENTILE_DISC(Invoice.Total.DIV(Float(100))).WITHIN_GROUP_ORDER_BY(Invoice.InvoiceDate.ASC()).AS("percentile_disc_2"),
		PERCENTILE_DISC(RawFloat("(select array_agg(s) from generate_series(0, 1, 0.2) as s)")).
			WITHIN_GROUP_ORDER_BY(Invoice.BillingAddress.DESC()).AS("percentile_disc_3"),

		PERCENTILE_CONT(Float(0.3)).WITHIN_GROUP_ORDER_BY(Invoice.Total).AS("percentile_cont_1"),
		PERCENTILE_CONT(Float(0.2)).WITHIN_GROUP_ORDER_BY(INTERVAL(1, HOUR).DESC()).AS("percentile_cont_int"),

		MODE().WITHIN_GROUP_ORDER_BY(Invoice.BillingPostalCode.DESC()).AS("mode_1"),
	).FROM(
		Invoice,
	).GROUP_BY(
		Invoice.Total,
	)

	testutils.AssertStatementSql(t, stmt, `
SELECT PERCENTILE_DISC ($1::double precision) WITHIN GROUP (ORDER BY "Invoice"."InvoiceId") AS "percentile_disc_1",
     PERCENTILE_DISC ("Invoice"."Total" / $2) WITHIN GROUP (ORDER BY "Invoice"."InvoiceDate" ASC) AS "percentile_disc_2",
     PERCENTILE_DISC ((select array_agg(s) from generate_series(0, 1, 0.2) as s)) WITHIN GROUP (ORDER BY "Invoice"."BillingAddress" DESC) AS "percentile_disc_3",
     PERCENTILE_CONT ($3::double precision) WITHIN GROUP (ORDER BY "Invoice"."Total") AS "percentile_cont_1",
     PERCENTILE_CONT ($4::double precision) WITHIN GROUP (ORDER BY INTERVAL '1 HOUR' DESC) AS "percentile_cont_int",
     MODE () WITHIN GROUP (ORDER BY "Invoice"."BillingPostalCode" DESC) AS "mode_1"
FROM chinook."Invoice"
GROUP BY "Invoice"."Total";
`, 0.1, 100.0, 0.3, 0.2)

	var dest struct {
		PercentileDisc1 string
		PercentileDisc2 string
		PercentileDisc3 string
		PercentileCont1 string
		Mode1           string
	}

	err := stmt.Query(db, &dest)

	require.NoError(t, err)
	testutils.AssertJSON(t, dest, `
{
	"PercentileDisc1": "41",
	"PercentileDisc2": "2009-01-19T00:00:00Z",
	"PercentileDisc3": "{\"Via Degli Scipioni, 43\",\"Qe 7 Bloco G\",\"Berger Stra�e 10\",\"696 Osborne Street\",\"2211 W Berry Street\",\"1033 N Park Ave\"}",
	"PercentileCont1": "0.99",
	"Mode1": "X1A 1N6"
}
`)
}

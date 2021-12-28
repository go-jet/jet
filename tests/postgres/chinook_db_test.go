package postgres

import (
	"context"
	"github.com/go-jet/jet/v2/internal/testutils"
	. "github.com/go-jet/jet/v2/postgres"
	"github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/chinook/model"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/chinook/table"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestSelect(t *testing.T) {
	stmt := Album.
		SELECT(Album.AllColumns).
		ORDER_BY(Album.AlbumId.ASC())

	//fmt.Println(stmt.DebugSql())

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT "Album"."AlbumId" AS "Album.AlbumId",
     "Album"."Title" AS "Album.Title",
     "Album"."ArtistId" AS "Album.ArtistId"
FROM chinook."Album"
ORDER BY "Album"."AlbumId" ASC;
`)
	dest := []model.Album{}

	err := stmt.Query(db, &dest)

	require.NoError(t, err)
	require.Equal(t, len(dest), 347)
	testutils.AssertDeepEqual(t, dest[0], album1)
	testutils.AssertDeepEqual(t, dest[1], album2)
	testutils.AssertDeepEqual(t, dest[len(dest)-1], album347)
	requireLogged(t, stmt)
}

func TestJoinEverything(t *testing.T) {

	manager := Employee.AS("Manager")

	stmt := Artist.
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
		LEFT_JOIN(manager, manager.EmployeeId.EQ(Employee.ReportsTo)).
		SELECT(
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
		).
		ORDER_BY(Artist.ArtistId, Album.AlbumId, Track.TrackId,
			Genre.GenreId, MediaType.MediaTypeId, Playlist.PlaylistId,
			Invoice.InvoiceId, Customer.CustomerId)

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

	err := stmt.Query(db, &dest)

	require.NoError(t, err)
	require.Equal(t, len(dest), 275)
	testutils.AssertJSONFile(t, dest, "./testdata/results/postgres/joined_everything.json")
	requireLogged(t, stmt)
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
		subQuery2.AllColumns(),
	).FROM(
		subQuery2,
	)

	//fmt.Println(mainQuery.Sql())

	testutils.AssertStatementSql(t, mainQuery, `
SELECT "subQuery2"."Artist.ArtistId" AS "Artist.ArtistId",
     "subQuery2"."Artist.Name" AS "Artist.Name",
     "subQuery2".custom_column_1 AS "custom_column_1",
     "subQuery2".custom_column_2 AS "custom_column_2"
FROM (
          SELECT "subQuery1"."Artist.ArtistId" AS "Artist.ArtistId",
               "subQuery1"."Artist.Name" AS "Artist.Name",
               "subQuery1".custom_column_1 AS "custom_column_1",
               $1 AS "custom_column_2"
          FROM (
                    SELECT "Artist"."ArtistId" AS "Artist.ArtistId",
                         "Artist"."Name" AS "Artist.Name",
                         $2 AS "custom_column_1"
                    FROM chinook."Artist"
                    ORDER BY "Artist"."ArtistId" ASC
               ) AS "subQuery1"
     ) AS "subQuery2";
`)
	var dest []struct {
		model.Artist
		CustomColumn1 string
		CustomColumn2 string
	}

	err := mainQuery.Query(db, &dest)
	require.NoError(t, err)

	require.Len(t, dest, 275)
	require.Equal(t, dest[0].Artist, model.Artist{
		ArtistId: 1,
		Name:     testutils.StringPtr("AC/DC"),
	})
	require.Equal(t, dest[0].CustomColumn1, "custom_column_1")
	require.Equal(t, dest[0].CustomColumn2, "custom_column_2")
}

func TestSelfJoin(t *testing.T) {
	var dest []struct {
		model.Employee

		Manager *model.Employee `alias:"Manager.*"`
	}

	manager := Employee.AS("Manager")

	stmt := Employee.
		LEFT_JOIN(manager, Employee.ReportsTo.EQ(manager.EmployeeId)).
		SELECT(
			Employee.EmployeeId,
			Employee.FirstName,
			Employee.LastName,
			manager.EmployeeId,
			manager.FirstName,
			manager.LastName,
		).
		ORDER_BY(Employee.EmployeeId)

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

func TestUnionForQuotedNames(t *testing.T) {

	stmt := UNION_ALL(
		Album.SELECT(Album.AllColumns).WHERE(Album.AlbumId.EQ(Int(1))),
		Album.SELECT(Album.AllColumns).WHERE(Album.AlbumId.EQ(Int(2))),
	).
		ORDER_BY(Album.AlbumId)

	//fmt.Println(stmt.DebugSql())
	testutils.AssertDebugStatementSql(t, stmt, `
(
     SELECT "Album"."AlbumId" AS "Album.AlbumId",
          "Album"."Title" AS "Album.Title",
          "Album"."ArtistId" AS "Album.ArtistId"
     FROM chinook."Album"
     WHERE "Album"."AlbumId" = 1
)
UNION ALL
(
     SELECT "Album"."AlbumId" AS "Album.AlbumId",
          "Album"."Title" AS "Album.Title",
          "Album"."ArtistId" AS "Album.ArtistId"
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

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	dest := []model.Album{}

	err := Album.
		CROSS_JOIN(Track).
		CROSS_JOIN(InvoiceLine).
		SELECT(Album.AllColumns, Track.AllColumns, InvoiceLine.AllColumns).
		QueryContext(ctx, db, &dest)

	require.Error(t, err, "context deadline exceeded")
}

func TestExecWithContext(t *testing.T) {

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
	first10Artist := Artist.
		SELECT(Artist.AllColumns).
		ORDER_BY(Artist.ArtistId).
		LIMIT(10).
		AsTable("first10Artist")

	artistID := Artist.ArtistId.From(first10Artist)

	first10Albums := Album.
		SELECT(Album.AllColumns).
		ORDER_BY(Album.AlbumId).
		LIMIT(10).
		AsTable("first10Albums")

	albumArtistID := Album.ArtistId.From(first10Albums)

	stmt := first10Artist.
		INNER_JOIN(first10Albums, artistID.EQ(albumArtistID)).
		SELECT(first10Artist.AllColumns(), first10Albums.AllColumns()).
		ORDER_BY(artistID)

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT "first10Artist"."Artist.ArtistId" AS "Artist.ArtistId",
     "first10Artist"."Artist.Name" AS "Artist.Name",
     "first10Albums"."Album.AlbumId" AS "Album.AlbumId",
     "first10Albums"."Album.Title" AS "Album.Title",
     "first10Albums"."Album.ArtistId" AS "Album.ArtistId"
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
               "Album"."ArtistId" AS "Album.ArtistId"
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
}

func Test_SchemaRename(t *testing.T) {

	Artist2 := Artist.FromSchema("chinook2")
	Album2 := Album.FromSchema("chinook2")

	first10Artist := Artist2.
		SELECT(Artist2.AllColumns).
		ORDER_BY(Artist2.ArtistId).
		LIMIT(10).
		AsTable("first10Artist")

	artistID := Artist2.ArtistId.From(first10Artist)

	first10Albums := Album2.
		SELECT(Album2.AllColumns).
		ORDER_BY(Album2.AlbumId).
		LIMIT(10).
		AsTable("first10Albums")

	albumArtistID := Album2.ArtistId.From(first10Albums)

	stmt := SELECT(first10Artist.AllColumns(), first10Albums.AllColumns()).
		FROM(first10Artist.
			INNER_JOIN(first10Albums, artistID.EQ(albumArtistID))).
		ORDER_BY(artistID)

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT "first10Artist"."Artist.ArtistId" AS "Artist.ArtistId",
     "first10Artist"."Artist.Name" AS "Artist.Name",
     "first10Albums"."Album.AlbumId" AS "Album.AlbumId",
     "first10Albums"."Album.Title" AS "Album.Title",
     "first10Albums"."Album.ArtistId" AS "Album.ArtistId"
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
               "Album"."ArtistId" AS "Album.ArtistId"
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

var album1 = model.Album{
	AlbumId:  1,
	Title:    "For Those About To Rock We Salute You",
	ArtistId: 1,
}

var album2 = model.Album{
	AlbumId:  2,
	Title:    "Balls to the Wall",
	ArtistId: 2,
}

var album347 = model.Album{
	AlbumId:  347,
	Title:    "Koyaanisqatsi (Soundtrack from the Motion Picture)",
	ArtistId: 275,
}

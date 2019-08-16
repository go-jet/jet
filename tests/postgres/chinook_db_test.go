package postgres

import (
	"context"
	"fmt"
	"github.com/go-jet/jet/internal/testutils"
	. "github.com/go-jet/jet/postgres"
	"github.com/go-jet/jet/tests/.gentestdata/jetdb/chinook/model"
	. "github.com/go-jet/jet/tests/.gentestdata/jetdb/chinook/table"
	"gotest.tools/assert"
	"testing"
	"time"
)

func TestSelect(t *testing.T) {
	stmt := Album.
		SELECT(Album.AllColumns).
		ORDER_BY(Album.AlbumId.ASC())

	fmt.Println(stmt.DebugSql())

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT "Album"."AlbumId" AS "Album.AlbumId",
     "Album"."Title" AS "Album.Title",
     "Album"."ArtistId" AS "Album.ArtistId"
FROM chinook."Album"
ORDER BY "Album"."AlbumId" ASC;
`)
	dest := []model.Album{}

	err := stmt.Query(db, &dest)

	assert.NilError(t, err)
	assert.Equal(t, len(dest), 347)
	assert.DeepEqual(t, dest[0], album1)
	assert.DeepEqual(t, dest[1], album2)
	assert.DeepEqual(t, dest[len(dest)-1], album347)
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

	err := stmt.Query(db, &dest)

	assert.NilError(t, err)
	assert.Equal(t, len(dest), 275)
	testutils.AssertJSONFile(t, dest, "./testdata/results/postgres/joined_everything.json")
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

	assert.NilError(t, err)
	assert.Equal(t, len(dest), 8)
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

	assert.NilError(t, err)

	assert.Equal(t, len(dest), 2)
	assert.DeepEqual(t, dest[0], album1)
	assert.DeepEqual(t, dest[1], album2)
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

	assert.Error(t, err, "context deadline exceeded")
}

func TestExecWithContext(t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err := Album.
		CROSS_JOIN(Track).
		CROSS_JOIN(InvoiceLine).
		SELECT(Album.AllColumns, Track.AllColumns, InvoiceLine.AllColumns).
		ExecContext(ctx, db)

	assert.Error(t, err, "pq: canceling statement due to user request")
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

	assert.NilError(t, err)

	//spew.Dump(dest)
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

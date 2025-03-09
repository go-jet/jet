//go:build bench
// +build bench

package mysql

import (
	"github.com/go-jet/jet/v2/internal/testutils"
	. "github.com/go-jet/jet/v2/mysql"
	"github.com/go-jet/jet/v2/tests/.gentestdata/mysql/dvds/model"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/mysql/dvds/table"
	"github.com/stretchr/testify/require"
	"testing"
)

type allInfo []struct {
	model.Actor

	Films []struct {
		model.Film

		Language   model.Language
		Categories []model.Category

		Inventories []struct {
			model.Inventory

			Rentals []struct {
				model.Rental

				Customer model.Customer
			}
		}
	}
}

func BenchmarkTestDVDsJoinEverything(b *testing.B) {
	for i := 0; i < b.N; i++ {
		testDVDsJoinEverything(b)
	}
}

func TestDVDsJoinEverything(t *testing.T) {
	testDVDsJoinEverything(t)
}

func testDVDsJoinEverything(t require.TestingT) {
	stmt := SELECT(
		Actor.AllColumns,
		Film.AllColumns,
		Language.AllColumns,
		Category.AllColumns,
		Inventory.AllColumns,
		Rental.AllColumns,
		Customer.AllColumns,
	).FROM(
		Actor.
			LEFT_JOIN(FilmActor, Actor.ActorID.EQ(FilmActor.ActorID)).
			LEFT_JOIN(Film, Film.FilmID.EQ(FilmActor.FilmID)).
			LEFT_JOIN(Language, Language.LanguageID.EQ(Film.LanguageID)).
			LEFT_JOIN(FilmCategory, FilmCategory.FilmID.EQ(Film.FilmID)).
			LEFT_JOIN(Category, Category.CategoryID.EQ(FilmCategory.CategoryID)).
			LEFT_JOIN(Inventory, Inventory.FilmID.EQ(Film.FilmID)).
			LEFT_JOIN(Rental, Rental.InventoryID.EQ(Inventory.InventoryID)).
			LEFT_JOIN(Customer, Customer.CustomerID.EQ(Rental.CustomerID)),
	).ORDER_BY(
		Actor.ActorID.ASC(),
		Film.FilmID.ASC(),
		Category.CategoryID.ASC(),
		Inventory.InventoryID.ASC(),
		Rental.RentalID.ASC(),
	)

	var dest allInfo

	err := stmt.Query(db, &dest)
	require.NoError(t, err)

	//testutils.SaveJSONFile(dest, "./testdata/results/mysql/dvds_join_everything.json")
	testutils.AssertJSONFile(t, dest, "./testdata/results/mysql/dvds_join_everything.json")
}

func BenchmarkTestDVDsJoinEverythingJSON(b *testing.B) {
	for i := 0; i < b.N; i++ {
		testDVDsJoinEverythingJSON(b)
	}
}

func TestDVDsJoinEverythingJSON(t *testing.T) {
	testDVDsJoinEverythingJSON(t)
}

func testDVDsJoinEverythingJSON(t require.TestingT) {
	stmt := SELECT_JSON_ARR(
		Actor.ActorID, Actor.FirstName, Actor.LastName, Actor.LastUpdate,

		SELECT_JSON_ARR(
			Film.AllColumns,

			SELECT_JSON_OBJ(Language.AllColumns).
				FROM(Language).
				WHERE(Language.LanguageID.EQ(Film.LanguageID)).AS("Language"),

			SELECT_JSON_ARR(Category.AllColumns).
				FROM(Category.INNER_JOIN(FilmCategory, FilmCategory.CategoryID.EQ(Category.CategoryID))).
				WHERE(FilmCategory.FilmID.EQ(Film.FilmID)).AS("Categories"),

			SELECT_JSON_ARR(
				Inventory.AllColumns,

				SELECT_JSON_ARR(
					Rental.AllColumns,

					SELECT_JSON_OBJ(Customer.AllColumns).
						FROM(Customer).
						WHERE(Customer.CustomerID.EQ(Rental.CustomerID)).AS("Customer"),
				).FROM(Rental).
					WHERE(Rental.InventoryID.EQ(Inventory.InventoryID)).
					ORDER_BY(Rental.RentalID).AS("Rentals"),
			).FROM(Inventory).
				WHERE(Inventory.FilmID.EQ(Film.FilmID)).
				ORDER_BY(Inventory.InventoryID).AS("Inventories"),
		).FROM(Film.
			INNER_JOIN(FilmActor, FilmActor.FilmID.EQ(Film.FilmID)),
		).WHERE(FilmActor.ActorID.EQ(Actor.ActorID)).
			ORDER_BY(Film.FilmID.ASC()).AS("Films"),
	).FROM(Actor).
		ORDER_BY(Actor.ActorID.ASC())

	//fmt.Println(stmt.DebugSql())

	var dest allInfo

	err := stmt.QueryContext(ctx, db, &dest)
	require.NoError(t, err)

	//testutils.SaveJSONFile(dest, "./testdata/results/mysql/dvds_join_everything2.json")
	testutils.AssertJSONFile(t, dest, "./testdata/results/mysql/dvds_join_everything.json")
}

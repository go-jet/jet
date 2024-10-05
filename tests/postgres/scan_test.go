package postgres

import (
	"context"
	"github.com/volatiletech/null/v8"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/go-jet/jet/v2/internal/testutils"
	. "github.com/go-jet/jet/v2/postgres"
	"github.com/go-jet/jet/v2/qrm"
	"github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/dvds/model"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/dvds/table"
)

var oneInventoryQuery = Inventory.
	SELECT(Inventory.AllColumns).
	LIMIT(1).
	ORDER_BY(Inventory.InventoryID)

func TestScanToInvalidDestination(t *testing.T) {

	t.Run("nil dest", func(t *testing.T) {
		testutils.AssertQueryPanicErr(t, oneInventoryQuery, db, nil, "jet: destination is nil")
	})

	t.Run("struct dest", func(t *testing.T) {
		testutils.AssertQueryPanicErr(t, oneInventoryQuery, db, struct{}{}, "jet: destination has to be a pointer to slice or pointer to struct")
	})

	t.Run("slice dest", func(t *testing.T) {
		testutils.AssertQueryPanicErr(t, oneInventoryQuery, db, []struct{}{}, "jet: destination has to be a pointer to slice or pointer to struct")
	})

	t.Run("slice of pointers to pointer dest", func(t *testing.T) {
		testutils.AssertQueryPanicErr(t, oneInventoryQuery, db, []**struct{}{}, "jet: destination has to be a pointer to slice or pointer to struct")
	})

	t.Run("map dest", func(t *testing.T) {
		testutils.AssertQueryPanicErr(t, oneInventoryQuery, db, &map[string]string{}, "jet: destination has to be a pointer to slice or pointer to struct")
	})

	t.Run("map dest", func(t *testing.T) {
		testutils.AssertQueryPanicErr(t, oneInventoryQuery, db, []map[string]string{}, "jet: destination has to be a pointer to slice or pointer to struct")
	})

	t.Run("map dest", func(t *testing.T) {
		testutils.AssertQueryPanicErr(t, oneInventoryQuery, db, &[]map[string]string{}, "jet: unsupported slice element type")
	})
}

func TestScanToValidDestination(t *testing.T) {
	t.Run("pointer to struct", func(t *testing.T) {
		dest := []struct{}{}
		err := oneInventoryQuery.Query(db, &dest)

		require.NoError(t, err)
	})

	t.Run("global query function scan", func(t *testing.T) {
		queryStr, args := oneInventoryQuery.Sql()
		dest := []struct{}{}
		rowProcessed, err := qrm.Query(nil, db, queryStr, args, &dest)
		require.Equal(t, rowProcessed, int64(1))
		require.NoError(t, err)
	})

	t.Run("pointer to slice", func(t *testing.T) {
		err := oneInventoryQuery.Query(db, &[]struct{}{})

		require.NoError(t, err)
	})

	t.Run("pointer to slice of pointer to structs", func(t *testing.T) {
		err := oneInventoryQuery.Query(db, &[]*struct{}{})

		require.NoError(t, err)
	})

	t.Run("pointer to slice of integers", func(t *testing.T) {
		var dest []int32

		err := oneInventoryQuery.Query(db, &dest)
		require.NoError(t, err)
		require.Equal(t, dest[0], int32(1))
	})

	t.Run("pointer to slice integer pointers", func(t *testing.T) {
		var dest []*int32

		err := oneInventoryQuery.Query(db, &dest)
		require.NoError(t, err)
		require.Equal(t, dest[0], testutils.PtrOf(int32(1)))
	})

	t.Run("NULL to integer", func(t *testing.T) {
		var dest struct {
			Int64  int64
			UInt64 uint64
		}
		err := SELECT(NULL.AS("int64"), NULL.AS("uint64")).Query(db, &dest)
		require.NoError(t, err)
		require.Equal(t, dest.Int64, int64(0))
		require.Equal(t, dest.UInt64, uint64(0))
	})
}

func TestScanToStruct(t *testing.T) {
	query := Inventory.
		SELECT(Inventory.AllColumns).
		ORDER_BY(Inventory.InventoryID)

	//fmt.Println(query.DebugSql())

	t.Run("one struct", func(t *testing.T) {
		dest := model.Inventory{}
		err := query.LIMIT(1).Query(db, &dest)

		require.NoError(t, err)
		testutils.AssertDeepEqual(t, inventory1, dest)
	})

	t.Run("multiple structs, just first one used", func(t *testing.T) {
		dest := model.Inventory{}
		err := query.LIMIT(10).Query(db, &dest)

		require.NoError(t, err)
		testutils.AssertDeepEqual(t, inventory1, dest)
	})

	t.Run("one struct", func(t *testing.T) {
		dest := struct {
			model.Inventory
		}{}
		err := query.LIMIT(1).Query(db, &dest)

		require.NoError(t, err)
		testutils.AssertDeepEqual(t, inventory1, dest.Inventory)
	})

	t.Run("one struct", func(t *testing.T) {
		dest := struct {
			*model.Inventory
		}{}
		err := query.LIMIT(1).Query(db, &dest)

		require.NoError(t, err)
		testutils.AssertDeepEqual(t, inventory1, *dest.Inventory)
	})

	t.Run("invalid dest", func(t *testing.T) {
		dest := struct {
			Inventory **model.Inventory
		}{}

		testutils.AssertQueryPanicErr(t, query, db, &dest, "jet: unsupported dest type: Inventory **model.Inventory")
	})

	t.Run("invalid dest 2", func(t *testing.T) {
		dest := struct {
			Inventory ***model.Inventory
		}{}

		testutils.AssertQueryPanicErr(t, query, db, &dest, "jet: unsupported dest type: Inventory ***model.Inventory")
	})

	t.Run("custom struct", func(t *testing.T) {
		type Inventory struct {
			InventoryID *int32 `sql:"primary_key"`
			FilmID      int16
			StoreID     *int16
		}

		dest := Inventory{}

		err := query.Query(db, &dest)

		require.NoError(t, err)

		require.Equal(t, *dest.InventoryID, int32(1))
		require.Equal(t, dest.FilmID, int16(1))
		require.Equal(t, *dest.StoreID, int16(1))
	})

	t.Run("type convert int32 to int", func(t *testing.T) {
		type Inventory struct {
			InventoryID int
			FilmID      string
		}

		dest := Inventory{}

		err := query.Query(db, &dest)

		require.NoError(t, err)
	})

	t.Run("type mismatch scanner type", func(t *testing.T) {
		type Inventory struct {
			InventoryID uuid.UUID
			FilmID      string
		}

		dest := Inventory{}

		err := query.Query(db, &dest)
		require.Error(t, err)
		require.EqualError(t, err, "jet: can't scan int64('\\x01') to 'InventoryID uuid.UUID': Scan: unable to scan type int64 into UUID")
	})

	t.Run("type mismatch base type", func(t *testing.T) {
		type Inventory struct {
			InventoryID int32
			FilmID      bool
		}

		dest := []Inventory{}

		err := query.OFFSET(10).Query(db, &dest)
		require.Error(t, err)
		require.EqualError(t, err, "jet: can't assign int64('\\x02') to 'FilmID bool': can't assign int64(2) to bool")
	})
}

func TestScanToNestedStruct(t *testing.T) {
	query := Inventory.
		INNER_JOIN(Film, Inventory.FilmID.EQ(Film.FilmID)).
		INNER_JOIN(Store, Inventory.StoreID.EQ(Store.StoreID)).
		SELECT(Inventory.AllColumns, Film.AllColumns, Store.AllColumns).
		WHERE(Inventory.InventoryID.EQ(Int(1)))

	t.Run("embedded structs", func(t *testing.T) {
		dest := struct {
			model.Inventory
			model.Film
			model.Store
		}{}

		err := query.Query(db, &dest)

		require.NoError(t, err)
		testutils.AssertDeepEqual(t, dest.Inventory, inventory1)
		testutils.AssertDeepEqual(t, dest.Film, film1)
		testutils.AssertDeepEqual(t, dest.Store, store1)
	})

	t.Run("embedded pointer structs", func(t *testing.T) {
		dest := struct {
			*model.Inventory
			*model.Film
			*model.Store
		}{}

		err := query.Query(db, &dest)

		require.NoError(t, err)
		testutils.AssertDeepEqual(t, *dest.Inventory, inventory1)
		testutils.AssertDeepEqual(t, *dest.Film, film1)
		testutils.AssertDeepEqual(t, *dest.Store, store1)
	})

	t.Run("embedded unused structs", func(t *testing.T) {
		dest := struct {
			model.Inventory
			model.Actor //unused
		}{}

		err := query.Query(db, &dest)

		require.NoError(t, err)
		testutils.AssertDeepEqual(t, dest.Inventory, inventory1)
		testutils.AssertDeepEqual(t, dest.Actor, model.Actor{})
	})

	t.Run("embedded unused pointer structs", func(t *testing.T) {
		dest := struct {
			model.Inventory
			*model.Actor //unused
		}{}

		err := query.Query(db, &dest)

		require.NoError(t, err)
		testutils.AssertDeepEqual(t, dest.Inventory, inventory1)
		testutils.AssertDeepEqual(t, dest.Actor, (*model.Actor)(nil))
	})

	t.Run("embedded unused pointer structs", func(t *testing.T) {
		dest := struct {
			model.Inventory
			Actor *model.Actor //unused
		}{}

		err := query.Query(db, &dest)

		require.NoError(t, err)
		testutils.AssertDeepEqual(t, dest.Inventory, inventory1)
		testutils.AssertDeepEqual(t, dest.Actor, (*model.Actor)(nil))
	})

	t.Run("embedded pointer to selected column", func(t *testing.T) {
		query := Inventory.
			INNER_JOIN(Film, Inventory.FilmID.EQ(Film.FilmID)).
			INNER_JOIN(Store, Inventory.StoreID.EQ(Store.StoreID)).
			SELECT(Inventory.AllColumns, Film.AllColumns, Store.AllColumns, String("").AS("actor.first_name")).
			WHERE(Inventory.InventoryID.EQ(Int(1)))

		dest := struct {
			model.Inventory
			Actor *model.Actor //unused
		}{}

		err := query.Query(db, &dest)

		require.NoError(t, err)
		testutils.AssertDeepEqual(t, dest.Inventory, inventory1)
		require.True(t, dest.Actor != nil)
	})

	t.Run("struct embedded unused pointer", func(t *testing.T) {
		dest := struct {
			model.Inventory
			Actor *struct {
				model.Actor
			} //unused
		}{}

		err := query.Query(db, &dest)

		require.NoError(t, err)
		testutils.AssertDeepEqual(t, dest.Inventory, inventory1)
		testutils.AssertDeepEqual(t, dest.Actor, (*struct{ model.Actor })(nil))
	})

	t.Run("multiple embedded unused pointer", func(t *testing.T) {
		dest := struct {
			model.Inventory
			Actor *struct {
				model.Actor    //unused
				model.Language //unesed
			}
		}{}

		err := query.Query(db, &dest)

		require.NoError(t, err)
		testutils.AssertDeepEqual(t, dest.Inventory, inventory1)
		testutils.AssertDeepEqual(t, dest.Actor, (*struct {
			model.Actor
			model.Language
		})(nil))
	})

	t.Run("field not nil, embedded selected model", func(t *testing.T) {
		dest := struct {
			model.Inventory
			Actor *struct {
				model.Actor //unselected
				model.Film  //selected
			}
		}{}

		err := query.Query(db, &dest)

		require.NoError(t, err)
		testutils.AssertDeepEqual(t, dest.Inventory, inventory1)
		require.True(t, dest.Actor != nil)
		testutils.AssertDeepEqual(t, dest.Actor.Actor, model.Actor{})
		testutils.AssertDeepEqual(t, dest.Actor.Film, film1)
	})

	t.Run("field not nil, deeply nested selected model", func(t *testing.T) {
		dest := struct {
			model.Inventory
			Actor *struct {
				model.Actor //unselected
				Film        *struct {
					*model.Film //selected
				}
			}
		}{}

		err := query.Query(db, &dest)

		require.NoError(t, err)
		testutils.AssertDeepEqual(t, dest.Inventory, inventory1)
		require.True(t, dest.Actor != nil)
		require.True(t, dest.Actor.Film != nil)
		testutils.AssertDeepEqual(t, dest.Actor.Film.Film, &film1)
	})

	t.Run("embedded structs", func(t *testing.T) {
		query := Inventory.
			INNER_JOIN(Film, Inventory.FilmID.EQ(Film.FilmID)).
			INNER_JOIN(Store, Inventory.StoreID.EQ(Store.StoreID)).
			INNER_JOIN(Language, Film.LanguageID.EQ(Language.LanguageID)).
			SELECT(Inventory.AllColumns, Film.AllColumns, Store.AllColumns, Language.AllColumns).
			WHERE(Inventory.InventoryID.EQ(Int(1)))

		type Language3 model.Language

		dest := struct {
			model.Inventory
			Film struct {
				model.Film

				Language  model.Language
				Language2 *model.Language `alias:"Language.*"`
				Language3 *Language3      `alias:"language"`
				Lang      struct {
					model.Language
				}
				Lang2 *struct {
					model.Language
				}
			}
			Store model.Store
		}{}

		err := query.Query(db, &dest)

		require.NoError(t, err)
		testutils.AssertDeepEqual(t, dest.Inventory, inventory1)
		testutils.AssertDeepEqual(t, dest.Film.Film, film1)
		testutils.AssertDeepEqual(t, dest.Store, store1)
		testutils.AssertDeepEqual(t, dest.Film.Language, language1)
		testutils.AssertDeepEqual(t, dest.Film.Lang.Language, language1)
		testutils.AssertDeepEqual(t, dest.Film.Lang2.Language, language1)
		testutils.AssertDeepEqual(t, dest.Film.Language2, &language1)
		testutils.AssertDeepEqual(t, model.Language(*dest.Film.Language3), language1)
	})
}

func TestScanToSlice(t *testing.T) {

	t.Run("slice of structs", func(t *testing.T) {
		query := Inventory.
			SELECT(Inventory.AllColumns).
			ORDER_BY(Inventory.InventoryID).
			LIMIT(10)

		t.Run("slice od inventory", func(t *testing.T) {
			dest := []model.Inventory{}

			err := query.Query(db, &dest)

			require.NoError(t, err)
			require.Equal(t, len(dest), 10)
			testutils.AssertDeepEqual(t, dest[0], inventory1)
			testutils.AssertDeepEqual(t, dest[1], inventory2)
		})

		t.Run("slice of ints", func(t *testing.T) {
			var dest []int32

			err := query.Query(db, &dest)
			require.NoError(t, err)
			testutils.AssertDeepEqual(t, dest, []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})

		})

		t.Run("slice type convertible", func(t *testing.T) {
			var dest []int

			err := query.Query(db, &dest)
			require.NoError(t, err)
		})

		t.Run("slice type mismatch", func(t *testing.T) {
			var dest []bool

			err := query.Query(db, &dest)
			require.Error(t, err)
			require.EqualError(t, err, `jet: can't append int64 to []bool slice: can't assign int64(2) to bool`)
		})
	})

	t.Run("slice of complex structs", func(t *testing.T) {
		query := Inventory.
			INNER_JOIN(Film, Inventory.FilmID.EQ(Film.FilmID)).
			INNER_JOIN(Store, Inventory.StoreID.EQ(Store.StoreID)).
			SELECT(
				Inventory.AllColumns,
				Film.AllColumns,
				Store.AllColumns,
			).
			ORDER_BY(Inventory.InventoryID).
			LIMIT(10)

		t.Run("struct with slice of ints", func(t *testing.T) {
			var dest struct {
				model.Film
				IDs []int32 `alias:"inventory.inventory_id"`
			}

			err := query.Query(db, &dest)

			require.NoError(t, err)
			testutils.AssertDeepEqual(t, dest.Film, film1)
			testutils.AssertDeepEqual(t, dest.IDs, []int32{1, 2, 3, 4, 5, 6, 7, 8})
		})

		t.Run("slice of structs with slice of ints", func(t *testing.T) {
			var dest []struct {
				model.Film
				IDs []int32 `alias:"inventory.inventory_id"`
			}

			err := query.Query(db, &dest)

			require.NoError(t, err)
			require.Equal(t, len(dest), 2)
			testutils.AssertDeepEqual(t, dest[0].Film, film1)
			testutils.AssertDeepEqual(t, dest[0].IDs, []int32{1, 2, 3, 4, 5, 6, 7, 8})
			testutils.AssertDeepEqual(t, dest[1].Film, film2)
			testutils.AssertDeepEqual(t, dest[1].IDs, []int32{9, 10})
		})

		t.Run("slice of structs with slice of pointer to ints", func(t *testing.T) {
			var dest []struct {
				model.Film
				IDs []*int32 `alias:"inventory.inventory_id"`
			}

			err := query.Query(db, &dest)

			require.NoError(t, err)
			require.Equal(t, len(dest), 2)
			testutils.AssertDeepEqual(t, dest[0].Film, film1)
			testutils.AssertDeepEqual(t, dest[0].IDs, []*int32{testutils.PtrOf(int32(1)), testutils.PtrOf(int32(2)), testutils.PtrOf(int32(3)), testutils.PtrOf(int32(4)),
				testutils.PtrOf(int32(5)), testutils.PtrOf(int32(6)), testutils.PtrOf(int32(7)), testutils.PtrOf(int32(8))})
			testutils.AssertDeepEqual(t, dest[1].Film, film2)
			testutils.AssertDeepEqual(t, dest[1].IDs, []*int32{testutils.PtrOf(int32(9)), testutils.PtrOf(int32(10))})
		})

		t.Run("complex struct 1", func(t *testing.T) {
			dest := []struct {
				model.Inventory
				model.Film
				model.Store
			}{}

			err := query.Query(db, &dest)

			require.NoError(t, err)
			require.Equal(t, len(dest), 10)
			testutils.AssertDeepEqual(t, dest[0].Inventory, inventory1)
			testutils.AssertDeepEqual(t, dest[0].Film, film1)
			testutils.AssertDeepEqual(t, dest[0].Store, store1)

			testutils.AssertDeepEqual(t, dest[1].Inventory, inventory2)
		})

		t.Run("complex struct 2", func(t *testing.T) {
			var dest []struct {
				*model.Inventory
				model.Film
				*model.Store
			}

			err := query.Query(db, &dest)

			require.NoError(t, err)
			require.Equal(t, len(dest), 10)
			testutils.AssertDeepEqual(t, dest[0].Inventory, &inventory1)
			testutils.AssertDeepEqual(t, dest[0].Film, film1)
			testutils.AssertDeepEqual(t, dest[0].Store, &store1)

			testutils.AssertDeepEqual(t, dest[1].Inventory, &inventory2)
		})

		t.Run("complex struct 3", func(t *testing.T) {
			var dest []struct {
				Inventory model.Inventory
				Film      *model.Film
				Store     struct {
					*model.Store
				}
			}

			err := query.Query(db, &dest)

			require.NoError(t, err)
			require.Equal(t, len(dest), 10)
			testutils.AssertDeepEqual(t, dest[0].Inventory, inventory1)
			testutils.AssertDeepEqual(t, dest[0].Film, &film1)
			testutils.AssertDeepEqual(t, dest[0].Store.Store, &store1)

			testutils.AssertDeepEqual(t, dest[1].Inventory, inventory2)
		})

		t.Run("complex struct 4", func(t *testing.T) {
			var dest []struct {
				model.Film

				Inventories []struct {
					model.Inventory
					model.Store
				}
			}

			err := query.Query(db, &dest)

			require.NoError(t, err)
			require.Equal(t, len(dest), 2)
			testutils.AssertDeepEqual(t, dest[0].Film, film1)
			testutils.AssertDeepEqual(t, len(dest[0].Inventories), 8)
			testutils.AssertDeepEqual(t, dest[0].Inventories[0].Inventory, inventory1)
			testutils.AssertDeepEqual(t, dest[0].Inventories[0].Store, store1)
		})

		t.Run("complex struct 5", func(t *testing.T) {
			var dest []struct {
				model.Film

				Inventories []struct {
					model.Inventory

					Rentals  *[]model.Rental
					Rentals2 []model.Rental
				}
			}

			err := query.Query(db, &dest)

			require.NoError(t, err)

			require.Equal(t, len(dest), 2)
			testutils.AssertDeepEqual(t, dest[0].Film, film1)
			require.Equal(t, len(dest[0].Inventories), 8)
			testutils.AssertDeepEqual(t, dest[0].Inventories[0].Inventory, inventory1)
			require.True(t, dest[0].Inventories[0].Rentals == nil)
			require.True(t, dest[0].Inventories[0].Rentals2 == nil)
		})
	})

	t.Run("slice of complex structs 2", func(t *testing.T) {
		query := Country.
			INNER_JOIN(City, City.CountryID.EQ(Country.CountryID)).
			INNER_JOIN(Address, Address.CityID.EQ(City.CityID)).
			INNER_JOIN(Customer, Customer.AddressID.EQ(Address.AddressID)).
			SELECT(Country.AllColumns, City.AllColumns, Address.AllColumns, Customer.AllColumns).
			ORDER_BY(Country.CountryID.ASC(), City.CityID.ASC(), Address.AddressID.ASC(), Customer.CustomerID.ASC()).
			LIMIT(1000)

		t.Run("dest1", func(t *testing.T) {
			var dest []struct {
				model.Country

				Cities []struct {
					model.City

					Adresses []struct {
						model.Address

						Customer model.Customer
					}
				}
			}

			err := query.Query(db, &dest)

			require.NoError(t, err)
			require.Equal(t, len(dest), 108)
			testutils.AssertDeepEqual(t, dest[100].Country, countryUk)
			require.Equal(t, len(dest[100].Cities), 8)
			testutils.AssertDeepEqual(t, dest[100].Cities[2].City, cityLondon)
			require.Equal(t, len(dest[100].Cities[2].Adresses), 2)
			testutils.AssertDeepEqual(t, dest[100].Cities[2].Adresses[0].Address, address256)
			testutils.AssertDeepEqual(t, dest[100].Cities[2].Adresses[0].Customer, customer256)
			testutils.AssertDeepEqual(t, dest[100].Cities[2].Adresses[1].Address, addres517)
			testutils.AssertDeepEqual(t, dest[100].Cities[2].Adresses[1].Customer, customer512)
		})

		t.Run("dest1", func(t *testing.T) {
			var dest []*struct {
				*model.Country

				Cities []*struct {
					*model.City

					Adresses *[]*struct {
						*model.Address

						Customer *model.Customer
					}
				}
			}

			err := query.Query(db, &dest)

			require.NoError(t, err)
			require.Equal(t, len(dest), 108)
			testutils.AssertDeepEqual(t, dest[100].Country, &countryUk)
			require.Equal(t, len(dest[100].Cities), 8)
			testutils.AssertDeepEqual(t, dest[100].Cities[2].City, &cityLondon)
			require.Equal(t, len(*dest[100].Cities[2].Adresses), 2)
			testutils.AssertDeepEqual(t, (*dest[100].Cities[2].Adresses)[0].Address, &address256)
			testutils.AssertDeepEqual(t, (*dest[100].Cities[2].Adresses)[0].Customer, &customer256)
			testutils.AssertDeepEqual(t, (*dest[100].Cities[2].Adresses)[1].Address, &addres517)
			testutils.AssertDeepEqual(t, (*dest[100].Cities[2].Adresses)[1].Customer, &customer512)
		})

	})

	t.Run("dest1", func(t *testing.T) {
		var dest []*struct {
			*model.Country

			Cities []**struct {
				*model.City
			}
		}

		testutils.AssertQueryPanicErr(t, oneInventoryQuery, db, &dest, "jet: unsupported slice element type at 'Cities []**struct { *model.City }'")
	})
}

func TestStructScanErrNoRows(t *testing.T) {
	query := SELECT(Customer.AllColumns).
		FROM(Customer).
		WHERE(Customer.CustomerID.EQ(Int(-1)))

	customer := model.Customer{}

	err := query.Query(db, &customer)

	require.Error(t, err, qrm.ErrNoRows.Error())
}

func TestStructScanAllNull(t *testing.T) {
	query := SELECT(NULL.AS("null1"), NULL.AS("null2"))

	dest := struct {
		Null1 *int
		Null2 *int
	}{}

	err := query.Query(db, &dest)

	require.NoError(t, err)
	testutils.AssertDeepEqual(t, dest, struct {
		Null1 *int
		Null2 *int
	}{})
}

func TestRowsScan(t *testing.T) {

	stmt := SELECT(
		Inventory.AllColumns,
	).FROM(
		Inventory,
	).ORDER_BY(
		Inventory.InventoryID.ASC(),
	)

	rows, err := stmt.Rows(context.Background(), db)
	require.NoError(t, err)

	for rows.Next() {
		var inventory model.Inventory
		err = rows.Scan(&inventory)
		require.NoError(t, err)

		require.NotEqual(t, inventory.InventoryID, int32(0))
		require.NotEqual(t, inventory.FilmID, int16(0))
		require.NotEqual(t, inventory.StoreID, int16(0))
		require.NotEqual(t, inventory.LastUpdate, time.Time{})

		if inventory.InventoryID == 2103 {
			require.Equal(t, inventory.FilmID, int16(456))
			require.Equal(t, inventory.StoreID, int16(2))
			require.Equal(t, inventory.LastUpdate.Format(time.RFC3339), "2006-02-15T10:09:17Z")
		}
	}

	err = rows.Close()
	require.NoError(t, err)
	err = rows.Err()
	require.NoError(t, err)

	requireLogged(t, stmt)
	requireQueryLogged(t, stmt, 0)
}

func TestScanNullColumn(t *testing.T) {
	stmt := SELECT(
		Address.AllColumns,
	).FROM(
		Address,
	).WHERE(
		Address.Address2.IS_NULL(),
	)

	var dest []model.Address

	err := stmt.Query(db, &dest)
	require.NoError(t, err)
	testutils.AssertJSON(t, dest, `
[
	{
		"AddressID": 1,
		"Address": "47 MySakila Drive",
		"Address2": null,
		"District": "Alberta",
		"CityID": 300,
		"PostalCode": "",
		"Phone": "",
		"LastUpdate": "2006-02-15T09:45:30Z"
	},
	{
		"AddressID": 2,
		"Address": "28 MySQL Boulevard",
		"Address2": null,
		"District": "QLD",
		"CityID": 576,
		"PostalCode": "",
		"Phone": "",
		"LastUpdate": "2006-02-15T09:45:30Z"
	},
	{
		"AddressID": 3,
		"Address": "23 Workhaven Lane",
		"Address2": null,
		"District": "Alberta",
		"CityID": 300,
		"PostalCode": "",
		"Phone": "14033335568",
		"LastUpdate": "2006-02-15T09:45:30Z"
	},
	{
		"AddressID": 4,
		"Address": "1411 Lillydale Drive",
		"Address2": null,
		"District": "QLD",
		"CityID": 576,
		"PostalCode": "",
		"Phone": "6172235589",
		"LastUpdate": "2006-02-15T09:45:30Z"
	}
]
`)
}

func TestRowsScanSetZeroValue(t *testing.T) {
	stmt := SELECT(
		Rental.AllColumns,
	).FROM(
		Rental,
	).WHERE(
		Rental.RentalID.IN(Int(16049), Int(15966)),
	).ORDER_BY(
		Rental.RentalID.DESC(),
	)

	rows, err := stmt.Rows(context.Background(), db)
	require.NoError(t, err)

	defer rows.Close()

	// destination object is used as destination for all rows scan.
	// this tests checks that ReturnedDate is set to nil with the second call
	// check qrm.setZeroValue
	var dest model.Rental

	for rows.Next() {
		err := rows.Scan(&dest)
		require.NoError(t, err)

		if dest.RentalID == 16049 {
			testutils.AssertJSON(t, dest, `
{
	"RentalID": 16049,
	"RentalDate": "2005-08-23T22:50:12Z",
	"InventoryID": 2666,
	"CustomerID": 393,
	"ReturnDate": "2005-08-30T01:01:12Z",
	"StaffID": 2,
	"LastUpdate": "2006-02-16T02:30:53Z"
}
`)
		} else {
			testutils.AssertJSON(t, dest, `
{
	"RentalID": 15966,
	"RentalDate": "2006-02-14T15:16:03Z",
	"InventoryID": 4472,
	"CustomerID": 374,
	"ReturnDate": null,
	"StaffID": 1,
	"LastUpdate": "2006-02-16T02:30:53Z"
}
`)
		}
	}

	err = rows.Close()
	require.NoError(t, err)
	err = rows.Err()
	require.NoError(t, err)
}

func TestScanNumericToFloat(t *testing.T) {
	type Number struct {
		Float32 float32
		Float64 float64
	}

	numeric := CAST(Decimal("1234567890.111")).AS_NUMERIC()

	stmt := SELECT(
		numeric.AS("number.float32"),
		numeric.AS("number.float64"),
	)

	var number Number
	err := stmt.Query(db, &number)
	require.NoError(t, err)
	require.Equal(t, number.Float32, float32(1.234568e+09))
	require.Equal(t, number.Float64, float64(1.234567890111e+09))
}

func TestScanNumericToIntegerError(t *testing.T) {

	var dest struct {
		Integer int32
	}

	err := SELECT(
		CAST(Decimal("1234567890.111")).AS_NUMERIC().AS("integer"),
	).Query(db, &dest)

	require.Error(t, err)

	if isPgxDriver() {
		require.Contains(t, err.Error(), `jet: can't assign string("1234567890.111") to 'Integer int32': converting driver.Value type string ("1234567890.111") to a int64: invalid syntax`)
	} else {
		require.Contains(t, err.Error(), `jet: can't assign []uint8("1234567890.111") to 'Integer int32': converting driver.Value type []uint8 ("1234567890.111") to a int64: invalid syntax`)
	}

}

func TestScanIntoCustomBaseTypes(t *testing.T) {

	type MyUint8 uint8
	type MyUint16 uint16
	type MyUint32 uint32
	type MyInt16 int16
	type MyFloat32 float32
	type MyFloat64 float64
	type MyString string
	type MyTime = time.Time

	type film struct {
		FilmID          MyUint16 `sql:"primary_key"`
		Title           MyString
		Description     *MyString
		ReleaseYear     *MyInt16
		LanguageID      MyUint8
		RentalDuration  MyUint8
		RentalRate      MyFloat32
		Length          *MyUint32
		ReplacementCost MyFloat64
		Rating          *model.MpaaRating
		LastUpdate      MyTime
		SpecialFeatures *MyString
		Fulltext        MyString
	}

	stmt := SELECT(
		Film.AllColumns,
	).FROM(
		Film,
	).ORDER_BY(
		Film.FilmID.ASC(),
	).LIMIT(3)

	var films []model.Film

	err := stmt.Query(db, &films)
	require.NoError(t, err)

	var myFilms []film

	err = stmt.Query(db, &myFilms)
	require.NoError(t, err)

	require.Equal(t, testutils.ToJSON(films), testutils.ToJSON(myFilms))
}

// QueryContext panic when the scanned value is nil and the destination is a slice of primitive
// https://github.com/go-jet/jet/issues/91
func TestScanToPrimitiveElementsSlice(t *testing.T) {
	tx, err := db.Begin()
	require.NoError(t, err)
	defer tx.Rollback()

	// add actor without associated film (so that destination Title array is NULL).
	_, err = Actor.INSERT().
		MODEL(
			model.Actor{
				ActorID:    201,
				FirstName:  "Brigitte",
				LastName:   "Bardot",
				LastUpdate: time.Time{},
			},
		).Exec(tx)
	require.NoError(t, err)

	stmt := SELECT(
		Actor.ActorID.AS("actor_id"),
		Film.Title.AS("title"),
	).FROM(
		Actor.
			LEFT_JOIN(FilmActor, Actor.ActorID.EQ(FilmActor.ActorID)).
			LEFT_JOIN(Film, Film.FilmID.EQ(FilmActor.FilmID)),
	).WHERE(
		Actor.ActorID.GT(Int(199)),
	).ORDER_BY(Actor.ActorID.DESC())

	var dest []struct {
		ActorID int `sql:"primary_key"`
		Title   []string
	}

	err = stmt.Query(tx, &dest)
	require.NoError(t, err)
	require.Equal(t, dest[0].ActorID, 201)
	require.Equal(t, dest[0].Title, []string(nil))
	require.Equal(t, dest[1].ActorID, 200)
	require.Len(t, dest[1].Title, 20)
}

// https://github.com/go-jet/jet/issues/127
func TestValuerTypeDebugSQL(t *testing.T) {
	type customer struct {
		CustomerID null.Int32 `sql:"primary_key"`
		StoreID    null.Int16
		FirstName  null.String
		LastName   string
		Email      null.String
		AddressID  int16
		Activebool null.Bool
		CreateDate null.Time
		LastUpdate null.Time
		Active     null.Int8
	}

	stmt := Customer.INSERT().
		MODEL(
			customer{
				CustomerID: null.Int32From(1234),
				StoreID:    null.Int16From(0),
				FirstName:  null.StringFrom("Joe"),
				LastName:   "",
				Email:      null.StringFromPtr(nil),
				AddressID:  1,
				Activebool: null.BoolFrom(true),
				CreateDate: null.TimeFrom(time.Date(2020, 2, 2, 10, 0, 0, 0, time.UTC)),
				LastUpdate: null.TimeFromPtr(nil),
				Active:     null.Int8From(1),
			},
		)

	testutils.AssertDebugStatementSql(t, stmt, `
INSERT INTO dvds.customer
VALUES (1234, 0, 'Joe', '', NULL, 1, TRUE, '2020-02-02 10:00:00Z', NULL, 1);
`)
	testutils.AssertExecAndRollback(t, stmt, db)
}

var address256 = model.Address{
	AddressID:  256,
	Address:    "1497 Yuzhou Drive",
	Address2:   testutils.PtrOf(""),
	District:   "England",
	CityID:     312,
	PostalCode: testutils.PtrOf("3433"),
	Phone:      "246810237916",
	LastUpdate: *testutils.TimestampWithoutTimeZone("2006-02-15 09:45:30", 0),
}

var addres517 = model.Address{
	AddressID:  517,
	Address:    "548 Uruapan Street",
	Address2:   testutils.PtrOf(""),
	District:   "Ontario",
	CityID:     312,
	PostalCode: testutils.PtrOf("35653"),
	Phone:      "879347453467",
	LastUpdate: *testutils.TimestampWithoutTimeZone("2006-02-15 09:45:30", 0),
}

var customer256 = model.Customer{
	CustomerID: 252,
	StoreID:    2,
	FirstName:  "Mattie",
	LastName:   "Hoffman",
	Email:      testutils.PtrOf("mattie.hoffman@sakilacustomer.org"),
	AddressID:  256,
	Activebool: true,
	CreateDate: *testutils.TimestampWithoutTimeZone("2006-02-14 00:00:00", 0),
	LastUpdate: testutils.TimestampWithoutTimeZone("2013-05-26 14:49:45.738", 0),
	Active:     testutils.PtrOf(int32(1)),
}

var customer512 = model.Customer{
	CustomerID: 512,
	StoreID:    1,
	FirstName:  "Cecil",
	LastName:   "Vines",
	Email:      testutils.PtrOf("cecil.vines@sakilacustomer.org"),
	AddressID:  517,
	Activebool: true,
	CreateDate: *testutils.TimestampWithoutTimeZone("2006-02-14 00:00:00", 0),
	LastUpdate: testutils.TimestampWithoutTimeZone("2013-05-26 14:49:45.738", 0),
	Active:     testutils.PtrOf(int32(1)),
}

var countryUk = model.Country{
	CountryID:  102,
	Country:    "United Kingdom",
	LastUpdate: *testutils.TimestampWithoutTimeZone("2006-02-15 09:44:00", 0),
}

var cityLondon = model.City{
	CityID:     312,
	City:       "London",
	CountryID:  102,
	LastUpdate: *testutils.TimestampWithoutTimeZone("2006-02-15 09:45:25", 0),
}

var inventory1 = model.Inventory{
	InventoryID: 1,
	FilmID:      1,
	StoreID:     1,
	LastUpdate:  *testutils.TimestampWithoutTimeZone("2006-02-15 10:09:17", 0),
}

var inventory2 = model.Inventory{
	InventoryID: 2,
	FilmID:      1,
	StoreID:     1,
	LastUpdate:  *testutils.TimestampWithoutTimeZone("2006-02-15 10:09:17", 0),
}

var film1 = model.Film{
	FilmID:          1,
	Title:           "Academy Dinosaur",
	Description:     testutils.PtrOf("A Epic Drama of a Feminist And a Mad Scientist who must Battle a Teacher in The Canadian Rockies"),
	ReleaseYear:     testutils.PtrOf(int32(2006)),
	LanguageID:      1,
	RentalDuration:  6,
	RentalRate:      0.99,
	Length:          testutils.PtrOf(int16(86)),
	ReplacementCost: 20.99,
	Rating:          &pgRating,
	LastUpdate:      *testutils.TimestampWithoutTimeZone("2013-05-26 14:50:58.951", 3),
	SpecialFeatures: testutils.PtrOf("{\"Deleted Scenes\",\"Behind the Scenes\"}"),
	Fulltext:        "'academi':1 'battl':15 'canadian':20 'dinosaur':2 'drama':5 'epic':4 'feminist':8 'mad':11 'must':14 'rocki':21 'scientist':12 'teacher':17",
}

var film2 = model.Film{
	FilmID:          2,
	Title:           "Ace Goldfinger",
	Description:     testutils.PtrOf("A Astounding Epistle of a Database Administrator And a Explorer who must Find a Car in Ancient China"),
	ReleaseYear:     testutils.PtrOf(int32(2006)),
	LanguageID:      1,
	RentalDuration:  3,
	RentalRate:      4.99,
	Length:          testutils.PtrOf(int16(48)),
	ReplacementCost: 12.99,
	Rating:          &gRating,
	LastUpdate:      *testutils.TimestampWithoutTimeZone("2013-05-26 14:50:58.951", 3),
	SpecialFeatures: testutils.PtrOf(`{Trailers,"Deleted Scenes"}`),
	Fulltext:        `'ace':1 'administr':9 'ancient':19 'astound':4 'car':17 'china':20 'databas':8 'epistl':5 'explor':12 'find':15 'goldfing':2 'must':14`,
}

var store1 = model.Store{
	StoreID:        1,
	ManagerStaffID: 1,
	AddressID:      1,
	LastUpdate:     *testutils.TimestampWithoutTimeZone("2006-02-15 09:57:12", 0),
}

var pgRating = model.MpaaRating_Pg
var gRating = model.MpaaRating_G

var language1 = model.Language{
	LanguageID: 1,
	Name:       "English             ",
	LastUpdate: *testutils.TimestampWithoutTimeZone("2006-02-15 10:02:19", 0),
}

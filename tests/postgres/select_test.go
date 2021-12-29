package postgres

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/go-jet/jet/v2/internal/testutils"
	. "github.com/go-jet/jet/v2/postgres"
	"github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/dvds/enum"
	"github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/dvds/model"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/dvds/table"
	"github.com/go-jet/jet/v2/tests/.gentestdata/jetdb/dvds/view"
)

func TestSelect_ScanToStruct(t *testing.T) {
	expectedSQL := `
SELECT DISTINCT actor.actor_id AS "actor.actor_id",
     actor.first_name AS "actor.first_name",
     actor.last_name AS "actor.last_name",
     actor.last_update AS "actor.last_update"
FROM dvds.actor
WHERE actor.actor_id = 2;
`

	query := Actor.
		SELECT(Actor.AllColumns).
		DISTINCT().
		WHERE(Actor.ActorID.EQ(Int(2)))

	testutils.AssertDebugStatementSql(t, query, expectedSQL, int64(2))

	actor := model.Actor{}
	err := query.Query(db, &actor)

	require.NoError(t, err)

	expectedActor := model.Actor{
		ActorID:    2,
		FirstName:  "Nick",
		LastName:   "Wahlberg",
		LastUpdate: *testutils.TimestampWithoutTimeZone("2013-05-26 14:47:57.62", 2),
	}

	testutils.AssertDeepEqual(t, actor, expectedActor)

	requireLogged(t, query)
}

func TestClassicSelect(t *testing.T) {
	expectedSQL := `
SELECT payment.payment_id AS "payment.payment_id",
     payment.customer_id AS "payment.customer_id",
     payment.staff_id AS "payment.staff_id",
     payment.rental_id AS "payment.rental_id",
     payment.amount AS "payment.amount",
     payment.payment_date AS "payment.payment_date",
     customer.customer_id AS "customer.customer_id",
     customer.store_id AS "customer.store_id",
     customer.first_name AS "customer.first_name",
     customer.last_name AS "customer.last_name",
     customer.email AS "customer.email",
     customer.address_id AS "customer.address_id",
     customer.activebool AS "customer.activebool",
     customer.create_date AS "customer.create_date",
     customer.last_update AS "customer.last_update",
     customer.active AS "customer.active"
FROM dvds.payment
     INNER JOIN dvds.customer ON (payment.customer_id = customer.customer_id)
ORDER BY payment.payment_id ASC
LIMIT 30;
`

	query := SELECT(
		Payment.AllColumns,
		Customer.AllColumns,
	).FROM(
		Payment.
			INNER_JOIN(Customer, Payment.CustomerID.EQ(Customer.CustomerID)),
	).ORDER_BY(
		Payment.PaymentID.ASC(),
	).LIMIT(30)

	testutils.AssertDebugStatementSql(t, query, expectedSQL, int64(30))

	dest := []model.Payment{}

	err := query.Query(db, &dest)

	require.NoError(t, err)
	require.Equal(t, len(dest), 30)

	requireLogged(t, query)
}

func TestSelect_ScanToSlice(t *testing.T) {
	expectedSQL := `
SELECT customer.customer_id AS "customer.customer_id",
     customer.store_id AS "customer.store_id",
     customer.first_name AS "customer.first_name",
     customer.last_name AS "customer.last_name",
     customer.email AS "customer.email",
     customer.address_id AS "customer.address_id",
     customer.activebool AS "customer.activebool",
     customer.create_date AS "customer.create_date",
     customer.last_update AS "customer.last_update",
     customer.active AS "customer.active"
FROM dvds.customer
ORDER BY customer.customer_id ASC;
`
	customers := []model.Customer{}

	query := Customer.SELECT(Customer.AllColumns).ORDER_BY(Customer.CustomerID.ASC())

	testutils.AssertDebugStatementSql(t, query, expectedSQL)

	err := query.Query(db, &customers)
	require.NoError(t, err)

	require.Equal(t, len(customers), 599)

	testutils.AssertDeepEqual(t, customer0, customers[0])
	testutils.AssertDeepEqual(t, customer1, customers[1])
	testutils.AssertDeepEqual(t, lastCustomer, customers[598])

	requireLogged(t, query)
}

func TestSelectAndUnionInProjection(t *testing.T) {
	expectedSQL := `
SELECT payment.payment_id AS "payment.payment_id",
     (
          SELECT customer.customer_id AS "customer.customer_id"
          FROM dvds.customer
          LIMIT 1
     ),
     (
          (
               SELECT payment.payment_id AS "payment.payment_id"
               FROM dvds.payment
               LIMIT 1
               OFFSET 10
          )
          UNION
          (
               SELECT payment.payment_id AS "payment.payment_id"
               FROM dvds.payment
               LIMIT 1
               OFFSET 2
          )
          LIMIT 1
     )
FROM dvds.payment
LIMIT 12;
`

	query := Payment.
		SELECT(
			Payment.PaymentID,
			Customer.SELECT(Customer.CustomerID).LIMIT(1),
			UNION(
				Payment.SELECT(Payment.PaymentID).LIMIT(1).OFFSET(10),
				Payment.SELECT(Payment.PaymentID).LIMIT(1).OFFSET(2),
			).LIMIT(1),
		).
		LIMIT(12)

	//fmt.Println(query.DebugSql())

	testutils.AssertDebugStatementSql(t, query, expectedSQL, int64(1), int64(1), int64(10), int64(1), int64(2), int64(1), int64(12))

	dest := []struct{}{}
	err := query.Query(db, &dest)
	require.NoError(t, err)
}

func TestJoinQueryStruct(t *testing.T) {

	expectedSQL := `
SELECT film_actor.actor_id AS "film_actor.actor_id",
     film_actor.film_id AS "film_actor.film_id",
     film_actor.last_update AS "film_actor.last_update",
     film.film_id AS "film.film_id",
     film.title AS "film.title",
     film.description AS "film.description",
     film.release_year AS "film.release_year",
     film.language_id AS "film.language_id",
     film.rental_duration AS "film.rental_duration",
     film.rental_rate AS "film.rental_rate",
     film.length AS "film.length",
     film.replacement_cost AS "film.replacement_cost",
     film.rating AS "film.rating",
     film.last_update AS "film.last_update",
     film.special_features AS "film.special_features",
     film.fulltext AS "film.fulltext",
     language.language_id AS "language.language_id",
     language.name AS "language.name",
     language.last_update AS "language.last_update",
     actor.actor_id AS "actor.actor_id",
     actor.first_name AS "actor.first_name",
     actor.last_name AS "actor.last_name",
     actor.last_update AS "actor.last_update",
     inventory.inventory_id AS "inventory.inventory_id",
     inventory.film_id AS "inventory.film_id",
     inventory.store_id AS "inventory.store_id",
     inventory.last_update AS "inventory.last_update",
     rental.rental_id AS "rental.rental_id",
     rental.rental_date AS "rental.rental_date",
     rental.inventory_id AS "rental.inventory_id",
     rental.customer_id AS "rental.customer_id",
     rental.return_date AS "rental.return_date",
     rental.staff_id AS "rental.staff_id",
     rental.last_update AS "rental.last_update"
FROM dvds.film_actor
     INNER JOIN dvds.actor ON (film_actor.actor_id = actor.actor_id)
     INNER JOIN dvds.film ON (film_actor.film_id = film.film_id)
     INNER JOIN dvds.language ON (film.language_id = language.language_id)
     INNER JOIN dvds.inventory ON (inventory.film_id = film.film_id)
     INNER JOIN dvds.rental ON (rental.inventory_id = inventory.inventory_id)
ORDER BY film.film_id ASC
LIMIT 1000;
`
	for i := 0; i < 2; i++ {
		query := FilmActor.
			INNER_JOIN(Actor, FilmActor.ActorID.EQ(Actor.ActorID)).
			INNER_JOIN(Film, FilmActor.FilmID.EQ(Film.FilmID)).
			INNER_JOIN(Language, Film.LanguageID.EQ(Language.LanguageID)).
			INNER_JOIN(Inventory, Inventory.FilmID.EQ(Film.FilmID)).
			INNER_JOIN(Rental, Rental.InventoryID.EQ(Inventory.InventoryID)).
			SELECT(
				FilmActor.AllColumns,
				Film.AllColumns,
				Language.AllColumns,
				Actor.AllColumns,
				Inventory.AllColumns,
				Rental.AllColumns,
			).
			//WHERE(FilmActor.ActorID.GtEqL(1).AND(FilmActor.ActorID.LtEqL(2))).
			ORDER_BY(Film.FilmID.ASC()).
			LIMIT(1000)

		testutils.AssertDebugStatementSql(t, query, expectedSQL, int64(1000))

		var languageActorFilm []struct {
			model.Language

			Films []struct {
				model.Film
				Actors []struct {
					model.Actor
				}

				Inventory []struct {
					model.Inventory

					Rental []model.Rental
				}
			}
		}

		err := query.Query(db, &languageActorFilm)

		require.NoError(t, err)
		require.Equal(t, len(languageActorFilm), 1)
		require.Equal(t, len(languageActorFilm[0].Films), 10)
		require.Equal(t, len(languageActorFilm[0].Films[0].Actors), 10)
	}
}

func TestJoinQuerySlice(t *testing.T) {
	expectedSQL := `
SELECT language.language_id AS "language.language_id",
     language.name AS "language.name",
     language.last_update AS "language.last_update",
     film.film_id AS "film.film_id",
     film.title AS "film.title",
     film.description AS "film.description",
     film.release_year AS "film.release_year",
     film.language_id AS "film.language_id",
     film.rental_duration AS "film.rental_duration",
     film.rental_rate AS "film.rental_rate",
     film.length AS "film.length",
     film.replacement_cost AS "film.replacement_cost",
     film.rating AS "film.rating",
     film.last_update AS "film.last_update",
     film.special_features AS "film.special_features",
     film.fulltext AS "film.fulltext"
FROM dvds.film
     INNER JOIN dvds.language ON (film.language_id = language.language_id)
WHERE film.rating = 'NC-17'
LIMIT 15;
`

	type FilmsPerLanguage struct {
		Language *model.Language
		Film     []model.Film
	}

	filmsPerLanguage := []FilmsPerLanguage{}
	limit := 15

	query := Film.
		INNER_JOIN(Language, Film.LanguageID.EQ(Language.LanguageID)).
		SELECT(Language.AllColumns, Film.AllColumns).
		WHERE(Film.Rating.EQ(enum.MpaaRating.Nc17)).
		LIMIT(15)

	testutils.AssertDebugStatementSql(t, query, expectedSQL, int64(15))

	err := query.Query(db, &filmsPerLanguage)

	require.NoError(t, err)
	require.Equal(t, len(filmsPerLanguage), 1)
	require.Equal(t, len(filmsPerLanguage[0].Film), limit)

	englishFilms := filmsPerLanguage[0]

	require.Equal(t, *englishFilms.Film[0].Rating, model.MpaaRating_Nc17)

	filmsPerLanguageWithPtrs := []*FilmsPerLanguage{}
	err = query.Query(db, &filmsPerLanguageWithPtrs)

	require.NoError(t, err)
	require.Equal(t, len(filmsPerLanguage), 1)
	require.Equal(t, len(filmsPerLanguage[0].Film), limit)
}

func TestExecution1(t *testing.T) {
	stmt := City.
		INNER_JOIN(Address, Address.CityID.EQ(City.CityID)).
		INNER_JOIN(Customer, Customer.AddressID.EQ(Address.AddressID)).
		SELECT(
			City.CityID,
			City.City,
			Address.AddressID,
			Address.Address,
			Customer.CustomerID,
			Customer.LastName,
		).
		WHERE(City.City.EQ(String("London")).OR(City.City.EQ(String("York")))).
		ORDER_BY(City.CityID, Address.AddressID, Customer.CustomerID)

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT city.city_id AS "city.city_id",
     city.city AS "city.city",
     address.address_id AS "address.address_id",
     address.address AS "address.address",
     customer.customer_id AS "customer.customer_id",
     customer.last_name AS "customer.last_name"
FROM dvds.city
     INNER JOIN dvds.address ON (address.city_id = city.city_id)
     INNER JOIN dvds.customer ON (customer.address_id = address.address_id)
WHERE (city.city = 'London') OR (city.city = 'York')
ORDER BY city.city_id, address.address_id, customer.customer_id;
`, "London", "York")

	var dest []struct {
		model.City

		Customers []struct {
			model.Customer

			Address model.Address
		}
	}

	err := stmt.Query(db, &dest)

	require.NoError(t, err)

	require.Equal(t, len(dest), 2)
	require.Equal(t, dest[0].City.City, "London")
	require.Equal(t, dest[1].City.City, "York")
	require.Equal(t, len(dest[0].Customers), 2)
	require.Equal(t, dest[0].Customers[0].LastName, "Hoffman")
	require.Equal(t, dest[0].Customers[1].LastName, "Vines")

}

func TestExecution2(t *testing.T) {

	type MyAddress struct {
		ID          int32 `sql:"primary_key"`
		AddressLine string
	}

	type MyCustomer struct {
		ID       int32 `sql:"primary_key"`
		LastName *string

		Address MyAddress
	}

	type MyCity struct {
		ID   int32 `sql:"primary_key"`
		Name string

		Customers []MyCustomer
	}

	dest := []MyCity{}

	stmt := City.
		INNER_JOIN(Address, Address.CityID.EQ(City.CityID)).
		INNER_JOIN(Customer, Customer.AddressID.EQ(Address.AddressID)).
		SELECT(
			City.CityID.AS("my_city.id"),
			City.City.AS("myCity.Name"),
			Address.AddressID.AS("My_Address.id"),
			Address.Address.AS("my address.address line"),
			Customer.CustomerID.AS("my_customer.id"),
			Customer.LastName.AS("my_customer.last_name"),
		).
		WHERE(City.City.EQ(String("London")).OR(City.City.EQ(String("York")))).
		ORDER_BY(City.CityID, Address.AddressID, Customer.CustomerID)

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT city.city_id AS "my_city.id",
     city.city AS "myCity.Name",
     address.address_id AS "My_Address.id",
     address.address AS "my address.address line",
     customer.customer_id AS "my_customer.id",
     customer.last_name AS "my_customer.last_name"
FROM dvds.city
     INNER JOIN dvds.address ON (address.city_id = city.city_id)
     INNER JOIN dvds.customer ON (customer.address_id = address.address_id)
WHERE (city.city = 'London') OR (city.city = 'York')
ORDER BY city.city_id, address.address_id, customer.customer_id;
`, "London", "York")

	err := stmt.Query(db, &dest)

	require.NoError(t, err)

	require.Equal(t, len(dest), 2)
	require.Equal(t, dest[0].Name, "London")
	require.Equal(t, dest[1].Name, "York")
	require.Equal(t, len(dest[0].Customers), 2)
	require.Equal(t, *dest[0].Customers[0].LastName, "Hoffman")
	require.Equal(t, *dest[0].Customers[1].LastName, "Vines")

}

func TestExecution3(t *testing.T) {

	var dest []struct {
		CityID   int32 `sql:"primary_key"`
		CityName string

		Customers []struct {
			CustomerID int32 `sql:"primary_key"`
			LastName   *string

			Address struct {
				AddressID   int32 `sql:"primary_key"`
				AddressLine string
			}
		}
	}

	stmt := City.
		INNER_JOIN(Address, Address.CityID.EQ(City.CityID)).
		INNER_JOIN(Customer, Customer.AddressID.EQ(Address.AddressID)).
		SELECT(
			City.CityID.AS("city_id"),
			City.City.AS("city_name"),
			Customer.CustomerID.AS("customer_id"),
			Customer.LastName.AS("last_name"),
			Address.AddressID.AS("address_id"),
			Address.Address.AS("address_line"),
		).
		WHERE(City.City.EQ(String("London")).OR(City.City.EQ(String("York")))).
		ORDER_BY(City.CityID, Address.AddressID, Customer.CustomerID)

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT city.city_id AS "city_id",
     city.city AS "city_name",
     customer.customer_id AS "customer_id",
     customer.last_name AS "last_name",
     address.address_id AS "address_id",
     address.address AS "address_line"
FROM dvds.city
     INNER JOIN dvds.address ON (address.city_id = city.city_id)
     INNER JOIN dvds.customer ON (customer.address_id = address.address_id)
WHERE (city.city = 'London') OR (city.city = 'York')
ORDER BY city.city_id, address.address_id, customer.customer_id;
`, "London", "York")

	err := stmt.Query(db, &dest)

	require.NoError(t, err)

	require.Equal(t, len(dest), 2)
	require.Equal(t, dest[0].CityName, "London")
	require.Equal(t, dest[1].CityName, "York")
	require.Equal(t, len(dest[0].Customers), 2)
	require.Equal(t, *dest[0].Customers[0].LastName, "Hoffman")
	require.Equal(t, *dest[0].Customers[1].LastName, "Vines")
}

func TestExecution4(t *testing.T) {

	var dest []struct {
		CityID   int32  `sql:"primary_key" alias:"city.city_id"`
		CityName string `alias:"city.city"`

		Customers []struct {
			CustomerID int32   `sql:"primary_key" alias:"customer_id"`
			LastName   *string `alias:"last_name"`

			Address struct {
				AddressID   int32  `sql:"primary_key" alias:"AddressId"`
				AddressLine string `alias:"address.address"`
			} `alias:"address.*"`
		} `alias:"customer"`
	}

	stmt := City.
		INNER_JOIN(Address, Address.CityID.EQ(City.CityID)).
		INNER_JOIN(Customer, Customer.AddressID.EQ(Address.AddressID)).
		SELECT(
			City.CityID,
			City.City,
			Customer.CustomerID,
			Customer.LastName,
			Address.AddressID,
			Address.Address,
		).
		WHERE(City.City.EQ(String("London")).OR(City.City.EQ(String("York")))).
		ORDER_BY(City.CityID, Address.AddressID, Customer.CustomerID)

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT city.city_id AS "city.city_id",
     city.city AS "city.city",
     customer.customer_id AS "customer.customer_id",
     customer.last_name AS "customer.last_name",
     address.address_id AS "address.address_id",
     address.address AS "address.address"
FROM dvds.city
     INNER JOIN dvds.address ON (address.city_id = city.city_id)
     INNER JOIN dvds.customer ON (customer.address_id = address.address_id)
WHERE (city.city = 'London') OR (city.city = 'York')
ORDER BY city.city_id, address.address_id, customer.customer_id;
`, "London", "York")

	err := stmt.Query(db, &dest)

	require.NoError(t, err)
	require.Equal(t, len(dest), 2)
	testutils.AssertJSON(t, dest, `
[
	{
		"CityID": 312,
		"CityName": "London",
		"Customers": [
			{
				"CustomerID": 252,
				"LastName": "Hoffman",
				"Address": {
					"AddressID": 256,
					"AddressLine": "1497 Yuzhou Drive"
				}
			},
			{
				"CustomerID": 512,
				"LastName": "Vines",
				"Address": {
					"AddressID": 517,
					"AddressLine": "548 Uruapan Street"
				}
			}
		]
	},
	{
		"CityID": 589,
		"CityName": "York",
		"Customers": [
			{
				"CustomerID": 497,
				"LastName": "Sledge",
				"Address": {
					"AddressID": 502,
					"AddressLine": "1515 Korla Way"
				}
			}
		]
	}
]
`)
}

func TestJoinQuerySliceWithPtrs(t *testing.T) {
	type FilmsPerLanguage struct {
		Language model.Language
		Film     *[]*model.Film
	}

	limit := int64(3)

	query := Film.INNER_JOIN(Language, Film.LanguageID.EQ(Language.LanguageID)).
		SELECT(Language.AllColumns, Film.AllColumns).
		LIMIT(limit)

	filmsPerLanguageWithPtrs := []*FilmsPerLanguage{}
	err := query.Query(db, &filmsPerLanguageWithPtrs)

	require.NoError(t, err)
	require.Equal(t, len(filmsPerLanguageWithPtrs), 1)
	require.Equal(t, len(*filmsPerLanguageWithPtrs[0].Film), int(limit))
}

func TestSelect_WithoutUniqueColumnSelected(t *testing.T) {
	query := Customer.SELECT(Customer.FirstName, Customer.LastName, Customer.Email)

	customers := []model.Customer{}

	err := query.Query(db, &customers)

	require.NoError(t, err)

	//spew.Dump(customers)

	require.Equal(t, len(customers), 599)
}

func TestSelectOrderByAscDesc(t *testing.T) {
	customersAsc := []model.Customer{}

	err := Customer.SELECT(Customer.CustomerID, Customer.FirstName, Customer.LastName).
		ORDER_BY(Customer.FirstName.ASC()).
		Query(db, &customersAsc)

	require.NoError(t, err)

	firstCustomerAsc := customersAsc[0]
	lastCustomerAsc := customersAsc[len(customersAsc)-1]

	customersDesc := []model.Customer{}
	err = Customer.SELECT(Customer.CustomerID, Customer.FirstName, Customer.LastName).
		ORDER_BY(Customer.FirstName.DESC()).
		Query(db, &customersDesc)

	require.NoError(t, err)

	firstCustomerDesc := customersDesc[0]
	lastCustomerDesc := customersDesc[len(customersAsc)-1]

	testutils.AssertDeepEqual(t, firstCustomerAsc, lastCustomerDesc)
	testutils.AssertDeepEqual(t, lastCustomerAsc, firstCustomerDesc)

	customersAscDesc := []model.Customer{}
	err = Customer.SELECT(Customer.CustomerID, Customer.FirstName, Customer.LastName).
		ORDER_BY(Customer.FirstName.ASC(), Customer.LastName.DESC()).
		Query(db, &customersAscDesc)

	require.NoError(t, err)

	customerAscDesc326 := model.Customer{
		CustomerID: 67,
		FirstName:  "Kelly",
		LastName:   "Torres",
	}

	customerAscDesc327 := model.Customer{
		CustomerID: 546,
		FirstName:  "Kelly",
		LastName:   "Knott",
	}

	testutils.AssertDeepEqual(t, customerAscDesc326, customersAscDesc[326])
	testutils.AssertDeepEqual(t, customerAscDesc327, customersAscDesc[327])
}

func TestSelectFullJoin(t *testing.T) {
	expectedSQL := `
SELECT customer.customer_id AS "customer.customer_id",
     customer.store_id AS "customer.store_id",
     customer.first_name AS "customer.first_name",
     customer.last_name AS "customer.last_name",
     customer.email AS "customer.email",
     customer.address_id AS "customer.address_id",
     customer.activebool AS "customer.activebool",
     customer.create_date AS "customer.create_date",
     customer.last_update AS "customer.last_update",
     customer.active AS "customer.active",
     address.address_id AS "address.address_id",
     address.address AS "address.address",
     address.address2 AS "address.address2",
     address.district AS "address.district",
     address.city_id AS "address.city_id",
     address.postal_code AS "address.postal_code",
     address.phone AS "address.phone",
     address.last_update AS "address.last_update"
FROM dvds.customer
     FULL JOIN dvds.address ON (customer.address_id = address.address_id)
ORDER BY customer.customer_id ASC;
`
	query := Customer.
		FULL_JOIN(Address, Customer.AddressID.EQ(Address.AddressID)).
		SELECT(Customer.AllColumns, Address.AllColumns).
		ORDER_BY(Customer.CustomerID.ASC())

	testutils.AssertDebugStatementSql(t, query, expectedSQL)

	allCustomersAndAddress := []struct {
		Address  *model.Address
		Customer *model.Customer
	}{}

	err := query.Query(db, &allCustomersAndAddress)

	require.NoError(t, err)
	require.Equal(t, len(allCustomersAndAddress), 603)

	testutils.AssertDeepEqual(t, allCustomersAndAddress[0].Customer, &customer0)
	require.True(t, allCustomersAndAddress[0].Address != nil)

	lastCustomerAddress := allCustomersAndAddress[len(allCustomersAndAddress)-1]

	require.True(t, lastCustomerAddress.Customer == nil)
	require.True(t, lastCustomerAddress.Address != nil)

}

func TestSelectFullCrossJoin(t *testing.T) {
	expectedSQL := `
SELECT customer.customer_id AS "customer.customer_id",
     customer.store_id AS "customer.store_id",
     customer.first_name AS "customer.first_name",
     customer.last_name AS "customer.last_name",
     customer.email AS "customer.email",
     customer.address_id AS "customer.address_id",
     customer.activebool AS "customer.activebool",
     customer.create_date AS "customer.create_date",
     customer.last_update AS "customer.last_update",
     customer.active AS "customer.active",
     address.address_id AS "address.address_id",
     address.address AS "address.address",
     address.address2 AS "address.address2",
     address.district AS "address.district",
     address.city_id AS "address.city_id",
     address.postal_code AS "address.postal_code",
     address.phone AS "address.phone",
     address.last_update AS "address.last_update"
FROM dvds.customer
     CROSS JOIN dvds.address
ORDER BY customer.customer_id ASC
LIMIT 1000;
`
	query := Customer.
		CROSS_JOIN(Address).
		SELECT(Customer.AllColumns, Address.AllColumns).
		ORDER_BY(Customer.CustomerID.ASC()).
		LIMIT(1000)

	testutils.AssertDebugStatementSql(t, query, expectedSQL, int64(1000))

	var customerAddresCrosJoined []struct {
		model.Customer
		model.Address
	}

	err := query.Query(db, &customerAddresCrosJoined)

	require.Equal(t, len(customerAddresCrosJoined), 1000)

	require.NoError(t, err)
}

func TestSelectSelfJoin(t *testing.T) {
	expectedSQL := `
SELECT f1.film_id AS "f1.film_id",
     f1.title AS "f1.title",
     f1.description AS "f1.description",
     f1.release_year AS "f1.release_year",
     f1.language_id AS "f1.language_id",
     f1.rental_duration AS "f1.rental_duration",
     f1.rental_rate AS "f1.rental_rate",
     f1.length AS "f1.length",
     f1.replacement_cost AS "f1.replacement_cost",
     f1.rating AS "f1.rating",
     f1.last_update AS "f1.last_update",
     f1.special_features AS "f1.special_features",
     f1.fulltext AS "f1.fulltext",
     f2.film_id AS "f2.film_id",
     f2.title AS "f2.title",
     f2.description AS "f2.description",
     f2.release_year AS "f2.release_year",
     f2.language_id AS "f2.language_id",
     f2.rental_duration AS "f2.rental_duration",
     f2.rental_rate AS "f2.rental_rate",
     f2.length AS "f2.length",
     f2.replacement_cost AS "f2.replacement_cost",
     f2.rating AS "f2.rating",
     f2.last_update AS "f2.last_update",
     f2.special_features AS "f2.special_features",
     f2.fulltext AS "f2.fulltext"
FROM dvds.film AS f1
     INNER JOIN dvds.film AS f2 ON ((f1.film_id < f2.film_id) AND (f1.length = f2.length))
ORDER BY f1.film_id ASC;
`
	f1 := Film.AS("f1")

	f2 := Film.AS("f2")

	query := f1.
		INNER_JOIN(f2, f1.FilmID.LT(f2.FilmID).AND(f1.Length.EQ(f2.Length))).
		SELECT(f1.AllColumns, f2.AllColumns).
		ORDER_BY(f1.FilmID.ASC())

	testutils.AssertDebugStatementSql(t, query, expectedSQL)

	type F1 model.Film
	type F2 model.Film

	var theSameLengthFilms []struct {
		F1 F1
		F2 F2
	}

	err := query.Query(db, &theSameLengthFilms)

	require.NoError(t, err)

	//spew.Dump(theSameLengthFilms)

	//require.Equal(t, len(theSameLengthFilms), 100)
}

func TestSelectAliasColumn(t *testing.T) {
	expectedSQL := `
SELECT f1.title AS "thesame_length_films.title1",
     f2.title AS "thesame_length_films.title2",
     f1.length AS "thesame_length_films.length"
FROM dvds.film AS f1
     INNER JOIN dvds.film AS f2 ON ((f1.film_id != f2.film_id) AND (f1.length = f2.length))
ORDER BY f1.length ASC, f1.title ASC, f2.title ASC
LIMIT 1000;
`
	f1 := Film.AS("f1")
	f2 := Film.AS("f2")

	f1.FilmID.EQ(Int(11))

	query := f1.
		INNER_JOIN(f2, f1.FilmID.NOT_EQ(f2.FilmID).AND(f1.Length.EQ(f2.Length))).
		SELECT(f1.Title.AS("thesame_length_films.title1"),
			f2.Title.AS("thesame_length_films.title2"),
			f1.Length.AS("thesame_length_films.length")).
		ORDER_BY(f1.Length.ASC(), f1.Title.ASC(), f2.Title.ASC()).
		LIMIT(1000)

	testutils.AssertDebugStatementSql(t, query, expectedSQL, int64(1000))

	type thesameLengthFilms struct {
		Title1 string
		Title2 string
		Length int16
	}
	var films []thesameLengthFilms

	err := query.Query(db, &films)

	require.NoError(t, err)

	require.Equal(t, len(films), 1000)
	testutils.AssertDeepEqual(t, films[0], thesameLengthFilms{"Alien Center", "Iron Moon", 46})
}

func TestSubQuery(t *testing.T) {
	rRatingFilms :=
		SELECT(
			Film.FilmID,
			Film.Title,
			Film.Rating,
		).FROM(
			Film,
		).WHERE(
			Film.Rating.EQ(enum.MpaaRating.R),
		).AsTable("rFilms")

	rFilmID := Film.FilmID.From(rRatingFilms)

	stmt :=
		SELECT(
			rRatingFilms.AllColumns(),
			Actor.AllColumns,
			FilmActor.AllColumns,
		).FROM(
			rRatingFilms.
				INNER_JOIN(FilmActor, FilmActor.FilmID.EQ(rFilmID)).
				INNER_JOIN(Actor, FilmActor.ActorID.EQ(Actor.ActorID)),
		).WHERE(
			rFilmID.LT(Int(50)),
		).ORDER_BY(
			rFilmID.ASC(),
			Actor.ActorID.ASC(),
		)

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT "rFilms"."film.film_id" AS "film.film_id",
     "rFilms"."film.title" AS "film.title",
     "rFilms"."film.rating" AS "film.rating",
     actor.actor_id AS "actor.actor_id",
     actor.first_name AS "actor.first_name",
     actor.last_name AS "actor.last_name",
     actor.last_update AS "actor.last_update",
     film_actor.actor_id AS "film_actor.actor_id",
     film_actor.film_id AS "film_actor.film_id",
     film_actor.last_update AS "film_actor.last_update"
FROM (
          SELECT film.film_id AS "film.film_id",
               film.title AS "film.title",
               film.rating AS "film.rating"
          FROM dvds.film
          WHERE film.rating = 'R'
     ) AS "rFilms"
     INNER JOIN dvds.film_actor ON (film_actor.film_id = "rFilms"."film.film_id")
     INNER JOIN dvds.actor ON (film_actor.actor_id = actor.actor_id)
WHERE "rFilms"."film.film_id" < 50
ORDER BY "rFilms"."film.film_id" ASC, actor.actor_id ASC;
`)

	var dest []struct {
		model.Film

		Actors []model.Actor
	}

	err := stmt.Query(db, &dest)
	require.NoError(t, err)
	require.Len(t, dest, 10)

	testutils.AssertJSON(t, dest[0], `
{
	"FilmID": 8,
	"Title": "Airport Pollock",
	"Description": null,
	"ReleaseYear": null,
	"LanguageID": 0,
	"RentalDuration": 0,
	"RentalRate": 0,
	"Length": null,
	"ReplacementCost": 0,
	"Rating": "R",
	"LastUpdate": "0001-01-01T00:00:00Z",
	"SpecialFeatures": null,
	"Fulltext": "",
	"Actors": [
		{
			"ActorID": 55,
			"FirstName": "Fay",
			"LastName": "Kilmer",
			"LastUpdate": "2013-05-26T14:47:57.62Z"
		},
		{
			"ActorID": 96,
			"FirstName": "Gene",
			"LastName": "Willis",
			"LastUpdate": "2013-05-26T14:47:57.62Z"
		},
		{
			"ActorID": 110,
			"FirstName": "Susan",
			"LastName": "Davis",
			"LastUpdate": "2013-05-26T14:47:57.62Z"
		},
		{
			"ActorID": 138,
			"FirstName": "Lucille",
			"LastName": "Dee",
			"LastUpdate": "2013-05-26T14:47:57.62Z"
		}
	]
}
`)

}

func TestSelectFunctions(t *testing.T) {
	expectedQuery := `
SELECT MAX(film.rental_rate) AS "max_film_rate"
FROM dvds.film;
`
	query := Film.SELECT(
		MAXf(Film.RentalRate).AS("max_film_rate"),
	)

	testutils.AssertDebugStatementSql(t, query, expectedQuery)

	ret := struct {
		MaxFilmRate float64
	}{}

	err := query.Query(db, &ret)

	require.NoError(t, err)
	require.Equal(t, ret.MaxFilmRate, 4.99)
}

func TestSelectQueryScalar(t *testing.T) {
	expectedSQL := `
SELECT film.film_id AS "film.film_id",
     film.title AS "film.title",
     film.description AS "film.description",
     film.release_year AS "film.release_year",
     film.language_id AS "film.language_id",
     film.rental_duration AS "film.rental_duration",
     film.rental_rate AS "film.rental_rate",
     film.length AS "film.length",
     film.replacement_cost AS "film.replacement_cost",
     film.rating AS "film.rating",
     film.last_update AS "film.last_update",
     film.special_features AS "film.special_features",
     film.fulltext AS "film.fulltext"
FROM dvds.film
WHERE film.rental_rate = (
          SELECT MAX(film.rental_rate)
          FROM dvds.film
     )
ORDER BY film.film_id ASC;
`

	maxFilmRentalRate := FloatExp(
		Film.
			SELECT(MAXf(Film.RentalRate)),
	)

	query := Film.
		SELECT(Film.AllColumns).
		WHERE(Film.RentalRate.EQ(maxFilmRentalRate)).
		ORDER_BY(Film.FilmID.ASC())

	testutils.AssertDebugStatementSql(t, query, expectedSQL)

	maxRentalRateFilms := []model.Film{}
	err := query.Query(db, &maxRentalRateFilms)

	require.NoError(t, err)

	require.Equal(t, len(maxRentalRateFilms), 336)

	gRating := model.MpaaRating_G

	testutils.AssertDeepEqual(t, maxRentalRateFilms[0], model.Film{
		FilmID:          2,
		Title:           "Ace Goldfinger",
		Description:     testutils.StringPtr("A Astounding Epistle of a Database Administrator And a Explorer who must Find a Car in Ancient China"),
		ReleaseYear:     testutils.Int32Ptr(2006),
		LanguageID:      1,
		RentalRate:      4.99,
		Length:          testutils.Int16Ptr(48),
		ReplacementCost: 12.99,
		Rating:          &gRating,
		RentalDuration:  3,
		LastUpdate:      *testutils.TimestampWithoutTimeZone("2013-05-26 14:50:58.951", 3),
		SpecialFeatures: testutils.StringPtr("{Trailers,\"Deleted Scenes\"}"),
		Fulltext:        "'ace':1 'administr':9 'ancient':19 'astound':4 'car':17 'china':20 'databas':8 'epistl':5 'explor':12 'find':15 'goldfing':2 'must':14",
	})
}

func TestSelectGroupByHaving(t *testing.T) {
	expectedSQL := `
SELECT customer.customer_id AS "customer.customer_id",
     customer.store_id AS "customer.store_id",
     customer.first_name AS "customer.first_name",
     customer.last_name AS "customer.last_name",
     customer.email AS "customer.email",
     customer.address_id AS "customer.address_id",
     customer.activebool AS "customer.activebool",
     customer.create_date AS "customer.create_date",
     customer.last_update AS "customer.last_update",
     customer.active AS "customer.active",
     SUM(payment.amount) AS "amount.sum",
     AVG(payment.amount) AS "amount.avg",
     MAX(payment.payment_date) AS "amount.max_date",
     MAX(payment.amount) AS "amount.max",
     MIN(payment.payment_date) AS "amount.min_date",
     MIN(payment.amount) AS "amount.min",
     COUNT(payment.amount) AS "amount.count"
FROM dvds.payment
     INNER JOIN dvds.customer ON (customer.customer_id = payment.customer_id)
GROUP BY customer.customer_id
HAVING SUM(payment.amount) > 125.6
ORDER BY customer.customer_id, SUM(payment.amount) ASC;
`
	query := Payment.
		INNER_JOIN(Customer, Customer.CustomerID.EQ(Payment.CustomerID)).
		SELECT(
			Customer.AllColumns,

			SUMf(Payment.Amount).AS("amount.sum"),
			AVG(Payment.Amount).AS("amount.avg"),
			MAX(Payment.PaymentDate).AS("amount.max_date"),
			MAXf(Payment.Amount).AS("amount.max"),
			MIN(Payment.PaymentDate).AS("amount.min_date"),
			MINf(Payment.Amount).AS("amount.min"),
			COUNT(Payment.Amount).AS("amount.count"),
		).
		GROUP_BY(Customer.CustomerID).
		HAVING(
			SUMf(Payment.Amount).GT(Float(125.6)),
		).
		ORDER_BY(
			Customer.CustomerID, SUMf(Payment.Amount).ASC(),
		)

	//fmt.Println(query.DebugSql())

	testutils.AssertDebugStatementSql(t, query, expectedSQL, float64(125.6))

	var dest []struct {
		model.Customer

		Amount struct {
			Sum   float64
			Avg   float64
			Max   float64
			Min   float64
			Count int64
		} `alias:"amount"`
	}

	err := query.Query(db, &dest)

	require.NoError(t, err)

	//testutils.PrintJson(dest)

	require.Equal(t, len(dest), 104)

	//testutils.SaveJsonFile(dest, "postgres/testdata/customer_payment_sum.json")
	testutils.AssertJSONFile(t, dest, "./testdata/results/postgres/customer_payment_sum.json")
}

func TestSelectGroupBy2(t *testing.T) {
	expectedSQL := `
SELECT customer.customer_id AS "customer.customer_id",
     customer.store_id AS "customer.store_id",
     customer.first_name AS "customer.first_name",
     customer.last_name AS "customer.last_name",
     customer.email AS "customer.email",
     customer.address_id AS "customer.address_id",
     customer.activebool AS "customer.activebool",
     customer.create_date AS "customer.create_date",
     customer.last_update AS "customer.last_update",
     customer.active AS "customer.active",
     customer_payment_sum.amount_sum AS "CustomerWithAmounts.AmountSum"
FROM dvds.customer
     INNER JOIN (
          SELECT payment.customer_id AS "payment.customer_id",
               SUM(payment.amount) AS "amount_sum"
          FROM dvds.payment
          GROUP BY payment.customer_id
     ) AS customer_payment_sum ON (customer.customer_id = customer_payment_sum."payment.customer_id")
ORDER BY customer_payment_sum.amount_sum ASC;
`

	customersPayments := Payment.
		SELECT(
			Payment.CustomerID,
			SUMf(Payment.Amount).AS("amount_sum"),
		).
		GROUP_BY(Payment.CustomerID).
		AsTable("customer_payment_sum")

	customerID := Payment.CustomerID.From(customersPayments)
	amountSum := FloatColumn("amount_sum").From(customersPayments)

	query := Customer.
		INNER_JOIN(customersPayments, Customer.CustomerID.EQ(customerID)).
		SELECT(
			Customer.AllColumns,
			amountSum.AS("CustomerWithAmounts.AmountSum"),
		).
		ORDER_BY(amountSum.ASC())

	testutils.AssertDebugStatementSql(t, query, expectedSQL)

	type CustomerWithAmounts struct {
		Customer  *model.Customer
		AmountSum float64
	}
	customersWithAmounts := []CustomerWithAmounts{}

	err := query.Query(db, &customersWithAmounts)
	require.NoError(t, err)
	require.Equal(t, len(customersWithAmounts), 599)

	testutils.AssertDeepEqual(t, customersWithAmounts[0].Customer, &model.Customer{
		CustomerID: 318,
		StoreID:    1,
		FirstName:  "Brian",
		LastName:   "Wyman",
		AddressID:  323,
		Email:      testutils.StringPtr("brian.wyman@sakilacustomer.org"),
		Activebool: true,
		CreateDate: *testutils.TimestampWithoutTimeZone("2006-02-14 00:00:00", 0),
		LastUpdate: testutils.TimestampWithoutTimeZone("2013-05-26 14:49:45.738", 3),
		Active:     testutils.Int32Ptr(1),
	})

	require.Equal(t, customersWithAmounts[0].AmountSum, 27.93)
}

func TestSelectStaff(t *testing.T) {
	staffs := []model.Staff{}

	err := Staff.SELECT(Staff.AllColumns).Query(db, &staffs)

	require.NoError(t, err)

	testutils.AssertJSON(t, staffs, `
[
	{
		"StaffID": 1,
		"FirstName": "Mike",
		"LastName": "Hillyer",
		"AddressID": 3,
		"Email": "Mike.Hillyer@sakilastaff.com",
		"StoreID": 1,
		"Active": true,
		"Username": "Mike",
		"Password": "8cb2237d0679ca88db6464eac60da96345513964",
		"LastUpdate": "2006-05-16T16:13:11.79328Z",
		"Picture": "iVBORw0KWgo="
	},
	{
		"StaffID": 2,
		"FirstName": "Jon",
		"LastName": "Stephens",
		"AddressID": 4,
		"Email": "Jon.Stephens@sakilastaff.com",
		"StoreID": 2,
		"Active": true,
		"Username": "Jon",
		"Password": "8cb2237d0679ca88db6464eac60da96345513964",
		"LastUpdate": "2006-05-16T16:13:11.79328Z",
		"Picture": null
	}
]
`)
}

func TestSelectTimeColumns(t *testing.T) {

	expectedSQL := `
SELECT payment.payment_id AS "payment.payment_id",
     payment.customer_id AS "payment.customer_id",
     payment.staff_id AS "payment.staff_id",
     payment.rental_id AS "payment.rental_id",
     payment.amount AS "payment.amount",
     payment.payment_date AS "payment.payment_date"
FROM dvds.payment
WHERE payment.payment_date < '2007-02-14 22:16:01'::timestamp without time zone
ORDER BY payment.payment_date ASC;
`

	query := Payment.SELECT(Payment.AllColumns).
		WHERE(Payment.PaymentDate.LT(Timestamp(2007, time.February, 14, 22, 16, 01, 0))).
		ORDER_BY(Payment.PaymentDate.ASC())

	testutils.AssertDebugStatementSql(t, query, expectedSQL, "2007-02-14 22:16:01")

	payments := []model.Payment{}

	err := query.Query(db, &payments)

	require.NoError(t, err)

	//spew.Dump(payments)

	require.Equal(t, len(payments), 9)
	testutils.AssertDeepEqual(t, payments[0], model.Payment{
		PaymentID:   17793,
		CustomerID:  416,
		StaffID:     2,
		RentalID:    1158,
		Amount:      2.99,
		PaymentDate: *testutils.TimestampWithoutTimeZone("2007-02-14 21:21:59.996577", 6),
	})
}

func TestUnion(t *testing.T) {
	expectedQuery := `
(
     SELECT payment.payment_id AS "payment.payment_id",
          payment.amount AS "payment.amount"
     FROM dvds.payment
     WHERE payment.amount <= 100
)
UNION ALL
(
     SELECT payment.payment_id AS "payment.payment_id",
          payment.amount AS "payment.amount"
     FROM dvds.payment
     WHERE payment.amount >= 200
)
ORDER BY "payment.payment_id" ASC, "payment.amount" DESC
LIMIT 10
OFFSET 20;
`
	query := UNION_ALL(
		Payment.
			SELECT(Payment.PaymentID.AS("payment.payment_id"), Payment.Amount).
			WHERE(Payment.Amount.LT_EQ(Float(100))),
		Payment.
			SELECT(Payment.PaymentID, Payment.Amount).
			WHERE(Payment.Amount.GT_EQ(Float(200))),
	).
		ORDER_BY(IntegerColumn("payment.payment_id").ASC(), Payment.Amount.DESC()).
		LIMIT(10).
		OFFSET(20)

	//fmt.Println(query.DebugSql())

	testutils.AssertDebugStatementSql(t, query, expectedQuery, float64(100), float64(200), int64(10), int64(20))

	dest := []model.Payment{}

	err := query.Query(db, &dest)

	require.NoError(t, err)
	require.Equal(t, len(dest), 10)
	testutils.AssertDeepEqual(t, dest[0], model.Payment{
		PaymentID: 17523,
		Amount:    4.99,
	})
	testutils.AssertDeepEqual(t, dest[1], model.Payment{
		PaymentID: 17524,
		Amount:    0.99,
	})
	testutils.AssertDeepEqual(t, dest[9], model.Payment{
		PaymentID: 17532,
		Amount:    8.99,
	})
}

func TestAllSetOperators(t *testing.T) {
	var select1 = Payment.SELECT(Payment.AllColumns).WHERE(Payment.PaymentID.GT_EQ(Int(17600)).AND(Payment.PaymentID.LT(Int(17610))))
	var select2 = Payment.SELECT(Payment.AllColumns).WHERE(Payment.PaymentID.GT_EQ(Int(17620)).AND(Payment.PaymentID.LT(Int(17630))))

	t.Run("UNION", func(t *testing.T) {
		query := select1.UNION(select2)

		dest := []model.Payment{}
		err := query.Query(db, &dest)

		require.NoError(t, err)
		require.Equal(t, len(dest), 20)
	})

	t.Run("UNION_ALL", func(t *testing.T) {
		query := select1.UNION_ALL(select2)

		dest := []model.Payment{}
		err := query.Query(db, &dest)

		require.NoError(t, err)
		require.Equal(t, len(dest), 20)
	})

	t.Run("INTERSECT", func(t *testing.T) {
		query := select1.INTERSECT(select2)

		dest := []model.Payment{}
		err := query.Query(db, &dest)

		require.NoError(t, err)
		require.Equal(t, len(dest), 0)
	})

	t.Run("INTERSECT_ALL", func(t *testing.T) {
		query := select1.INTERSECT_ALL(select2)

		dest := []model.Payment{}
		err := query.Query(db, &dest)

		require.NoError(t, err)
		require.Equal(t, len(dest), 0)
	})

	t.Run("EXCEPT", func(t *testing.T) {
		query := select1.EXCEPT(select2)

		dest := []model.Payment{}
		err := query.Query(db, &dest)

		require.NoError(t, err)
		require.Equal(t, len(dest), 10)
	})

	t.Run("EXCEPT_ALL", func(t *testing.T) {
		query := select1.EXCEPT_ALL(select2)

		dest := []model.Payment{}
		err := query.Query(db, &dest)

		require.NoError(t, err)
		require.Equal(t, len(dest), 10)
	})
}

func TestSelectWithCase(t *testing.T) {
	expectedQuery := `
SELECT (CASE payment.staff_id WHEN 1 THEN 'ONE' WHEN 2 THEN 'TWO' WHEN 3 THEN 'THREE' ELSE 'OTHER' END) AS "staff_id_num"
FROM dvds.payment
ORDER BY payment.payment_id ASC
LIMIT 20;
`
	query := Payment.SELECT(
		CASE(Payment.StaffID).
			WHEN(Int(1)).THEN(String("ONE")).
			WHEN(Int(2)).THEN(String("TWO")).
			WHEN(Int(3)).THEN(String("THREE")).
			ELSE(String("OTHER")).AS("staff_id_num"),
	).
		ORDER_BY(Payment.PaymentID.ASC()).
		LIMIT(20)

	testutils.AssertDebugStatementSql(t, query, expectedQuery, int64(1), "ONE", int64(2), "TWO", int64(3), "THREE", "OTHER", int64(20))

	dest := []struct {
		StaffIDNum string
	}{}

	err := query.Query(db, &dest)

	require.NoError(t, err)
	require.Equal(t, len(dest), 20)
	require.Equal(t, dest[0].StaffIDNum, "TWO")
	require.Equal(t, dest[1].StaffIDNum, "ONE")
}

func getRowLockTestData() map[RowLock]string {
	return map[RowLock]string{
		UPDATE():        "UPDATE",
		NO_KEY_UPDATE(): "NO KEY UPDATE",
		SHARE():         "SHARE",
		KEY_SHARE():     "KEY SHARE",
	}
}

func TestRowLock(t *testing.T) {
	expectedSQL := `
SELECT *
FROM dvds.address
LIMIT 3
FOR`
	query := Address.
		SELECT(STAR).
		LIMIT(3)

	for lockType, lockTypeStr := range getRowLockTestData() {
		query.FOR(lockType)

		testutils.AssertDebugStatementSql(t, query, expectedSQL+" "+lockTypeStr+";\n", int64(3))

		tx, _ := db.Begin()

		res, err := query.Exec(tx)
		require.NoError(t, err)
		rowsAffected, _ := res.RowsAffected()
		require.Equal(t, rowsAffected, int64(3))

		err = tx.Rollback()
		require.NoError(t, err)
	}

	for lockType, lockTypeStr := range getRowLockTestData() {
		query.FOR(lockType.NOWAIT())

		testutils.AssertDebugStatementSql(t, query, expectedSQL+" "+lockTypeStr+" NOWAIT;\n", int64(3))

		tx, _ := db.Begin()

		res, err := query.Exec(tx)
		require.NoError(t, err)
		rowsAffected, _ := res.RowsAffected()
		require.Equal(t, rowsAffected, int64(3))

		err = tx.Rollback()
		require.NoError(t, err)
	}

	for lockType, lockTypeStr := range getRowLockTestData() {
		query.FOR(lockType.SKIP_LOCKED())

		testutils.AssertDebugStatementSql(t, query, expectedSQL+" "+lockTypeStr+" SKIP LOCKED;\n", int64(3))

		tx, _ := db.Begin()

		res, err := query.Exec(tx)
		require.NoError(t, err)
		rowsAffected, _ := res.RowsAffected()
		require.Equal(t, rowsAffected, int64(3))

		err = tx.Rollback()
		require.NoError(t, err)
	}
}

func TestQuickStart(t *testing.T) {

	var expectedSQL = `
SELECT actor.actor_id AS "actor.actor_id",
     actor.first_name AS "actor.first_name",
     actor.last_name AS "actor.last_name",
     actor.last_update AS "actor.last_update",
     film.film_id AS "film.film_id",
     film.title AS "film.title",
     film.description AS "film.description",
     film.release_year AS "film.release_year",
     film.language_id AS "film.language_id",
     film.rental_duration AS "film.rental_duration",
     film.rental_rate AS "film.rental_rate",
     film.length AS "film.length",
     film.replacement_cost AS "film.replacement_cost",
     film.rating AS "film.rating",
     film.last_update AS "film.last_update",
     film.special_features AS "film.special_features",
     film.fulltext AS "film.fulltext",
     language.language_id AS "language.language_id",
     language.name AS "language.name",
     language.last_update AS "language.last_update",
     category.category_id AS "category.category_id",
     category.name AS "category.name",
     category.last_update AS "category.last_update"
FROM dvds.actor
     INNER JOIN dvds.film_actor ON (actor.actor_id = film_actor.actor_id)
     INNER JOIN dvds.film ON (film.film_id = film_actor.film_id)
     INNER JOIN dvds.language ON (language.language_id = film.language_id)
     INNER JOIN dvds.film_category ON (film_category.film_id = film.film_id)
     INNER JOIN dvds.category ON (category.category_id = film_category.category_id)
WHERE ((language.name = 'English') AND (category.name != 'Action')) AND (film.length > 180)
ORDER BY actor.actor_id ASC, film.film_id ASC;
`

	stmt := SELECT(
		Actor.ActorID, Actor.FirstName, Actor.LastName, Actor.LastUpdate, // list of all actor columns (equivalent to Actor.AllColumns)
		Film.AllColumns, // list of all film columns (equivalent to Film.FilmID, Film.Title, ...)
		Language.AllColumns,
		Category.AllColumns,
	).FROM(
		Actor.
			INNER_JOIN(FilmActor, Actor.ActorID.EQ(FilmActor.ActorID)). // INNER JOIN Actor with FilmActor on condition Actor.ActorID = FilmActor.ActorID
			INNER_JOIN(Film, Film.FilmID.EQ(FilmActor.FilmID)).         // then with Film, Language, FilmCategory and Category.
			INNER_JOIN(Language, Language.LanguageID.EQ(Film.LanguageID)).
			INNER_JOIN(FilmCategory, FilmCategory.FilmID.EQ(Film.FilmID)).
			INNER_JOIN(Category, Category.CategoryID.EQ(FilmCategory.CategoryID)),
	).WHERE(
		Language.Name.EQ(String("English")). // note that every column has type.
							AND(Category.Name.NOT_EQ(String("Action"))). // String column Language.Name and Category.Name can be compared only with string expression
							AND(Film.Length.GT(Int(180))),               // Film.Length is integer column and can be compared only with integer expression
	).ORDER_BY(
		Actor.ActorID.ASC(),
		Film.FilmID.ASC(),
	)

	testutils.AssertDebugStatementSql(t, stmt, expectedSQL, "English", "Action", int64(180))

	var dest []struct {
		model.Actor

		Films []struct {
			model.Film

			Language model.Language

			Categories []model.Category
		}
	}

	err := stmt.Query(db, &dest)
	require.NoError(t, err)

	//jsonSave("./testdata/quick-start-dest.json", dest)
	testutils.AssertJSONFile(t, dest, "./testdata/results/postgres/quick-start-dest.json")

	var dest2 []struct {
		model.Category

		Films  []model.Film
		Actors []model.Actor
	}

	err = stmt.Query(db, &dest2)
	require.NoError(t, err)

	//jsonSave("./testdata/quick-start-dest2.json", dest2)
	testutils.AssertJSONFile(t, dest2, "./testdata/results/postgres/quick-start-dest2.json")
}

func TestQuickStartWithSubQueries(t *testing.T) {

	filmLogerThan180 := Film.
		SELECT(Film.AllColumns).
		WHERE(Film.Length.GT(Int(180))).
		AsTable("films")

	filmID := Film.FilmID.From(filmLogerThan180)
	filmLanguageID := Film.LanguageID.From(filmLogerThan180)

	categoriesNotAction := Category.
		SELECT(Category.AllColumns).
		WHERE(Category.Name.NOT_EQ(String("Action"))).
		AsTable("categories")

	categoryID := Category.CategoryID.From(categoriesNotAction)

	stmt := Actor.
		INNER_JOIN(FilmActor, Actor.ActorID.EQ(FilmActor.ActorID)).
		INNER_JOIN(filmLogerThan180, filmID.EQ(FilmActor.FilmID)).
		INNER_JOIN(Language, Language.LanguageID.EQ(filmLanguageID)).
		INNER_JOIN(FilmCategory, FilmCategory.FilmID.EQ(filmID)).
		INNER_JOIN(categoriesNotAction, categoryID.EQ(FilmCategory.CategoryID)).
		SELECT(
			Actor.AllColumns,
			filmLogerThan180.AllColumns(),
			Language.AllColumns,
			categoriesNotAction.AllColumns(),
		).ORDER_BY(
		Actor.ActorID.ASC(),
		filmID.ASC(),
	)

	var dest []struct {
		model.Actor

		Films []struct {
			model.Film

			Language model.Language

			Categories []model.Category
		}
	}

	err := stmt.Query(db, &dest)
	require.NoError(t, err)

	//jsonSave("./testdata/quick-start-dest.json", dest)
	testutils.AssertJSONFile(t, dest, "./testdata/results/postgres/quick-start-dest.json")

	var dest2 []struct {
		model.Category

		Films  []model.Film
		Actors []model.Actor
	}

	err = stmt.Query(db, &dest2)
	require.NoError(t, err)

	//jsonSave("./testdata/quick-start-dest2.json", dest2)
	testutils.AssertJSONFile(t, dest2, "./testdata/results/postgres/quick-start-dest2.json")
}

func TestExpressionWrappers(t *testing.T) {
	query := SELECT(
		BoolExp(Raw("true")),
		IntExp(Raw("11")),
		FloatExp(Raw("11.22")),
		StringExp(Raw("'stringer'")),
		TimeExp(Raw("'raw'")),
		TimezExp(Raw("'raw'")),
		TimestampExp(Raw("'raw'")),
		TimestampzExp(Raw("'raw'")),
		DateExp(Raw("'date'")),
	)

	testutils.AssertStatementSql(t, query, `
SELECT true,
     11,
     11.22,
     'stringer',
     'raw',
     'raw',
     'raw',
     'raw',
     'date';
`)

	dest := []struct{}{}
	err := query.Query(db, &dest)
	require.NoError(t, err)
}

func TestWindowFunction(t *testing.T) {
	var expectedSQL = `
SELECT AVG(payment.amount) OVER (),
     AVG(payment.amount) OVER (PARTITION BY payment.customer_id),
     MAX(payment.amount) OVER (ORDER BY payment.payment_date DESC),
     MIN(payment.amount) OVER (PARTITION BY payment.customer_id ORDER BY payment.payment_date DESC),
     SUM(payment.amount) OVER (PARTITION BY payment.customer_id ORDER BY payment.payment_date DESC ROWS BETWEEN 1 PRECEDING AND 6 FOLLOWING),
     SUM(payment.amount) OVER (PARTITION BY payment.customer_id ORDER BY payment.payment_date DESC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
     MAX(payment.customer_id) OVER (ORDER BY payment.payment_date DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING),
     MIN(payment.customer_id) OVER (PARTITION BY payment.customer_id ORDER BY payment.payment_date DESC),
     SUM(payment.customer_id) OVER (PARTITION BY payment.customer_id ORDER BY payment.payment_date DESC),
     ROW_NUMBER() OVER (ORDER BY payment.payment_date),
     RANK() OVER (ORDER BY payment.payment_date),
     DENSE_RANK() OVER (ORDER BY payment.payment_date),
     CUME_DIST() OVER (ORDER BY payment.payment_date),
     NTILE(11) OVER (ORDER BY payment.payment_date),
     LAG(payment.amount) OVER (ORDER BY payment.payment_date),
     LAG(payment.amount) OVER (ORDER BY payment.payment_date),
     LAG(payment.amount, 2, payment.amount) OVER (ORDER BY payment.payment_date),
     LAG(payment.amount, 2, $1) OVER (ORDER BY payment.payment_date),
     LEAD(payment.amount) OVER (ORDER BY payment.payment_date),
     LEAD(payment.amount) OVER (ORDER BY payment.payment_date),
     LEAD(payment.amount, 2, payment.amount) OVER (ORDER BY payment.payment_date),
     LEAD(payment.amount, 2, $2) OVER (ORDER BY payment.payment_date),
     FIRST_VALUE(payment.amount) OVER (ORDER BY payment.payment_date),
     LAST_VALUE(payment.amount) OVER (ORDER BY payment.payment_date),
     NTH_VALUE(payment.amount, 3) OVER (ORDER BY payment.payment_date)
FROM dvds.payment
WHERE payment.payment_id < $3
GROUP BY payment.amount, payment.customer_id, payment.payment_date;
`
	query := Payment.
		SELECT(
			AVG(Payment.Amount).OVER(),
			AVG(Payment.Amount).OVER(PARTITION_BY(Payment.CustomerID)),
			MAXf(Payment.Amount).OVER(ORDER_BY(Payment.PaymentDate.DESC())),
			MINf(Payment.Amount).OVER(PARTITION_BY(Payment.CustomerID).ORDER_BY(Payment.PaymentDate.DESC())),
			SUMf(Payment.Amount).OVER(PARTITION_BY(Payment.CustomerID).
				ORDER_BY(Payment.PaymentDate.DESC()).ROWS(PRECEDING(1), FOLLOWING(6))),
			SUMf(Payment.Amount).OVER(PARTITION_BY(Payment.CustomerID).
				ORDER_BY(Payment.PaymentDate.DESC()).RANGE(PRECEDING(UNBOUNDED), FOLLOWING(UNBOUNDED))),
			MAXi(Payment.CustomerID).OVER(ORDER_BY(Payment.PaymentDate.DESC()).ROWS(CURRENT_ROW, FOLLOWING(UNBOUNDED))),
			MINi(Payment.CustomerID).OVER(PARTITION_BY(Payment.CustomerID).ORDER_BY(Payment.PaymentDate.DESC())),
			SUMi(Payment.CustomerID).OVER(PARTITION_BY(Payment.CustomerID).ORDER_BY(Payment.PaymentDate.DESC())),
			ROW_NUMBER().OVER(ORDER_BY(Payment.PaymentDate)),
			RANK().OVER(ORDER_BY(Payment.PaymentDate)),
			DENSE_RANK().OVER(ORDER_BY(Payment.PaymentDate)),
			CUME_DIST().OVER(ORDER_BY(Payment.PaymentDate)),
			NTILE(11).OVER(ORDER_BY(Payment.PaymentDate)),
			LAG(Payment.Amount).OVER(ORDER_BY(Payment.PaymentDate)),
			LAG(Payment.Amount, 2).OVER(ORDER_BY(Payment.PaymentDate)),
			LAG(Payment.Amount, 2, Payment.Amount).OVER(ORDER_BY(Payment.PaymentDate)),
			LAG(Payment.Amount, 2, 100).OVER(ORDER_BY(Payment.PaymentDate)),
			LEAD(Payment.Amount).OVER(ORDER_BY(Payment.PaymentDate)),
			LEAD(Payment.Amount, 2).OVER(ORDER_BY(Payment.PaymentDate)),
			LEAD(Payment.Amount, 2, Payment.Amount).OVER(ORDER_BY(Payment.PaymentDate)),
			LEAD(Payment.Amount, 2, 100).OVER(ORDER_BY(Payment.PaymentDate)),
			FIRST_VALUE(Payment.Amount).OVER(ORDER_BY(Payment.PaymentDate)),
			LAST_VALUE(Payment.Amount).OVER(ORDER_BY(Payment.PaymentDate)),
			NTH_VALUE(Payment.Amount, 3).OVER(ORDER_BY(Payment.PaymentDate)),
		).GROUP_BY(Payment.Amount, Payment.CustomerID, Payment.PaymentDate).
		WHERE(Payment.PaymentID.LT(Int(10)))

	//fmt.Println(query.Sql())

	testutils.AssertStatementSql(t, query, expectedSQL, 100, 100, int64(10))

	dest := []struct{}{}
	err := query.Query(db, &dest)
	require.NoError(t, err)
}

func TestWindowClause(t *testing.T) {
	var expectedSQL = `
SELECT AVG(payment.amount) OVER (),
     AVG(payment.amount) OVER (w1),
     AVG(payment.amount) OVER (w2 ORDER BY payment.customer_id RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
     AVG(payment.amount) OVER (w3 RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
FROM dvds.payment
WHERE payment.payment_id < $1
WINDOW w1 AS (PARTITION BY payment.payment_date), w2 AS (w1), w3 AS (w2 ORDER BY payment.customer_id)
ORDER BY payment.customer_id;
`
	query := Payment.SELECT(
		AVG(Payment.Amount).OVER(),
		AVG(Payment.Amount).OVER(Window("w1")),
		AVG(Payment.Amount).OVER(
			Window("w2").
				ORDER_BY(Payment.CustomerID).
				RANGE(PRECEDING(UNBOUNDED), FOLLOWING(UNBOUNDED)),
		),
		AVG(Payment.Amount).OVER(Window("w3").RANGE(PRECEDING(UNBOUNDED), FOLLOWING(UNBOUNDED))),
	).
		WHERE(Payment.PaymentID.LT(Int(10))).
		WINDOW("w1").AS(PARTITION_BY(Payment.PaymentDate)).
		WINDOW("w2").AS(Window("w1")).
		WINDOW("w3").AS(Window("w2").ORDER_BY(Payment.CustomerID)).
		ORDER_BY(Payment.CustomerID)

	//fmt.Println(query.Sql())

	testutils.AssertStatementSql(t, query, expectedSQL, int64(10))

	dest := []struct{}{}
	err := query.Query(db, &dest)

	require.NoError(t, err)
}

func TestSimpleView(t *testing.T) {

	query := SELECT(
		view.ActorInfo.AllColumns,
	).
		FROM(view.ActorInfo).
		ORDER_BY(view.ActorInfo.ActorID).
		LIMIT(10)

	type ActorInfo struct {
		ActorID   int
		FirstName string
		LastName  string
		FilmInfo  string
	}

	var dest []ActorInfo

	err := query.Query(db, &dest)
	require.NoError(t, err)

	testutils.AssertJSON(t, dest[1:2], `
[
	{
		"ActorID": 2,
		"FirstName": "Nick",
		"LastName": "Wahlberg",
		"FilmInfo": "Action: Bull Shawshank, Animation: Fight Jawbreaker, Children: Jersey Sassy, Classics: Dracula Crystal, Gilbert Pelican, Comedy: Mallrats United, Rushmore Mermaid, Documentary: Adaptation Holes, Drama: Wardrobe Phantom, Family: Apache Divine, Chisum Behavior, Indian Love, Maguire Apache, Foreign: Baby Hall, Happiness United, Games: Roof Champion, Music: Lucky Flying, New: Destiny Saturday, Flash Wars, Jekyll Frogmen, Mask Peach, Sci-Fi: Chainsaw Uptown, Goodfellas Salute, Travel: Liaisons Sweet, Smile Earring"
	}
]
`)

}

func TestJoinViewWithTable(t *testing.T) {
	query := SELECT(
		view.CustomerList.AllColumns,
		Rental.AllColumns,
	).
		FROM(view.CustomerList.
			INNER_JOIN(Rental, view.CustomerList.ID.EQ(Rental.CustomerID)),
		).
		ORDER_BY(view.CustomerList.ID).
		WHERE(view.CustomerList.ID.LT_EQ(Int(2)))

	var dest []struct {
		model.CustomerList `sql:"primary_key=ID"`
		Rentals            []model.Rental
	}

	//fmt.Println(query.DebugSql())

	err := query.Query(db, &dest)
	require.NoError(t, err)

	require.Equal(t, len(dest), 2)
	require.Equal(t, len(dest[0].Rentals), 32)
	require.Equal(t, len(dest[1].Rentals), 27)
}

func TestDynamicProjectionList(t *testing.T) {

	var request struct {
		ColumnsToSelect []string
		ShowFullName    bool
	}

	request.ColumnsToSelect = []string{"customer_id", "create_date"}
	request.ShowFullName = true

	// ...

	projectionList := ProjectionList{}

	for _, columnName := range request.ColumnsToSelect {
		switch columnName {
		case Customer.CustomerID.Name():
			projectionList = append(projectionList, Customer.CustomerID)
		case Customer.Email.Name():
			projectionList = append(projectionList, Customer.Email)
		case Customer.CreateDate.Name():
			projectionList = append(projectionList, Customer.CreateDate)
		}
	}

	var showFullName bool
	if showFullName {
		projectionList = append(projectionList, Customer.FirstName.CONCAT(Customer.LastName))
	}

	stmt := SELECT(projectionList).
		FROM(Customer).
		LIMIT(3)

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT customer.customer_id AS "customer.customer_id",
     customer.create_date AS "customer.create_date"
FROM dvds.customer
LIMIT 3;
`)
	var dest []model.Customer
	err := stmt.Query(db, &dest)
	require.NoError(t, err)

	require.Equal(t, len(dest), 3)
}

func TestDynamicCondition(t *testing.T) {
	var request struct {
		CustomerID *int64
		Email      *string
		Active     *bool
	}

	request.CustomerID = testutils.Int64Ptr(1)
	request.Active = testutils.BoolPtr(true)

	// ...

	condition := Bool(true)

	if request.CustomerID != nil {
		condition = condition.AND(Customer.CustomerID.EQ(Int(*request.CustomerID)))
	}
	if request.Email != nil {
		condition = condition.AND(Customer.Email.EQ(String(*request.Email)))
	}
	if request.Active != nil {
		condition = condition.AND(Customer.Activebool.EQ(Bool(*request.Active)))
	}

	stmt := SELECT(Customer.AllColumns).
		FROM(Customer).
		WHERE(condition)

	testutils.AssertStatementSql(t, stmt, `
SELECT customer.customer_id AS "customer.customer_id",
     customer.store_id AS "customer.store_id",
     customer.first_name AS "customer.first_name",
     customer.last_name AS "customer.last_name",
     customer.email AS "customer.email",
     customer.address_id AS "customer.address_id",
     customer.activebool AS "customer.activebool",
     customer.create_date AS "customer.create_date",
     customer.last_update AS "customer.last_update",
     customer.active AS "customer.active"
FROM dvds.customer
WHERE ($1::boolean AND (customer.customer_id = $2)) AND (customer.activebool = $3::boolean);
`, true, int64(1), true)

	dest := []model.Customer{}
	err := stmt.Query(db, &dest)
	require.NoError(t, err)
	require.Len(t, dest, 1)
	testutils.AssertDeepEqual(t, dest[0], customer0)
}

func TestLateral(t *testing.T) {

	languages := LATERAL(
		SELECT(
			Language.AllColumns,
		).FROM(
			Language,
		).WHERE(
			Language.Name.NOT_IN(String("spanish")).
				AND(Film.LanguageID.EQ(Language.LanguageID)),
		),
	).AS("films")

	stmt := SELECT(
		Film.FilmID,
		Film.Title,
		languages.AllColumns(),
	).FROM(
		Film.CROSS_JOIN(languages),
	).WHERE(
		Film.FilmID.EQ(Int(1)),
	).ORDER_BY(
		Film.FilmID,
	).LIMIT(1)

	testutils.AssertDebugStatementSql(t, stmt, `
SELECT film.film_id AS "film.film_id",
     film.title AS "film.title",
     films."language.language_id" AS "language.language_id",
     films."language.name" AS "language.name",
     films."language.last_update" AS "language.last_update"
FROM dvds.film
     CROSS JOIN LATERAL (
          SELECT language.language_id AS "language.language_id",
               language.name AS "language.name",
               language.last_update AS "language.last_update"
          FROM dvds.language
          WHERE (language.name NOT IN ('spanish')) AND (film.language_id = language.language_id)
     ) AS films
WHERE film.film_id = 1
ORDER BY film.film_id
LIMIT 1;
`)

	type FilmLanguage struct {
		model.Film
		model.Language
	}

	var dest []FilmLanguage

	err := stmt.Query(db, &dest)
	require.NoError(t, err)
	require.Equal(t, dest[0].Film.Title, "Academy Dinosaur")
	require.Equal(t, dest[0].Language.Name, "English             ")

	t.Run("implicit cross join", func(t *testing.T) {
		stmt2 := SELECT(
			Film.FilmID,
			Film.Title,
			languages.AllColumns(),
		).FROM(
			Film,
			languages,
		).WHERE(
			Film.FilmID.EQ(Int(1)),
		).ORDER_BY(
			Film.FilmID,
		).LIMIT(1)

		testutils.AssertDebugStatementSql(t, stmt2, `
SELECT film.film_id AS "film.film_id",
     film.title AS "film.title",
     films."language.language_id" AS "language.language_id",
     films."language.name" AS "language.name",
     films."language.last_update" AS "language.last_update"
FROM dvds.film,
     LATERAL (
          SELECT language.language_id AS "language.language_id",
               language.name AS "language.name",
               language.last_update AS "language.last_update"
          FROM dvds.language
          WHERE (language.name NOT IN ('spanish')) AND (film.language_id = language.language_id)
     ) AS films
WHERE film.film_id = 1
ORDER BY film.film_id
LIMIT 1;
`)

		var dest2 []FilmLanguage

		err2 := stmt2.Query(db, &dest2)
		require.NoError(t, err2)
		require.Equal(t, dest, dest2)
	})
}

func TestSelectColumnListWithExcludedColumns(t *testing.T) {

	t.Run("one column", func(t *testing.T) {
		stmt := SELECT(
			Address.AllColumns.Except(Address.LastUpdate),
		).FROM(
			Address,
		)

		testutils.AssertDebugStatementSql(t, stmt, `
SELECT address.address_id AS "address.address_id",
     address.address AS "address.address",
     address.address2 AS "address.address2",
     address.district AS "address.district",
     address.city_id AS "address.city_id",
     address.postal_code AS "address.postal_code",
     address.phone AS "address.phone"
FROM dvds.address;
`)
		var dest []model.Address
		require.NoError(t, stmt.Query(db, &dest))
		require.Len(t, dest, 603)
	})

	t.Run("multiple columns", func(t *testing.T) {
		expectedSQL := `
SELECT address.address_id AS "address.address_id",
     address.address AS "address.address",
     address.address2 AS "address.address2",
     address.district AS "address.district",
     address.city_id AS "address.city_id"
FROM dvds.address;
`
		// list of columns
		stmt := SELECT(
			Address.AllColumns.Except(Address.PostalCode, Address.Phone, Address.LastUpdate),
		).FROM(
			Address,
		)
		testutils.AssertDebugStatementSql(t, stmt, expectedSQL)

		// column list
		excludedColumns := ColumnList{Address.PostalCode, Address.Phone, Address.LastUpdate, Film.Title} // Film.Title is ignored
		stmt = SELECT(
			Address.AllColumns.Except(excludedColumns),
		).FROM(Address)

		testutils.AssertDebugStatementSql(t, stmt, expectedSQL)

		// column list with just column names
		excludedColumns = ColumnList{StringColumn("postal_code"), StringColumn("phone"), TimestampColumn("last_update")}
		stmt = SELECT(
			Address.AllColumns.Except(excludedColumns),
		).FROM(Address)

		testutils.AssertDebugStatementSql(t, stmt, expectedSQL)

		var dest []model.Address
		require.NoError(t, stmt.Query(db, &dest))
		require.Len(t, dest, 603)
	})
}

type FilmWrap struct {
	model.Film

	Actors []ActorWrap
}

type ActorWrap struct {
	model.Actor

	Films []FilmWrap
}

func TestRecursionScanNxM(t *testing.T) {

	stmt := SELECT(
		Actor.AllColumns,
		Film.AllColumns,
	).FROM(
		Actor.
			INNER_JOIN(FilmActor, Actor.ActorID.EQ(FilmActor.ActorID)).
			INNER_JOIN(Film, Film.FilmID.EQ(FilmActor.FilmID)),
	).ORDER_BY(
		Actor.ActorID,
		Film.FilmID,
	).LIMIT(100)

	t.Run("film->actors", func(t *testing.T) {
		var films []FilmWrap
		err := stmt.Query(db, &films)

		require.NoError(t, err)
		require.Len(t, films, 95)
		testutils.AssertJSON(t, films[:2], `
[
	{
		"FilmID": 1,
		"Title": "Academy Dinosaur",
		"Description": "A Epic Drama of a Feminist And a Mad Scientist who must Battle a Teacher in The Canadian Rockies",
		"ReleaseYear": 2006,
		"LanguageID": 1,
		"RentalDuration": 6,
		"RentalRate": 0.99,
		"Length": 86,
		"ReplacementCost": 20.99,
		"Rating": "PG",
		"LastUpdate": "2013-05-26T14:50:58.951Z",
		"SpecialFeatures": "{\"Deleted Scenes\",\"Behind the Scenes\"}",
		"Fulltext": "'academi':1 'battl':15 'canadian':20 'dinosaur':2 'drama':5 'epic':4 'feminist':8 'mad':11 'must':14 'rocki':21 'scientist':12 'teacher':17",
		"Actors": [
			{
				"ActorID": 1,
				"FirstName": "Penelope",
				"LastName": "Guiness",
				"LastUpdate": "2013-05-26T14:47:57.62Z",
				"Films": null
			}
		]
	},
	{
		"FilmID": 23,
		"Title": "Anaconda Confessions",
		"Description": "A Lacklusture Display of a Dentist And a Dentist who must Fight a Girl in Australia",
		"ReleaseYear": 2006,
		"LanguageID": 1,
		"RentalDuration": 3,
		"RentalRate": 0.99,
		"Length": 92,
		"ReplacementCost": 9.99,
		"Rating": "R",
		"LastUpdate": "2013-05-26T14:50:58.951Z",
		"SpecialFeatures": "{Trailers,\"Deleted Scenes\"}",
		"Fulltext": "'anaconda':1 'australia':18 'confess':2 'dentist':8,11 'display':5 'fight':14 'girl':16 'lacklustur':4 'must':13",
		"Actors": [
			{
				"ActorID": 1,
				"FirstName": "Penelope",
				"LastName": "Guiness",
				"LastUpdate": "2013-05-26T14:47:57.62Z",
				"Films": null
			},
			{
				"ActorID": 4,
				"FirstName": "Jennifer",
				"LastName": "Davis",
				"LastUpdate": "2013-05-26T14:47:57.62Z",
				"Films": null
			}
		]
	}
]
`)

	})

	t.Run("actors->films", func(t *testing.T) {
		var actors []ActorWrap

		err := stmt.Query(db, &actors)

		require.NoError(t, err)
		require.Equal(t, len(actors), 5)
		require.Equal(t, actors[0].ActorID, int32(1))
		require.Equal(t, actors[0].FirstName, "Penelope")
		require.Len(t, actors[0].Films, 19)
		testutils.AssertJSON(t, actors[0].Films[:2], `
[
	{
		"FilmID": 1,
		"Title": "Academy Dinosaur",
		"Description": "A Epic Drama of a Feminist And a Mad Scientist who must Battle a Teacher in The Canadian Rockies",
		"ReleaseYear": 2006,
		"LanguageID": 1,
		"RentalDuration": 6,
		"RentalRate": 0.99,
		"Length": 86,
		"ReplacementCost": 20.99,
		"Rating": "PG",
		"LastUpdate": "2013-05-26T14:50:58.951Z",
		"SpecialFeatures": "{\"Deleted Scenes\",\"Behind the Scenes\"}",
		"Fulltext": "'academi':1 'battl':15 'canadian':20 'dinosaur':2 'drama':5 'epic':4 'feminist':8 'mad':11 'must':14 'rocki':21 'scientist':12 'teacher':17",
		"Actors": null
	},
	{
		"FilmID": 23,
		"Title": "Anaconda Confessions",
		"Description": "A Lacklusture Display of a Dentist And a Dentist who must Fight a Girl in Australia",
		"ReleaseYear": 2006,
		"LanguageID": 1,
		"RentalDuration": 3,
		"RentalRate": 0.99,
		"Length": 92,
		"ReplacementCost": 9.99,
		"Rating": "R",
		"LastUpdate": "2013-05-26T14:50:58.951Z",
		"SpecialFeatures": "{Trailers,\"Deleted Scenes\"}",
		"Fulltext": "'anaconda':1 'australia':18 'confess':2 'dentist':8,11 'display':5 'fight':14 'girl':16 'lacklustur':4 'must':13",
		"Actors": null
	}
]
`)
	})
}

type StoreWrap struct {
	model.Store

	Staffs []StaffWrap
}

type StaffWrap struct {
	model.Staff

	Store StoreWrap
}

func TestRecursionScanNx1(t *testing.T) {
	stmt := SELECT(
		Store.AllColumns,
		Staff.AllColumns,
	).FROM(
		Store.
			INNER_JOIN(Staff, Staff.StoreID.EQ(Store.StoreID)),
	).ORDER_BY(
		Store.StoreID,
		Staff.StaffID,
	)

	t.Run("store->staff", func(t *testing.T) {
		var stores []StoreWrap

		err := stmt.Query(db, &stores)

		require.NoError(t, err)
		require.Len(t, stores, 2)

		testutils.AssertJSON(t, stores, `
[
	{
		"StoreID": 1,
		"ManagerStaffID": 1,
		"AddressID": 1,
		"LastUpdate": "2006-02-15T09:57:12Z",
		"Staffs": [
			{
				"StaffID": 1,
				"FirstName": "Mike",
				"LastName": "Hillyer",
				"AddressID": 3,
				"Email": "Mike.Hillyer@sakilastaff.com",
				"StoreID": 1,
				"Active": true,
				"Username": "Mike",
				"Password": "8cb2237d0679ca88db6464eac60da96345513964",
				"LastUpdate": "2006-05-16T16:13:11.79328Z",
				"Picture": "iVBORw0KWgo=",
				"Store": {
					"StoreID": 0,
					"ManagerStaffID": 0,
					"AddressID": 0,
					"LastUpdate": "0001-01-01T00:00:00Z",
					"Staffs": null
				}
			}
		]
	},
	{
		"StoreID": 2,
		"ManagerStaffID": 2,
		"AddressID": 2,
		"LastUpdate": "2006-02-15T09:57:12Z",
		"Staffs": [
			{
				"StaffID": 2,
				"FirstName": "Jon",
				"LastName": "Stephens",
				"AddressID": 4,
				"Email": "Jon.Stephens@sakilastaff.com",
				"StoreID": 2,
				"Active": true,
				"Username": "Jon",
				"Password": "8cb2237d0679ca88db6464eac60da96345513964",
				"LastUpdate": "2006-05-16T16:13:11.79328Z",
				"Picture": null,
				"Store": {
					"StoreID": 0,
					"ManagerStaffID": 0,
					"AddressID": 0,
					"LastUpdate": "0001-01-01T00:00:00Z",
					"Staffs": null
				}
			}
		]
	}
]
`)
	})

	t.Run("staff->store", func(t *testing.T) {

		var staffs []StaffWrap

		err := stmt.Query(db, &staffs)
		require.NoError(t, err)

		testutils.AssertJSON(t, staffs, `
[
	{
		"StaffID": 1,
		"FirstName": "Mike",
		"LastName": "Hillyer",
		"AddressID": 3,
		"Email": "Mike.Hillyer@sakilastaff.com",
		"StoreID": 1,
		"Active": true,
		"Username": "Mike",
		"Password": "8cb2237d0679ca88db6464eac60da96345513964",
		"LastUpdate": "2006-05-16T16:13:11.79328Z",
		"Picture": "iVBORw0KWgo=",
		"Store": {
			"StoreID": 1,
			"ManagerStaffID": 1,
			"AddressID": 1,
			"LastUpdate": "2006-02-15T09:57:12Z",
			"Staffs": null
		}
	},
	{
		"StaffID": 2,
		"FirstName": "Jon",
		"LastName": "Stephens",
		"AddressID": 4,
		"Email": "Jon.Stephens@sakilastaff.com",
		"StoreID": 2,
		"Active": true,
		"Username": "Jon",
		"Password": "8cb2237d0679ca88db6464eac60da96345513964",
		"LastUpdate": "2006-05-16T16:13:11.79328Z",
		"Picture": null,
		"Store": {
			"StoreID": 2,
			"ManagerStaffID": 2,
			"AddressID": 2,
			"LastUpdate": "2006-02-15T09:57:12Z",
			"Staffs": null
		}
	}
]
`)
	})
}

// In parameterized statements integer literals, like Int(num), are replaced with a placeholders. For some expressions,
// postgres interpreter will not have enough information to deduce the type. If this is the case postgres returns an error.
// Int8, Int16, .... functions will add automatic type cast over placeholder, so type deduction is always possible.
func TestLiteralTypeDeduction(t *testing.T) {
	stmt := SELECT(
		SUM(
			CASE().WHEN(Staff.Active.IS_TRUE()).
				THEN(Int8(6)).   // if Int8 and Int32 are replaced with Int,
				ELSE(Int32(-1)), // execution of this statement will return an error
		).AS("num_passed"),
	).FROM(Staff)

	testutils.AssertStatementSql(t, stmt, `
SELECT SUM((CASE WHEN staff.active IS TRUE THEN $1::smallint ELSE $2::integer END)) AS "num_passed"
FROM dvds.staff;
`)

	err := stmt.Query(db, &struct{}{})
	require.NoError(t, err)
}

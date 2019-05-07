package tests

import (
	"fmt"
	"github.com/sub0zero/go-sqlbuilder/sqlbuilder"
	"github.com/sub0zero/go-sqlbuilder/tests/.test_files/dvd_rental/dvds/model"
	. "github.com/sub0zero/go-sqlbuilder/tests/.test_files/dvd_rental/dvds/table"
	"gotest.tools/assert"
	"strings"
	"testing"
	"time"
)

func TestSelect_ScanToStruct(t *testing.T) {
	actor := model.Actor{}
	query := Actor.SELECT(Actor.AllColumns).ORDER_BY(Actor.ActorID.ASC())

	queryStr, args, err := query.Sql()

	fmt.Println(queryStr)

	assert.Equal(t, queryStr, `SELECT actor.actor_id AS "actor.actor_id", actor.first_name AS "actor.first_name", actor.last_name AS "actor.last_name", actor.last_update AS "actor.last_update" FROM dvds.actor ORDER BY actor.actor_id ASC`)
	assert.Equal(t, len(args), 0)

	err = query.Query(db, &actor)

	assert.NilError(t, err)

	expectedActor := model.Actor{
		ActorID:    1,
		FirstName:  "Penelope",
		LastName:   "Guiness",
		LastUpdate: *timeWithoutTimeZone("2013-05-26 14:47:57.62", 2),
	}

	assert.DeepEqual(t, actor, expectedActor)
}

func TestClassicSelect(t *testing.T) {
	query := sqlbuilder.SELECT(Payment.AllColumns, Customer.AllColumns).
		FROM(Payment.INNER_JOIN(Customer, Payment.CustomerID.Eq(Customer.CustomerID))).
		ORDER_BY(Payment.PaymentID.ASC()).
		LIMIT(30)

	queryStr, args, err := query.Sql()

	assert.NilError(t, err)
	fmt.Println(queryStr)
	fmt.Println(args)

	dest := []model.Payment{}

	err = query.Query(db, &dest)

	assert.NilError(t, err)
}

func TestSelect_ScanToSlice(t *testing.T) {
	customers := []model.Customer{}

	query := Customer.SELECT(Customer.AllColumns).ORDER_BY(Customer.CustomerID.ASC())

	queryStr, args, err := query.Sql()
	assert.NilError(t, err)
	fmt.Println(queryStr)

	assert.Equal(t, queryStr, `SELECT customer.customer_id AS "customer.customer_id", customer.store_id AS "customer.store_id", customer.first_name AS "customer.first_name", customer.last_name AS "customer.last_name", customer.email AS "customer.email", customer.address_id AS "customer.address_id", customer.activebool AS "customer.activebool", customer.create_date AS "customer.create_date", customer.last_update AS "customer.last_update", customer.active AS "customer.active" FROM dvds.customer ORDER BY customer.customer_id ASC`)
	assert.Equal(t, len(args), 0)

	err = query.Query(db, &customers)
	assert.NilError(t, err)

	assert.Equal(t, len(customers), 599)

	assert.DeepEqual(t, customer0, customers[0])
	assert.DeepEqual(t, customer1, customers[1])
	assert.DeepEqual(t, lastCustomer, customers[598])
}

func TestSelectAndUnionInProjection(t *testing.T) {

	query := Payment.
		SELECT(
			Payment.PaymentID,
			Customer.SELECT(Customer.CustomerID).LIMIT(1),
			sqlbuilder.UNION(Payment.SELECT(Payment.PaymentID).LIMIT(1).OFFSET(10), Payment.SELECT(Payment.PaymentID).LIMIT(1).OFFSET(2)).LIMIT(1),
		).
		LIMIT(12)

	queryStr, args, err := query.Sql()

	assert.NilError(t, err)
	fmt.Println(queryStr)
	fmt.Println(args)
}

//func TestJoinQueryStruct(t *testing.T) {
//
//	query := FilmActor.
//		INNER_JOIN(Actor, FilmActor.ActorID.Eq(Actor.ActorID)).
//		INNER_JOIN(Film, FilmActor.FilmID.Eq(Film.FilmID)).
//		INNER_JOIN(Language, Film.LanguageID.Eq(Language.LanguageID)).
//		SELECT(FilmActor.AllColumns, Film.AllColumns, Language.AllColumns, Actor.AllColumns).
//		WHERE(FilmActor.ActorID.GtEq(1).AND(FilmActor.ActorID.LteLiteral(2)))
//
//	queryStr, args, err := query.Sql()
//	assert.NilError(t, err)
//	assert.Equal(t, queryStr, `SELECT film_actor.actor_id AS "film_actor.actor_id", film_actor.film_id AS "film_actor.film_id", film_actor.last_update AS "film_actor.last_update",film.film_id AS "film.film_id", film.title AS "film.title", film.description AS "film.description", film.release_year AS "film.release_year", film.language_id AS "film.language_id", film.rental_duration AS "film.rental_duration", film.rental_rate AS "film.rental_rate", film.length AS "film.length", film.replacement_cost AS "film.replacement_cost", film.rating AS "film.rating", film.last_update AS "film.last_update", film.special_features AS "film.special_features", film.fulltext AS "film.fulltext",language.language_id AS "language.language_id", language.name AS "language.name", language.last_update AS "language.last_update",actor.actor_id AS "actor.actor_id", actor.first_name AS "actor.first_name", actor.last_name AS "actor.last_name", actor.last_update AS "actor.last_update" FROM dvds.film_actor JOIN dvds.actor ON film_actor.actor_id = actor.actor_id JOIN dvds.film ON film_actor.film_id = film.film_id JOIN dvds.language ON film.language_id = language.language_id WHERE (film_actor.actor_id>=1 AND film_actor.actor_id<=2)`)
//
//	//fmt.Println(queryStr)
//
//	filmActor := []model.FilmActor{}
//
//	err = query.Execute(db, &filmActor)
//
//	assert.NilError(t, err)
//
//	//fmt.Println("ACTORS: --------------------")
//	//spew.Dump(filmActor)
//}

func TestJoinQuerySlice(t *testing.T) {
	type FilmsPerLanguage struct {
		Language *model.Language
		Film     []model.Film
	}

	filmsPerLanguage := []FilmsPerLanguage{}
	limit := 15

	query := Film.
		INNER_JOIN(Language, Film.LanguageID.Eq(Language.LanguageID)).
		SELECT(Language.AllColumns, Film.AllColumns).
		WHERE(Film.Rating.EqString(string(model.MpaaRating_NC17))).
		LIMIT(15)

	queryStr, args, err := query.Sql()

	assert.NilError(t, err)
	fmt.Println(queryStr)
	assert.Equal(t, queryStr, `SELECT language.language_id AS "language.language_id", language.name AS "language.name", language.last_update AS "language.last_update", film.film_id AS "film.film_id", film.title AS "film.title", film.description AS "film.description", film.release_year AS "film.release_year", film.language_id AS "film.language_id", film.rental_duration AS "film.rental_duration", film.rental_rate AS "film.rental_rate", film.length AS "film.length", film.replacement_cost AS "film.replacement_cost", film.rating AS "film.rating", film.last_update AS "film.last_update", film.special_features AS "film.special_features", film.fulltext AS "film.fulltext" FROM dvds.film JOIN dvds.language ON film.language_id = language.language_id WHERE film.rating = $1 LIMIT $2`)

	assert.Equal(t, len(args), 2)
	assert.Equal(t, args[0], string(model.MpaaRating_NC17))
	assert.Equal(t, args[1], int64(15))

	err = query.Query(db, &filmsPerLanguage)

	assert.NilError(t, err)

	//fmt.Println("--------------- result --------------- ")
	//spew.Dump(filmsPerLanguage)

	//spew.Dump(filmsPerLanguage)

	assert.Equal(t, len(filmsPerLanguage), 1)
	assert.Equal(t, len(filmsPerLanguage[0].Film), limit)

	englishFilms := filmsPerLanguage[0]

	assert.Equal(t, *englishFilms.Film[0].Rating, model.MpaaRating_NC17)

	//spew.Dump(filmsPerLanguage)

	filmsPerLanguageWithPtrs := []*FilmsPerLanguage{}
	err = query.Query(db, &filmsPerLanguageWithPtrs)

	assert.NilError(t, err)
	assert.Equal(t, len(filmsPerLanguage), 1)
	assert.Equal(t, len(filmsPerLanguage[0].Film), limit)
}

func TestJoinQuerySliceWithPtrs(t *testing.T) {
	type FilmsPerLanguage struct {
		Language model.Language
		Film     *[]*model.Film
	}

	limit := int64(3)

	query := Film.INNER_JOIN(Language, Film.LanguageID.Eq(Language.LanguageID)).
		SELECT(Language.AllColumns, Film.AllColumns).
		LIMIT(limit)

	filmsPerLanguageWithPtrs := []*FilmsPerLanguage{}
	err := query.Query(db, &filmsPerLanguageWithPtrs)

	//spew.Dump(filmsPerLanguageWithPtrs)

	assert.NilError(t, err)
	assert.Equal(t, len(filmsPerLanguageWithPtrs), 1)
	assert.Equal(t, len(*filmsPerLanguageWithPtrs[0].Film), int(limit))
}

func TestSelect_WithoutUniqueColumnSelected(t *testing.T) {
	query := Customer.SELECT(Customer.FirstName, Customer.LastName, Customer.Email)

	customers := []model.Customer{}

	err := query.Query(db, &customers)

	assert.NilError(t, err)

	//spew.Dump(customers)

	assert.Equal(t, len(customers), 599)
}

func TestSelectOrderByAscDesc(t *testing.T) {
	customersAsc := []model.Customer{}

	err := Customer.SELECT(Customer.CustomerID, Customer.FirstName, Customer.LastName).
		ORDER_BY(Customer.FirstName.ASC()).
		Query(db, &customersAsc)

	assert.NilError(t, err)

	firstCustomerAsc := customersAsc[0]
	lastCustomerAsc := customersAsc[len(customersAsc)-1]

	customersDesc := []model.Customer{}
	err = Customer.SELECT(Customer.CustomerID, Customer.FirstName, Customer.LastName).
		ORDER_BY(Customer.FirstName.DESC()).
		Query(db, &customersDesc)

	assert.NilError(t, err)

	firstCustomerDesc := customersDesc[0]
	lastCustomerDesc := customersDesc[len(customersAsc)-1]

	assert.DeepEqual(t, firstCustomerAsc, lastCustomerDesc)
	assert.DeepEqual(t, lastCustomerAsc, firstCustomerDesc)

	customersAscDesc := []model.Customer{}
	err = Customer.SELECT(Customer.CustomerID, Customer.FirstName, Customer.LastName).
		ORDER_BY(Customer.FirstName.ASC(), Customer.LastName.DESC()).
		Query(db, &customersAscDesc)

	assert.NilError(t, err)

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

	assert.DeepEqual(t, customerAscDesc326, customersAscDesc[326])
	assert.DeepEqual(t, customerAscDesc327, customersAscDesc[327])
}

func TestSelectFullJoin(t *testing.T) {
	query := Customer.
		FULL_JOIN(Address, Customer.AddressID.Eq(Address.AddressID)).
		SELECT(Customer.AllColumns, Address.AllColumns).
		ORDER_BY(Customer.CustomerID.ASC())

	queryStr, args, err := query.Sql()

	assert.NilError(t, err)

	assert.Equal(t, queryStr, `SELECT customer.customer_id AS "customer.customer_id", customer.store_id AS "customer.store_id", customer.first_name AS "customer.first_name", customer.last_name AS "customer.last_name", customer.email AS "customer.email", customer.address_id AS "customer.address_id", customer.activebool AS "customer.activebool", customer.create_date AS "customer.create_date", customer.last_update AS "customer.last_update", customer.active AS "customer.active", address.address_id AS "address.address_id", address.address AS "address.address", address.address2 AS "address.address2", address.district AS "address.district", address.city_id AS "address.city_id", address.postal_code AS "address.postal_code", address.phone AS "address.phone", address.last_update AS "address.last_update" FROM dvds.customer FULL JOIN dvds.address ON customer.address_id = address.address_id ORDER BY customer.customer_id ASC`)
	assert.Equal(t, len(args), 0)

	allCustomersAndAddress := []struct {
		Address  *model.Address
		Customer *model.Customer
	}{}

	err = query.Query(db, &allCustomersAndAddress)

	assert.NilError(t, err)
	assert.Equal(t, len(allCustomersAndAddress), 603)

	assert.DeepEqual(t, allCustomersAndAddress[0].Customer, &customer0)
	assert.Assert(t, allCustomersAndAddress[0].Address != nil)

	lastCustomerAddress := allCustomersAndAddress[len(allCustomersAndAddress)-1]

	assert.Assert(t, lastCustomerAddress.Customer == nil)
	assert.Assert(t, lastCustomerAddress.Address != nil)

}

func TestSelectFullCrossJoin(t *testing.T) {
	query := Customer.
		CROSS_JOIN(Address).
		SELECT(Customer.AllColumns, Address.AllColumns).
		ORDER_BY(Customer.CustomerID.ASC()).
		LIMIT(1000)

	queryStr, args, err := query.Sql()

	assert.NilError(t, err)
	assert.Equal(t, queryStr, `SELECT customer.customer_id AS "customer.customer_id", customer.store_id AS "customer.store_id", customer.first_name AS "customer.first_name", customer.last_name AS "customer.last_name", customer.email AS "customer.email", customer.address_id AS "customer.address_id", customer.activebool AS "customer.activebool", customer.create_date AS "customer.create_date", customer.last_update AS "customer.last_update", customer.active AS "customer.active", address.address_id AS "address.address_id", address.address AS "address.address", address.address2 AS "address.address2", address.district AS "address.district", address.city_id AS "address.city_id", address.postal_code AS "address.postal_code", address.phone AS "address.phone", address.last_update AS "address.last_update" FROM dvds.customer CROSS JOIN dvds.address ORDER BY customer.customer_id ASC LIMIT $1`)
	assert.Equal(t, len(args), 1)

	customerAddresCrosJoined := []model.Customer{}

	err = query.Query(db, &customerAddresCrosJoined)

	assert.Equal(t, len(customerAddresCrosJoined), 1000)

	assert.NilError(t, err)
}

func TestSelectSelfJoin(t *testing.T) {

	f1 := Film.AS("f1")

	//spew.Dump(f1)
	f2 := Film.AS("f2")

	query := f1.
		INNER_JOIN(f2, f1.FilmID.NotEq(f2.FilmID).AND(f1.Length.Eq(f2.Length))).
		SELECT(f1.AllColumns, f2.AllColumns).
		ORDER_BY(f1.FilmID.ASC())

	queryStr, args, err := query.Sql()
	assert.Equal(t, len(args), 0)

	assert.NilError(t, err)

	fmt.Println(queryStr)

	type F1 model.Film
	type F2 model.Film

	theSameLengthFilms := []struct {
		F1 F1
		F2 F2
	}{}

	err = query.Query(db, &theSameLengthFilms)

	assert.NilError(t, err)

	//spew.Dump(theSameLengthFilms[0])

	assert.Equal(t, len(theSameLengthFilms), 6972)
}

func TestSelectAliasColumn(t *testing.T) {
	f1 := Film.AS("f1")
	f2 := Film.AS("f2")

	type thesameLengthFilms struct {
		Title1 string
		Title2 string
		Length int16
	}

	query := f1.
		INNER_JOIN(f2, f1.FilmID.NotEq(f2.FilmID).AND(f1.Length.Eq(f2.Length))).
		SELECT(f1.Title.AS("thesame_length_films.title1"),
			f2.Title.AS("thesame_length_films.title2"),
			f1.Length.AS("thesame_length_films.length")).
		ORDER_BY(f1.Length.ASC(), f1.Title.ASC(), f2.Title.ASC()).
		LIMIT(1000)

	queryStr, args, err := query.Sql()

	assert.NilError(t, err)
	assert.Equal(t, len(args), 1)
	fmt.Println(queryStr)

	films := []thesameLengthFilms{}

	err = query.Query(db, &films)

	assert.NilError(t, err)

	//spew.Dump(films)

	assert.Equal(t, len(films), 1000)
	assert.DeepEqual(t, films[0], thesameLengthFilms{"Alien Center", "Iron Moon", 46})
}

type Manager staff

type staff struct {
	StaffID   int32 `sql:"unique"`
	FirstName string
	LastName  string
	//Address    *model.Address
	//Email      *string
	//StoreID    int16
	//Active     bool
	//Username   string
	//Password   *string
	//LastUpdate time.Time
	*Manager //`sqlbuilder:"manager"`
}

func TestSelectSelfReferenceType(t *testing.T) {

	manager := Staff.AS("manager")

	query := Staff.
		INNER_JOIN(Address, Staff.AddressID.Eq(Address.AddressID)).
		INNER_JOIN(manager, Staff.StaffID.Eq(manager.StaffID)).
		SELECT(Staff.StaffID, Staff.FirstName, Staff.LastName, Address.AllColumns, manager.StaffID, manager.FirstName)

	queryStr, args, err := query.Sql()
	assert.NilError(t, err)
	fmt.Println(queryStr)
	assert.Equal(t, len(args), 0)

	staffs := []staff{}

	err = query.Query(db, &staffs)

	assert.NilError(t, err)

	//spew.Dump(staffs)
}

func TestSubQuery(t *testing.T) {

	//selectStmtTable := Actor.SELECT(Actor.FirstName, Actor.LastName).AsTable("table_expression")
	//
	//query := selectStmtTable.SELECT(
	//	selectStmtTable.RefStringColumn(Actor.FirstName).AS("nesto"),
	//	selectStmtTable.RefIntColumnName("actor.last_name").AS("nesto2"),
	//	)
	//
	//queryStr, args, err := query.Sql()
	//
	//assert.NilError(t, err)
	//
	//fmt.Println(queryStr)
	//
	//avrgCustomer := sqlbuilder.NumExp(Customer.SELECT(Customer.LastName).LIMIT(1))
	//
	//Customer.
	//	INNER_JOIN(selectStmtTable, Customer.LastName.Eq(selectStmtTable.RefStringColumn(Actor.FirstName))).
	//	SELECT(Customer.AllColumns, selectStmtTable.RefIntColumnName("first_name")).
	//	WHERE(Actor.LastName.Neq(avrgCustomer))

	rFilmsOnly := Film.SELECT(Film.FilmID, Film.Title, Film.Rating).
		WHERE(Film.Rating.EqString("R")).
		AsTable("films")

	query := Actor.INNER_JOIN(FilmActor, Actor.ActorID.Eq(FilmActor.FilmID)).
		INNER_JOIN(rFilmsOnly, FilmActor.FilmID.Eq(rFilmsOnly.RefIntColumn(Film.FilmID))).
		SELECT(
			Actor.AllColumns,
			FilmActor.AllColumns,
			rFilmsOnly.RefStringColumn(Film.Title).AS("film.title"),
			rFilmsOnly.RefStringColumn(Film.Rating).AS("film.rating"),
		)

	queryStr, args, err := query.Sql()

	assert.NilError(t, err)
	assert.Equal(t, len(args), 1)
	fmt.Println(queryStr)

}

func TestSelectFunctions(t *testing.T) {
	query := Film.SELECT(sqlbuilder.MAX(Film.RentalRate).AS("max_film_rate"))

	str, args, err := query.Sql()

	assert.NilError(t, err)

	assert.Equal(t, str, `SELECT MAX(film.rental_rate) AS "max_film_rate" FROM dvds.film`)
	assert.Equal(t, len(args), 0)
	fmt.Println(str)
}

func TestSelectQueryScalar(t *testing.T) {

	maxFilmRentalRate := sqlbuilder.NumExp(Film.SELECT(sqlbuilder.MAX(Film.RentalRate)))

	query := Film.SELECT(Film.AllColumns).
		WHERE(Film.RentalRate.Eq(maxFilmRentalRate)).
		ORDER_BY(Film.FilmID.ASC())

	queryStr, args, err := query.Sql()

	assert.NilError(t, err)
	assert.Equal(t, len(args), 0)
	fmt.Println(queryStr)

	maxRentalRateFilms := []model.Film{}
	err = query.Query(db, &maxRentalRateFilms)

	assert.NilError(t, err)

	assert.Equal(t, len(maxRentalRateFilms), 336)

	gRating := model.MpaaRating_G

	assert.DeepEqual(t, maxRentalRateFilms[0], model.Film{
		FilmID:          2,
		Title:           "Ace Goldfinger",
		Description:     stringPtr("A Astounding Epistle of a Database Administrator And a Explorer who must Find a Car in Ancient China"),
		ReleaseYear:     int32Ptr(2006),
		Language:        nil,
		RentalRate:      4.99,
		Length:          int16Ptr(48),
		ReplacementCost: 12.99,
		Rating:          &gRating,
		RentalDuration:  3,
		LastUpdate:      *timeWithoutTimeZone("2013-05-26 14:50:58.951", 3),
		SpecialFeatures: stringPtr("{Trailers,\"Deleted Scenes\"}"),
		Fulltext:        "'ace':1 'administr':9 'ancient':19 'astound':4 'car':17 'china':20 'databas':8 'epistl':5 'explor':12 'find':15 'goldfing':2 'must':14",
	})

	//spew.Dump(maxRentalRateFilms[0])
}

func TestSelectGroupByHaving(t *testing.T) {
	customersPaymentQuery := Payment.
		SELECT(
			Payment.CustomerID.AS("customer_payment_sum.customer_id"),
			sqlbuilder.SUM(Payment.Amount).AS("customer_payment_sum.amount_sum"),
		).
		GROUP_BY(Payment.CustomerID).
		ORDER_BY(sqlbuilder.SUM(Payment.Amount).ASC()).
		HAVING(sqlbuilder.SUM(Payment.Amount).Gt(sqlbuilder.NewNumericLiteral(100)))

	queryStr, args, err := customersPaymentQuery.Sql()

	assert.NilError(t, err)
	fmt.Println(queryStr)
	assert.Equal(t, len(args), 1)
	assert.Equal(t, queryStr, `SELECT payment.customer_id AS "customer_payment_sum.customer_id", SUM(payment.amount) AS "customer_payment_sum.amount_sum" FROM dvds.payment GROUP BY payment.customer_id HAVING SUM(payment.amount) > $1 ORDER BY SUM(payment.amount) ASC`)

	type CustomerPaymentSum struct {
		CustomerID int16
		AmountSum  float64
	}

	customerPaymentSum := []CustomerPaymentSum{}

	err = customersPaymentQuery.Query(db, &customerPaymentSum)

	assert.NilError(t, err)

	assert.Equal(t, len(customerPaymentSum), 296)
	assert.DeepEqual(t, customerPaymentSum[0], CustomerPaymentSum{
		CustomerID: 135,
		AmountSum:  100.72,
	})
}

func TestSelectGroupBy2(t *testing.T) {
	type CustomerWithAmounts struct {
		Customer  *model.Customer
		AmountSum float64
	}
	customersWithAmounts := []CustomerWithAmounts{}

	customersPaymentSubQuery := Payment.
		SELECT(
			Payment.CustomerID,
			sqlbuilder.SUM(Payment.Amount).AS("amount_sum"),
		).
		GROUP_BY(Payment.CustomerID)

	customersPaymentTable := customersPaymentSubQuery.AsTable("customer_payment_sum")
	amountSumColumn := customersPaymentTable.RefIntColumnName("amount_sum")

	query := Customer.
		INNER_JOIN(customersPaymentTable, Customer.CustomerID.Eq(customersPaymentTable.RefIntColumn(Payment.CustomerID))).
		SELECT(Customer.AllColumns, amountSumColumn.AS("customer_with_amounts.amount_sum")).
		ORDER_BY(amountSumColumn.ASC())

	queryStr, args, err := query.Sql()
	assert.NilError(t, err)
	fmt.Println(queryStr)
	assert.Equal(t, len(args), 0)

	err = query.Query(db, &customersWithAmounts)
	assert.NilError(t, err)
	//spew.Dump(customersWithAmounts)

	assert.Equal(t, len(customersWithAmounts), 599)

	assert.DeepEqual(t, customersWithAmounts[0].Customer, &model.Customer{
		CustomerID: 318,
		StoreID:    1,
		FirstName:  "Brian",
		LastName:   "Wyman",
		Email:      stringPtr("brian.wyman@sakilacustomer.org"),
		Activebool: true,
		CreateDate: *timeWithoutTimeZone("2006-02-14 00:00:00", 0),
		LastUpdate: timeWithoutTimeZone("2013-05-26 14:49:45.738", 3),
		Active:     int32Ptr(1),
	})

	assert.Equal(t, customersWithAmounts[0].AmountSum, 27.93)
}

func TestSelectTimeColumns(t *testing.T) {
	query := Payment.SELECT(Payment.AllColumns).
		WHERE(Payment.PaymentDate.LtEqL("2007-02-14 22:16:01")).
		ORDER_BY(Payment.PaymentDate.ASC())

	queryStr, args, err := query.Sql()

	assert.NilError(t, err)
	assert.Equal(t, len(args), 1)
	fmt.Println(queryStr)

	payments := []model.Payment{}

	err = query.Query(db, &payments)

	assert.NilError(t, err)

	//spew.Dump(payments)

	assert.Equal(t, len(payments), 9)
	assert.DeepEqual(t, payments[0], model.Payment{
		PaymentID:   17793,
		Amount:      2.99,
		PaymentDate: *timeWithoutTimeZone("2007-02-14 21:21:59.996577", 6),
	})
}

func TestUnion(t *testing.T) {
	query := sqlbuilder.UNION(
		Payment.
			SELECT(Payment.PaymentID.AS("payment.payment_id"), Payment.Amount).
			WHERE(Payment.Amount.LtEqL(100)),
		Payment.
			SELECT(Payment.PaymentID, Payment.Amount).
			WHERE(Payment.Amount.GtEqL(200)),
	).
		ORDER_BY(sqlbuilder.RefColumn("payment.payment_id").ASC(), Payment.Amount.DESC()).
		LIMIT(10).OFFSET(20)

	queryStr, args, err := query.Sql()

	assert.NilError(t, err)

	fmt.Println(queryStr)
	fmt.Println(args)

	dest := []model.Payment{}

	err = query.Query(db, &dest)

	assert.NilError(t, err)
	assert.Equal(t, len(dest), 10)
	assert.DeepEqual(t, dest[0], model.Payment{
		PaymentID: 17523,
		Amount:    4.99,
	})
	assert.DeepEqual(t, dest[1], model.Payment{
		PaymentID: 17524,
		Amount:    0.99,
	})
	assert.DeepEqual(t, dest[9], model.Payment{
		PaymentID: 17532,
		Amount:    8.99,
	})
}

func TestSelectWithCase(t *testing.T) {
	query := Payment.SELECT(
		sqlbuilder.CASE(Payment.StaffID).
			WHEN(sqlbuilder.IntLiteral(1)).THEN(sqlbuilder.Literal("ONE")).
			WHEN(sqlbuilder.IntLiteral(2)).THEN(sqlbuilder.Literal("TWO")).
			WHEN(sqlbuilder.IntLiteral(3)).THEN(sqlbuilder.Literal("THREE")).
			ELSE(sqlbuilder.Literal("OTHER")).AS("staff_id_num"),
	).
		ORDER_BY(Payment.PaymentID.ASC()).
		LIMIT(20)

	queryStr, _, err := query.Sql()

	assert.NilError(t, err)
	assert.Equal(t, queryStr, `SELECT (CASE payment.staff_id WHEN $1 THEN $2 WHEN $3 THEN $4 WHEN $5 THEN $6 ELSE $7 END) AS "staff_id_num" FROM dvds.payment ORDER BY payment.payment_id ASC LIMIT $8`)

	dest := []struct {
		StaffIdNum string
	}{}

	err = query.Query(db, &dest)

	assert.NilError(t, err)
	assert.Equal(t, len(dest), 20)
	assert.Equal(t, dest[0].StaffIdNum, "TWO")
	assert.Equal(t, dest[1].StaffIdNum, "ONE")
}

func TestLockTable(t *testing.T) {
	query := Address.LOCK().IN(sqlbuilder.LOCK_EXCLUSIVE).NOWAIT()

	queryStr, _, err := query.Sql()

	assert.NilError(t, err)
	assert.Equal(t, queryStr, `LOCK TABLE dvds.address IN EXCLUSIVE MODE NOWAIT`)

	tx, _ := db.Begin()

	_, err = query.Execute(tx)

	assert.NilError(t, err)
}

func int16Ptr(i int16) *int16 {
	return &i
}

func int32Ptr(i int32) *int32 {
	return &i
}

func stringPtr(s string) *string {
	return &s
}

func timeWithoutTimeZone(t string, precision int) *time.Time {

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

var customer0 = model.Customer{
	CustomerID: 1,
	StoreID:    1,
	FirstName:  "Mary",
	LastName:   "Smith",
	Email:      stringPtr("mary.smith@sakilacustomer.org"),
	Address:    nil,
	Activebool: true,
	CreateDate: *timeWithoutTimeZone("2006-02-14 00:00:00", 0),
	LastUpdate: timeWithoutTimeZone("2013-05-26 14:49:45.738", 3),
	Active:     int32Ptr(1),
}

var customer1 = model.Customer{
	CustomerID: 2,
	StoreID:    1,
	FirstName:  "Patricia",
	LastName:   "Johnson",
	Email:      stringPtr("patricia.johnson@sakilacustomer.org"),
	Address:    nil,
	Activebool: true,
	CreateDate: *timeWithoutTimeZone("2006-02-14 00:00:00", 0),
	LastUpdate: timeWithoutTimeZone("2013-05-26 14:49:45.738", 3),
	Active:     int32Ptr(1),
}

var lastCustomer = model.Customer{
	CustomerID: 599,
	StoreID:    2,
	FirstName:  "Austin",
	LastName:   "Cintron",
	Email:      stringPtr("austin.cintron@sakilacustomer.org"),
	Address:    nil,
	Activebool: true,
	CreateDate: *timeWithoutTimeZone("2006-02-14 00:00:00", 0),
	LastUpdate: timeWithoutTimeZone("2013-05-26 14:49:45.738", 3),
	Active:     int32Ptr(1),
}

package tests

import (
	"database/sql"
	"fmt"
	"github.com/sub0Zero/go-sqlbuilder/generator"
	"github.com/sub0Zero/go-sqlbuilder/sqlbuilder"
	"github.com/sub0Zero/go-sqlbuilder/tests/.test_files/dvd_rental/dvds/model"
	. "github.com/sub0Zero/go-sqlbuilder/tests/.test_files/dvd_rental/dvds/table"
	"gotest.tools/assert"
	"os"
	"testing"
	"time"
)

const (
	folderPath = ".test_files/"
	host       = "localhost"
	port       = 5432
	user       = "postgres"
	password   = "postgres"
	dbname     = "dvd_rental"
	schemaName = "dvds"
)

var connectString = fmt.Sprintf("host=%s port=%d user=%s "+"password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
var db *sql.DB

//go:generate generator -db "host=localhost port=5432 user=postgres password=postgres dbname=dvd_rental sslmode=disable" -dbName dvd_rental -schema dvds -path .test_files

func TestMain(m *testing.M) {
	fmt.Println("Begin")
	var err error
	db, err = sql.Open("postgres", connectString)
	if err != nil {
		panic("Failed to connect to test db")
	}
	defer db.Close()

	ret := m.Run()

	db.Close()
	fmt.Println("END")

	os.Exit(ret)
}

func TestGenerateModel(t *testing.T) {

	err := generator.Generate(folderPath, connectString, dbname, schemaName)

	assert.NilError(t, err)

	//err = generator.Generate(folderPath, connectString, dbname, "sport")
	//
	//assert.NilError(t, err)
}

func TestSelect_ScanToStruct(t *testing.T) {
	actor := model.Actor{}
	err := Actor.Select(Actor.AllColumns).Execute(db, &actor)

	assert.NilError(t, err)

	expectedActor := model.Actor{
		ActorID:    1,
		FirstName:  "Penelope",
		LastName:   "Guiness",
		LastUpdate: *timeWithoutTimeZone("2013-05-26 14:47:57.62 +0000"),
	}

	assert.DeepEqual(t, actor, expectedActor)
}

func TestSelect_ScanToSlice(t *testing.T) {
	customers := []model.Customer{}

	query := Customer.Select(Customer.AllColumns).OrderBy(Customer.CustomerID.Asc())

	queryStr, err := query.String()
	assert.NilError(t, err)
	fmt.Println(queryStr)
	assert.Equal(t, queryStr, `SELECT customer.customer_id AS "customer.customer_id", customer.store_id AS "customer.store_id", customer.first_name AS "customer.first_name", customer.last_name AS "customer.last_name", customer.email AS "customer.email", customer.address_id AS "customer.address_id", customer.activebool AS "customer.activebool", customer.create_date AS "customer.create_date", customer.last_update AS "customer.last_update", customer.active AS "customer.active" FROM dvds.customer ORDER BY customer.customer_id ASC`)

	err = query.Execute(db, &customers)
	assert.NilError(t, err)

	assert.Equal(t, len(customers), 599)

	assert.DeepEqual(t, customer0, customers[0])
	assert.DeepEqual(t, customer1, customers[1])
	assert.DeepEqual(t, lastCustomer, customers[598])
}

func TestJoinQueryStruct(t *testing.T) {

	query := FilmActor.
		InnerJoinUsing(Actor, FilmActor.ActorID, Actor.ActorID).
		InnerJoinUsing(Film, FilmActor.FilmID, Film.FilmID).
		InnerJoinUsing(Language, Film.LanguageID, Language.LanguageID).
		Select(FilmActor.AllColumns, Film.AllColumns, Language.AllColumns, Actor.AllColumns).
		Where(FilmActor.ActorID.GteLiteral(1).And(FilmActor.ActorID.LteLiteral(2)))

	queryStr, err := query.String()
	assert.NilError(t, err)
	assert.Equal(t, queryStr, `SELECT film_actor.actor_id AS "film_actor.actor_id", film_actor.film_id AS "film_actor.film_id", film_actor.last_update AS "film_actor.last_update",film.film_id AS "film.film_id", film.title AS "film.title", film.description AS "film.description", film.release_year AS "film.release_year", film.language_id AS "film.language_id", film.rental_duration AS "film.rental_duration", film.rental_rate AS "film.rental_rate", film.length AS "film.length", film.replacement_cost AS "film.replacement_cost", film.rating AS "film.rating", film.last_update AS "film.last_update", film.special_features AS "film.special_features", film.fulltext AS "film.fulltext",language.language_id AS "language.language_id", language.name AS "language.name", language.last_update AS "language.last_update",actor.actor_id AS "actor.actor_id", actor.first_name AS "actor.first_name", actor.last_name AS "actor.last_name", actor.last_update AS "actor.last_update" FROM dvds.film_actor JOIN dvds.actor ON film_actor.actor_id = actor.actor_id JOIN dvds.film ON film_actor.film_id = film.film_id JOIN dvds.language ON film.language_id = language.language_id WHERE (film_actor.actor_id>=1 AND film_actor.actor_id<=2)`)

	//fmt.Println(queryStr)

	filmActor := []model.FilmActor{}

	err = query.Execute(db, &filmActor)

	assert.NilError(t, err)

	//fmt.Println("ACTORS: --------------------")
	//spew.Dump(filmActor)
}

func TestJoinQuerySlice(t *testing.T) {
	type FilmsPerLanguage struct {
		Language *model.Language
		Film     *[]model.Film
	}

	filmsPerLanguage := []FilmsPerLanguage{}
	limit := 15

	query := Film.InnerJoinUsing(Language, Film.LanguageID, Language.LanguageID).
		Select(Language.AllColumns, Film.AllColumns).
		Limit(15)

	queryStr, err := query.String()

	assert.NilError(t, err)
	assert.Equal(t, queryStr, `SELECT language.language_id AS "language.language_id", language.name AS "language.name", language.last_update AS "language.last_update",film.film_id AS "film.film_id", film.title AS "film.title", film.description AS "film.description", film.release_year AS "film.release_year", film.language_id AS "film.language_id", film.rental_duration AS "film.rental_duration", film.rental_rate AS "film.rental_rate", film.length AS "film.length", film.replacement_cost AS "film.replacement_cost", film.rating AS "film.rating", film.last_update AS "film.last_update", film.special_features AS "film.special_features", film.fulltext AS "film.fulltext" FROM dvds.film JOIN dvds.language ON film.language_id = language.language_id LIMIT 15`)

	//fmt.Println(queryStr)

	err = query.Execute(db, &filmsPerLanguage)

	assert.NilError(t, err)

	//fmt.Println("--------------- result --------------- ")
	//spew.Dump(filmsPerLanguage)

	//spew.Dump(filmsPerLanguage)

	assert.Equal(t, len(filmsPerLanguage), 1)
	assert.Equal(t, len(*filmsPerLanguage[0].Film), limit)

	//spew.Dump(filmsPerLanguage)

	filmsPerLanguageWithPtrs := []*FilmsPerLanguage{}
	err = query.Execute(db, &filmsPerLanguageWithPtrs)

	assert.NilError(t, err)
	assert.Equal(t, len(filmsPerLanguage), 1)
	assert.Equal(t, len(*filmsPerLanguage[0].Film), limit)
}

func TestJoinQuerySliceWithPtrs(t *testing.T) {
	type FilmsPerLanguage struct {
		Language model.Language
		Film     *[]*model.Film
	}

	limit := int64(3)

	query := Film.InnerJoinUsing(Language, Film.LanguageID, Language.LanguageID).
		Select(Language.AllColumns, Film.AllColumns).
		Limit(limit)

	filmsPerLanguageWithPtrs := []*FilmsPerLanguage{}
	err := query.Execute(db, &filmsPerLanguageWithPtrs)

	//spew.Dump(filmsPerLanguageWithPtrs)

	assert.NilError(t, err)
	assert.Equal(t, len(filmsPerLanguageWithPtrs), 1)
	assert.Equal(t, len(*filmsPerLanguageWithPtrs[0].Film), int(limit))
}

func TestSelect_WithoutUniqueColumnSelected(t *testing.T) {
	query := Customer.Select(Customer.FirstName, Customer.LastName, Customer.Email)

	customers := []model.Customer{}

	err := query.Execute(db, &customers)

	assert.NilError(t, err)

	//spew.Dump(customers)

	assert.Equal(t, len(customers), 599)
}

func TestSelectOrderByAscDesc(t *testing.T) {
	customersAsc := []model.Customer{}

	err := Customer.Select(Customer.CustomerID, Customer.FirstName, Customer.LastName).
		OrderBy(Customer.FirstName.Asc()).
		Execute(db, &customersAsc)

	assert.NilError(t, err)

	firstCustomerAsc := customersAsc[0]
	lastCustomerAsc := customersAsc[len(customersAsc)-1]

	customersDesc := []model.Customer{}
	err = Customer.Select(Customer.CustomerID, Customer.FirstName, Customer.LastName).
		OrderBy(Customer.FirstName.Desc()).
		Execute(db, &customersDesc)

	assert.NilError(t, err)

	firstCustomerDesc := customersDesc[0]
	lastCustomerDesc := customersDesc[len(customersAsc)-1]

	assert.DeepEqual(t, firstCustomerAsc, lastCustomerDesc)
	assert.DeepEqual(t, lastCustomerAsc, firstCustomerDesc)

	customersAscDesc := []model.Customer{}
	err = Customer.Select(Customer.CustomerID, Customer.FirstName, Customer.LastName).
		OrderBy(Customer.FirstName.Asc(), Customer.LastName.Desc()).
		Execute(db, &customersAscDesc)

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
		FullJoin(Address, Customer.AddressID, Address.AddressID).
		Select(Customer.AllColumns, Address.AllColumns).
		OrderBy(Customer.CustomerID.Asc())

	queryStr, err := query.String()

	assert.NilError(t, err)

	assert.Equal(t, queryStr, `SELECT customer.customer_id AS "customer.customer_id", customer.store_id AS "customer.store_id", customer.first_name AS "customer.first_name", customer.last_name AS "customer.last_name", customer.email AS "customer.email", customer.address_id AS "customer.address_id", customer.activebool AS "customer.activebool", customer.create_date AS "customer.create_date", customer.last_update AS "customer.last_update", customer.active AS "customer.active",address.address_id AS "address.address_id", address.address AS "address.address", address.address2 AS "address.address2", address.district AS "address.district", address.city_id AS "address.city_id", address.postal_code AS "address.postal_code", address.phone AS "address.phone", address.last_update AS "address.last_update" FROM dvds.customer FULL JOIN dvds.address ON customer.address_id = address.address_id ORDER BY customer.customer_id ASC`)

	allCustomersAndAddress := []struct {
		Address  *model.Address
		Customer *model.Customer
	}{}

	err = query.Execute(db, &allCustomersAndAddress)

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
		CrossJoin(Address).
		Select(Customer.AllColumns, Address.AllColumns).
		OrderBy(Customer.CustomerID.Asc()).
		Limit(1000)

	queryStr, err := query.String()

	assert.NilError(t, err)
	assert.Equal(t, queryStr, `SELECT customer.customer_id AS "customer.customer_id", customer.store_id AS "customer.store_id", customer.first_name AS "customer.first_name", customer.last_name AS "customer.last_name", customer.email AS "customer.email", customer.address_id AS "customer.address_id", customer.activebool AS "customer.activebool", customer.create_date AS "customer.create_date", customer.last_update AS "customer.last_update", customer.active AS "customer.active",address.address_id AS "address.address_id", address.address AS "address.address", address.address2 AS "address.address2", address.district AS "address.district", address.city_id AS "address.city_id", address.postal_code AS "address.postal_code", address.phone AS "address.phone", address.last_update AS "address.last_update" FROM dvds.customer CROSS JOIN dvds.address ORDER BY customer.customer_id ASC LIMIT 1000`)

	customerAddresCrosJoined := []model.Customer{}

	err = query.Execute(db, &customerAddresCrosJoined)

	assert.Equal(t, len(customerAddresCrosJoined), 1000)

	assert.NilError(t, err)
}

func TestSelectSelfJoin(t *testing.T) {

	f1 := Film.As("f1")

	//spew.Dump(f1)
	f2 := Film.As("f2")

	query := f1.
		InnerJoinOn(f2, f1.FilmID.Neq(f2.FilmID).And(f1.Length.Eq(f2.Length))).
		Select(f1.AllColumns, f2.AllColumns).
		OrderBy(f1.FilmID)

	queryStr, err := query.String()

	assert.NilError(t, err)

	fmt.Println(queryStr)

	type F1 model.Film
	type F2 model.Film

	theSameLengthFilms := []struct {
		F1 F1
		F2 F2
	}{}

	err = query.Execute(db, &theSameLengthFilms)

	assert.NilError(t, err)

	//spew.Dump(theSameLengthFilms[0])

	assert.Equal(t, len(theSameLengthFilms), 6972)
}

func TestSelectAliasColumn(t *testing.T) {
	f1 := Film.As("f1")
	f2 := Film.As("f2")

	type thesameLengthFilms struct {
		Title1 string
		Title2 string
		Length int16
	}

	query := f1.
		InnerJoinOn(f2, f1.FilmID.Neq(f2.FilmID).And(f1.Length.Eq(f2.Length))).
		Select(f1.Title.As("thesame_length_films.title1"),
			f2.Title.As("thesame_length_films.title2"),
			f1.Length.As("thesame_length_films.length")).
		OrderBy(f1.Length.Asc(), f1.Title.Asc(), f2.Title.Asc()).
		Limit(1000)

	queryStr, err := query.String()

	assert.NilError(t, err)

	fmt.Println(queryStr)

	films := []thesameLengthFilms{}

	err = query.Execute(db, &films)

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

	manager := Staff.As("manager")

	query := Staff.
		InnerJoinUsing(Address, Staff.AddressID, Address.AddressID).
		InnerJoinUsing(manager, Staff.StaffID, manager.StaffID).
		Select(Staff.StaffID, Staff.FirstName, Staff.LastName, Address.AllColumns, manager.StaffID, manager.FirstName)

	queryStr, err := query.String()
	assert.NilError(t, err)
	fmt.Println(queryStr)

	staffs := []staff{}

	err = query.Execute(db, &staffs)

	assert.NilError(t, err)

	//spew.Dump(staffs)
}

func TestSubQuery(t *testing.T) {

	//selectStmtTable := Actor.Select(Actor.FirstName, Actor.LastName).AsTable("table_expression")
	//
	//query := selectStmtTable.Select(
	//	selectStmtTable.ColumnFrom(Actor.FirstName).As("nesto"),
	//	selectStmtTable.Column("actor.last_name").As("nesto2"),
	//	)
	//
	//queryStr, err := query.String()
	//
	//assert.NilError(t, err)
	//
	//fmt.Println(queryStr)

	//avrgCustomer := Customer.Select(Customer.LastName).Limit(1).AsExpression()
	//
	//Customer.
	//	InnerJoinUsing(selectStmtTable, Customer.LastName, selectStmtTable.Column("first_name")).
	//	Select(Customer.AllColumns, selectStmtTable.Column("first_name")).
	//	Where(Actor.LastName.Neq(avrgCustomer))

	rFilmsOnly := Film.Select(Film.FilmID, Film.Title, Film.Rating).
		Where(Film.Rating.Eq(sqlbuilder.Literal("R"))).
		AsTable("films")

	query := Actor.InnerJoinUsing(FilmActor, Actor.ActorID, FilmActor.FilmID).
		InnerJoinUsing(rFilmsOnly, FilmActor.FilmID, rFilmsOnly.ColumnFrom(Film.FilmID)).
		Select(
			Actor.AllColumns,
			FilmActor.AllColumns,
			rFilmsOnly.ColumnFrom(Film.Title).As("film.title"),
			rFilmsOnly.ColumnFrom(Film.Rating).As("film.rating"),
		)

	queryStr, err := query.String()

	assert.NilError(t, err)

	fmt.Println(queryStr)

}

func TestSelectFunctions(t *testing.T) {
	query := Film.Select(sqlbuilder.MAX(Film.RentalRate).As("max_film_rate"))

	str, err := query.String()

	assert.NilError(t, err)

	assert.Equal(t, str, `SELECT MAX(film.rental_rate) AS "max_film_rate" FROM dvds.film`)

	fmt.Println(str)
}

func TestSelectQueryScalar(t *testing.T) {

	maxFilmRentalRate := Film.Select(sqlbuilder.MAX(Film.RentalRate))

	query := Film.Select(Film.AllColumns).
		Where(Film.RentalRate.Eq(maxFilmRentalRate)).
		OrderBy(Film.FilmID)

	queryStr, err := query.String()

	assert.NilError(t, err)

	fmt.Println(queryStr)

	maxRentalRateFilms := []model.Film{}
	err = query.Execute(db, &maxRentalRateFilms)

	assert.NilError(t, err)

	assert.Equal(t, len(maxRentalRateFilms), 336)

	assert.DeepEqual(t, maxRentalRateFilms[0], model.Film{
		FilmID:          2,
		Title:           "Ace Goldfinger",
		Description:     stringPtr("A Astounding Epistle of a Database Administrator And a Explorer who must Find a Car in Ancient China"),
		ReleaseYear:     int32Ptr(2006),
		Language:        nil,
		RentalRate:      4.99,
		Length:          int16Ptr(48),
		ReplacementCost: 12.99,
		Rating:          stringPtr("G"),
		RentalDuration:  3,
		LastUpdate:      *timeWithoutTimeZone("2013-05-26 14:50:58.951 +0000"),
		SpecialFeatures: stringPtr("{Trailers,\"Deleted Scenes\"}"),
		Fulltext:        "'ace':1 'administr':9 'ancient':19 'astound':4 'car':17 'china':20 'databas':8 'epistl':5 'explor':12 'find':15 'goldfing':2 'must':14",
	})

	//spew.Dump(maxRentalRateFilms[0])
}

func TestSelectGroupByHaving(t *testing.T) {
	customersPaymentQuery := Payment.
		Select(
			Payment.CustomerID.As("customer_payment_sum.customer_id"),
			sqlbuilder.SUM(Payment.Amount).As("customer_payment_sum.amount_sum"),
		).
		GroupBy(Payment.CustomerID).
		OrderBy(sqlbuilder.SUM(Payment.Amount)).
		HAVING(sqlbuilder.Gt(sqlbuilder.SUM(Payment.Amount), sqlbuilder.Literal(100)))

	queryStr, err := customersPaymentQuery.String()

	assert.NilError(t, err)
	fmt.Println(queryStr)

	assert.Equal(t, queryStr, `SELECT payment.customer_id AS "customer_payment_sum.customer_id",SUM(payment.amount) AS "customer_payment_sum.amount_sum" FROM dvds.payment GROUP BY payment.customer_id HAVING SUM(payment.amount)>100 ORDER BY SUM(payment.amount)`)

	type CustomerPaymentSum struct {
		CustomerID int16
		AmountSum  float64
	}

	customerPaymentSum := []CustomerPaymentSum{}

	err = customersPaymentQuery.Execute(db, &customerPaymentSum)

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
		Select(
			Payment.CustomerID,
			sqlbuilder.SUM(Payment.Amount).As("amount_sum"),
		).
		GroupBy(Payment.CustomerID)

	customersPaymentTable := customersPaymentSubQuery.AsTable("customer_payment_sum")
	amountSumColumn := customersPaymentTable.Column("amount_sum")

	query := Customer.
		InnerJoinUsing(customersPaymentTable, Customer.CustomerID, customersPaymentTable.ColumnFrom(Payment.CustomerID)).
		Select(Customer.AllColumns, amountSumColumn.As("customer_with_amounts.amount_sum")).
		OrderBy(amountSumColumn)

	queryStr, err := query.String()
	assert.NilError(t, err)
	fmt.Println(queryStr)

	err = query.Execute(db, &customersWithAmounts)
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
		CreateDate: *timeWithoutTimeZone("2006-02-14 00:00:00 +0000"),
		LastUpdate: timeWithoutTimeZone("2013-05-26 14:49:45.738 +0000"),
		Active:     int32Ptr(1),
	})

	assert.Equal(t, customersWithAmounts[0].AmountSum, 27.93)

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

func timeWithoutTimeZone(t string) *time.Time {
	time, err := time.Parse("2006-01-02 15:04:05 -0700", t)

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
	CreateDate: *timeWithoutTimeZone("2006-02-14 00:00:00 +0000"),
	LastUpdate: timeWithoutTimeZone("2013-05-26 14:49:45.738 +0000"),
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
	CreateDate: *timeWithoutTimeZone("2006-02-14 00:00:00 +0000"),
	LastUpdate: timeWithoutTimeZone("2013-05-26 14:49:45.738 +0000"),
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
	CreateDate: *timeWithoutTimeZone("2006-02-14 00:00:00 +0000"),
	LastUpdate: timeWithoutTimeZone("2013-05-26 14:49:45.738 +0000"),
	Active:     int32Ptr(1),
}

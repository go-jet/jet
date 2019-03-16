package tests

import (
	"database/sql"
	"fmt"
	"github.com/sub0Zero/go-sqlbuilder/generator"
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
		Films    *[]model.Film
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

	assert.Equal(t, len(filmsPerLanguage), 1)
	assert.Equal(t, len(*filmsPerLanguage[0].Films), limit)

	//spew.Dump(filmsPerLanguage)

	filmsPerLanguageWithPtrs := []*FilmsPerLanguage{}
	err = query.Execute(db, &filmsPerLanguageWithPtrs)

	assert.NilError(t, err)
	assert.Equal(t, len(filmsPerLanguage), 1)
	assert.Equal(t, len(*filmsPerLanguage[0].Films), limit)

}

func TestJoinQuerySliceWithPtrs(t *testing.T) {
	type FilmsPerLanguage struct {
		Language model.Language
		Films    *[]*model.Film
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
	assert.Equal(t, len(*filmsPerLanguageWithPtrs[0].Films), int(limit))
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

	assert.NilError(t, err)
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

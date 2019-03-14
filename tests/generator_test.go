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
	err := Actor.Select(Actor.All...).Execute(db, &actor)

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

	query := Customer.Select(Customer.All...)

	queryStr, err := query.String()

	assert.NilError(t, err)
	assert.Equal(t, queryStr, `SELECT customer.customer_id AS "customer.customer_id",customer.store_id AS "customer.store_id",customer.first_name AS "customer.first_name",customer.last_name AS "customer.last_name",customer.email AS "customer.email",customer.address_id AS "customer.address_id",customer.activebool AS "customer.activebool",customer.create_date AS "customer.create_date",customer.last_update AS "customer.last_update",customer.active AS "customer.active" FROM dvds.customer`)

	err = query.Execute(db, &customers)

	assert.NilError(t, err)

	customer0 := model.Customer{
		CustomerID: 524,
		StoreID:    1,
		FirstName:  "Jared",
		LastName:   "Ely",
		Email:      stringPtr("austin.cintron@sakilacustomer.org"),
		Address:    nil,
		Activebool: true,
		CreateDate: *timeWithoutTimeZone("2006-02-14 00:00:00 +0000"),
		LastUpdate: timeWithoutTimeZone("2013-05-26 14:49:45.738 +0000"),
		Active:     int32Ptr(1),
	}

	customer1 := model.Customer{
		CustomerID: 1,
		StoreID:    1,
		FirstName:  "Mary",
		LastName:   "Smith",
		Email:      stringPtr("austin.cintron@sakilacustomer.org"),
		Address:    nil,
		Activebool: true,
		CreateDate: *timeWithoutTimeZone("2006-02-14 00:00:00 +0000"),
		LastUpdate: timeWithoutTimeZone("2013-05-26 14:49:45.738 +0000"),
		Active:     int32Ptr(1),
	}

	lastCustomer := model.Customer{
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

	assert.Equal(t, len(customers), 599)

	assert.DeepEqual(t, customer0, customers[0])
	assert.DeepEqual(t, customer1, customers[1])
	assert.DeepEqual(t, lastCustomer, customers[598])
}

func TestJoinQueryStruct(t *testing.T) {

	//filmActor := model.FilmActor{}
	allFilmActorColumns := append(append(append(FilmActor.All, Film.All...), Language.All...), Actor.All...)
	query := FilmActor.
		InnerJoinOn(Actor, sqlbuilder.Eq(FilmActor.ActorID, Actor.ActorID)).
		InnerJoinOn(Film, sqlbuilder.Eq(FilmActor.FilmID, Film.FilmID)).
		InnerJoinOn(Language, sqlbuilder.Eq(Film.LanguageID, Language.LanguageID)).
		Select(allFilmActorColumns...).
		Where(sqlbuilder.And(sqlbuilder.Gte(FilmActor.ActorID, sqlbuilder.Literal(1)), sqlbuilder.Lte(FilmActor.ActorID, sqlbuilder.Literal(2))))

	queryStr, err := query.String()
	assert.NilError(t, err)

	fmt.Println(queryStr)

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

	query := Film.InnerJoinOn(Language, sqlbuilder.Eq(Film.LanguageID, Language.LanguageID)).
		Select(append(Language.All, Film.All...)...).
		Limit(15)

	queryStr, _ := query.String()

	fmt.Println(queryStr)

	err := query.Execute(db, &filmsPerLanguage)

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

	query := Film.InnerJoinOn(Language, sqlbuilder.Eq(Film.LanguageID, Language.LanguageID)).
		Select(append(Language.All, Film.All...)...).
		Limit(limit)

	filmsPerLanguageWithPtrs := []*FilmsPerLanguage{}
	err := query.Execute(db, &filmsPerLanguageWithPtrs)

	//spew.Dump(filmsPerLanguageWithPtrs)

	assert.NilError(t, err)
	assert.Equal(t, len(filmsPerLanguageWithPtrs), 1)
	assert.Equal(t, len(*filmsPerLanguageWithPtrs[0].Films), int(limit))
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

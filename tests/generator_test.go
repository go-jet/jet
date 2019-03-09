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

func TestSelectQuery(t *testing.T) {
	//query := Actor.InnerJoinOn(Store, Eq(Actor.ActorID, Store.StoreID)).
	//	Select(Store.StoreID, Store.AddressID, Actor.ActorID)
	//
	//queryStr, err := query.String(schemaName)
	//
	//assert.NilError(t, err)
	//
	//assert.Equal(t, queryStr, "SELECT store.store_id,store.address_id,actor.actor_id FROM dvds.actor JOIN dvds.store ON actor.actor_id=store.store_id")
	//
	//err = query.Execute(db, nil)

	customers := []model.Customer{}

	query := Customer.Select(Customer.All...)

	queryStr, err := query.String()

	assert.NilError(t, err)
	assert.Equal(t, queryStr, `SELECT customer.customer_id AS "customer.customer_id",customer.store_id AS "customer.store_id",customer.first_name AS "customer.first_name",customer.last_name AS "customer.last_name",customer.email AS "customer.email",customer.address_id AS "customer.address_id",customer.activebool AS "customer.activebool",customer.create_date AS "customer.create_date",customer.last_update AS "customer.last_update",customer.active AS "customer.active" FROM dvds.customer`)
	//fmt.Println(queryStr)

	err = query.Execute(db, &customers)

	//fmt.Println(customers)
	//
	//spew.Sdump(customers)

	assert.NilError(t, err)

	assert.Equal(t, len(customers), 599)

	actor := model.Actor{}
	err = Actor.Select(Actor.All...).Execute(db, &actor)

	assert.NilError(t, err)

	//spew.Dump(actor)
	//time, _ := time.Parse("2006-01-02 15:04:05.00MST", "2013-05-26 14:47:57.62MST")
	assert.Equal(t, actor.ActorID, int32(1))
	assert.Equal(t, actor.FirstName, "Penelope")
	assert.Equal(t, actor.LastName, "Guiness")
}

func TestJoinQuery(t *testing.T) {

	//filmActor := model.FilmActor{}
	allFilmActorColumns := append(append(Actor.All, Film.All...), Language.All...)
	query := FilmActor.
		InnerJoinOn(Actor, sqlbuilder.Eq(FilmActor.ActorID, Actor.ActorID)).
		InnerJoinOn(Film, sqlbuilder.Eq(FilmActor.FilmID, Film.FilmID)).
		InnerJoinOn(Language, sqlbuilder.Eq(Film.LanguageID, Language.LanguageID)).
		Select(allFilmActorColumns...).
		Where(sqlbuilder.Eq(FilmActor.ActorID, sqlbuilder.Literal(1)))

	queryStr, err := query.String()
	assert.NilError(t, err)

	fmt.Println(queryStr)

	filmActor := model.FilmActor{}

	err = query.Execute(db, &filmActor)

	assert.NilError(t, err)

	//spew.Dump(filmActor)
}

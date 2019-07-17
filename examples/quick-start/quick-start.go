package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/lib/pq"

	// dot import so go code would resemble as much as native SQL
	// dot import is not mandatory
	. "github.com/go-jet/jet"
	. "github.com/go-jet/jet/examples/quick-start/.gen/jetdb/dvds/table"

	"github.com/go-jet/jet/examples/quick-start/.gen/jetdb/dvds/model"
	"github.com/go-jet/jet/tests/dbconfig"
)

func main() {

	db, err := sql.Open("postgres", dbconfig.ConnectString)
	panicOnError(err)
	defer db.Close()

	stmt := SELECT(
		Actor.ActorID, Actor.FirstName, Actor.LastName, Actor.LastUpdate,
		Film.AllColumns,
		Language.AllColumns,
		Category.AllColumns,
	).FROM(
		Actor.
			INNER_JOIN(FilmActor, Actor.ActorID.EQ(FilmActor.ActorID)).
			INNER_JOIN(Film, Film.FilmID.EQ(FilmActor.FilmID)).
			INNER_JOIN(Language, Language.LanguageID.EQ(Film.LanguageID)).
			INNER_JOIN(FilmCategory, FilmCategory.FilmID.EQ(Film.FilmID)).
			INNER_JOIN(Category, Category.CategoryID.EQ(FilmCategory.CategoryID)),
	).WHERE(
		Language.Name.EQ(String("English")).
			AND(Category.Name.NOT_EQ(String("Action"))).
			AND(Film.Length.GT(Int(180))),
	).ORDER_BY(
		Actor.ActorID.ASC(),
		Film.FilmID.ASC(),
	)

	query, args, err := stmt.Sql()
	panicOnError(err)

	fmt.Println("Parameterized query: ")
	fmt.Println(query)
	fmt.Println("Arguments: ")
	fmt.Println(args)

	debugSql, err := stmt.DebugSql()
	panicOnError(err)

	fmt.Println("Debug sql: ")
	fmt.Println(debugSql)

	var dest []struct {
		model.Actor
		Films []struct {
			model.Film
			Language   model.Language
			Categories []model.Category
		}
	}

	err = stmt.Query(db, &dest)
	panicOnError(err)

	fmt.Println("dest to json: ")
	jsonText, _ := json.MarshalIndent(dest, "", "\t")
	fmt.Println(string(jsonText))

	// New Destination

	var dest2 []struct {
		model.Category

		Films  []model.Film
		Actors []model.Actor
	}

	err = stmt.Query(db, &dest2)
	panicOnError(err)

	fmt.Println("dest2 to json: ")
	jsonText, _ = json.MarshalIndent(dest2, "", "\t")
	fmt.Println(string(jsonText))
}

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

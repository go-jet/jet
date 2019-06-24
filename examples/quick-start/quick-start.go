package main

import (
	"database/sql"
	"encoding/json"
	"fmt"

	. "github.com/go-jet/jet"                                           // dot import so go code would resemble as much as native SQL
	. "github.com/go-jet/jet/examples/quick-start/gen/jetdb/dvds/table" // dot import is not mandatory

	"github.com/go-jet/jet/examples/quick-start/gen/jetdb/dvds/model"
	"github.com/go-jet/jet/tests/dbconfig"
)

func main() {

	db, err := sql.Open("postgres", dbconfig.ConnectString)
	panicOnError(err)
	defer db.Close()

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
		Language.Name.EQ(String("English")). // note every column has type.
							AND(Category.Name.NOT_EQ(String("Action"))). // String column Language.Name and Category.Name can be compared only with string columns and expressions
							AND(Film.Length.GT(Int(180))),               // Film.Length is integer column and can be compared only with integer columns and expressions
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
			Language model.Language
			Category []model.Category
		}
	}

	err = stmt.Query(db, &dest)
	panicOnError(err)

	fmt.Println("dest to json: ")
	jsonText, _ := json.MarshalIndent(dest, "", "\t")
	fmt.Println(string(jsonText))

	var dest2 []struct {
		model.Category

		Film  []model.Film
		Actor []model.Actor
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

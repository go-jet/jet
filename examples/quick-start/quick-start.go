package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"

	_ "github.com/lib/pq"

	// dot import so that jet go code would resemble as much as native SQL
	// dot import is not mandatory
	. "github.com/go-jet/jet/v2/examples/quick-start/.gen/jetdb/dvds/table"
	. "github.com/go-jet/jet/v2/postgres"

	"github.com/go-jet/jet/v2/examples/quick-start/.gen/jetdb/dvds/enum"
	"github.com/go-jet/jet/v2/examples/quick-start/.gen/jetdb/dvds/model"
)

const (
	host     = "localhost"
	port     = 5432
	user     = "jet"
	password = "jet"
	dbName   = "jetdb"
)

func main() {
	// Connect to database
	var connectString = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbName)

	db, err := sql.Open("postgres", connectString)
	panicOnError(err)
	defer db.Close()

	// Write query
	stmt := SELECT(
		Actor.ActorID, Actor.FirstName, Actor.LastName, Actor.LastUpdate, // or just Actor.AllColumns
		Film.AllColumns,
		Language.AllColumns.Except(Language.LastUpdate), // all language columns except last_update
		Category.AllColumns,
	).FROM(
		Actor.
			INNER_JOIN(FilmActor, Actor.ActorID.EQ(FilmActor.ActorID)).
			INNER_JOIN(Film, Film.FilmID.EQ(FilmActor.FilmID)).
			INNER_JOIN(Language, Language.LanguageID.EQ(Film.LanguageID)).
			INNER_JOIN(FilmCategory, FilmCategory.FilmID.EQ(Film.FilmID)).
			INNER_JOIN(Category, Category.CategoryID.EQ(FilmCategory.CategoryID)),
	).WHERE(
		AND(
			Language.Name.EQ(Char(20)("English")), // string columns Language.Name and Category.Name can be compared only with string expression
			Category.Name.NOT_EQ(Text("Action")),
			Film.Length.GT(Int32(180)), // Film.Length is integer column and can be compared only with integer expression
			Film.Rating.NOT_EQ(enum.MpaaRating.R),
			String("Trailers").EQ(ANY(Film.SpecialFeatures)), // type safety is also enforced on array element types
		),
	).ORDER_BY(
		Actor.ActorID.ASC(),
		Film.FilmID.ASC(),
	)

	// Execute query and store result
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

	printStatementInfo(stmt)
	jsonSave("./dest.json", dest)

	// New Destination

	var dest2 []struct {
		model.Category

		Films  []model.Film
		Actors []model.Actor
	}

	err = stmt.Query(db, &dest2)
	panicOnError(err)

	jsonSave("./dest2.json", dest2)
}

func jsonSave(path string, v interface{}) {
	jsonText, _ := json.MarshalIndent(v, "", "\t")

	err := os.WriteFile(path, jsonText, 0600)

	panicOnError(err)
}

func printStatementInfo(stmt SelectStatement) {
	query, args := stmt.Sql()

	fmt.Println("Parameterized query: ")
	fmt.Println("==============================")
	fmt.Println(query)
	fmt.Println("Arguments: ")
	fmt.Println(args)

	debugSQL := stmt.DebugSql()

	fmt.Println("\n\nDebug sql: ")
	fmt.Println("==============================")
	fmt.Println(debugSQL)
}

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

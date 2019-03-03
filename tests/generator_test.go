package tests

import (
	"fmt"
	"github.com/sub0Zero/go-sqlbuilder/generator"
	. "github.com/sub0Zero/go-sqlbuilder/sqlbuilder"
	"github.com/sub0Zero/go-sqlbuilder/tests/.test_files/dvd_rental/public/table"
	"gotest.tools/assert"
	"testing"
)

var (
	folderPath = ".test_files/"
	host       = "localhost"
	port       = 5432
	user       = "postgres"
	password   = "postgres"
	dbname     = "dvd_rental"
	schemaName = "public"
)

//go:generate generator -db "host=localhost port=5432 user=postgres password=postgres dbname=dvd_rental sslmode=disable" -dbName dvd_rental -schema public -path .test_files

func TestGenerateModel(t *testing.T) {
	connectString := fmt.Sprintf("host=%s port=%d user=%s "+"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	err := generator.Generate(folderPath, connectString, dbname, schemaName)

	assert.NilError(t, err)
}

func TestSelectQuery(t *testing.T) {
	query, err := table.Actor.InnerJoinOn(table.Store, Eq(table.Actor.ActorID, table.Store.StoreID)).
		Select(table.Store.StoreID, table.Store.AddressID, table.Actor.ActorID).String(schemaName)

	assert.NilError(t, err)

	assert.Equal(t, query, "SELECT store.store_id,store.address_id,actor.actor_id FROM public.actor JOIN public.store ON actor.actor_id=store.store_id")
}

package tests

import (
	"database/sql"
	"github.com/go-jet/jet/tests/.test_files/dvd_rental/dvds/model"
	"github.com/go-jet/jet/tests/dbconfig"
	_ "github.com/lib/pq"
	"github.com/pkg/profile"
	"gotest.tools/assert"
	"os"
	"reflect"
	"testing"
)

var db *sql.DB

func TestMain(m *testing.M) {
	defer profile.Start().Stop()

	var err error
	db, err = sql.Open("postgres", dbconfig.ConnectString)
	if err != nil {
		panic("Failed to connect to test db")
	}
	defer db.Close()

	ret := m.Run()

	os.Exit(ret)
}

func TestGenerateModel(t *testing.T) {

	actor := model.Actor{}

	assert.Equal(t, reflect.TypeOf(actor.ActorID).String(), "int32")
	actorIDField, ok := reflect.TypeOf(actor).FieldByName("ActorID")
	assert.Assert(t, ok)
	assert.Equal(t, actorIDField.Tag.Get("sql"), "unique")
	assert.Equal(t, reflect.TypeOf(actor.FirstName).String(), "string")
	assert.Equal(t, reflect.TypeOf(actor.LastName).String(), "string")
	assert.Equal(t, reflect.TypeOf(actor.LastUpdate).String(), "time.Time")

	filmActor := model.FilmActor{}

	assert.Equal(t, reflect.TypeOf(filmActor.FilmID).String(), "int16")
	filmIDField, ok := reflect.TypeOf(filmActor).FieldByName("FilmID")
	assert.Assert(t, ok)
	assert.Equal(t, filmIDField.Tag.Get("sql"), "unique")

	assert.Equal(t, reflect.TypeOf(filmActor.ActorID).String(), "int16")
	actorIDField, ok = reflect.TypeOf(filmActor).FieldByName("ActorID")
	assert.Assert(t, ok)
	assert.Equal(t, filmIDField.Tag.Get("sql"), "unique")

	staff := model.Staff{}

	assert.Equal(t, reflect.TypeOf(staff.Email).String(), "*string")
	assert.Equal(t, reflect.TypeOf(staff.Picture).String(), "[]uint8")
}

package sqlite

import (
	"github.com/go-jet/jet/v2/internal/testutils"
	"github.com/go-jet/jet/v2/qrm"
	"github.com/go-jet/jet/v2/internal/utils/ptr"
	"github.com/stretchr/testify/require"
	"testing"

	. "github.com/go-jet/jet/v2/sqlite"
	"github.com/go-jet/jet/v2/tests/.gentestdata/sqlite/test_sample/model"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/sqlite/test_sample/table"
)

func TestMutableColumnsExcludeGeneratedColumn(t *testing.T) {

	t.Run("should not have the generated column in mutableColumns", func(t *testing.T) {
		require.Equal(t, 2, len(People.MutableColumns))
		require.Equal(t, People.PeopleName, People.MutableColumns[0])
		require.Equal(t, People.PeopleHeightCm, People.MutableColumns[1])
	})

	t.Run("should query with all columns", func(t *testing.T) {
		query := SELECT(
			People.AllColumns,
		).FROM(
			People,
		).WHERE(
			People.PeopleID.EQ(Int(3)),
		)

		testutils.AssertStatementSql(t, query, `
SELECT people.people_id AS "people.people_id",
     people.people_name AS "people.people_name",
     people.people_height_cm AS "people.people_height_cm",
     people.people_height_inch AS "people.people_height_inch",
     people.people_height_feet AS "people.people_height_feet"
FROM people
WHERE people.people_id = ?;
`)
		var result model.People

		err := query.Query(sampleDB, &result)
		require.NoError(t, err)

		require.Equal(t, "Carla", result.PeopleName)
		require.Equal(t, 155., *result.PeopleHeightCm)
		require.InEpsilon(t, 61.02, *result.PeopleHeightInch, 1e-3)
	})

	t.Run("should insert without generated columns", func(t *testing.T) {
		testutils.ExecuteInTxAndRollback(t, sampleDB, func(tx qrm.DB) {
			insertQuery := People.INSERT(
				People.MutableColumns,
			).MODEL(
				model.People{
					PeopleName:     "Dario",
					PeopleHeightCm: ptr.Of(190.0),
				},
			).RETURNING(
				People.AllColumns,
			)

			testutils.AssertDebugStatementSql(t, insertQuery, `
INSERT INTO people (people_name, people_height_cm)
VALUES ('Dario', 190)
RETURNING people.people_id AS "people.people_id",
          people.people_name AS "people.people_name",
          people.people_height_cm AS "people.people_height_cm",
          people.people_height_inch AS "people.people_height_inch",
          people.people_height_feet AS "people.people_height_feet";
`)
			var result model.People
			err := insertQuery.Query(tx, &result)
			require.NoError(t, err)

			require.Equal(t, "Dario", result.PeopleName)
			require.Equal(t, 190., *result.PeopleHeightCm)
			require.InEpsilon(t, float32(74.80314), *result.PeopleHeightInch, 1e-3)
			require.InEpsilon(t, float32(6.233595), *result.PeopleHeightFeet, 1e-3)
		})
	})
}

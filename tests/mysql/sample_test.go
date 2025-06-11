package mysql

import (
	"github.com/go-jet/jet/v2/internal/testutils"
	"github.com/go-jet/jet/v2/internal/utils/ptr"
	"github.com/go-jet/jet/v2/qrm"
	"github.com/stretchr/testify/require"
	"testing"

	. "github.com/go-jet/jet/v2/mysql"
	"github.com/go-jet/jet/v2/tests/.gentestdata/mysql/test_sample/model"
	. "github.com/go-jet/jet/v2/tests/.gentestdata/mysql/test_sample/table"

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

		var result model.People

		err := query.Query(db, &result)
		require.NoError(t, err)

		require.Equal(t, "Carla", result.PeopleName)
		require.Equal(t, 155., *result.PeopleHeightCm)
		require.InEpsilon(t, 61.02, *result.PeopleHeightIn, 1e-3)
	})

	t.Run("should insert without generated columns", func(t *testing.T) {
		testutils.ExecuteInTxAndRollback(t, db, func(tx qrm.DB) {
			insertQuery := People.INSERT(
				People.MutableColumns,
			).MODEL(
				model.People{
					PeopleName:     "Dario",
					PeopleHeightCm: ptr.Of(120.0),
				},
			)

			testutils.AssertDebugStatementSql(t, insertQuery, `
INSERT INTO test_sample.people (people_name, people_height_cm)
VALUES ('Dario', 120);
`)
			_, err := insertQuery.Exec(tx)
			require.NoError(t, err)

			var result model.People
			selectQuery := SELECT(
				People.MutableColumns,
			).FROM(
				People,
			).ORDER_BY(
				People.PeopleID.DESC(),
			).LIMIT(1)

			err = selectQuery.Query(tx, &result)
			require.NoError(t, err)

			require.Equal(t, "Dario", result.PeopleName)
			require.Equal(t, 120., *result.PeopleHeightCm)

			query := SELECT(
				People.AllColumns,
			).FROM(
				People,
			).ORDER_BY(
				People.PeopleID.DESC(),
			).LIMIT(1)

			result = model.People{}

			err = query.Query(tx, &result)
			require.NoError(t, err)

			require.Equal(t, "Dario", result.PeopleName)
			require.Equal(t, 120., *result.PeopleHeightCm)
			require.InEpsilon(t, 47.24, *result.PeopleHeightIn, 1e-3)
		})
	})
}
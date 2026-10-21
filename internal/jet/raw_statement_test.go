package jet_test

import (
	"strings"
	"testing"

	"github.com/go-jet/jet/v2/internal/jet"
	"github.com/go-jet/jet/v2/mysql"
	"github.com/go-jet/jet/v2/postgres"
	"github.com/go-jet/jet/v2/sqlite"
	"github.com/stretchr/testify/require"
)

func TestRawStatementOverlappingArgumentNames(t *testing.T) {
	dialects := []struct {
		name      string
		statement func(string, ...map[string]interface{}) jet.SerializerStatement
		numbered  bool
	}{
		{"postgres", postgres.RawStatement, true},
		{"mysql", mysql.RawStatement, false},
		{"sqlite", sqlite.RawStatement, false},
	}
	cases := []struct {
		name, query, numberedSQL, questionSQL, debugSQL string
		args                                            map[string]interface{}
		numberedArgs, questionArgs                      []interface{}
	}{
		{name: "shorter first", query: "SELECT :level, :levels, :level",
			args:        map[string]interface{}{":level": 11, ":levels": 22},
			numberedSQL: "SELECT $1, $2, $1;", questionSQL: "SELECT ?, ?, ?;", debugSQL: "SELECT 11, 22, 11;",
			numberedArgs: []interface{}{11, 22}, questionArgs: []interface{}{11, 22, 11}},
		{name: "longer first", query: "SELECT :levels, :level, :levels",
			args:        map[string]interface{}{":level": 11, ":levels": 22},
			numberedSQL: "SELECT $1, $2, $1;", questionSQL: "SELECT ?, ?, ?;", debugSQL: "SELECT 22, 11, 22;",
			numberedArgs: []interface{}{22, 11}, questionArgs: []interface{}{22, 11, 22}},
		{name: "custom numeric names", query: "SELECT #1, #10, #100, #1",
			args:        map[string]interface{}{"#1": 1, "#10": 10, "#100": 100},
			numberedSQL: "SELECT $1, $2, $3, $1;", questionSQL: "SELECT ?, ?, ?, ?;", debugSQL: "SELECT 1, 10, 100, 1;",
			numberedArgs: []interface{}{1, 10, 100}, questionArgs: []interface{}{1, 10, 100, 1}},
		{name: "debug values contain names", query: "SELECT :level, :levels",
			args:        map[string]interface{}{":level": ":levels", ":levels": ":level"},
			numberedSQL: "SELECT $1, $2;", questionSQL: "SELECT ?, ?;", debugSQL: "SELECT ':levels', ':level';",
			numberedArgs: []interface{}{":levels", ":level"}, questionArgs: []interface{}{":levels", ":level"}},
	}
	for _, dialect := range dialects {
		t.Run(dialect.name, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					stmt := dialect.statement(tc.query, tc.args)
					sql, args := stmt.Sql()
					expectedSQL, expectedArgs := tc.questionSQL, tc.questionArgs
					if dialect.numbered {
						expectedSQL, expectedArgs = tc.numberedSQL, tc.numberedArgs
					}
					require.Equal(t, expectedSQL, strings.TrimSpace(sql))
					require.Equal(t, expectedArgs, args)
					require.Equal(t, tc.debugSQL, strings.TrimSpace(stmt.DebugSql()))
				})
			}
		})
	}
}

func TestRawExpressionOverlappingArgumentNames(t *testing.T) {
	cases := []struct {
		name string
		stmt jet.SerializerStatement
		sql  string
		args []interface{}
	}{
		{"postgres", postgres.SELECT(postgres.Int(700).ADD(postgres.RawInt(":level + :levels + :level",
			postgres.RawArgs{":level": 11, ":levels": 22}))),
			"SELECT $1 + ($2 + $3 + $2);", []interface{}{int64(700), 11, 22}},
		{"mysql", mysql.SELECT(mysql.Int(700).ADD(mysql.RawInt(":level + :levels + :level",
			mysql.RawArgs{":level": 11, ":levels": 22}))),
			"SELECT ? + (? + ? + ?);", []interface{}{int64(700), 11, 22, 11}},
		{"sqlite", sqlite.SELECT(sqlite.Int(700).ADD(sqlite.RawInt(":level + :levels + :level",
			sqlite.RawArgs{":level": 11, ":levels": 22}))),
			"SELECT ? + (? + ? + ?);", []interface{}{int64(700), 11, 22, 11}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sql, args := tc.stmt.Sql()
			require.Equal(t, tc.sql, strings.Join(strings.Fields(sql), " "))
			require.Equal(t, tc.args, args)
			require.Equal(t, "SELECT 700 + (11 + 22 + 11);", strings.Join(strings.Fields(tc.stmt.DebugSql()), " "))
		})
	}
}

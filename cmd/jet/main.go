package main

//go:generate sh -c "printf 'package main\n\nconst version = \"'%s'\"\n' $(git describe --tags --abbrev=0) > version.go"

import (
	"flag"
	"fmt"
	"os"
	"slices"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"

	"github.com/go-jet/jet/v2/generator/metadata"
	mysqlgen "github.com/go-jet/jet/v2/generator/mysql"
	postgresgen "github.com/go-jet/jet/v2/generator/postgres"
	sqlitegen "github.com/go-jet/jet/v2/generator/sqlite"
	"github.com/go-jet/jet/v2/generator/template"
	"github.com/go-jet/jet/v2/internal/3rdparty/snaker"
	"github.com/go-jet/jet/v2/internal/jet"
	"github.com/go-jet/jet/v2/internal/utils/errfmt"
	"github.com/go-jet/jet/v2/internal/utils/strslice"
	"github.com/go-jet/jet/v2/mysql"
	postgres2 "github.com/go-jet/jet/v2/postgres"
	"github.com/go-jet/jet/v2/sqlite"
)

var (
	source string

	dsn        string
	host       string
	port       int
	user       string
	password   string
	sslmode    string
	params     string
	dbName     string
	schemaName string

	ignoreTables string
	ignoreViews  string
	ignoreEnums  string

	skipModel      bool
	skipSQLBuilder bool

	destDir  string
	modelPkg string
	tablePkg string
	viewPkg  string
	enumPkg  string

	tables string
	views  string
	enums  string

	modelJsonTag string
)

type templateFilter struct {
	names  []string
	ignore bool
}

func init() {
	flag.StringVar(&source, "source", "", "Database system name (postgres, mysql, cockroachdb, mariadb or sqlite)")

	flag.StringVar(&dsn, "dsn", "", `Data source name. Unified format for connecting to database.
    	PostgreSQL: https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING
		Example:
			postgresql://user:pass@localhost:5432/dbname
    	MySQL: https://dev.mysql.com/doc/refman/8.0/en/connecting-using-uri-or-key-value-pairs.html
		Example:
			mysql://jet:jet@tcp(localhost:3306)/dvds
    	SQLite: https://www.sqlite.org/c3ref/open.html#urifilenameexamples
		Example:
			file://path/to/database/file`)
	flag.StringVar(&host, "host", "", "Database host path. Used only if dsn is not set. (Example: localhost)")
	flag.IntVar(&port, "port", 0, "Database port. Used only if dsn is not set.")
	flag.StringVar(&user, "user", "", "Database user. Used only if dsn is not set.")
	flag.StringVar(&password, "password", "", "The user’s password. Used only if dsn is not set.")
	flag.StringVar(&dbName, "dbname", "", "Database name. Used only if dsn is not set.")
	flag.StringVar(&schemaName, "schema", "public", `Database schema name. (default "public")(PostgreSQL only)`)
	flag.StringVar(&params, "params", "", "Additional connection string parameters(optional). Used only if dsn is not set.")
	flag.StringVar(&sslmode, "sslmode", "disable", `Whether or not to use SSL. Used only if dsn is not set. (optional)(default "disable")(PostgreSQL only)`)
	flag.StringVar(&ignoreTables, "ignore-tables", "", `Comma-separated list of tables to ignore.`)
	flag.StringVar(&ignoreViews, "ignore-views", "", `Comma-separated list of views to ignore.`)
	flag.StringVar(&ignoreEnums, "ignore-enums", "", `Comma-separated list of enums to ignore.`)
	flag.BoolVar(&skipModel, "skip-model", false, `Skip model generation.`)
	flag.BoolVar(&skipSQLBuilder, "skip-sql-builder", false, `Skip SQL builder generation.`)

	flag.StringVar(&destDir, "path", "", "Destination directory for files generated.")
	flag.StringVar(&modelPkg, "rel-model-path", "model", "Relative path for the Model files package from the destination directory.")
	flag.StringVar(&tablePkg, "rel-table-path", "table", "Relative path for the Table files package from the destination directory.")
	flag.StringVar(&viewPkg, "rel-view-path", "view", "Relative path for the View files package from the destination directory.")
	flag.StringVar(&enumPkg, "rel-enum-path", "enum", "Relative path for the Enum files package from the destination directory.")
	flag.StringVar(&modelJsonTag, "model-json-tag", "", "Json tag model to be included in Go structs. (optional)(default <empty>)(allowed values: <empty>, pascal-case, camel-case, snake-case")

	flag.StringVar(&tables, "tables", "", `Comma-separated list of tables to generate.`)
	flag.StringVar(&views, "views", "", `Comma-separated list of views to generate.`)
	flag.StringVar(&enums, "enums", "", `Comma-separated list of enums to generate.`)
}

func main() {
	flag.Usage = usage
	flag.Parse()

	if dsn == "" && (source == "" || host == "" || port == 0 || user == "" || dbName == "") {
		printErrorAndExit("ERROR: required flag(s) missing")
	}

	if !slices.Contains([]string{"", "snake-case", "pascal-case", "camel-case"}, modelJsonTag) {
		printErrorAndExit("ERROR: json tag does not contain correct value")
	}

	source := getSource()
	tablesFilter := createTemplateFilter(ignoreTables, tables, "tables")
	viewsFilter := createTemplateFilter(ignoreViews, views, "views")
	enumsFilter := createTemplateFilter(ignoreEnums, enums, "enums")

	var err error

	switch source {
	case "postgresql", "postgres", "cockroachdb", "cockroach":
		generatorTemplate := genTemplate(postgres2.Dialect, tablesFilter, viewsFilter, enumsFilter)

		if dsn != "" {
			err = postgresgen.GenerateDSN(dsn, schemaName, destDir, generatorTemplate)
			break
		}

		dbConn := postgresgen.DBConnection{
			Host:     host,
			Port:     port,
			User:     user,
			Password: password,
			SslMode:  sslmode,
			Params:   params,

			DBName:     dbName,
			SchemaName: schemaName,
		}

		err = postgresgen.Generate(
			destDir,
			dbConn,
			generatorTemplate,
		)

	case "mysql", "mysqlx", "mariadb":
		generatorTemplate := genTemplate(mysql.Dialect, tablesFilter, viewsFilter, enumsFilter)

		if dsn != "" {
			err = mysqlgen.GenerateDSN(dsn, destDir, generatorTemplate)
			break
		}

		dbConn := mysqlgen.DBConnection{
			Host:     host,
			Port:     port,
			User:     user,
			Password: password,
			Params:   params,
			DBName:   dbName,
		}

		err = mysqlgen.Generate(
			destDir,
			dbConn,
			generatorTemplate,
		)
	case "sqlite":
		if dsn == "" {
			printErrorAndExit("ERROR: required -dsn flag missing.")
		}

		err = sqlitegen.GenerateDSN(
			dsn,
			destDir,
			genTemplate(sqlite.Dialect, tablesFilter, viewsFilter, enumsFilter),
		)

	case "":
		printErrorAndExit("ERROR: required -source or -dsn flag missing.")

	default:
		printErrorAndExit("ERROR: unknown data source " + source + ". Only postgres, mysql, mariadb and sqlite are supported.")
	}

	if err != nil {
		fmt.Println(errfmt.Trace(err))
		os.Exit(2)
	}
}

func usage() {
	fmt.Println("Jet generator", version)
	fmt.Println()
	fmt.Println("Usage:")

	order := []string{
		"source", "dsn", "host", "port", "user", "password", "dbname", "schema", "params", "sslmode",
		"path",
		"ignore-tables", "ignore-views", "ignore-enums",
		"skip-model", "skip-sql-builder",
		"rel-model-path", "rel-table-path", "rel-view-path", "rel-enum-path", "tables", "views",
		"enums",
	}

	for _, name := range order {
		flagEntry := flag.CommandLine.Lookup(name)
		fmt.Printf("  -%s\n", flagEntry.Name)
		fmt.Printf("\t%s\n", flagEntry.Usage)
	}

	fmt.Println()
	fmt.Println(`Example commands:

	$ jet -dsn=postgresql://jet:jet@localhost:5432/jetdb?sslmode=disable -schema=dvds -path=./gen
	$ jet -dsn=postgres://jet:jet@localhost:26257/jetdb?sslmode=disable -schema=dvds -path=./gen   #cockroachdb
	$ jet -source=postgres -dsn="user=jet password=jet host=localhost port=5432 dbname=jetdb" -schema=dvds -path=./gen
	$ jet -source=mysql -host=localhost -port=3306 -user=jet -password=jet -dbname=jetdb -path=./gen
	$ jet -source=sqlite -dsn="file://path/to/sqlite/database/file" -path=./gen
	$ jet -source=sqlite -dsn="file://path/to/sqlite/database/file" -path=./gen -rel-model-path=./entity
	`)
}

func printErrorAndExit(error string) {
	fmt.Println("\n", error)
	fmt.Println()
	flag.Usage()
	os.Exit(1)
}

func getSource() string {
	if source != "" {
		return strings.TrimSpace(strings.ToLower(source))
	}

	return detectSchema(dsn)
}

func detectSchema(dsn string) string {
	match := strings.SplitN(dsn, "://", 2)
	if len(match) < 2 { // not found
		return ""
	}

	protocol := match[0]

	if protocol == "file" {
		return "sqlite"
	}

	return strings.ToLower(match[0])
}

func parseList(list string) []string {
	ret := strings.Split(list, ",")

	for i := 0; i < len(ret); i++ {
		ret[i] = strings.ToLower(strings.TrimSpace(ret[i]))
	}

	return ret
}

func genTemplate(dialect jet.Dialect, tablesFilter, viewsFilter, enumsFilter templateFilter) template.Template {
	return template.Default(dialect).
		UseSchema(func(schemaMetaData metadata.Schema) template.Schema {
			return template.DefaultSchema(schemaMetaData).
				UseModel(template.DefaultModel().ShouldSkip(skipModel).UsePath(modelPkg).
					UseTable(func(table metadata.Table) template.TableModel {
						if shouldSkipTable(table, tablesFilter) {
							return template.TableModel{Skip: true}
						}
						return template.DefaultTableModel(table).
							UseField(func(columnMetaData metadata.Column) template.TableModelField {
								defaultTableModelField := template.DefaultTableModelField(columnMetaData)
								tags := createModelTags(columnMetaData)
								return defaultTableModelField.UseTags(tags...)
							})
					}).
					UseView(func(view metadata.Table) template.ViewModel {
						if shouldSkipTable(view, viewsFilter) {
							return template.ViewModel{Skip: true}
						}
						return template.DefaultViewModel(view).
							UseField(func(columnMetaData metadata.Column) template.TableModelField {
								defaultTableModelField := template.DefaultTableModelField(columnMetaData)
								tags := createModelTags(columnMetaData)
								return defaultTableModelField.UseTags(tags...)
							})
					}).
					UseEnum(func(enum metadata.Enum) template.EnumModel {
						if shouldSkipEnum(enum, enumsFilter) {
							return template.EnumModel{Skip: true}
						}
						return template.DefaultEnumModel(enum)
					}),
				).
				UseSQLBuilder(template.DefaultSQLBuilder().ShouldSkip(skipSQLBuilder).
					UseTable(func(table metadata.Table) template.TableSQLBuilder {
						if shouldSkipTable(table, tablesFilter) {
							return template.TableSQLBuilder{Skip: true}
						}

						return template.DefaultTableSQLBuilder(table).UsePath(tablePkg)
					}).
					UseView(func(table metadata.Table) template.ViewSQLBuilder {
						if shouldSkipTable(table, viewsFilter) {
							return template.ViewSQLBuilder{Skip: true}
						}

						return template.DefaultViewSQLBuilder(table).UsePath(viewPkg)
					}).
					UseEnum(func(enum metadata.Enum) template.EnumSQLBuilder {
						if shouldSkipEnum(enum, enumsFilter) {
							return template.EnumSQLBuilder{Skip: true}
						}

						return template.DefaultEnumSQLBuilder(enum).UsePath(enumPkg)
					}),
				)
		})
}

func createTemplateFilter(ignoreList, allowList, filterType string) templateFilter {
	if ignoreList != "" && allowList != "" {
		printErrorAndExit(fmt.Sprintf("ERROR: cannot use both -%s and -ignore-%s flags simultaneously. Please specify only one option.", filterType, filterType))
	}

	if allowList != "" {
		return templateFilter{
			names:  parseList(allowList),
			ignore: false,
		}
	}

	return templateFilter{
		names:  parseList(ignoreList),
		ignore: true,
	}
}

func shouldSkipTable(table metadata.Table, filter templateFilter) bool {
	if filter.ignore {
		return strslice.Contains(filter.names, strings.ToLower(table.Name))
	}

	return !strslice.Contains(filter.names, strings.ToLower(table.Name))
}

func shouldSkipEnum(enum metadata.Enum, filter templateFilter) bool {
	if filter.ignore {
		return strslice.Contains(filter.names, strings.ToLower(enum.Name))
	}

	return !strslice.Contains(filter.names, strings.ToLower(enum.Name))
}

func createModelTags(columnMetaData metadata.Column) []string {
	var tags []string
	switch modelJsonTag {
	case "snake-case":
		tags = append(tags, fmt.Sprintf(`json:"%s"`, snaker.CamelToSnake(columnMetaData.Name)))
	case "camel-case":
		tags = append(tags, fmt.Sprintf(`json:"%s"`, snaker.SnakeToCamel(columnMetaData.Name, false)))
	case "pascal-case":
		tags = append(tags, fmt.Sprintf(`json:"%s"`, snaker.SnakeToCamel(columnMetaData.Name, true)))
	}
	return tags
}

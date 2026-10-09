package postgres

import (
	"testing"

	"github.com/go-jet/jet/v2/internal/testutils"
)

func TestJsonExpression_Operators(t *testing.T) {
	data := JsonColumn("data")
	key := StringColumn("key")
	doc := NewTable("public", "doc", "", data, key)

	stmt := SELECT(
		data.Arrow(key).AS("v1"),
		data.ArrowText(key).AS("v2"),
		data.HashArrow(StringArrayColumn("path")).AS("v3"),
		data.HashArrowText(StringArrayColumn("path")).AS("v4"),
		data.Concat(JSON([]byte(`{"b":2}`))).AS("v5"),
	).FROM(doc)

	testutils.AssertStatementSql(t, stmt, `
SELECT (doc.data -> doc.key) AS "v1",
     (doc.data ->> doc.key) AS "v2",
     (doc.data #> path) AS "v3",
     (doc.data #>> path) AS "v4",
     (doc.data || $1::json) AS "v5"
FROM public.doc;
`, []byte(`{"b":2}`))
}

func TestJsonbColumn_Operators(t *testing.T) {
	data := JsonColumn("data")
	dataB := JsonbColumn("data_b")
	key := StringColumn("key")
	doc := NewTable("public", "doc", "", data, dataB, key)

	stmt := SELECT(
		dataB.Arrow(key).AS("v1"),
		dataB.ArrowText(key).AS("v2"),
		dataB.HashArrowText(StringArrayColumn("path")).AS("v3"),
		dataB.Contains(JSONB([]byte(`{"a":1}`))).AS("v4"),
		dataB.IsContainedBy(JSONB([]byte(`{"a":1}`))).AS("v5"),
		dataB.Exists(key).AS("v6"),
		dataB.ExistsAny(key).AS("v7"),
		dataB.ExistsAll(key).AS("v8"),
		dataB.Remove(key).AS("v9"),
		dataB.RemovePath(StringArrayColumn("path")).AS("v10"),
		dataB.Concat(JSONB([]byte(`{"b":2}`))).AS("v11"),
	).FROM(doc)

	testutils.AssertStatementSql(t, stmt, `
SELECT (doc.data_b -> doc.key) AS "v1",
     (doc.data_b ->> doc.key) AS "v2",
     (doc.data_b #>> path) AS "v3",
     (doc.data_b @> $1::jsonb) AS "v4",
     (doc.data_b <@ $2::jsonb) AS "v5",
     (doc.data_b ? doc.key) AS "v6",
     (doc.data_b ?| doc.key) AS "v7",
     (doc.data_b ?& doc.key) AS "v8",
     (doc.data_b - doc.key) AS "v9",
     (doc.data_b #- path) AS "v10",
     (doc.data_b || $3::jsonb) AS "v11"
FROM public.doc;
`, []byte(`{"a":1}`), []byte(`{"a":1}`), []byte(`{"b":2}`))
}

func TestJsonToJsonbRequiresExplicitCast(t *testing.T) {
	// jsonb-only operators are not available on a json column; converting to
	// jsonb requires an explicit cast, which is the intended separation.
	data := JsonColumn("data")
	key := StringColumn("key")
	doc := NewTable("public", "doc", "", data, key)

	stmt := SELECT(
		CAST(data).AS_JSONB().Exists(key).AS("v1"),
		CAST(data).AS_JSONB().Contains(JSONB([]byte(`{"a":1}`))).AS("v2"),
	).FROM(doc)

	testutils.AssertStatementSql(t, stmt, `
SELECT (doc.data::jsonb ? doc.key) AS "v1",
     (doc.data::jsonb @> $1::jsonb) AS "v2"
FROM public.doc;
`, []byte(`{"a":1}`))
}

func TestJsonLiterals(t *testing.T) {
	stmt := SELECT(
		JSON([]byte(`{"a":1}`)).AS("j"),
		JSONB([]byte(`{"b":2}`)).AS("jb"),
	)

	testutils.AssertStatementSql(t, stmt, `
SELECT $1::json AS "j",
     $2::jsonb AS "jb";
`, []byte(`{"a":1}`), []byte(`{"b":2}`))
}

func TestJsonFunctions(t *testing.T) {
	salary := IntegerColumn("salary")
	name := StringColumn("name")
	doc := NewTable("public", "doc", "", salary, name)

	stmt := SELECT(
		JSON_BUILD_OBJECT(Text("salary"), salary, Text("name"), name).AS("obj"),
		JSONB_BUILD_OBJECT(Text("salary"), salary).AS("objb"),
		JSON_BUILD_ARRAY(salary, name).AS("arr"),
		JSONB_BUILD_ARRAY(salary).AS("arrb"),
		TO_JSON(salary).AS("tj"),
		TO_JSONB(salary).AS("tjb"),
		JSON_ARRAY_LENGTH(JSON_BUILD_ARRAY(salary)).AS("len"),
		JSONB_PRETTY(TO_JSONB(salary)).AS("pretty"),
	).FROM(doc)

	testutils.AssertStatementSql(t, stmt, `
SELECT json_build_object($1::text, doc.salary, $2::text, doc.name) AS "obj",
     jsonb_build_object($3::text, doc.salary) AS "objb",
     json_build_array(doc.salary, doc.name) AS "arr",
     jsonb_build_array(doc.salary) AS "arrb",
     to_json(doc.salary) AS "tj",
     to_jsonb(doc.salary) AS "tjb",
     json_array_length(json_build_array(doc.salary)) AS "len",
     jsonb_pretty(to_jsonb(doc.salary)) AS "pretty"
FROM public.doc;
`, "salary", "name", "salary")
}

func TestJsonBuildObjectDynamicKeys(t *testing.T) {
	// a text column can be a key: json_build_object(p.id, p.salary)
	id := StringColumn("id")
	salary := IntegerColumn("salary")
	emp := NewTable("public", "emp", "p", id, salary)

	stmt := SELECT(
		JSON_BUILD_OBJECT(id, salary).AS("obj"),
	).FROM(emp)

	testutils.AssertStatementSql(t, stmt, `
SELECT json_build_object(p.id, p.salary) AS "obj"
FROM public.emp AS p;
`)
}

func TestJsonbFunctionResultSupportsJsonbOperators(t *testing.T) {
	// a jsonb_build_object(...) result is a jsonb expression, so jsonb-only ops work
	stmt := SELECT(
		JSONB_BUILD_OBJECT(Text("a"), Int(1)).Contains(JSONB([]byte(`{"a":1}`))).AS("c"),
	)

	testutils.AssertStatementSql(t, stmt, `
SELECT (jsonb_build_object($1::text, $2) @> $3::jsonb) AS "c";
`, "a", int64(1), []byte(`{"a":1}`))
}

func TestJsonAgg(t *testing.T) {
	salary := IntegerColumn("salary")
	name := StringColumn("name")
	emp := NewTable("public", "emp", "e", salary, name)

	stmt := SELECT(
		JSON_AGG(
			JSON_BUILD_OBJECT(Text("salary"), salary, Text("name"), name),
		).ORDER_BY(salary.ASC()).AS("employees"),
	).FROM(emp)

	testutils.AssertStatementSql(t, stmt, `
SELECT json_agg((json_build_object($1::text, e.salary, $2::text, e.name)) ORDER BY e.salary ASC) AS "employees"
FROM public.emp AS e;
`, "salary", "name")
}

func TestJsonbAgg(t *testing.T) {
	salary := IntegerColumn("salary")
	name := StringColumn("name")
	emp := NewTable("public", "emp", "e", salary, name)

	stmt := SELECT(
		JSONB_AGG(
			JSONB_BUILD_OBJECT(Text("salary"), salary, Text("name"), name),
		).ORDER_BY(name.DESC()).AS("employees"),
	).FROM(emp)

	testutils.AssertStatementSql(t, stmt, `
SELECT jsonb_agg((jsonb_build_object($1::text, e.salary, $2::text, e.name)) ORDER BY e.name DESC) AS "employees"
FROM public.emp AS e;
`, "salary", "name")
}

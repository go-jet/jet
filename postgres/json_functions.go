package postgres

// JSON_BUILD_OBJECT returns a json_build_object expression (json type). It takes
// alternating key/value arguments, e.g.:
//
//	JSON_BUILD_OBJECT(Text("salary"), salary, Text("name"), name)
//
// which serializes to json_build_object($1::text, salary, $2::text, name).
// Keys and values are arbitrary expressions, so dynamic keys are supported:
//
//	JSON_BUILD_OBJECT(p.employeeKey, p.salary)
func JSON_BUILD_OBJECT(args ...Expression) JsonExpression {
	return JsonExp(Func("json_build_object", args...))
}

// JSONB_BUILD_OBJECT returns a jsonb_build_object expression (jsonb type).
// See JSON_BUILD_OBJECT for the argument form.
func JSONB_BUILD_OBJECT(args ...Expression) JsonbExpression {
	return JsonbExp(Func("jsonb_build_object", args...))
}

// JSON_BUILD_ARRAY returns a json_build_array expression (json type).
func JSON_BUILD_ARRAY(args ...Expression) JsonExpression {
	return JsonExp(Func("json_build_array", args...))
}

// JSONB_BUILD_ARRAY returns a jsonb_build_array expression (jsonb type).
func JSONB_BUILD_ARRAY(args ...Expression) JsonbExpression {
	return JsonbExp(Func("jsonb_build_array", args...))
}

// TO_JSON returns a to_json expression (json type) that converts any value to json.
func TO_JSON(value Expression) JsonExpression {
	return JsonExp(Func("to_json", value))
}

// TO_JSONB returns a to_jsonb expression (jsonb type) that converts any value to jsonb.
func TO_JSONB(value Expression) JsonbExpression {
	return JsonbExp(Func("to_jsonb", value))
}

// JSON_ARRAY_LENGTH returns a json_array_length expression (integer result).
func JSON_ARRAY_LENGTH(value JsonExpression) IntegerExpression {
	return IntExp(Func("json_array_length", value))
}

// JSONB_ARRAY_LENGTH returns a jsonb_array_length expression (integer result).
func JSONB_ARRAY_LENGTH(value JsonbExpression) IntegerExpression {
	return IntExp(Func("jsonb_array_length", value))
}

// JSON_EXTRACT_PATH returns a json_extract_path expression (json type) that
// extracts the JSON sub-object at the specified path.
func JSON_EXTRACT_PATH(value JsonExpression, pathElems ...Expression) JsonExpression {
	return JsonExp(Func("json_extract_path", append([]Expression{value}, pathElems...)...))
}

// JSONB_EXTRACT_PATH returns a jsonb_extract_path expression (jsonb type) that
// extracts the JSON sub-object at the specified path.
func JSONB_EXTRACT_PATH(value JsonbExpression, pathElems ...Expression) JsonbExpression {
	return JsonbExp(Func("jsonb_extract_path", append([]Expression{value}, pathElems...)...))
}

// JSON_OBJECT returns a json_object expression (json type). It takes alternating
// key/value text arguments.
func JSON_OBJECT(args ...Expression) JsonExpression {
	return JsonExp(Func("json_object", args...))
}

// JSONB_OBJECT returns a jsonb_object expression (jsonb type).
func JSONB_OBJECT(args ...Expression) JsonbExpression {
	return JsonbExp(Func("jsonb_object", args...))
}

// JSONB_PRETTY returns a jsonb_pretty expression (text result) that adds
// indentation to the given jsonb value.
func JSONB_PRETTY(value JsonbExpression) StringExpression {
	return StringExp(Func("jsonb_pretty", value))
}

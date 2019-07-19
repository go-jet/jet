package snaker

// Package snaker provides methods to convert CamelCase names to snake_case and back.
// It considers the list of allowed initialsms used by github.com/golang/lint/golint (e.g. ID or HTTP)

import (
	"strings"
	"unicode"
)

// SnakeToCamel returns a string converted from snake case to uppercase
func SnakeToCamel(s string) string {
	return snakeToCamel(s, true)
}

func snakeToCamel(s string, upperCase bool) string {
	if len(s) == 0 {
		return s
	}
	var result string

	words := strings.Split(s, "_")

	for i, word := range words {
		if exception := snakeToCamelExceptions[word]; len(exception) > 0 {
			result += exception
			continue
		}

		if upperCase || i > 0 {
			if upper := strings.ToUpper(word); commonInitialisms[upper] {
				result += upper
				continue
			}
		}

		if upperCase || i > 0 {
			result += camelizeWord(word, len(words) > 1)
		} else {
			result += word
		}
	}

	return result
}

func camelizeWord(word string, force bool) string {
	runes := []rune(word)

	for i, r := range runes {
		if i == 0 {
			runes[i] = unicode.ToUpper(r)
		} else {
			if !force && unicode.IsLower(r) { // already camelCase
				return string(runes)
			}

			runes[i] = unicode.ToLower(r)
		}
	}

	return string(runes)
}

// commonInitialisms, taken from
// https://github.com/golang/lint/blob/206c0f020eba0f7fbcfbc467a5eb808037df2ed6/lint.go#L731
var commonInitialisms = map[string]bool{
	"ACL":   true,
	"API":   true,
	"ASCII": true,
	"CPU":   true,
	"CSS":   true,
	"DNS":   true,
	"EOF":   true,
	"ETA":   true,
	"GPU":   true,
	"GUID":  true,
	"HTML":  true,
	"HTTP":  true,
	"HTTPS": true,
	"ID":    true,
	"IP":    true,
	"JSON":  true,
	"LHS":   true,
	"OS":    true,
	"QPS":   true,
	"RAM":   true,
	"RHS":   true,
	"RPC":   true,
	"SLA":   true,
	"SMTP":  true,
	"SQL":   true,
	"SSH":   true,
	"TCP":   true,
	"TLS":   true,
	"TTL":   true,
	"UDP":   true,
	"UI":    true,
	"UID":   true,
	"UUID":  true,
	"URI":   true,
	"URL":   true,
	"UTF8":  true,
	"VM":    true,
	"XML":   true,
	"XMPP":  true,
	"XSRF":  true,
	"XSS":   true,
	"OAuth": true,
}

// add exceptions here for things that are not automatically convertable
var snakeToCamelExceptions = map[string]string{
	"oauth": "OAuth",
}

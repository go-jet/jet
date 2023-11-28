package dbidentifier

import (
	"github.com/go-jet/jet/v2/internal/3rdparty/snaker"
	"regexp"
	"strings"
)

// ToGoIdentifier converts database identifier to Go identifier.
func ToGoIdentifier(databaseIdentifier string) string {
	return snaker.SnakeToCamel(replaceInvalidChars(databaseIdentifier))
}

// ToGoFileName converts database identifier to Go file name.
func ToGoFileName(databaseIdentifier string) string {
	return strings.ToLower(replaceInvalidChars(databaseIdentifier))
}

var regx = regexp.MustCompile(`[^_a-zA-Z0-9]{1,30}`)

func replaceInvalidChars(str string) string {
	return regx.ReplaceAllString(str, "_")
}

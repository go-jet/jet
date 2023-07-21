package dbidentifier

import (
	"github.com/go-jet/jet/v2/internal/3rdparty/snaker"
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

func replaceInvalidChars(str string) string {
	str = strings.Replace(str, " ", "_", -1)
	str = strings.Replace(str, "-", "_", -1)
	str = strings.Replace(str, ".", "_", -1)

	return str
}

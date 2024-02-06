package dbidentifier

import (
	"strings"

	"github.com/go-jet/jet/v2/internal/3rdparty/snaker"
)

// ToGoIdentifier converts database identifier to Go identifier.
func ToGoIdentifier(databaseIdentifier string) string {
	return snaker.SnakeToCamel(replaceInvalidChars(replaceSuffixes(databaseIdentifier)))
}

// ToGoFileName converts database identifier to Go file name.
func ToGoFileName(databaseIdentifier string) string {
	return strings.ToLower(replaceInvalidChars(replaceSuffixes(databaseIdentifier)))
}

func replaceInvalidChars(str string) string {
	str = strings.Replace(str, " ", "_", -1)
	str = strings.Replace(str, ".", "_", -1)
	str = strings.Replace(str, "+", "_", -1)
	str = strings.Replace(str, "-", "_", -1)

	return str
}

func replaceSuffixes(str string) string {
	if strings.HasSuffix(str, "-") {
		str = strings.TrimSuffix(str, "-")
		str = str + "_Minus"
	}

	if strings.HasSuffix(str, "+") {
		str = strings.TrimSuffix(str, "+")
		str = str + "_Plus"
	}
	return str
}

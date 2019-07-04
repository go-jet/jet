package util

import (
	"github.com/go-jet/jet/internal/3rdparty/snaker"
	"strings"
)

func ToGoIdentifier(databaseIdentifier string) string {
	if len(databaseIdentifier) == 0 {
		return databaseIdentifier
	}

	return snaker.SnakeToCamel(replaceInvalidChars(databaseIdentifier))
}

func ToGoFileName(databaseIdentifier string) string {
	return strings.ToLower(replaceInvalidChars(databaseIdentifier))
}

func replaceInvalidChars(str string) string {
	str = strings.Replace(str, " ", "_", -1)
	str = strings.Replace(str, "-", "_", -1)

	return str
}

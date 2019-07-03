package util

import (
	"github.com/go-jet/jet/internal/3rdparty/snaker"
	"strings"
)

func ToGoIdentifier(databaseIdentifier string) string {
	if len(databaseIdentifier) == 0 {
		return databaseIdentifier
	}
	databaseIdentifier = strings.ReplaceAll(databaseIdentifier, " ", "_")
	databaseIdentifier = strings.ReplaceAll(databaseIdentifier, "-", "_")

	return snaker.SnakeToCamel(databaseIdentifier)
}

func ToGoFileName(databaseIdentifier string) string {
	databaseIdentifier = strings.ReplaceAll(databaseIdentifier, " ", "_")
	databaseIdentifier = strings.ReplaceAll(databaseIdentifier, "-", "_")

	return strings.ToLower(databaseIdentifier)
}

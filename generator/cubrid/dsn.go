package cubrid

import (
	"errors"
	"strings"
)

// extractDBName parses the database name from a CUBRID DSN string.
func extractDBName(dsn string) (string, error) {
	if idx := strings.Index(dsn, "://"); idx != -1 {
		dsn = dsn[idx+3:]
	}
	if idx := strings.Index(dsn, "@"); idx != -1 {
		dsn = dsn[idx+1:]
	}
	if idx := strings.Index(dsn, "/"); idx != -1 {
		dsn = dsn[idx+1:]
	} else {
		return "", errors.New("database name is required in DSN")
	}
	if idx := strings.Index(dsn, "?"); idx != -1 {
		dsn = dsn[:idx]
	}
	dbName := strings.TrimSpace(dsn)
	if dbName == "" {
		return "", errors.New("database name is required in DSN")
	}
	return dbName, nil
}

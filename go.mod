module github.com/go-jet/jet/v2

go 1.25.0

// used by jet generator
require (
	github.com/go-sql-driver/mysql v1.10.0
	github.com/google/uuid v1.6.0
	github.com/lib/pq v1.12.3
	github.com/mattn/go-sqlite3 v1.14.44
)

// used in tests
require (
	github.com/google/go-cmp v0.7.0
	github.com/stretchr/testify v1.11.1
)

require github.com/jackc/pgx/v5 v5.9.2

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	golang.org/x/text v0.29.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

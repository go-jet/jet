module github.com/go-jet/jet/v2

go 1.24.0

// used by jet generator
require (
	github.com/go-sql-driver/mysql v1.10.0
	github.com/google/uuid v1.6.0
	github.com/lib/pq v1.12.3
	github.com/mattn/go-sqlite3 v1.14.45
)

// used in tests
require (
	github.com/google/go-cmp v0.7.0
	github.com/stretchr/testify v1.11.1
)

require (
	filippo.io/edwards25519 v1.2.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/kr/pretty v0.3.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

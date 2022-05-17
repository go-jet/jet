module github.com/go-jet/jet/v2

go 1.11

require (
	github.com/go-sql-driver/mysql v1.5.0
	github.com/google/uuid v1.1.1
	github.com/jackc/pgconn v1.12.1
	github.com/lib/pq v1.10.5
	github.com/mattn/go-sqlite3 v1.14.8
)

// test dependencies
require (
	github.com/google/go-cmp v0.5.8
	github.com/jackc/pgx/v4 v4.16.1
	github.com/pkg/profile v1.6.0
	github.com/shopspring/decimal v1.3.1
	github.com/stretchr/testify v1.7.0
	github.com/volatiletech/null/v8 v8.1.2
)

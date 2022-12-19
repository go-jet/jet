module github.com/go-jet/jet/v2

go 1.11

require (
	github.com/go-sql-driver/mysql v1.5.0
	github.com/google/uuid v1.3.0
	github.com/jackc/pgconn v1.13.0
	github.com/lib/pq v1.10.5
	github.com/mattn/go-sqlite3 v1.14.16
)

// test dependencies
require (
	github.com/google/go-cmp v0.5.8
	github.com/jackc/pgx/v4 v4.17.2
	github.com/pkg/profile v1.6.0
	github.com/shopspring/decimal v1.3.1
	github.com/stretchr/testify v1.8.0
	github.com/volatiletech/null/v8 v8.1.2
	gopkg.in/guregu/null.v4 v4.0.0
)

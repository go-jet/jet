module github.com/go-jet/jet/v2

go 1.23.0

toolchain go1.23.8

// used by jet generator
require (
	github.com/go-sql-driver/mysql v1.9.3
	github.com/google/uuid v1.6.0
	github.com/jackc/pgconn v1.14.3
	github.com/jackc/pgtype v1.14.4
	github.com/lib/pq v1.10.9
	github.com/mattn/go-sqlite3 v1.14.28
)

// used in tests
require (
	github.com/bytedance/sonic v1.13.3
	github.com/google/go-cmp v0.7.0
	github.com/jackc/pgx/v5 v5.7.4
	github.com/pkg/profile v1.7.0
	github.com/shopspring/decimal v1.4.0
	github.com/stretchr/testify v1.10.0
	github.com/volatiletech/null/v8 v8.1.2
	gopkg.in/guregu/null.v4 v4.0.0
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/bytedance/sonic/loader v0.2.4 // indirect
	github.com/cloudwego/base64x v0.1.5 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/felixge/fgprof v0.9.3 // indirect
	github.com/friendsofgo/errors v0.9.2 // indirect
	github.com/gofrs/uuid v4.0.0+incompatible // indirect
	github.com/google/pprof v0.0.0-20211214055906-6f57359322fd // indirect
	github.com/jackc/chunkreader/v2 v2.0.1 // indirect
	github.com/jackc/pgio v1.0.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgproto3/v2 v2.3.3 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/klauspost/cpuid/v2 v2.0.9 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rogpeppe/go-internal v1.6.1 // indirect
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	github.com/volatiletech/inflect v0.0.1 // indirect
	github.com/volatiletech/randomize v0.0.1 // indirect
	github.com/volatiletech/strmangle v0.0.1 // indirect
	golang.org/x/arch v0.0.0-20210923205945-b76863e36670 // indirect
	golang.org/x/crypto v0.35.0 // indirect
	golang.org/x/sync v0.11.0 // indirect
	golang.org/x/text v0.22.0 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

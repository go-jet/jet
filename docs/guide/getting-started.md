# Getting Started

Jet is a framework for writing type-safe SQL queries in Go and mapping query results into an arbitrary
object structure. Jet currently supports `PostgreSQL`, `MySQL`, `CockroachDB`,
`MariaDB` and `SQLite`.

It is the easiest and fastest way to write complex SQL queries and map their results into nested Go
structs. **It is not an ORM.**

![jet](/jet.png)

## Motivation

See the introductory article: [Jet - type-safe SQL builder for Go](https://medium.com/@go.jet/jet-5f3667efa0cc).

## Installation

Add jet as a dependency to your `go.mod` project:

```sh
go get -u github.com/go-jet/jet/v2
```

The jet generator can be installed in one of the following ways:

- Using `go install`:

  ```sh
  go install github.com/go-jet/jet/v2/cmd/jet@latest
  ```

  The generator is installed to the directory named by the `GOBIN` environment variable, which defaults to
  `$GOPATH/bin` or `$HOME/go/bin` if `GOPATH` is not set.

- Build into a specific folder:

  ```sh
  git clone https://github.com/go-jet/jet.git
  cd jet && go build -o dir_path ./cmd/jet
  ```

  Make sure `dir_path` is added to the `PATH` environment variable.

## Usage

Jet requires a running database instance with an already defined schema (tables, views, enums, etc.), so that
the `jet` generator can generate SQL Builder and Model files. Generation is fast and can run as a pre-build
step, but committing the generated files to the project repository is recommended.

Sample command:

```sh
jet -dsn=postgresql://jet:jet@localhost:5432/jetdb?sslmode=disable -schema=dvds -path=./.gen
```

Detailed information about the generator can be found in the [Generator](./generator) section.

Next, import the generated SQL Builder and Model packages:

```go
import . "some_path/.gen/jetdb/dvds/table"
import "some_path/.gen/jetdb/dvds/model"
```

Then import the dialect package matching your database:

::: code-group

```go [PostgreSQL / CockroachDB]
import . "github.com/go-jet/jet/v2/postgres"
```

```go [MySQL / MariaDB]
import . "github.com/go-jet/jet/v2/mysql"
```

```go [SQLite]
import . "github.com/go-jet/jet/v2/sqlite"
```

:::

_The dot import makes Go code resemble native SQL as closely as possible. It is not mandatory._

Write SQL:

```go
// sub-query
rRatingFilms :=
    SELECT(
        Film.FilmID,
        Film.Title,
        Film.Rating,
    ).FROM(
        Film,
    ).WHERE(
        Film.Rating.EQ(enum.MpaaRating.R),
    ).AsTable("rFilms")

// export column from sub-query
rFilmID := Film.FilmID.From(rRatingFilms)

// main-query
query := SELECT(
        Actor.AllColumns,
        FilmActor.AllColumns,
        rRatingFilms.AllColumns(),
    ).FROM(
        rRatingFilms.
            INNER_JOIN(FilmActor, FilmActor.FilmID.EQ(rFilmID)).
            INNER_JOIN(Actor, Actor.ActorID.EQ(FilmActor.ActorID)),
    ).ORDER_BY(
        rFilmID,
        Actor.ActorID,
    )
```

Store the result into the desired destination:

```go
var dest []struct {
    model.Film

    Actors []model.Actor
}

err := query.Query(db, &dest)
```

## Where to go next

- [Generator](./generator) - generating SQL Builder and Model files from your schema
- [SQL Builder](./sql-builder) and [Expressions](./expressions) - writing queries
- [Statements](../statements/) - SELECT, INSERT, UPDATE, DELETE, LOCK, WITH, VALUES
- [Query Result Mapping](./qrm) - how results are scanned into destinations
- [FAQ](../faq)

# Salmon

`salmon` is an opinionated golang migration library for SQLite databases.

Rules:
- versions start at 0
- version gaps are not allowed
- each migration should increment the last version by one
- enforced naming convention: "V{version number}__{description}.sql"

## Installation
```bash
go get github.com/thales-maciel/salmon
```

### Basic usage
```go
package main

import (
    "context"
	"database/sql"

    "github.com/thales-maciel/salmon"
    _ "github.com/mattn/go-sqlite3"
)

func main() {
    ctx := context.Background()
	db, err := sql.Open("sqlite3", "db")
    if err != nil { panic(err) }
    err := salmon.Migrate(ctx, db, &salmon.Opts{
        Dir: "migrations",
    })
}
```

### Using embed.FS
```go
package main

import (
    "context"
	"database/sql"

    "github.com/thales-maciel/salmon"
    _ "github.com/mattn/go-sqlite3"
)

//go:embed migrations/*.sql
var embedMigrations embed.FS

func main() {
    ctx := context.Background()
	db, err := sql.Open("sqlite3", "db")
    if err != nil { panic(err) }
    err := salmon.Migrate(ctx, db, &salmon.Opts{
        Dir: "migrations",
        FS: embedMigrations,
    })
}
```

obviously inspired by [goose](https://github.com/pressly/goose)

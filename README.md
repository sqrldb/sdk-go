# SquirrelDB Go SDK

Official Go client for SquirrelDB.

## Installation

```bash
go get github.com/sqrldb/sdk-go
```

## Quick Start

```go
package main

import (
    "fmt"
    "os"

    squirreldb "github.com/sqrldb/sdk-go"
)

func main() {
    db, err := squirreldb.Connect(squirreldb.Config{
        Host:  "localhost",
        Port:  8080,
        Token: os.Getenv("SQUIRRELDB_TOKEN"),
    })
    if err != nil {
        panic(err)
    }
    defer db.Close()

    // Insert a document
    user, err := db.Table("users").Insert(map[string]interface{}{
        "name":  "Alice",
        "email": "alice@example.com",
    })
    if err != nil {
        panic(err)
    }
    fmt.Printf("Created user: %s\n", user["id"])

    // Query documents
    users, err := db.Table("users").
        Filter("u => u.status === 'active'").
        Run()
    if err != nil {
        panic(err)
    }
    fmt.Printf("Found %d active users\n", len(users))

    // Subscribe to changes
    changes, err := db.Table("messages").Changes()
    if err != nil {
        panic(err)
    }

    for change := range changes {
        fmt.Printf("Change: %s - %v\n", change.Operation, change.NewValue)
    }
}
```

## Documentation

Visit [squirreldb.com/docs/sdks](https://squirreldb.com/docs/sdks) for full documentation.

## License

Apache License 2.0 - see [LICENSE](LICENSE) for details.

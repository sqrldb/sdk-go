// Basic example demonstrating SquirrelDB Go SDK usage.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	squirreldb "github.com/squirreldb/squirreldb-sdk-go"
)

func main() {
	ctx := context.Background()

	// Connect to SquirrelDB server via TCP
	client, err := squirreldb.Connect(ctx, &squirreldb.Options{
		Host: "localhost",
		Port: 8082,
	})
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer client.Close()

	fmt.Println("Connected!")

	// Ping the server
	if err := client.Ping(ctx); err != nil {
		log.Fatalf("Ping failed: %v", err)
	}
	fmt.Println("Ping successful!")

	// List collections
	collections, err := client.ListCollections(ctx)
	if err != nil {
		log.Fatalf("Failed to list collections: %v", err)
	}
	fmt.Printf("Collections: %v\n", collections)

	// Insert a document
	doc, err := client.Insert(ctx, "users", map[string]interface{}{
		"name":   "Alice",
		"email":  "alice@example.com",
		"active": true,
	})
	if err != nil {
		log.Fatalf("Failed to insert: %v", err)
	}
	fmt.Printf("Inserted document: %+v\n", doc)

	// Query documents
	docs, err := client.Query(ctx, `db.table("users").filter(u => u.active).run()`)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	var prettyJSON []byte
	prettyJSON, _ = json.MarshalIndent(docs, "", "  ")
	fmt.Printf("Active users: %s\n", prettyJSON)

	// Update the document
	updated, err := client.Update(ctx, "users", doc.Id, map[string]interface{}{
		"name":   "Alice Updated",
		"email":  "alice.updated@example.com",
		"active": true,
	})
	if err != nil {
		log.Fatalf("Failed to update: %v", err)
	}
	fmt.Printf("Updated document: %+v\n", updated)

	// Subscribe to changes
	fmt.Println("\nSubscribing to user changes...")
	fmt.Println("(Insert/update/delete users from another client to see changes)")
	fmt.Println("Press Ctrl+C to exit.")

	subID, err := client.Subscribe(ctx, `db.table("users").changes()`, func(change squirreldb.ChangeEvent) {
		switch change.Type {
		case squirreldb.ChangeTypeInitial:
			fmt.Printf("Initial: %+v\n", change.Document)
		case squirreldb.ChangeTypeInsert:
			fmt.Printf("Insert: %+v\n", change.New)
		case squirreldb.ChangeTypeUpdate:
			fmt.Printf("Update: %+v -> %+v\n", change.Old, change.New)
		case squirreldb.ChangeTypeDelete:
			fmt.Printf("Delete: %+v\n", change.Old)
		}
	})
	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	// Handle shutdown gracefully
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	<-sigCh
	fmt.Println("\nUnsubscribing...")
	client.Unsubscribe(ctx, subID)
	client.Close()
}

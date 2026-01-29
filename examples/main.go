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

	squirreldb "github.com/squirreldb/squirreldb-go"
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

	fmt.Printf("Connected! Session ID: %s\n", client.SessionID())

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
	data, err := client.Query(ctx, `db.table("users").filter(u => u.active).run()`)
	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}
	var prettyJSON []byte
	prettyJSON, _ = json.MarshalIndent(json.RawMessage(data), "", "  ")
	fmt.Printf("Active users: %s\n", prettyJSON)

	// Update the document
	updated, err := client.Update(ctx, "users", doc.ID, map[string]interface{}{
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

	sub, err := client.SubscribeRaw(ctx, `db.table("users").changes()`)
	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	// Handle shutdown gracefully
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		fmt.Println("\nUnsubscribing...")
		sub.Unsubscribe()
		client.Close()
		os.Exit(0)
	}()

	// Process changes
	for change := range sub.Changes() {
		switch change.Type {
		case "initial":
			fmt.Printf("Initial: %+v\n", change.Document)
		case "insert":
			fmt.Printf("Insert: %+v\n", change.New)
		case "update":
			fmt.Printf("Update: %s -> %+v\n", change.Old, change.New)
		case "delete":
			fmt.Printf("Delete: %s\n", change.Old)
		}
	}
}

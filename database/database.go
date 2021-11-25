package database

import (
	"fmt"

	"github.com/hashicorp/go-memdb"
)

func DatabaseInMemory() {

	// Create a sample struct
	type Person struct {
		Email string
		Name  string
		Age   int
	}

	// Create the DB schema
	schema := &memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			"person": {
				Name: "person",
				Indexes: map[string]*memdb.IndexSchema{
					"id": {
						Name:    "id",
						Unique:  true,
						Indexer: &memdb.StringFieldIndex{Field: "Email"},
					},
					"age": {
						Name:    "age",
						Unique:  false,
						Indexer: &memdb.IntFieldIndex{Field: "Age"},
					},
				},
			},
		},
	}

	// Create a new data base
	db, err := memdb.NewMemDB(schema)
	if err != nil {
		panic(err)
	}

	// Create a write transaction
	txn := db.Txn(true)

	// Insert some people
	people := []*Person{
		{"renata@gmail.com", "Renata", 33},
		{"talita@gmail.com", "Talita", 30},
		{"tiago@gmail.com", "Tiago", 25},
		{"paulo@gmail.com", "Paulo", 59},
	}
	for _, p := range people {
		if err := txn.Insert("person", p); err != nil {
			panic(err)
		}
	}

	// Commit the transaction
	txn.Commit()

	// Create read-only transaction
	txn = db.Txn(true)
	defer txn.Abort()

	// List all the people
	listPeople, err := txn.Get("person", "id")
	if err != nil {
		panic(err)
	}

	fmt.Print("\n[Memory database] All the people \n\n")
	for obj := listPeople.Next(); obj != nil; obj = listPeople.Next() {
		p := obj.(*Person)
		fmt.Printf("Name: %s | Idade: %d\n", p.Name, p.Age)
	}

	// Range scan over people with ages between 25 and 35 inclusive
	listPeople, err = txn.LowerBound("person", "age", 25)
	if err != nil {
		panic(err)
	}

	fmt.Print("\n[Memory database] Peoples ages range 25 a 35 years: \n\n")
	for obj := listPeople.Next(); obj != nil; obj = listPeople.Next() {
		p := obj.(*Person)
		if p.Age > 35 {
			break
		}
		fmt.Printf("- %s\n", p.Name)
	}

	// New List - Insert others Peoples
	peopleNew := []*Person{
		{"julio@gmail.com", "Julio", 37},
		{"jaqueline@gmail.com", "Talita", 25},
	}
	for _, p := range peopleNew {
		if err := txn.Insert("person", p); err != nil {
			panic(err)
		}
	}

	// Commit the transaction
	txn.Commit()

	// Create read-only transaction
	txn = db.Txn(true)
	defer txn.Abort()

	// List all the people
	newListPeople, err := txn.Get("person", "id")
	if err != nil {
		panic(err)
	}

	fmt.Print("\n[Memory database] New List => All the people \n\n")
	for obj := newListPeople.Next(); obj != nil; obj = newListPeople.Next() {
		p := obj.(*Person)
		fmt.Printf("Name: %s | Idade: %d\n", p.Name, p.Age)
	}

}

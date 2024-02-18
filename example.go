package main

import (
	"fmt"
	"sync"
)

// Define a struct
type Person struct {
	Name    string
	Age     int
	Address string
}

func main() {
	// Create a sync.Map with string keys and pointers to Person values
	var people sync.Map

	// Add some initial values to the map
	people.Store("john", &Person{"John Doe", 25, "123 Main St"})
	people.Store("jane", &Person{"Jane Smith", 30, "456 Oak St"})

	// Display the initial values
	fmt.Println("Initial Values:")
	displayValues(&people, "john")
	displayValues(&people, "jane")

	// Modify the fields of the struct values in the map using Load and Store
	val, _ := people.Load("john")
	val.(*Person).Age = 26

	// Display the modified values
	fmt.Println("\nModified Values:")
	displayValues(&people, "john")
	displayValues(&people, "jane")
}

// displayValues is a helper function to display the values stored in the sync.Map
func displayValues(people *sync.Map, key string) {
	if value, ok := people.Load(key); ok {
		personPtr := value.(*Person)
		fmt.Printf("%s: %+v\n", key, *personPtr)
	} else {
		fmt.Printf("%s not found\n", key)
	}
}

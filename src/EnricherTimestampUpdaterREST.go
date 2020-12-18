package main

import (
	"fmt"
	"log"
	"net/http"
)

type Person struct {
	Name string
	Age  int
}

func handleJSONAsString(w http.ResponseWriter, r *http.Request) {

	s, err := getBodyAsString(w, r)

	if err != nil {
		fmt.Println("Error occurred: %v", err)
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(s))
}

func handleJSONAsMap(w http.ResponseWriter, r *http.Request) {

	mymap, err := getBodyAsMap(w, r)

	if err != nil {
		fmt.Println("Error occurred: %v", err)
	}

	name := mymap["Name"]
	fmt.Println("Name is ", name)

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("Mapped"))
}

func apiResponse(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"message":"GOODBYE CRUEL WORLD!"}`))
}

// TODO: change the name of main if want to use it as entry point
func mainXXXXXXXXXXX() {
	http.HandleFunc("/", apiResponse)
	http.HandleFunc("/json", handleJSONAsString)
	http.HandleFunc("/map", handleJSONAsMap)
	log.Fatal(http.ListenAndServe(":8080", nil))
}

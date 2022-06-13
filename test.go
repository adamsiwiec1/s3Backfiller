package main

import (
	"fmt"
	"strings"
)

func main() {
	s := "{\n    \"ETag\": \"\\\"9ddf4f70a582c5ffcf81ed71c0feda95\\\"\"\n}\n"
	l := strings.Split(s, "\\\"")
	fmt.Println(l[1])
}

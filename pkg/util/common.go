package util

import (
	"encoding/json"
	"fmt"
)

func BuildJson(v interface{}) string {
	bytes, _ := json.MarshalIndent(v, "", "    ")
	return string(bytes)
}

func PrintJson(v interface{}) {
	fmt.Println(BuildJson(v))
}

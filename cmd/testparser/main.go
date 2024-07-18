package main

import (
	"github.com/louisun/go-rzlib/internal/testparser"
	"os"
)

func main() {
	testparser.ExtractFailedInfoFromReader(os.Stdin)
}

package testparser

import (
	"bufio"
	"fmt"
	"github.com/louisun/go-rzlib/internal/color"
	"go.uber.org/zap"
	"io"
	"os"
	"regexp"
	"strings"
)

var log *zap.Logger

var failRegex = regexp.MustCompile(`--- FAIL: .*\n`)
var passRegex = regexp.MustCompile(`--- PASS: .*\n`)
var goLineRegex = regexp.MustCompile(`(.*_test\.go)(:\d+:)?.*?\n`)

func init() {
	var err error
	log, err = zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
}

func ExtractFailedInfoFromFile(fileName string) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal("read file failed", zap.Error(err))
	}

	defer file.Close()
	ExtractFailedInfoFromReader(file)
}

func ExtractFailedInfoFromReader(r io.Reader) {
	scanner := bufio.NewScanner(r)

	var currentBlock []string
	var resultBlocks []string
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "=== RUN") {
			if len(currentBlock) > 0 {
				block := strings.Join(currentBlock, "\n")
				if strings.Contains(block, "--- FAIL") {
					resultBlocks = append(resultBlocks, block)
				}
			}
			currentBlock = []string{line}
		} else {
			currentBlock = append(currentBlock, line)
		}
	}

	if len(currentBlock) > 0 {
		block := strings.Join(currentBlock, "\n")
		if strings.Contains(block, "--- FAIL") {
			resultBlocks = append(resultBlocks, block)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal("scanner error: ", zap.Error(err))
	}

	for i, block := range resultBlocks {
		if i > 0 {
			fmt.Println()
		}

		color.BlackWhitePrinter("-------------- 🚨 第 %v 个 FAIL --------------", i+1)

		fmt.Println()
		fmt.Println()

		block = failRegex.ReplaceAllStringFunc(block, func(match string) string {
			return color.RedSprinter(match)
		})

		block = passRegex.ReplaceAllStringFunc(block, func(match string) string {
			return color.GreenSprinter(match)
		})
		block = goLineRegex.ReplaceAllStringFunc(block, func(match string) string {
			return color.BlueSprinter(match)
		})

		fmt.Println(block)
	}
}

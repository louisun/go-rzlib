package color

import "github.com/fatih/color"

var (
	BlackWhitePrinter = color.New(color.FgBlack, color.BgHiWhite).PrintfFunc()
	RedPrinter        = color.New(color.FgRed).PrintfFunc()
	RedSprinter       = color.New(color.FgRed).SprintfFunc()
	GreenSprinter     = color.New(color.FgGreen).SprintfFunc()
	BlueSprinter      = color.New(color.FgBlue).SprintfFunc()
)

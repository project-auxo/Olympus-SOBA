package main

import (
	"flag"

	"github.com/Project-Auxo/Olympus/pkg/auditor"
)

func main() {
	loaderFilePathPtr := flag.String("file", "", "Path to the loader.go file.")
	verbosePtr := flag.Bool("v", true, "Print to stdout.")
	flag.Parse()

	loaderFilePath := *loaderFilePathPtr
	verbose := *verbosePtr

	auditor.CheckConsistency(verbose, loaderFilePath)
}
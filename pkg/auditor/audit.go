package auditor

import (
	"fmt"
	"log"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"

	"github.com/google/go-cmp/cmp"

	"github.com/Project-Auxo/Olympus/pkg/service"

)


/*
																							service folder
																										|
																										v
actor.config --(autogenerate)--> loader.go --(run audit.go) --> validation
*/


// CheckConsistency ensures consistency between service/loader.go and the folders found
// within service/
func CheckConsistency(serviceFolderPath string, verbose bool) {
	valid := true
	loaderFilePath := fmt.Sprintf("%s/loader.go", serviceFolderPath)

	fset := token.NewFileSet()
	node, err := parser.ParseFile(fset, loaderFilePath, nil, parser.ParseComments)
	if err != nil {
		log.Fatal(err)
	}

	if diff := checkImports(node); diff != "" {
		if verbose {
			fmt.Println("checkImports failed: (-expected, +got)", diff)
		}
		valid = false
	}
	

	fmt.Println("##############################")
	if valid {
		fmt.Println("OK: All audit checks passed.")
	}else {
		fmt.Println("Audit check(s) failed.")
	}
}

func checkImports(node *ast.File) (diff string) {
	targetWord := "service"
	expected := service.AvailableServices() // The folders that exist in service/
	var loaderImports []string

	for _, i := range node.Imports {
		pathValue := i.Path.Value
		if index := strings.Index(pathValue, targetWord); index != -1 {
			pathValue = pathValue[index+len(targetWord)+1:]
			pathValue = strings.Trim(pathValue, `'"`)
			loaderImports = append(loaderImports, pathValue)
		}
	}
	diff = cmp.Diff(expected, loaderImports)
	return
}
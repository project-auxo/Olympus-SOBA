package auditor

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"log"
	"regexp"
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

// TODO: Errors in the check should return where the error is in the line.
// TODO: Write an audit tester file.
// TODO: Declare global error types.



// CheckConsistency ensures consistency between service/loader.go and the folders found
// within service/
func CheckConsistency(verbose bool, serviceFolderPath ...string) {
	expectedServices := service.AvailableServices("")
	valid := true
	loaderName := "loader.go"
	if len(serviceFolderPath) > 1 {
		loaderName = serviceFolderPath[1]
	}
	loaderFilePath := fmt.Sprintf("%s/%s", serviceFolderPath[0], loaderName)

	fset := token.NewFileSet()
	node, err := parser.ParseFile(fset, loaderFilePath, nil, parser.ParseComments)
	if err != nil {
		log.Fatal(err)
	}

	if diff, _ := checkImports(node, expectedServices); diff != "" {
		if verbose {
			fmt.Println("checkImports failed: (-expected, +got)", diff)
		}
		valid = false
	}
	if diff, _ := checkServiceImplemented(node, fset, expectedServices); diff != "" {
		if verbose {
			fmt.Println(
				"checkServiceImplemented failed: (+not fully implemented)", diff)
		}
		valid = false
	}
	
	fmt.Println("##############################")
	if valid {
		fmt.Println("OK: All audit checks passed.")
	}else {
		fmt.Println("Audit check(s) failed.")
	}
	fmt.Println("##############################")
}

// checkImports checks that the service imports correspond to those found within
// the service folder.
func checkImports(node *ast.File, expectedServices []string) (
	diff string, got []string) {
	targetWord := "service"
	var loaderImports []string

	for _, i := range node.Imports {
		pathValue := i.Path.Value
		if index := strings.Index(pathValue, targetWord); index != -1 {
			pathValue = pathValue[index+len(targetWord)+1:]
			pathValue = strings.Trim(pathValue, `'"`)
			loaderImports = append(loaderImports, pathValue)
		}
	}
	diff = cmp.Diff(expectedServices, loaderImports)
	return diff, loaderImports
}

// checkServiceImplemented checks that the service is called with the correct
// client and actor usage.
func checkServiceImplemented(
	node *ast.File, fset *token.FileSet, expectedServices []string) (
		diff string, got []string) {
	var buf bytes.Buffer
	for _, fnDeclaration := range node.Decls {
		fn, ok := fnDeclaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		body := fn.Body
		for _, line := range body.List {
			format.Node(&buf, fset, line)	
		}
	}
	strBuf := buf.String()

	gotServicesSet := make(map[string]struct{})
	helperCheckServiceImplemented(&gotServicesSet, strBuf)

	// Ensure that the x.ClientRequest and x.ActorResponse function calls appear
	// in the code (implies existence of switch statements on service).
	for _, service := range expectedServices {
		clientRequest := fmt.Sprintf("%s.ClientRequest", service)
		actorResponse := fmt.Sprintf("%s.ActorResponse", service)
		if strings.Contains(
			strBuf, clientRequest) && strings.Contains(strBuf, actorResponse) {
				delete(gotServicesSet, service)
		}
	}

	finalGot := make([]string, 0, len(gotServicesSet))
	for service := range gotServicesSet {
		finalGot = append(finalGot, service)
	}
	diff = cmp.Diff([]string{}, finalGot)
	return diff, finalGot
}

// helperCheckServiceImplemented is a helper function that finds all the
// services that *appear* to be implemented in loader.go
func helperCheckServiceImplemented(
	gotServiceSet *map[string]struct{}, strBuf string) {
	re := regexp.MustCompile(`case ".*"`)
	for _, statement := range re.FindAllString(strBuf, -1) {
		serviceName := strings.Trim(statement[len("case")+1:], `'"`)
		if _, ok := (*gotServiceSet)[serviceName]; ok {
			continue
		}
		(*gotServiceSet)[serviceName] = struct{}{}
	}
}
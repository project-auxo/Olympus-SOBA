package auditor

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"log"
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/Project-Auxo/Olympus/pkg/service"
)


func ParserHelper(loaderFileName string) (fset *token.FileSet, node *ast.File) {
	loaderFilePath := fmt.Sprintf("./testdata/%s", loaderFileName)
	fset = token.NewFileSet()
	node, err := parser.ParseFile(fset, loaderFilePath, nil, parser.ParseComments)
	if err != nil {
		log.Fatal(err)
	}
	return
}

// TestCheckImports tests whether the testLoader.go file makes the correct
// service imports based on the mock service directory, e.g. testdata/Missing
func TestCheckImports(t *testing.T) {
	importTestCases := []struct {
		testName string
		availableServicesPath string
		expectedResult string
	}{
		// Okay test case to see whether checkImport function captures 
		// no discrepancy between the expected service imports and the resultant
		// service imports.
		{
			"Okay",
			"./testdata/Okay",
			"",
		},
		// Missing test case to see whether checkImport function captures the
		// discrepancy between hypothetical directories with members of
		// 'availableServices' and the directories imported in testLoader.go
		{
			"Missing",
			"./testdata/Missing",
			cmp.Diff([]string{"a", "b", "c"}, []string{"a", "b"}),
		},
	}

	for _, tc := range importTestCases {
		t.Run(tc.testName, func(t *testing.T) {
			availableServices := service.AvailableServices(tc.availableServicesPath)
			_, node := ParserHelper(fmt.Sprintf("%s/loader.go", tc.testName))
			got, _ := checkImports(node, availableServices)
			if got != tc.expectedResult {
				t.Errorf(
					"import discrepancy found: expected: %s, got: %s", tc.
					expectedResult, got)
			}
		})
	}
}


func TestCheckServiceImplemented(t *testing.T) {
	serviceImplementedTestCases := []struct {
		testName string
		availableServicesPath string
		expectedResult string
	}{
		// Okay test case ensures that the correct calls are made for each service
		// on the client and actor side.
		{
			"Okay",
			"./testdata/Okay",
			"",
		},
		// Missing test case to see whether checkServiceImplemented can detect when
		// calls on the client or actor side are missing.
		{
			"Missing",
			"./testdata/Missing",
			cmp.Diff([]string{}, []string{"b"}),
		},
	}

	for _, tc := range serviceImplementedTestCases {
		t.Run(tc.testName, func (t *testing.T) {
			availableServices := service.AvailableServices(tc.availableServicesPath)
			fset, node := ParserHelper(fmt.Sprintf("%s/loader.go", tc.testName))
			got, _ := checkServiceImplemented(node, fset, availableServices)
			if got != tc.expectedResult {
				t.Errorf(
					"one or more available services not correctly implemented: " +
					"expected: %s, got: %s", tc.expectedResult, got)
			}
		})
	}
}

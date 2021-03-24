package config

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

var configPath = "./testdata"

// TestActorServices_Populated checks that the correct actor services are being
// loaded in a config file whose actor services are populated.
func TestActorServices(t *testing.T) {
	expected := []string{"a", "b", "c"}
	configName := "config_test_populated"
	if diff := cmp.Diff(
		expected, GetActorServices(configPath, configName)); diff != "" {
			t.Errorf("unexpected actor services diff (-expected, +got): %s", diff)
	}
}
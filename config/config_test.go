package config

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

var configPath = "./testdata"

// TestLoadConfig_ActorServices checks that the correct actor services are being
// loaded in a config file whose actor services are populated.
func TestLoadConfig_ActorServices(t *testing.T) {
	expected := []string{"a", "b", "c"}
	configName := "config_test_populated"
	configuration, _ := LoadConfig(configPath, configName)

	if diff := cmp.Diff(expected, configuration.Actor.Services); diff != "" {
			t.Errorf("unexpected actor services diff (-expected, +got): %s", diff)
	}
}
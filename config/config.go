package config

import (
	"fmt"

	"github.com/spf13/viper"
)

// Configurations exported.
type Configurations struct {
	Multi bool
	Broker BrokerConfig
}

type BrokerConfig struct {
	Hostname string
	Port int
}

func LoadConfig(configPath string) (configuration Configurations, err error) {
	viper.SetConfigName("config")
	viper.AddConfigPath(configPath)
	viper.SetConfigType("yml")
	err = viper.ReadInConfig()
	if err != nil {
		return
	}

	err = viper.Unmarshal(&configuration)
	if err != nil {
		fmt.Printf("Unable to decode into struct, %v", err)
	}
	return
}
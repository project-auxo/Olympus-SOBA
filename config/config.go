package config

import (
	"fmt"

	"github.com/spf13/viper"
)

type Configurations struct {
	Multi bool
	Broker BrokerConfig
	Actor ActorConfig
}

type BrokerConfig struct {
	Hostname string
	Port int
}

type ActorConfig struct {
	Hostname string
	Port int
	Services []string `yaml:"services"`
}

func LoadConfig(configPath ...string) (configuration Configurations, err error) {
	configName := "config"
	if len(configPath) > 1 {
		configName = configPath[1]
	}

	viper.SetConfigName(configName)
	viper.SetConfigType("yml")
	viper.AddConfigPath(configPath[0])
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
package config

import (
	"strings"
	"sync"

	"github.com/spf13/viper"
)

var once sync.Once
var config *Config

func InitConfig() {
	viper.AddConfigPath(".")
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	if err := viper.ReadInConfig(); err != nil {
		panic(err)
	}

	if err := viper.Unmarshal(&config); err != nil {
		panic(err)
	}
}

func GetConfig() Config {
	if config == nil {
		once.Do(func() {
			InitConfig()
		})
	}

	return *config
}

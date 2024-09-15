package config

type Config struct {
	Elastic ElasticsearchConfig `mapstructure:"elasticsearch"`
}

type ElasticsearchConfig struct {
	Host     string `mapstructure:"host"`
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
}

package config

type Config struct {
	Elastic  ElasticsearchConfig `mapstructure:"elasticsearch"`
	SnmpList []SnmpConfig        `mapstructure:"snmp_list"`
	Redis    RedisConfig         `mapstructure:"redis"`
}

type ElasticsearchConfig struct {
	Host     string `mapstructure:"host"`
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
}

type SnmpConfig struct {
	AgentHost  string `mapstructure:"agent_host"`
	TargetHost string `mapstructure:"target_host"`
	TargetPort int    `mapstructure:"target_port"`
}

type RedisConfig struct {
	Host     string `mapstructure:"host"`
	Port     string `mapstructure:"port"`
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
}

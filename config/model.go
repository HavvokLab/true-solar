package config

import "time"

// Performance alarm constants
const (
	LowPerformanceAlarm = "PerformanceLow"
	SumPerformanceAlarm = "SumPerformanceLow"
)

const (
	PerformanceAlarmSnmpBatchSize  = 25
	PerformanceAlarmSnmpBatchDelay = 5 * time.Second
)

// Performance alarm type constants (enum values)
const (
	PerformanceAlarmTypePerformanceLow = iota + 1
	PerformanceAlarmTypeSumPerformanceLow
)

// Fallback config values for performance alarms (based on database defaults)
const (
	// LowPerformanceAlarm fallback values
	LowPerformanceAlarmInterval   = 24
	LowPerformanceAlarmHitDay     = 5
	LowPerformanceAlarmPercentage = 60.0
	LowPerformanceAlarmDuration   = 7

	// SumPerformanceAlarm fallback values
	SumPerformanceAlarmInterval   = 24
	SumPerformanceAlarmHitDay     = 5 // Default since DB has NULL
	SumPerformanceAlarmPercentage = 50.0
	SumPerformanceAlarmDuration   = 30
)

type Config struct {
	Elastic  ElasticsearchConfig `mapstructure:"elasticsearch"`
	SnmpList []SnmpConfig        `mapstructure:"snmp_list"`
	Redis    RedisConfig         `mapstructure:"redis"`
	Crontab  CrontabConfig       `mapstructure:"crontab"`
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

type CrontabConfig struct {
	CollectTime             string `mapstructure:"collect_time"`
	AlarmTime               string `mapstructure:"alarm_time"`
	LowPerformanceAlarmTime string `mapstructure:"low_performance_alarm_time"`
	SumPerformanceAlarmTime string `mapstructure:"sum_performance_alarm_time"`
}

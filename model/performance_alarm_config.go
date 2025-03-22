package model

import "time"

type PerformanceAlarmConfig struct {
	ID         int64      `gorm:"column:id" json:"id"`
	Name       string     `gorm:"column:name" json:"name"`
	Interval   int        `gorm:"column:interval" json:"interval"`
	HitDay     *int       `gorm:"column:hit_day" json:"hit_day"`
	Percentage float64    `gorm:"column:percentage" json:"percentage"`
	Duration   *int       `gorm:"column:duration" json:"duration"`
	CreatedAt  *time.Time `gorm:"created_at" json:"created_at"`
	UpdatedAt  *time.Time `gorm:"updated_at" json:"updated_at"`
}

func (*PerformanceAlarmConfig) TableName() string {
	return "tbl_performance_alarm_config"
}

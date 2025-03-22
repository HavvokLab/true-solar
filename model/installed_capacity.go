package model

import "time"

type InstalledCapacity struct {
	ID               int64      `gorm:"column:id" json:"id"`
	EfficiencyFactor float64    `gorm:"column:efficiency_factor" json:"efficiency_factor"`
	FocusHour        int        `gorm:"column:focus_hour" json:"focus_hour"`
	CreatedAt        *time.Time `gorm:"column:created_at" json:"created_at"`
	UpdatedAt        *time.Time `gorm:"column:updated_at" json:"updated_at"`
}

func (*InstalledCapacity) TableName() string {
	return "tbl_installed_capacity"
}

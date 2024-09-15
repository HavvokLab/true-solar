package model

import (
	"time"
)

type SiteRegionMapping struct {
	ID        int64      `gorm:"column:id" json:"id"`
	Code      string     `gorm:"column:code" json:"code"`
	Name      string     `gorm:"column:name" json:"name"`
	Area      *string    `gorm:"column:area" json:"area"`
	CreatedAt *time.Time `gorm:"created_at" json:"created_at"`
	UpdatedAt *time.Time `gorm:"updated_at" json:"updated_at"`
}

func (*SiteRegionMapping) TableName() string {
	return "tbl_site_region_mapping"
}

type Regions struct {
	Regions []AreaWithCity `json:"regions"`
}

type AreaWithCity struct {
	Area   string              `json:"area"`
	Cities []SiteRegionMapping `json:"cities"`
}

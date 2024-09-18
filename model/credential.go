package model

import (
	"time"
)

type HuaweiCredential struct {
	ID        int64      `gorm:"column:id" json:"id"`
	Username  string     `gorm:"column:username" json:"username"`
	Password  string     `gorm:"column:password" json:"password"`
	Owner     string     `gorm:"column:owner" json:"owner"`
	Version   int        `gorm:"column:version" json:"version"`
	CreatedAt *time.Time `gorm:"column:created_at" json:"created_at"`
	UpdatedAt *time.Time `gorm:"column:updated_at" json:"updated_at"`
}

func (*HuaweiCredential) TableName() string {
	return "tbl_huawei_credentials"
}

type KstarCredential struct {
	ID        int64      `gorm:"column:id" json:"id"`
	Username  string     `gorm:"column:username" json:"username"`
	Password  string     `gorm:"column:password" json:"password"`
	Owner     string     `gorm:"column:owner" json:"owner"`
	CreatedAt *time.Time `gorm:"column:created_at" json:"created_at"`
	UpdatedAt *time.Time `gorm:"column:updated_at" json:"updated_at"`
}

func (*KstarCredential) TableName() string {
	return "tbl_kstar_credentials"
}

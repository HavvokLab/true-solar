package repo

import (
	"github.com/HavvokLab/true-solar/model"
	"gorm.io/gorm"
)

type InstalledCapacityRepo interface {
	FindOne() (*model.InstalledCapacity, error)
}

type installedCapacityRepo struct {
	db *gorm.DB
}

func NewInstalledCapacityRepo(db *gorm.DB) InstalledCapacityRepo {
	return &installedCapacityRepo{db: db}
}

func (r *installedCapacityRepo) FindOne() (*model.InstalledCapacity, error) {
	tx := r.db.Session(&gorm.Session{})
	var installedCapacity model.InstalledCapacity
	err := tx.First(&installedCapacity).Error
	if err != nil {
		return nil, err
	}

	return &installedCapacity, nil
}

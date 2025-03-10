package repo

import (
	"github.com/HavvokLab/true-solar/model"
	"gorm.io/gorm"
)

type SolarmanCredentialRepo interface {
	FindAll() ([]model.SolarmanCredential, error)
	Create(credential *model.SolarmanCredential) error
	Update(id int64, credential *model.SolarmanCredential) error
	Delete(id int64) error
}

type solarmanCredentialRepo struct {
	db *gorm.DB
}

func NewSolarmanCredentialRepo(db *gorm.DB) SolarmanCredentialRepo {
	return &solarmanCredentialRepo{db: db}
}

func (r *solarmanCredentialRepo) FindAll() ([]model.SolarmanCredential, error) {
	var credentials []model.SolarmanCredential
	tx := r.db.Session(&gorm.Session{})
	if err := tx.Find(&credentials).Error; err != nil {
		return nil, err
	}

	return credentials, nil
}

func (r *solarmanCredentialRepo) Create(credential *model.SolarmanCredential) error {
	tx := r.db.Session(&gorm.Session{})
	if err := tx.Create(credential).Error; err != nil {
		return err
	}

	return nil
}

func (r *solarmanCredentialRepo) Update(id int64, credential *model.SolarmanCredential) error {
	tx := r.db.Session(&gorm.Session{})
	if err := tx.Where("id = ?", id).Updates(credential).Error; err != nil {
		return err
	}

	return nil
}

func (r *solarmanCredentialRepo) Delete(id int64) error {
	tx := r.db.Session(&gorm.Session{})
	if err := tx.Where("id = ?", id).Delete(&model.SolarmanCredential{}).Error; err != nil {
		return err
	}

	return nil
}

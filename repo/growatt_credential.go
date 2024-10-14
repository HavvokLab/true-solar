package repo

import (
	"github.com/HavvokLab/true-solar/model"
	"gorm.io/gorm"
)

type GrowattCredentialRepo interface {
	FindAll() ([]model.GrowattCredential, error)
	Create(credential *model.GrowattCredential) error
	Update(id int64, credential *model.GrowattCredential) error
	Delete(id int64) error
}

type growattCredentialRepo struct {
	db *gorm.DB
}

func NewGrowattCredentialRepo(db *gorm.DB) GrowattCredentialRepo {
	return &growattCredentialRepo{db: db}
}

func (r *growattCredentialRepo) FindAll() ([]model.GrowattCredential, error) {
	var credentials []model.GrowattCredential
	tx := r.db.Session(&gorm.Session{})
	if err := tx.Find(&credentials).Error; err != nil {
		return nil, err
	}

	return credentials, nil
}

func (r *growattCredentialRepo) Create(credential *model.GrowattCredential) error {
	tx := r.db.Session(&gorm.Session{})
	if err := tx.Create(credential).Error; err != nil {
		return err
	}

	return nil
}

func (r *growattCredentialRepo) Update(id int64, credential *model.GrowattCredential) error {
	tx := r.db.Session(&gorm.Session{})
	if err := tx.Where("id = ?", id).Updates(credential).Error; err != nil {
		return err
	}

	return nil
}

func (r *growattCredentialRepo) Delete(id int64) error {
	tx := r.db.Session(&gorm.Session{})
	if err := tx.Where("id = ?", id).Delete(&model.GrowattCredential{}).Error; err != nil {
		return err
	}

	return nil
}

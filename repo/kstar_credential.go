package repo

import (
	"github.com/HavvokLab/true-solar/model"
	"gorm.io/gorm"
)

type KStarCredentialRepo interface {
	FindAll() ([]model.KstarCredential, error)
	Create(credential *model.KstarCredential) error
	Update(id int64, credential *model.KstarCredential) error
	Delete(id int64) error
}

type kStarCredentialRepo struct {
	db *gorm.DB
}

func NewKStarCredentialRepo(db *gorm.DB) KStarCredentialRepo {
	return &kStarCredentialRepo{db: db}
}

func (r *kStarCredentialRepo) FindAll() ([]model.KstarCredential, error) {
	var credentials []model.KstarCredential
	tx := r.db.Session(&gorm.Session{})
	if err := tx.Find(&credentials).Error; err != nil {
		return nil, err
	}

	return credentials, nil
}

func (r *kStarCredentialRepo) Create(credential *model.KstarCredential) error {
	tx := r.db.Session(&gorm.Session{})
	if err := tx.Create(credential).Error; err != nil {
		return err
	}

	return nil
}

func (r *kStarCredentialRepo) Update(id int64, credential *model.KstarCredential) error {
	tx := r.db.Session(&gorm.Session{})
	if err := tx.Where("id = ?", id).Updates(credential).Error; err != nil {
		return err
	}

	return nil
}

func (r *kStarCredentialRepo) Delete(id int64) error {
	tx := r.db.Session(&gorm.Session{})
	if err := tx.Where("id = ?", id).Delete(&model.KstarCredential{}).Error; err != nil {
		return err
	}

	return nil
}

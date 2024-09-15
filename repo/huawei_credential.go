package repo

import (
	"github.com/HavvokLab/true-solar/model"
	"gorm.io/gorm"
)

type HuaweiCredentialRepo interface {
	FindAll() ([]model.HuaweiCredential, error)
	Create(credential *model.HuaweiCredential) error
	Update(id int64, credential *model.HuaweiCredential) error
	Delete(id int64) error
}

type huaweiCredentialRepo struct {
	db *gorm.DB
}

func NewHuaweiCredentialRepo(db *gorm.DB) HuaweiCredentialRepo {
	return &huaweiCredentialRepo{db: db}
}

func (r *huaweiCredentialRepo) FindAll() ([]model.HuaweiCredential, error) {
	var credentials []model.HuaweiCredential
	tx := r.db.Session(&gorm.Session{})
	if err := tx.Find(&credentials).Error; err != nil {
		return nil, err
	}

	return credentials, nil
}

func (r *huaweiCredentialRepo) Create(credential *model.HuaweiCredential) error {
	tx := r.db.Session(&gorm.Session{})
	if err := tx.Create(credential).Error; err != nil {
		return err
	}

	return nil
}

func (r *huaweiCredentialRepo) Update(id int64, credential *model.HuaweiCredential) error {
	tx := r.db.Session(&gorm.Session{})
	if err := tx.Where("id = ?", id).Updates(credential).Error; err != nil {
		return err
	}

	return nil
}

func (r *huaweiCredentialRepo) Delete(id int64) error {
	tx := r.db.Session(&gorm.Session{})
	if err := tx.Where("id = ?", id).Delete(&model.HuaweiCredential{}).Error; err != nil {
		return err
	}

	return nil
}

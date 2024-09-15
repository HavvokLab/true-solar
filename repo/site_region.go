package repo

import (
	"github.com/HavvokLab/true-solar/model"
	"gorm.io/gorm"
)

type SiteRegionMappingRepo interface {
	Count() (int64, error)
	GetSiteRegionMappings() ([]model.SiteRegionMapping, error)
	GetSiteRegionMappingsWithPagination(limit, offset int) ([]model.SiteRegionMapping, error)
	GetAreaNotNull() ([]model.SiteRegionMapping, error)
	CreateCity(data *model.SiteRegionMapping) error
	UpdateCity(id int64, data *model.SiteRegionMapping) error
	DeleteCity(id int64) error
	UpdateCityToNullArea(area string) error
	UpdateSiteRegionMapping(codeListString, area string) error
}

type siteRegionMappingRepo struct {
	db *gorm.DB
}

func NewSiteRegionMappingRepo(db *gorm.DB) *siteRegionMappingRepo {
	return &siteRegionMappingRepo{db}
}

func (r *siteRegionMappingRepo) Count() (int64, error) {
	tx := r.db.Session(&gorm.Session{})
	var count int64
	err := tx.Model(&model.SiteRegionMapping{}).Count(&count).Error
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (r *siteRegionMappingRepo) GetSiteRegionMappings() ([]model.SiteRegionMapping, error) {
	tx := r.db.Session(&gorm.Session{})
	var siteRegionMappings []model.SiteRegionMapping
	err := tx.Find(&siteRegionMappings, "code NOT LIKE 'EMPTY-%'").Error
	if err != nil {
		return nil, err
	}

	return siteRegionMappings, nil
}

func (r *siteRegionMappingRepo) GetSiteRegionMappingsWithPagination(limit, offset int) ([]model.SiteRegionMapping, error) {
	var siteRegionMappings []model.SiteRegionMapping
	tx := r.db.Session(&gorm.Session{})
	err := tx.Offset(offset).Limit(limit).Find(&siteRegionMappings, "code NOT LIKE 'EMPTY-%'").Error
	if err != nil {
		return nil, err
	}

	return siteRegionMappings, nil
}

func (r *siteRegionMappingRepo) GetAreaNotNull() ([]model.SiteRegionMapping, error) {
	tx := r.db.Session(&gorm.Session{})
	var siteRegionMappings []model.SiteRegionMapping
	err := tx.Find(&siteRegionMappings, "code LIKE 'EMPTY-%' AND area NOT NULL").Error
	if err != nil {
		return nil, err
	}

	return siteRegionMappings, nil
}

func (r *siteRegionMappingRepo) CreateCity(data *model.SiteRegionMapping) error {
	tx := r.db.Session(&gorm.Session{})
	err := tx.Create(data).Error
	if err != nil {
		return err
	}

	return nil
}

func (r *siteRegionMappingRepo) UpdateCity(id int64, data *model.SiteRegionMapping) error {
	tx := r.db.Session(&gorm.Session{})
	err := tx.Model(&model.SiteRegionMapping{}).Where("id = ?", id).Updates(data).Error
	if err != nil {
		return err
	}

	return nil
}

func (r *siteRegionMappingRepo) DeleteCity(id int64) error {
	tx := r.db.Session(&gorm.Session{})
	err := tx.Delete(&model.SiteRegionMapping{}, id).Error
	if err != nil {
		return err
	}

	return nil
}

func (r *siteRegionMappingRepo) UpdateCityToNullArea(area string) error {
	tx := r.db.Session(&gorm.Session{})
	err := tx.Model(&model.SiteRegionMapping{}).Where("area = ?", area).Update("area", nil).Error
	if err != nil {
		return err
	}

	return nil
}

func (r *siteRegionMappingRepo) UpdateSiteRegionMapping(codeListString, area string) error {
	tx := r.db.Session(&gorm.Session{})
	err := tx.Model(&model.SiteRegionMapping{}).Where("code IN ?", codeListString).Update("area", area).Error
	if err != nil {
		return err
	}

	return nil
}

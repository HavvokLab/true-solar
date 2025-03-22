package repo

import (
	"github.com/HavvokLab/true-solar/model"
	"github.com/HavvokLab/true-solar/setting"
	"gorm.io/gorm"
)

type PerformanceAlarmConfigRepo interface {
	GetLowPerformanceAlarmConfig() (*model.PerformanceAlarmConfig, error)
	GetSumPerformanceAlarmConfig() (*model.PerformanceAlarmConfig, error)
}

type performanceAlarmConfigRepo struct {
	db *gorm.DB
}

func NewPerformanceAlarmConfigRepo(db *gorm.DB) PerformanceAlarmConfigRepo {
	return &performanceAlarmConfigRepo{
		db: db,
	}
}

func (r *performanceAlarmConfigRepo) GetLowPerformanceAlarmConfig() (*model.PerformanceAlarmConfig, error) {
	tx := r.db.Session(&gorm.Session{})
	data := model.PerformanceAlarmConfig{}
	if err := tx.Find(&data, "name = ?", setting.LowPerformanceAlarm).Error; err != nil {
		return nil, err
	}

	return &data, nil
}

func (r *performanceAlarmConfigRepo) GetSumPerformanceAlarmConfig() (*model.PerformanceAlarmConfig, error) {
	tx := r.db.Session(&gorm.Session{})
	data := model.PerformanceAlarmConfig{}
	if err := tx.Find(&data, "name = ?", setting.SumPerformanceAlarm).Error; err != nil {
		return nil, err
	}

	return &data, nil
}

package repo

import (
	"errors"

	"github.com/HavvokLab/true-solar/config"
	"github.com/HavvokLab/true-solar/model"
	"go.openly.dev/pointy"
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
	if err := tx.First(&data, "name = ?", config.LowPerformanceAlarm).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// Return fallback config when not found
			return &model.PerformanceAlarmConfig{
				Name:       config.LowPerformanceAlarm,
				Interval:   config.LowPerformanceAlarmInterval,
				HitDay:     pointy.Int(config.LowPerformanceAlarmHitDay),
				Percentage: config.LowPerformanceAlarmPercentage,
				Duration:   pointy.Int(config.LowPerformanceAlarmDuration),
			}, nil
		}
		return nil, err
	}

	return &data, nil
}

func (r *performanceAlarmConfigRepo) GetSumPerformanceAlarmConfig() (*model.PerformanceAlarmConfig, error) {
	tx := r.db.Session(&gorm.Session{})
	data := model.PerformanceAlarmConfig{}
	if err := tx.First(&data, "name = ?", config.SumPerformanceAlarm).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			// Return fallback config when not found
			return &model.PerformanceAlarmConfig{
				Name:       config.SumPerformanceAlarm,
				Interval:   config.SumPerformanceAlarmInterval,
				HitDay:     pointy.Int(config.SumPerformanceAlarmHitDay),
				Percentage: config.SumPerformanceAlarmPercentage,
				Duration:   pointy.Int(config.SumPerformanceAlarmDuration),
			}, nil
		}
		return nil, err
	}

	return &data, nil
}

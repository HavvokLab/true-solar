package repo

import (
	"github.com/HavvokLab/true-solar/model"
	"github.com/olivere/elastic/v7"
)

type solarMock struct {
}

func NewSolarMockRepo() *solarMock {
	return &solarMock{}
}

func (r *solarMock) BulkIndex(index string, docs []interface{}) error {
	return nil
}

func (r *solarMock) UpsertSiteStation(docs []model.SiteItem) error {
	return nil
}

func (r *solarMock) GetPerformanceLow(duration int, efficiencyFactor float64, focusHour int, thresholdPct float64) ([]*elastic.AggregationBucketCompositeItem, error) {
	return nil, nil
}

func (r *solarMock) GetSumPerformanceLow(duration int) ([]*elastic.AggregationBucketCompositeItem, error) {
	return nil, nil
}

func (r *solarMock) GetUniquePlantByIndex(index string) ([]*elastic.AggregationBucketKeyItem, error) {
	return nil, nil
}

func (r *solarMock) GetPerformanceAlarm(index string) ([]*model.SnmpPerformanceAlarmItem, error) {
	return nil, nil
}

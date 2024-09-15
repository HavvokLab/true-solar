package repo

import "github.com/HavvokLab/true-solar/model"

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

package repo

import (
	"context"
	"errors"
	"fmt"

	"github.com/HavvokLab/true-solar/model"
	"github.com/olivere/elastic/v7"
)

type SolarRepo interface {
	BulkIndex(index string, docs []interface{}) error
	UpsertSiteStation(docs []model.SiteItem) error
}

type solarRepo struct {
	elastic *elastic.Client
}

func NewSolarRepo(elastic *elastic.Client) *solarRepo {
	return &solarRepo{
		elastic: elastic,
	}
}

func (r *solarRepo) SearchIndex() *elastic.SearchService {
	index := fmt.Sprintf("%v*", model.SolarIndex)
	return r.elastic.Search(index)
}

func (r *solarRepo) CreateIndexIfNotExist(index string) error {
	ctx := context.Background()
	if exist, err := r.elastic.IndexExists(index).Do(ctx); err != nil {
		if !exist {
			result, err := r.elastic.CreateIndex(index).Do(ctx)
			if err != nil {
				return err
			}

			if !result.Acknowledged {
				return errors.New("elasticsearch did not acknowledge")
			}
		}
	}

	return nil
}

// |=> Implementation
func (r *solarRepo) BulkIndex(index string, docs []interface{}) error {
	if err := r.CreateIndexIfNotExist(index); err != nil {
		return err
	}

	bulk := r.elastic.Bulk()
	for _, doc := range docs {
		bulk.Add(elastic.NewBulkIndexRequest().Index(index).Doc(doc))
	}

	ctx := context.Background()
	if _, err := bulk.Do(ctx); err != nil {
		return err
	}

	return nil
}

func (r *solarRepo) UpsertSiteStation(docs []model.SiteItem) error {
	index := model.SiteStationIndex
	err := r.CreateIndexIfNotExist(index)
	if err != nil {
		return err
	}

	bulk := r.elastic.Bulk()
	for _, doc := range docs {
		bulk.Add(elastic.NewBulkUpdateRequest().Index(index).Id(doc.SiteID).Doc(doc).DocAsUpsert(true))
	}

	_, err = bulk.Do(context.Background())
	if err != nil {
		return err
	}

	return nil
}

package repo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/HavvokLab/true-solar/model"
	"github.com/olivere/elastic/v7"
)

// Elasticsearch timeout constants
const (
	// DefaultESTimeout is used for standard queries and bulk operations
	DefaultESTimeout = 30 * time.Second
	// ScrollESTimeout is used for scroll operations that may take longer
	ScrollESTimeout = 5 * time.Minute
	// ScrollKeepAlive is the server-side scroll context keepalive duration
	ScrollKeepAlive = "2m"
	// MaxRetryAttempts for connection errors like port exhaustion
	MaxRetryAttempts = 3
	// BaseRetryDelay is the base delay for exponential backoff
	BaseRetryDelay = 2 * time.Second
)

// isRetryableError checks if the error is a connection error that can be retried
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "cannot assign requested address") ||
		strings.Contains(errStr, "connection reset") ||
		strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "i/o timeout")
}

type SolarRepo interface {
	BulkIndex(index string, docs []interface{}) error
	UpsertSiteStation(docs []model.SiteItem) error
	GetPerformanceLow(duration int, efficiencyFactor float64, focusHour int, thresholdPct float64) ([]*elastic.AggregationBucketCompositeItem, error)
	GetSumPerformanceLow(duration int) ([]*elastic.AggregationBucketCompositeItem, error)
	GetUniquePlantByIndex(index string) ([]*elastic.AggregationBucketKeyItem, error)
	GetPerformanceAlarm(index string) ([]*model.SnmpPerformanceAlarmItem, error)
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
	ctx, cancel := context.WithTimeout(context.Background(), DefaultESTimeout)
	defer cancel()

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

	var lastErr error
	for attempt := 0; attempt <= MaxRetryAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), DefaultESTimeout)
		_, lastErr = bulk.Do(ctx)
		cancel()

		if lastErr == nil {
			return nil
		}

		// Only retry on connection errors like port exhaustion
		if !isRetryableError(lastErr) {
			return lastErr
		}

		// Exponential backoff: 2s, 4s, 8s
		if attempt < MaxRetryAttempts {
			time.Sleep(BaseRetryDelay * time.Duration(1<<attempt))
		}
	}

	return lastErr
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

	ctx, cancel := context.WithTimeout(context.Background(), DefaultESTimeout)
	defer cancel()

	_, err = bulk.Do(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (r *solarRepo) GetPerformanceLow(duration int, efficiencyFactor float64, focusHour int, thresholdPct float64) ([]*elastic.AggregationBucketCompositeItem, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ScrollESTimeout)
	defer cancel()

	compositeAggregation := elastic.NewCompositeAggregation().
		Size(10000).
		Sources(elastic.NewCompositeAggregationDateHistogramValuesSource("date").Field("@timestamp").CalendarInterval("day").Format("yyyy-MM-dd"),
			elastic.NewCompositeAggregationTermsValuesSource("vendor_type").Field("vendor_type.keyword"),
			elastic.NewCompositeAggregationTermsValuesSource("id").Field("id.keyword")).
		SubAggregation("max_daily", elastic.NewMaxAggregation().Field("daily_production")).
		SubAggregation("avg_capacity", elastic.NewAvgAggregation().Field("installed_capacity")).
		SubAggregation("threshold_percentage", elastic.NewBucketScriptAggregation().
			BucketsPathsMap(map[string]string{"capacity": "avg_capacity"}).
			Script(elastic.NewScript("params.capacity * params.efficiency_factor * params.focus_hour * params.threshold_percentage").
				Params(map[string]interface{}{
					"efficiency_factor":    efficiencyFactor,
					"focus_hour":           focusHour,
					"threshold_percentage": thresholdPct,
				}))).
		SubAggregation("under_threshold", elastic.NewBucketSelectorAggregation().
			BucketsPathsMap(map[string]string{"threshold": "threshold_percentage", "daily": "max_daily"}).
			Script(elastic.NewScript("params.daily <= params.threshold"))).
		SubAggregation("hits", elastic.NewTopHitsAggregation().
			Size(1).
			FetchSourceContext(
				elastic.NewFetchSourceContext(true).Include(
					"id", "name", "vendor_type", "node_type", "ac_phase", "plant_status",
					"area", "site_id", "site_city_code", "site_city_name", "installed_capacity",
				)))

	searchQuery := r.SearchIndex().
		Size(0).
		Query(elastic.NewBoolQuery().Must(
			elastic.NewMatchQuery("data_type", model.DataTypePlant),
			elastic.NewRangeQuery("@timestamp").Gte(fmt.Sprintf("now-%dd/d", duration)).Lte("now-1d/d"),
		)).
		Aggregation("performance_alarm", compositeAggregation)

	items := make([]*elastic.AggregationBucketCompositeItem, 0)
	result, err := searchQuery.Pretty(true).Do(ctx)
	if err != nil {
		return nil, err
	}

	if result.Aggregations == nil {
		return nil, errors.New("cannot get result aggregations")
	}

	performanceAlarm, found := result.Aggregations.Composite("performance_alarm")
	if !found {
		return nil, errors.New("cannot get result composite performance alarm")
	}

	items = append(items, performanceAlarm.Buckets...)
	if len(performanceAlarm.AfterKey) > 0 {
		afterKey := performanceAlarm.AfterKey

		for {
			query := r.SearchIndex().
				Size(0).
				Query(elastic.NewBoolQuery().Must(
					elastic.NewMatchQuery("data_type", model.DataTypePlant),
					elastic.NewRangeQuery("@timestamp").Gte(fmt.Sprintf("now-%dd/d", duration)).Lte("now-1d/d"),
				)).
				Aggregation("performance_alarm", compositeAggregation.AggregateAfter(afterKey))

			result, err := query.Pretty(true).Do(ctx)
			if err != nil {
				return nil, err
			}

			if result.Aggregations == nil {
				return nil, errors.New("cannot get result aggregations")
			}

			performanceAlarm, found := result.Aggregations.Composite("performance_alarm")
			if !found {
				return nil, errors.New("cannot get result composite performance alarm")
			}

			items = append(items, performanceAlarm.Buckets...)

			if len(performanceAlarm.AfterKey) == 0 {
				break
			}

			afterKey = performanceAlarm.AfterKey
		}
	}

	return items, nil
}

func (r *solarRepo) GetSumPerformanceLow(duration int) ([]*elastic.AggregationBucketCompositeItem, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ScrollESTimeout)
	defer cancel()

	items := make([]*elastic.AggregationBucketCompositeItem, 0)

	compositeAggregation := elastic.NewCompositeAggregation().
		Size(10000).
		Sources(elastic.NewCompositeAggregationDateHistogramValuesSource("date").Field("@timestamp").CalendarInterval("day").Format("yyyy-MM-dd"),
			elastic.NewCompositeAggregationTermsValuesSource("vendor_type").Field("vendor_type.keyword"),
			elastic.NewCompositeAggregationTermsValuesSource("id").Field("id.keyword")).
		SubAggregation("max_daily", elastic.NewMaxAggregation().Field("daily_production")).
		SubAggregation("avg_capacity", elastic.NewAvgAggregation().Field("installed_capacity")).
		SubAggregation("hits", elastic.NewTopHitsAggregation().
			Size(1).
			FetchSourceContext(
				elastic.NewFetchSourceContext(true).Include(
					"id", "name", "vendor_type", "node_type", "ac_phase", "plant_status",
					"area", "site_id", "site_city_code", "site_city_name", "installed_capacity",
				)))

	searchQuery := r.SearchIndex().
		Size(0).
		Query(elastic.NewBoolQuery().Must(
			elastic.NewMatchQuery("data_type", model.DataTypePlant),
			elastic.NewRangeQuery("@timestamp").Gte(fmt.Sprintf("now-%dd/d", duration)).Lte("now-1d/d"),
		)).
		Aggregation("performance_alarm", compositeAggregation)

	firstResult, err := searchQuery.Pretty(true).Do(ctx)
	if err != nil {
		return nil, err
	}

	if firstResult.Aggregations == nil {
		return nil, errors.New("cannot get result aggregations")
	}

	performanceAlarm, found := firstResult.Aggregations.Composite("performance_alarm")
	if !found {
		return nil, errors.New("cannot get result composite performance alarm")
	}

	items = append(items, performanceAlarm.Buckets...)

	if len(performanceAlarm.AfterKey) > 0 {
		afterKey := performanceAlarm.AfterKey

		for {
			searchQuery = r.SearchIndex().
				Size(0).
				Query(elastic.NewBoolQuery().Must(
					elastic.NewMatchQuery("data_type", model.DataTypePlant),
					elastic.NewRangeQuery("@timestamp").Gte(fmt.Sprintf("now-%dd/d", duration)).Lte("now-1d/d"),
				)).
				Aggregation("performance_alarm", compositeAggregation.AggregateAfter(afterKey))

			result, err := searchQuery.Pretty(true).Do(ctx)
			if err != nil {
				return nil, err
			}

			if firstResult.Aggregations == nil {
				return nil, errors.New("cannot get result aggregations")
			}

			performanceAlarm, found := result.Aggregations.Composite("performance_alarm")
			if !found {
				return nil, errors.New("cannot get result composite performance alarm")
			}

			items = append(items, performanceAlarm.Buckets...)

			if len(performanceAlarm.AfterKey) == 0 {
				break
			}

			afterKey = performanceAlarm.AfterKey
		}
	}

	return items, err
}

func (r *solarRepo) GetUniquePlantByIndex(index string) ([]*elastic.AggregationBucketKeyItem, error) {
	ctx, cancel := context.WithTimeout(context.Background(), DefaultESTimeout)
	defer cancel()

	termAggregation := elastic.NewTermsAggregation().
		Field("name.keyword").
		Size(10000)

	termAggregation = termAggregation.
		SubAggregation(
			"data",
			elastic.
				NewTopHitsAggregation().
				Size(1).
				FetchSourceContext(
					elastic.NewFetchSourceContext(true).
						Include("name", "area", "vendor_type", "installed_capacity", "location", "owner"),
				),
		)

	searchQuery := r.elastic.Search(index).
		Size(0).
		Query(elastic.NewBoolQuery().Must(
			elastic.NewMatchQuery("data_type", model.DataTypePlant),
		)).
		Aggregation("plant", termAggregation)

	firstResult, err := searchQuery.Pretty(true).Do(ctx)
	if err != nil {
		return nil, err
	}

	if firstResult.Aggregations == nil {
		return nil, errors.New("cannot get result aggregations")
	}

	plant, found := firstResult.Aggregations.Terms("plant")
	if !found {
		return nil, errors.New("cannot get result term plant")
	}

	return plant.Buckets, nil
}

func (r *solarRepo) GetPerformanceAlarm(index string) ([]*model.SnmpPerformanceAlarmItem, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ScrollESTimeout)
	defer cancel()

	scroll := r.elastic.Scroll(index).Size(1000).Scroll(ScrollKeepAlive)
	var scrollID string

	// Ensure scroll context is cleared to release server resources
	defer func() {
		if scrollID != "" {
			// Use background context for cleanup since main context may be cancelled
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_, _ = r.elastic.ClearScroll(scrollID).Do(cleanupCtx)
		}
	}()

	items := make([]*model.SnmpPerformanceAlarmItem, 0)
	for {
		results, err := scroll.Do(ctx)
		if err != nil {
			break
		}

		// Track scroll ID for cleanup
		if results.ScrollId != "" {
			scrollID = results.ScrollId
		}

		for _, hit := range results.Hits.Hits {
			item := &model.SnmpPerformanceAlarmItem{}
			buf, _ := hit.Source.MarshalJSON()
			if err := json.Unmarshal(buf, &item); err != nil {
				continue
			}
			items = append(items, item)
		}

		if len(results.Hits.Hits) < 1000 {
			break
		}
	}

	return items, nil
}

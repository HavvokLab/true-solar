package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/HavvokLab/true-solar/infra"
	"github.com/HavvokLab/true-solar/pkg/logger"
	"github.com/gocarina/gocsv"
	"github.com/olivere/elastic/v7"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type Document struct {
	ID                 string  `csv:"id"`
	Owner              string  `csv:"owner"`
	Vendor             string  `csv:"vendor"`
	Area               string  `csv:"area"`
	Month              string  `csv:"month"`
	Name               string  `csv:"name"`
	Capacity           float64 `csv:"capacity"`
	Monthly            float64 `csv:"new_monthly"`
	Target2            float64 `csv:"target_2"`
	ProductionToTarget float64 `csv:"production_to_target"`
	After              float64 `csv:"after"`
	Diff               float64 `csv:"diff"`
	Solution           string  `csv:"solution"`
}

func (d Document) Date() (*time.Time, error) {
	layout := "Jan/06"
	date, err := time.Parse(layout, d.Month)
	if err != nil {
		return nil, err
	}
	return &date, nil
}

var (
	docLog zerolog.Logger
)

func init() {
	date := time.Now().Format("2006-01-02_15:04:05")
	logger.Init(fmt.Sprintf("delete_doc_%s.log", date))
	docLog = zerolog.New(logger.NewWriter(fmt.Sprintf("deleted_doc_%s.log", date))).With().Timestamp().Caller().Logger()
}

func loadDocument() ([]Document, error) {
	file, err := os.OpenFile("deletion_document.csv", os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	docs := make([]Document, 0)
	if err := gocsv.UnmarshalFile(file, &docs); err != nil {
		return nil, err
	}

	return docs, nil
}

func searchDocument(elasticClient *elastic.Client, doc *Document) error {
	query := elastic.NewBoolQuery().Must(
		elastic.NewTermQuery("name.keyword", doc.Name),
		elastic.NewRangeQuery("monthly_production").Gte(doc.Monthly),
	)

	date, err := doc.Date()
	if err != nil {
		return err
	}

	index := fmt.Sprintf("solarcell-%v.*", date.Format("2006.01"))
	searchResult, err := elasticClient.Search().Index(index).Query(query).Do(context.Background())
	if err != nil {
		docLog.Error().
			Str("date", date.Format("2006-01")).
			Str("name", doc.Name).
			Err(err).Msg("error search document")
		return err
	}

	docLog.Info().
		Str("date", date.Format("2006-01")).
		Str("name", doc.Name).
		Any("result", searchResult).
		Msg("document")

	return nil
}

func deleteDocument(elasticClient *elastic.Client, doc *Document) error {
	return nil
}

func main() {
	docs, err := loadDocument()
	if err != nil {
		log.Panic().Err(err).Msg("error load document")
	}

	elasticClient, err := infra.NewElasticClient()
	if err != nil {
		log.Panic().Err(err).Msg("error create elastic client")
	}

	deleteDocs := make([]Document, 0)
	for _, doc := range docs {
		if err := searchDocument(elasticClient, &doc); err != nil {
			continue
		}

		deleteDocs = append(deleteDocs, doc)
	}

	fmt.Println(len(deleteDocs))
	// total := len(docs)
	// for i, doc := range docs {
	// 	count := fmt.Sprintf("%d/%d", i, total)
	// 	if err := deleteDocument(elasticClient, &doc); err != nil {
	// 		log.Error().Err(err).Str("count", count).Msg("error delete document")
	// 	}
	// }
}

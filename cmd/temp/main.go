package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/HavvokLab/true-solar/infra"
	"github.com/HavvokLab/true-solar/model"
	"github.com/gocarina/gocsv"
	"github.com/olivere/elastic/v7"
	"github.com/rs/zerolog/log"
)

type Temp struct {
	Id          string `csv:"id"`
	Month       string `csv:"month"`
	Vendor      string `csv:"vendor"`
	Area        string `csv:"area"`
	SiteName    string `csv:"site_name"`
	Owner       string `csv:"owner"`
	Capacity    string `csv:"capacity"`
	NewCapacity string `csv:"new_capacity"`
}

func (t *Temp) Index() string {
	return fmt.Sprintf("%v-%v.*", model.SolarIndex, t.Date().Format("2006.01"))
}

func (t *Temp) Date() time.Time {
	date, err := time.Parse("1/2/2006", t.Month)
	if err != nil {
		log.Warn().Err(err).Str("month", t.Month).Msg("failed to parse date")
		return time.Time{}
	}
	return date
}

func (t *Temp) CapacityFloat() float64 {
	capacity, err := strconv.ParseFloat(t.Capacity, 64)
	if err != nil {
		return 0
	}
	return capacity
}

func (t *Temp) NewCapacityFloat() float64 {
	newCapacity, err := strconv.ParseFloat(t.NewCapacity, 64)
	if err != nil {
		return 0
	}
	return newCapacity
}

// func main() {
// 	log.Info().Msg("opening temp.csv file")
// 	file, err := os.Open("temp.csv")
// 	if err != nil {
// 		log.Fatal().Err(err).Msg("failed to open file")
// 	}
// 	defer file.Close()

// 	log.Info().Msg("unmarshaling CSV file")
// 	var temps []Temp
// 	if err := gocsv.UnmarshalFile(file, &temps); err != nil {
// 		log.Fatal().Err(err).Msg("failed to unmarshal CSV")
// 	}
// 	log.Info().Int("records", len(temps)).Msg("CSV file unmarshaled successfully")

// 	log.Info().Msg("processing records and creating bulk documents")
// 	bulkDocuments := make([]model.BulkDocument, 0)
// 	for i, temp := range temps {
// 		log.Debug().
// 			Int("index", i).
// 			Str("vendor", temp.Vendor).
// 			Str("site_name", temp.SiteName).
// 			Str("id", temp.Id).
// 			Msg("processing record")

// 		ids := findDocumentId(context.Background(), &temp)
// 		date := temp.Date()

// 		log.Debug().
// 			Int("matches", len(ids)).
// 			Time("date", date).
// 			Float64("new_capacity", temp.NewCapacityFloat()).
// 			Msg("found matching documents")

// 		for _, id := range ids {
// 			doc := model.BulkDocument{
// 				Date:       &date,
// 				BulkType:   model.BulkUpdate,
// 				DocumentId: id,
// 				Document: map[string]any{
// 					"installed_capacity": temp.NewCapacityFloat(),
// 				},
// 			}
// 			bulkDocuments = append(bulkDocuments, doc)
// 		}
// 	}
// 	log.Info().Int("total_documents", len(bulkDocuments)).Msg("finished creating bulk documents")

// 	log.Info().Msg("creating output JSON file")
// 	file, err = os.Create("documents.json")
// 	if err != nil {
// 		log.Fatal().Err(err).Msg("failed to create file")
// 	}
// 	defer file.Close()

// 	log.Info().Msg("encoding documents to JSON")
// 	encoder := json.NewEncoder(file)
// 	encoder.SetIndent("", "  ")
// 	if err := encoder.Encode(bulkDocuments); err != nil {
// 		log.Fatal().Err(err).Msg("failed to encode documents")
// 	}

// 	log.Info().Int("count", len(bulkDocuments)).Msg("✅ documents written to file")
// }

// func findDocumentId(ctx context.Context, temp *Temp) []string {
// 	query := elastic.NewBoolQuery().
// 		Must(
// 			elastic.NewTermQuery("data_type.keyword", model.DataTypePlant),
// 			elastic.NewTermQuery("vendor_type.keyword", temp.Vendor),
// 			elastic.NewTermQuery("name.keyword", temp.SiteName),
// 			elastic.NewTermQuery("id.keyword", temp.Id),
// 		)

// 	client := infra.ElasticClient
// 	searchResult, err := client.Search().
// 		Index(temp.Index()).
// 		Query(query).
// 		Size(1000).
// 		FetchSourceContext(elastic.NewFetchSourceContext(true).Include("_id")).
// 		Do(ctx)

// 	if err != nil {
// 		log.Fatal().Err(err).Msg("failed to search for document")
// 	}

// 	ids := make([]string, 0, len(searchResult.Hits.Hits))
// 	for _, hit := range searchResult.Hits.Hits {
// 		ids = append(ids, hit.Id)
// 	}

//		return ids
//	}
type SearchPayload struct {
	Temp *Temp
	Req  *elastic.SearchRequest
}

func main() {
	// Load CSV
	log.Info().Msg("opening temp.csv file")
	file, err := os.Open("temp.csv")
	if err != nil {
		log.Fatal().Err(err).Msg("failed to open file")
	}
	defer file.Close()

	var temps []Temp
	if err := gocsv.UnmarshalFile(file, &temps); err != nil {
		log.Fatal().Err(err).Msg("failed to unmarshal CSV")
	}
	log.Info().Int("records", len(temps)).Msg("CSV file unmarshaled successfully")

	// Prepare msearch
	client := infra.ElasticClient
	ctx := context.Background()
	var msearchRequests []SearchPayload

	for i, temp := range temps {
		// date := temp.Date()
		query := elastic.NewBoolQuery().
			Must(
				elastic.NewTermQuery("data_type.keyword", model.DataTypePlant),
				elastic.NewTermQuery("vendor_type.keyword", temp.Vendor),
				elastic.NewTermQuery("name.keyword", temp.SiteName),
				elastic.NewTermQuery("id.keyword", temp.Id),
			)

		req := elastic.NewSearchRequest().
			Index(temp.Index()).
			Query(query).
			FetchSourceContext(elastic.NewFetchSourceContext(true).Include("_id")).
			Size(1000)

		msearchRequests = append(msearchRequests, SearchPayload{Temp: &temps[i], Req: req})
	}

	// Execute msearch
	msearch := client.MultiSearch()
	for _, s := range msearchRequests {
		msearch.Add(s.Req)
	}
	resp, err := msearch.Do(ctx)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to execute msearch")
	}

	// Build bulk documents from results
	if len(resp.Responses) != len(msearchRequests) {
		log.Fatal().Msg("unexpected response length from msearch")
	}

	var bulkDocuments []model.BulkDocument
	for i, res := range resp.Responses {
		temp := msearchRequests[i].Temp
		date := temp.Date()

		if res.Error != nil {
			log.Warn().Any("error", res.Error).Msgf("skipping errored msearch result at index %d", i)
			continue
		}

		for _, hit := range res.Hits.Hits {
			doc := model.BulkDocument{
				Date:       &date,
				BulkType:   model.BulkUpdate,
				DocumentId: hit.Id,
				Document: map[string]any{
					"installed_capacity": temp.NewCapacityFloat(),
				},
			}
			bulkDocuments = append(bulkDocuments, doc)
		}
	}

	log.Info().Int("total_documents", len(bulkDocuments)).Msg("finished creating bulk documents")

	// Write JSON file
	file, err = os.Create("documents.json")
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create file")
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(bulkDocuments); err != nil {
		log.Fatal().Err(err).Msg("failed to encode documents")
	}

	log.Info().Int("count", len(bulkDocuments)).Msg("✅ documents written to file")
}

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/HavvokLab/true-solar/infra"
	"github.com/HavvokLab/true-solar/model"
	"github.com/go-playground/validator/v10"
	"github.com/olivere/elastic/v7"
	"github.com/rs/zerolog/log"
)

const DefaultFileName = "documents.json"

func main() {
	filename := flag.String("f", DefaultFileName, "filename to process")
	flag.Parse()

	file, err := os.Open(*filename)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to open file")
	}
	defer file.Close()

	var docs []model.BulkDocument
	if err := json.NewDecoder(file).Decode(&docs); err != nil {
		log.Fatal().Err(err).Msg("failed to decode file")
	}

	vld := validator.New()
	bulk := infra.ElasticClient.Bulk()
	for _, doc := range docs {
		if err := vld.Struct(doc); err != nil {
			log.Warn().Err(err).Any("document", doc).Msg("⚠️ invalid document")
			continue
		}

		// Validate document ID for update and delete operations
		if (doc.BulkType == model.BulkUpdate || doc.BulkType == model.BulkDelete) && doc.DocumentId == "" {
			log.Warn().Any("document", doc).Msg("⚠️ document_id required for update/delete operations")
			continue
		}

		switch doc.BulkType {
		case model.BulkInsert:
			plantItem := model.PlantItem{}
			buf, err := json.Marshal(doc.Document)
			if err != nil {
				log.Warn().Err(err).Any("document", doc).Msg("⚠️ invalid document")
				continue
			}

			if err := json.Unmarshal(buf, &plantItem); err != nil {
				log.Warn().Err(err).Any("document", doc).Msg("⚠️ invalid document")
				continue
			}

			req := elastic.NewBulkCreateRequest()
			req.Index(indexName(doc.Date))
			req.Doc(plantItem)
			bulk.Add(req)

		case model.BulkUpdate:
			if len(doc.Document) == 0 {
				log.Warn().Any("document", doc).Msg("⚠️ empty document for update operation")
				continue
			}

			req := elastic.NewBulkUpdateRequest()
			req.Index(indexName(doc.Date))
			req.Doc(doc.Document)
			req.Id(doc.DocumentId)
			bulk.Add(req)

		case model.BulkDelete:
			req := elastic.NewBulkDeleteRequest()
			req.Index(indexName(doc.Date))
			req.Id(doc.DocumentId)
			bulk.Add(req)
		}
	}

	// Check if there are any operations to perform
	if bulk.NumberOfActions() == 0 {
		log.Info().Msg("no valid documents to process")
		return
	}

	resp, err := bulk.Do(context.Background())
	if err != nil {
		log.Fatal().Err(err).Msg("failed to execute bulk")
	}

	if resp.Errors {
		for _, item := range resp.Failed() {
			buf, _ := json.Marshal(item)
			log.Error().Any("error", item.Error).Any("document", string(buf)).Msg("⚠️ bulk failed")
		}
	}

	log.Info().Int("processed", bulk.NumberOfActions()).Msg("✅ bulk success")
}

func indexName(date *time.Time) string {
	return fmt.Sprintf("%v-%v.*", model.SolarIndex, date.Format("2006.01.02"))
}

package model

import "time"

type BulkType string

const (
	BulkInsert BulkType = "insert"
	BulkUpdate BulkType = "update"
	BulkDelete BulkType = "delete"
)

type BulkDocument struct {
	Date       *time.Time     `json:"date" validate:"required"`
	BulkType   BulkType       `json:"bulk_type" validate:"required,oneof=insert update delete"`
	DocumentId string         `json:"document_id"`
	Document   map[string]any `json:"document,omitempty"`
}

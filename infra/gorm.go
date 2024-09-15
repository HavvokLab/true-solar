package infra

import (
	"github.com/rs/zerolog/log"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

var GormDB *gorm.DB

func init() {
	var err error
	GormDB, err = NewGormDB()
	if err != nil {
		log.Panic().Err(err).Msg("failed to initialize gorm db")
	}
}

func NewGormDB(paths ...string) (*gorm.DB, error) {
	var path string = "database.db"
	if len(paths) > 0 {
		path = paths[0]
	}

	db, err := gorm.Open(sqlite.Open(path), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	return db, nil
}

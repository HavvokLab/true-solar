package main

import (
	"strings"
	"time"

	"github.com/HavvokLab/true-solar/infra"
	"github.com/HavvokLab/true-solar/model"
	"github.com/HavvokLab/true-solar/pkg/logger"
	"github.com/HavvokLab/true-solar/repo"
	"github.com/HavvokLab/true-solar/troubleshoot"
	"github.com/rs/zerolog/log"
)

func init() {
	logger.Init("troubleshoot.log")
	loc, _ := time.LoadLocation("Asia/Bangkok")
	time.Local = loc
}

func main() {
	start, end, vendor := parseFlags()
	switch strings.ToLower(vendor) {
	case model.VendorTypeGrowatt:
		collectGrowatt(start, end)
	default:
		log.Panic().Msgf("vendor %s not supported", vendor)
	}
}

func collectGrowatt(start, end time.Time) {
	credRepo := repo.NewGrowattCredentialRepo(infra.GormDB)
	credentials, err := credRepo.FindAll()
	if err != nil {
		log.Panic().Err(err).Msg("error find all credentials")
	}

	for _, credential := range credentials {
		serv := troubleshoot.NewGrowattTroubleshoot(
			repo.NewSolarRepo(infra.ElasticClient),
			repo.NewSiteRegionMappingRepo(infra.GormDB),
		)

		serv.ExecuteByRange(&credential, start, end)
		break
	}
}

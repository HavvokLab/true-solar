package main

import (
	"strings"
	"time"

	"github.com/HavvokLab/true-solar/flags"
	"github.com/HavvokLab/true-solar/infra"
	"github.com/HavvokLab/true-solar/model"
	"github.com/HavvokLab/true-solar/pkg/logger"
	"github.com/HavvokLab/true-solar/repo"
	"github.com/HavvokLab/true-solar/troubleshoot"
	"github.com/gammazero/workerpool"
	"github.com/rs/zerolog/log"
)

var (
	WorkerPoolSize = 5
)

func init() {
	logger.Init("troubleshoot.log")
	loc, _ := time.LoadLocation("Asia/Bangkok")
	time.Local = loc
}

func main() {
	workerPoolSize, start, end, vendor := flags.TroubleshootFlags()
	WorkerPoolSize = workerPoolSize

	switch strings.ToLower(vendor) {
	case model.VendorTypeGrowatt:
		collectGrowatt(start, end)
	case model.VendorTypeInvt:
		collectSolarman(start, end)
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

	pool := workerpool.New(WorkerPoolSize)
	for _, credential := range credentials {
		serv := troubleshoot.NewGrowattTroubleshoot(
			repo.NewSolarRepo(infra.ElasticClient),
			repo.NewSiteRegionMappingRepo(infra.GormDB),
		)

		clone := credential
		pool.Submit(func() {
			serv.ExecuteByRange(&clone, start, end)
		})
	}
	pool.StopWait()
}

func collectSolarman(start, end time.Time) {
	credRepo := repo.NewSolarmanCredentialRepo(infra.GormDB)
	credentials, err := credRepo.FindAll()
	if err != nil {
		log.Panic().Err(err).Msg("error find all credentials")
	}

	pool := workerpool.New(WorkerPoolSize)
	for _, credential := range credentials {
		serv := troubleshoot.NewSolarmanTroubleshoot(
			repo.NewSolarRepo(infra.ElasticClient),
			repo.NewSiteRegionMappingRepo(infra.GormDB),
		)

		clone := credential
		pool.Submit(func() {
			serv.ExecuteByRange(&clone, start, end)
		})
	}
	pool.StopWait()
}

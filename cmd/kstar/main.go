package main

import (
	"time"

	"github.com/HavvokLab/true-solar/collector"
	"github.com/HavvokLab/true-solar/infra"
	"github.com/HavvokLab/true-solar/pkg/logger"
	"github.com/HavvokLab/true-solar/repo"
	"github.com/rs/zerolog/log"
	"github.com/sourcegraph/conc"
)

func init() {
	logger.Init("kstar.log")
	loc, _ := time.LoadLocation("Asia/Bangkok")
	time.Local = loc
}

func main() {
	// cron := gocron.NewScheduler(time.Local)
	// cron.Cron("*/16 7-19 * * *").Do(collect)
	// cron.Cron("1 1-6 * * *").Do(collect)
	// cron.StartBlocking()
	collect()
}

func collect() {
	credRepo := repo.NewKStarCredentialRepo(infra.GormDB)
	credentials, err := credRepo.FindAll()
	if err != nil {
		log.Panic().Err(err).Msg("error find all credentials")
	}

	wg := conc.NewWaitGroup()
	for _, credential := range credentials {
		cred := credential
		wg.Go(func() {
			serv := collector.NewKstarCollector(
				repo.NewSolarMockRepo(),
				repo.NewSiteRegionMappingRepo(infra.GormDB),
			)

			serv.Execute(&cred)
		})
	}

	if r := wg.WaitAndRecover(); r != nil {
		log.Panic().Any("recover", r.Value).Msg("error wait group")
	}
}

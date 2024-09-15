package main

import (
	"time"

	"github.com/HavvokLab/true-solar/collector"
	"github.com/HavvokLab/true-solar/infra"
	"github.com/HavvokLab/true-solar/pkg/logger"
	"github.com/HavvokLab/true-solar/repo"
	"github.com/go-co-op/gocron"
	"github.com/rs/zerolog/log"
	"github.com/sourcegraph/conc"
)

func init() {
	logger.Init("huawei2.log")
	loc, _ := time.LoadLocation("Asia/Bangkok")
	time.Local = loc
}

func main() {
	cron := gocron.NewScheduler(time.Local)
	cron.Cron("0 17 * * *").Do(collect)
	cron.Cron("0 19 * * *").Do(collect)
	cron.StartBlocking()
}

func collect() {
	credRepo := repo.NewHuaweiCredentialRepo(infra.GormDB)
	credentials, err := credRepo.FindAll()
	if err != nil {
		log.Panic().Err(err).Msg("error find all credentials")
	}

	wg := conc.NewWaitGroup()
	for _, credential := range credentials {
		cred := credential
		if cred.Version != 2 {
			continue
		}

		wg.Go(func() {
			serv := collector.NewHuawei2Collector(
				repo.NewSolarRepo(infra.ElasticClient),
				repo.NewSiteRegionMappingRepo(infra.GormDB),
			)

			serv.Execute(&cred)
		})
	}

	if r := wg.WaitAndRecover(); r != nil {
		log.Panic().Any("recover", r.Value).Msg("error wait group")
	}
}

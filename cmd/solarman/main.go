package main

import (
	"time"

	"github.com/HavvokLab/true-solar/alarm"
	"github.com/HavvokLab/true-solar/collector"
	"github.com/HavvokLab/true-solar/config"
	"github.com/HavvokLab/true-solar/infra"
	"github.com/HavvokLab/true-solar/pkg/logger"
	"github.com/HavvokLab/true-solar/repo"
	"github.com/HavvokLab/true-solar/setting"
	"github.com/go-co-op/gocron"
	"github.com/rs/zerolog/log"
	"github.com/sourcegraph/conc"
)

func init() {
	logger.Init("solarman.log")
	loc, _ := time.LoadLocation("Asia/Bangkok")
	time.Local = loc
}

func main() {
	cron := gocron.NewScheduler(time.Local)
	cron.Cron(setting.CrontabCollectDayTime).Do(collect)
	cron.Cron(setting.CrontabCollectNightTime).Do(collect)
	cron.Cron(setting.CrontabAlarmTime).Do(runAlarm)
	cron.StartBlocking()
}

func collect() {
	credRepo := repo.NewSolarmanCredentialRepo(infra.GormDB)
	credentials, err := credRepo.FindAll()
	if err != nil {
		log.Panic().Err(err).Msg("error find all credentials")
	}

	wg := conc.NewWaitGroup()
	now := time.Now()
	for _, credential := range credentials {
		cred := credential
		wg.Go(func() {
			serv := collector.NewSolarmanCollector(
				repo.NewSolarRepo(infra.ElasticClient),
				repo.NewSiteRegionMappingRepo(infra.GormDB),
			)

			serv.Execute(now, &cred)
		})
	}

	if r := wg.WaitAndRecover(); r != nil {
		log.Panic().Any("recover", r.Value).Msg("error wait group")
	}
}

func runAlarm() {
	credRepo := repo.NewSolarmanCredentialRepo(infra.GormDB)
	credentials, err := credRepo.FindAll()
	if err != nil {
		log.Panic().Err(err).Msg("error find all credentials")
	}

	snmp, err := infra.NewSnmpOrchestrator(infra.TrapTypeSolarmanAlarm, config.GetConfig().SnmpList)
	if err != nil {
		log.Panic().Err(err).Msg("error create snmp orchestrator")
	}

	rdb, err := infra.NewRedis()
	if err != nil {
		log.Panic().Err(err).Msg("error create redis")
	}

	wg := conc.NewWaitGroup()
	for _, credential := range credentials {
		cred := credential
		wg.Go(func() {
			serv := alarm.NewSolarmanAlarm(
				repo.NewSolarRepo(infra.ElasticClient),
				snmp,
				rdb,
			)

			serv.Run(&cred)
		})
	}

	if r := wg.WaitAndRecover(); r != nil {
		log.Panic().Any("recover", r.Value).Msg("error wait group")
	}
}

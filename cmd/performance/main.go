package main

import (
	"time"

	"github.com/HavvokLab/true-solar/alarm"
	"github.com/HavvokLab/true-solar/config"
	"github.com/HavvokLab/true-solar/infra"
	"github.com/HavvokLab/true-solar/pkg/logger"
	"github.com/HavvokLab/true-solar/repo"
	"github.com/HavvokLab/true-solar/setting"
	"github.com/go-co-op/gocron"
	"github.com/rs/zerolog/log"
)

func init() {
	logger.Init("performance_alarm.log")
	loc, _ := time.LoadLocation("Asia/Bangkok")
	time.Local = loc
}

func main() {
	cron := gocron.NewScheduler(time.Local)
	cron.Cron(setting.CrontabLowPerformanceAlarmTime).Do(lowPerformanceAlarm)
	cron.Cron(setting.CrontabSumPerformanceAlarmTime).Do(sumPerformanceAlarm)
	cron.StartBlocking()
}

func lowPerformanceAlarm() {
	snmp, err := infra.NewSnmpOrchestrator(infra.TrapTypeClearAlarm, config.GetConfig().SnmpList)
	if err != nil {
		log.Panic().Err(err).Msg("error create snmp orchestrator")
	}

	solarRepo := repo.NewSolarRepo(infra.ElasticClient)
	installedCapacityRepo := repo.NewInstalledCapacityRepo(infra.GormDB)
	performanceAlarmConfigRepo := repo.NewPerformanceAlarmConfigRepo(infra.GormDB)
	lowAlarm := alarm.NewLowPerformanceAlarm(
		solarRepo,
		installedCapacityRepo,
		performanceAlarmConfigRepo,
		snmp,
	)

	if err := lowAlarm.Run(); err != nil {
		log.Error().Err(err).Msg("error run low performance alarm")
	}
}

func sumPerformanceAlarm() {
	snmp, err := infra.NewSnmpOrchestrator(infra.TrapTypeClearAlarm, config.GetConfig().SnmpList)
	if err != nil {
		log.Panic().Err(err).Msg("error create snmp orchestrator")
	}

	solarRepo := repo.NewSolarRepo(infra.ElasticClient)
	installedCapacityRepo := repo.NewInstalledCapacityRepo(infra.GormDB)
	performanceAlarmConfigRepo := repo.NewPerformanceAlarmConfigRepo(infra.GormDB)
	sumAlarm := alarm.NewSumPerformanceAlarm(
		solarRepo,
		installedCapacityRepo,
		performanceAlarmConfigRepo,
		snmp,
	)

	if err := sumAlarm.Run(); err != nil {
		log.Error().Err(err).Msg("error run sum performance alarm")
	}

}

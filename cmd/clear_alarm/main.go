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
	logger.Init("clear_alarm.log")
	loc, _ := time.LoadLocation("Asia/Bangkok")
	time.Local = loc
}

func main() {
	cron := gocron.NewScheduler(time.Local)
	cron.Cron(setting.CrontabClearPerformanceAlarmTime).Do(runPerformanceAlarm)
	cron.StartBlocking()
}

//! unused alarm, but keep it for future use
// func runAlarm() {
// 	snmp, err := infra.NewSnmpOrchestrator(infra.TrapTypeClearAlarm, config.GetConfig().SnmpList)
// 	if err != nil {
// 		log.Panic().Err(err).Msg("error create snmp orchestrator")
// 	}

// 	clearAlarm := alarm.NewClearAlarm(repo.NewSolarRepo(infra.ElasticClient), snmp)
// 	if err := clearAlarm.Run(); err != nil {
// 		log.Panic().Err(err).Msg("error run clear alarm")
// 	}
// }

func runPerformanceAlarm() {
	snmp, err := infra.NewSnmpOrchestrator(infra.TrapTypeClearAlarm, config.GetConfig().SnmpList)
	if err != nil {
		log.Panic().Err(err).Msg("error create snmp orchestrator")
	}

	clearAlarm := alarm.NewClearAlarm(repo.NewSolarRepo(infra.ElasticClient), snmp)
	if err := clearAlarm.ClearPerformanceAlarm(); err != nil {
		log.Panic().Err(err).Msg("error run clear alarm")
	}
}

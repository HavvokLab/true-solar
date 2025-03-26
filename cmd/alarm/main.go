package main

import (
	"flag"
	"time"

	"github.com/HavvokLab/true-solar/alarm"
	"github.com/HavvokLab/true-solar/config"
	"github.com/HavvokLab/true-solar/infra"
	"github.com/HavvokLab/true-solar/pkg/logger"
	"github.com/HavvokLab/true-solar/repo"
	"github.com/rs/zerolog/log"
	"github.com/sourcegraph/conc"
)

// parseFlags parses the workerPoolSize, startDate, endDate, and vendor flags and returns them.
func parseFlags() string {
	// Define flags
	vendor := flag.String("vendor", "", "Vendor name")

	// Parse flags
	flag.Parse()

	return *vendor
}

func init() {
	logger.Init("alarm.log")
	loc, _ := time.LoadLocation("Asia/Bangkok")
	time.Local = loc
}

func main() {
	vendor := parseFlags()
	log.Info().Msgf("start alarm for vendor: %s", vendor)
	switch vendor {
	case "growatt":
		growatt()
	case "kstar":
		kstar()
	case "huawei":
		huawei()
	case "solarman":
		solarman()
	default:
		log.Panic().Msg("invalid vendor")
	}
}

func growatt() {
	credRepo := repo.NewGrowattCredentialRepo(infra.GormDB)
	credentials, err := credRepo.FindAll()
	if err != nil {
		log.Panic().Err(err).Msg("error find all credentials")
	}
	log.Info().Msgf("found %d credentials", len(credentials))
	snmp, err := infra.NewSnmpOrchestrator(infra.TrapTypeGrowattAlarm, config.GetConfig().SnmpList)
	if err != nil {
		log.Panic().Err(err).Msg("error create snmp orchestrator")
	}
	log.Info().Msg("create snmp orchestrator success")
	rdb, err := infra.NewRedis()
	if err != nil {
		log.Panic().Err(err).Msg("error create redis")
	}
	log.Info().Msg("create redis success")
	wg := conc.NewWaitGroup()
	for _, credential := range credentials {
		cred := credential
		wg.Go(func() {
			serv := alarm.NewGrowattAlarm(
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

func huawei() {
	credRepo := repo.NewHuaweiCredentialRepo(infra.GormDB)
	credentials, err := credRepo.FindAll()
	if err != nil {
		log.Panic().Err(err).Msg("error find all credentials")
	}
	log.Info().Msgf("found %d credentials", len(credentials))

	snmp, err := infra.NewSnmpOrchestrator(infra.TrapTypeHuaweiAlarm, config.GetConfig().SnmpList)
	if err != nil {
		log.Panic().Err(err).Msg("error create snmp orchestrator")
	}
	log.Info().Msg("create snmp orchestrator success")

	rdb, err := infra.NewRedis()
	if err != nil {
		log.Panic().Err(err).Msg("error create redis")
	}
	log.Info().Msg("create redis success")

	wg := conc.NewWaitGroup()
	for _, credential := range credentials {
		cred := credential
		if cred.Version != 1 {
			continue
		}

		wg.Go(func() {
			serv := alarm.NewHuaweiAlarm(
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

func kstar() {
	credRepo := repo.NewKStarCredentialRepo(infra.GormDB)
	credentials, err := credRepo.FindAll()
	if err != nil {
		log.Panic().Err(err).Msg("error find all credentials")
	}
	log.Info().Msgf("found %d credentials", len(credentials))
	snmp, err := infra.NewSnmpOrchestrator(infra.TrapTypeKstarAlarm, config.GetConfig().SnmpList)
	if err != nil {
		log.Panic().Err(err).Msg("error create snmp orchestrator")
	}
	log.Info().Msg("create snmp orchestrator success")
	rdb, err := infra.NewRedis()
	if err != nil {
		log.Panic().Err(err).Msg("error create redis")
	}
	log.Info().Msg("create redis success")
	wg := conc.NewWaitGroup()
	for _, credential := range credentials {
		cred := credential
		wg.Go(func() {
			serv := alarm.NewKstarAlarm(
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

func solarman() {
	credRepo := repo.NewSolarmanCredentialRepo(infra.GormDB)
	credentials, err := credRepo.FindAll()
	if err != nil {
		log.Panic().Err(err).Msg("error find all credentials")
	}
	log.Info().Msgf("found %d credentials", len(credentials))
	snmp, err := infra.NewSnmpOrchestrator(infra.TrapTypeSolarmanAlarm, config.GetConfig().SnmpList)
	if err != nil {
		log.Panic().Err(err).Msg("error create snmp orchestrator")
	}
	log.Info().Msg("create snmp orchestrator success")
	rdb, err := infra.NewRedis()
	if err != nil {
		log.Panic().Err(err).Msg("error create redis")
	}
	log.Info().Msg("create redis success")
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

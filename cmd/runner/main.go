package main

import (
	"fmt"
	"time"

	"github.com/HavvokLab/true-solar/alarm"
	"github.com/HavvokLab/true-solar/collector"
	"github.com/HavvokLab/true-solar/config"
	"github.com/HavvokLab/true-solar/infra"
	"github.com/HavvokLab/true-solar/pkg/logger"
	"github.com/HavvokLab/true-solar/repo"
	"github.com/go-co-op/gocron"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/sourcegraph/conc"
)

const (
	huaweiSupportedVersion  = 1
	huawei2SupportedVersion = 2

	lowPerformanceMaxRetries = 5
	lowPerformanceRetryDelay = 5 * time.Minute
)

var (
	growattJobLogger     = newVendorLogger("growatt.log")
	kstarJobLogger       = newVendorLogger("kstar.log")
	huaweiJobLogger      = newVendorLogger("huawei.log")
	huawei2JobLogger     = newVendorLogger("huawei2.log")
	solarmanJobLogger    = newVendorLogger("solarman.log")
	clearAlarmJobLogger  = newVendorLogger("clear_alarm.log")
	performanceJobLogger = newVendorLogger("performance_alarm.log")
)

func main() {
	logger.Init("runner.log")
	if loc, err := time.LoadLocation("Asia/Bangkok"); err == nil {
		time.Local = loc
	}

	cron := gocron.NewScheduler(time.Local)
	if err := registerJobs(cron); err != nil {
		log.Fatal().Err(err).Msg("failed to register runner jobs")
	}

	log.Info().Msg("starting runner scheduler")
	cron.StartBlocking()
}

func registerJobs(cron *gocron.Scheduler) error {
	registrars := []func(*gocron.Scheduler) error{
		scheduleGrowattJobs,
		scheduleKstarJobs,
		scheduleHuaweiJobs,
		scheduleHuawei2Jobs,
		scheduleSolarmanJobs,
		schedulePerformanceJobs,
	}

	for _, registrar := range registrars {
		if err := registrar(cron); err != nil {
			return err
		}
	}

	return nil
}

func scheduleGrowattJobs(cron *gocron.Scheduler) error {
	cfg := config.GetConfig()
	if err := addCronJob(cron, cfg.Crontab.CollectTime, "growatt_collect", growattJobLogger, func() error {
		return runGrowattCollect(growattJobLogger)
	}); err != nil {
		return err
	}

	if err := addCronJob(cron, cfg.Crontab.AlarmTime, "growatt_alarm", growattJobLogger, func() error {
		return runGrowattAlarm(growattJobLogger)
	}); err != nil {
		return err
	}

	return nil
}

func scheduleKstarJobs(cron *gocron.Scheduler) error {
	cfg := config.GetConfig()
	if err := addCronJob(cron, cfg.Crontab.CollectTime, "kstar_collect", kstarJobLogger, func() error {
		return runKstarCollect(kstarJobLogger)
	}); err != nil {
		return err
	}

	if err := addCronJob(cron, cfg.Crontab.AlarmTime, "kstar_alarm", kstarJobLogger, func() error {
		return runKstarAlarm(kstarJobLogger)
	}); err != nil {
		return err
	}

	return nil
}

func scheduleHuaweiJobs(cron *gocron.Scheduler) error {
	cfg := config.GetConfig()
	if err := addCronJob(cron, cfg.Crontab.CollectTime, "huawei_collect", huaweiJobLogger, func() error {
		return runHuaweiCollect(huaweiJobLogger)
	}); err != nil {
		return err
	}

	if err := addCronJob(cron, cfg.Crontab.AlarmTime, "huawei_alarm", huaweiJobLogger, func() error {
		return runHuaweiAlarm(huaweiJobLogger)
	}); err != nil {
		return err
	}

	return nil
}

func scheduleHuawei2Jobs(cron *gocron.Scheduler) error {
	cfg := config.GetConfig()
	if err := addCronJob(cron, cfg.Crontab.CollectTime, "huawei2_collect", huawei2JobLogger, func() error {
		return runHuawei2Collect(huawei2JobLogger)
	}); err != nil {
		return err
	}

	return nil
}

func scheduleSolarmanJobs(cron *gocron.Scheduler) error {
	cfg := config.GetConfig()
	if err := addCronJob(cron, cfg.Crontab.CollectTime, "solarman_collect", solarmanJobLogger, func() error {
		return runSolarmanCollect(solarmanJobLogger)
	}); err != nil {
		return err
	}

	if err := addCronJob(cron, cfg.Crontab.AlarmTime, "solarman_alarm", solarmanJobLogger, func() error {
		return runSolarmanAlarm(solarmanJobLogger)
	}); err != nil {
		return err
	}

	return nil
}

func schedulePerformanceJobs(cron *gocron.Scheduler) error {
	cfg := config.GetConfig()
	if err := addCronJob(cron, cfg.Crontab.LowPerformanceAlarmTime, "low_performance_alarm", performanceJobLogger, func() error {
		return runLowPerformanceAlarm(performanceJobLogger)
	}); err != nil {
		return err
	}

	if err := addCronJob(cron, cfg.Crontab.SumPerformanceAlarmTime, "sum_performance_alarm", performanceJobLogger, func() error {
		return runSumPerformanceAlarm(performanceJobLogger)
	}); err != nil {
		return err
	}

	return nil
}

func addCronJob(cron *gocron.Scheduler, cronExpr, name string, jobLogger zerolog.Logger, fn func() error) error {
	if _, err := cron.Cron(cronExpr).StartImmediately().SingletonMode().Do(func() {
		safeRun(jobLogger, name, fn)
	}); err != nil {
		return fmt.Errorf("failed to schedule %s: %w", name, err)
	}

	return nil
}

func safeRun(jobLogger zerolog.Logger, name string, fn func() error) {
	log := jobLogger.With().Str("job", name).Logger()
	log.Info().Msg("job started")
	defer func() {
		if r := recover(); r != nil {
			log.Error().Any("recover", r).Msg("job panicked")
		}
	}()

	if err := fn(); err != nil {
		log.Error().Err(err).Msg("job finished with error")
		return
	}

	log.Info().Msg("job finished successfully")
}

func runGrowattCollect(jobLogger zerolog.Logger) error {
	defer guardJob(jobLogger, "growatt_collect")

	credRepo := repo.NewGrowattCredentialRepo(infra.GormDB)
	credentials, err := credRepo.FindAll()
	if err != nil {
		jobLogger.Error().Err(err).Msg("failed to find growatt credentials")
		return err
	}

	if len(credentials) == 0 {
		jobLogger.Info().Msg("no growatt credentials found")
		return nil
	}

	wg := conc.NewWaitGroup()
	now := time.Now()
	for _, credential := range credentials {
		cred := credential
		wg.Go(func() {
			serv := collector.NewGrowattCollector(
				repo.NewSolarRepo(infra.ElasticClient),
				repo.NewSiteRegionMappingRepo(infra.GormDB),
			)

			serv.Execute(now, &cred)
		})
	}

	if recovered := wg.WaitAndRecover(); recovered != nil {
		err := fmt.Errorf("growatt collect panic: %v", recovered.Value)
		jobLogger.Error().Err(err).Msg("collector recovered from panic")
		return err
	}

	return nil
}

func runGrowattAlarm(jobLogger zerolog.Logger) error {
	defer guardJob(jobLogger, "growatt_alarm")

	credRepo := repo.NewGrowattCredentialRepo(infra.GormDB)
	credentials, err := credRepo.FindAll()
	if err != nil {
		jobLogger.Error().Err(err).Msg("failed to find growatt credentials")
		return err
	}

	if len(credentials) == 0 {
		jobLogger.Info().Msg("no growatt credentials found")
		return nil
	}

	snmp, err := infra.NewSnmpOrchestrator(infra.TrapTypeGrowattAlarm, config.GetConfig().SnmpList)
	if err != nil {
		jobLogger.Error().Err(err).Msg("failed to create snmp orchestrator")
		return err
	}

	rdb, err := infra.NewRedis()
	if err != nil {
		jobLogger.Error().Err(err).Msg("failed to create redis client")
		return err
	}
	defer rdb.Close()

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

	if recovered := wg.WaitAndRecover(); recovered != nil {
		err := fmt.Errorf("growatt alarm panic: %v", recovered.Value)
		jobLogger.Error().Err(err).Msg("alarm recovered from panic")
		return err
	}

	return nil
}

func runKstarCollect(jobLogger zerolog.Logger) error {
	defer guardJob(jobLogger, "kstar_collect")

	credRepo := repo.NewKStarCredentialRepo(infra.GormDB)
	credentials, err := credRepo.FindAll()
	if err != nil {
		jobLogger.Error().Err(err).Msg("failed to find kstar credentials")
		return err
	}

	if len(credentials) == 0 {
		jobLogger.Info().Msg("no kstar credentials found")
		return nil
	}

	wg := conc.NewWaitGroup()
	for _, credential := range credentials {
		cred := credential
		wg.Go(func() {
			serv := collector.NewKstarCollector(
				repo.NewSolarRepo(infra.ElasticClient),
				repo.NewSiteRegionMappingRepo(infra.GormDB),
			)

			serv.Execute(&cred)
		})
	}

	if recovered := wg.WaitAndRecover(); recovered != nil {
		err := fmt.Errorf("kstar collect panic: %v", recovered.Value)
		jobLogger.Error().Err(err).Msg("collector recovered from panic")
		return err
	}

	return nil
}

func runKstarAlarm(jobLogger zerolog.Logger) error {
	defer guardJob(jobLogger, "kstar_alarm")

	credRepo := repo.NewKStarCredentialRepo(infra.GormDB)
	credentials, err := credRepo.FindAll()
	if err != nil {
		jobLogger.Error().Err(err).Msg("failed to find kstar credentials")
		return err
	}

	if len(credentials) == 0 {
		jobLogger.Info().Msg("no kstar credentials found")
		return nil
	}

	snmp, err := infra.NewSnmpOrchestrator(infra.TrapTypeKstarAlarm, config.GetConfig().SnmpList)
	if err != nil {
		jobLogger.Error().Err(err).Msg("failed to create snmp orchestrator")
		return err
	}

	rdb, err := infra.NewRedis()
	if err != nil {
		jobLogger.Error().Err(err).Msg("failed to create redis client")
		return err
	}
	defer rdb.Close()

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

	if recovered := wg.WaitAndRecover(); recovered != nil {
		err := fmt.Errorf("kstar alarm panic: %v", recovered.Value)
		jobLogger.Error().Err(err).Msg("alarm recovered from panic")
		return err
	}

	return nil
}

func runHuaweiCollect(jobLogger zerolog.Logger) error {
	defer guardJob(jobLogger, "huawei_collect")

	credRepo := repo.NewHuaweiCredentialRepo(infra.GormDB)
	credentials, err := credRepo.FindAll()
	if err != nil {
		jobLogger.Error().Err(err).Msg("failed to find huawei credentials")
		return err
	}

	wg := conc.NewWaitGroup()
	for _, credential := range credentials {
		cred := credential
		if cred.Version != huaweiSupportedVersion {
			continue
		}

		wg.Go(func() {
			serv := collector.NewHuaweiCollector(
				repo.NewSolarRepo(infra.ElasticClient),
				repo.NewSiteRegionMappingRepo(infra.GormDB),
			)

			serv.Execute(&cred)
		})
	}

	if recovered := wg.WaitAndRecover(); recovered != nil {
		err := fmt.Errorf("huawei collect panic: %v", recovered.Value)
		jobLogger.Error().Err(err).Msg("collector recovered from panic")
		return err
	}

	return nil
}

func runHuaweiAlarm(jobLogger zerolog.Logger) error {
	defer guardJob(jobLogger, "huawei_alarm")

	credRepo := repo.NewHuaweiCredentialRepo(infra.GormDB)
	credentials, err := credRepo.FindAll()
	if err != nil {
		jobLogger.Error().Err(err).Msg("failed to find huawei credentials")
		return err
	}

	snmp, err := infra.NewSnmpOrchestrator(infra.TrapTypeHuaweiAlarm, config.GetConfig().SnmpList)
	if err != nil {
		jobLogger.Error().Err(err).Msg("failed to create snmp orchestrator")
		return err
	}

	rdb, err := infra.NewRedis()
	if err != nil {
		jobLogger.Error().Err(err).Msg("failed to create redis client")
		return err
	}
	defer rdb.Close()

	wg := conc.NewWaitGroup()
	for _, credential := range credentials {
		cred := credential
		if cred.Version != huaweiSupportedVersion {
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

	if recovered := wg.WaitAndRecover(); recovered != nil {
		err := fmt.Errorf("huawei alarm panic: %v", recovered.Value)
		jobLogger.Error().Err(err).Msg("alarm recovered from panic")
		return err
	}

	return nil
}

func runHuawei2Collect(jobLogger zerolog.Logger) error {
	defer guardJob(jobLogger, "huawei2_collect")

	credRepo := repo.NewHuaweiCredentialRepo(infra.GormDB)
	credentials, err := credRepo.FindAll()
	if err != nil {
		jobLogger.Error().Err(err).Msg("failed to find huawei credentials")
		return err
	}

	wg := conc.NewWaitGroup()
	for _, credential := range credentials {
		cred := credential
		if cred.Version != huawei2SupportedVersion {
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

	if recovered := wg.WaitAndRecover(); recovered != nil {
		err := fmt.Errorf("huawei2 collect panic: %v", recovered.Value)
		jobLogger.Error().Err(err).Msg("collector recovered from panic")
		return err
	}

	return nil
}

func runSolarmanCollect(jobLogger zerolog.Logger) error {
	defer guardJob(jobLogger, "solarman_collect")

	credRepo := repo.NewSolarmanCredentialRepo(infra.GormDB)
	credentials, err := credRepo.FindAll()
	if err != nil {
		jobLogger.Error().Err(err).Msg("failed to find solarman credentials")
		return err
	}

	if len(credentials) == 0 {
		jobLogger.Info().Msg("no solarman credentials found")
		return nil
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

	if recovered := wg.WaitAndRecover(); recovered != nil {
		err := fmt.Errorf("solarman collect panic: %v", recovered.Value)
		jobLogger.Error().Err(err).Msg("collector recovered from panic")
		return err
	}

	return nil
}

func runSolarmanAlarm(jobLogger zerolog.Logger) error {
	defer guardJob(jobLogger, "solarman_alarm")

	credRepo := repo.NewSolarmanCredentialRepo(infra.GormDB)
	credentials, err := credRepo.FindAll()
	if err != nil {
		jobLogger.Error().Err(err).Msg("failed to find solarman credentials")
		return err
	}

	if len(credentials) == 0 {
		jobLogger.Info().Msg("no solarman credentials found")
		return nil
	}

	snmp, err := infra.NewSnmpOrchestrator(infra.TrapTypeSolarmanAlarm, config.GetConfig().SnmpList)
	if err != nil {
		jobLogger.Error().Err(err).Msg("failed to create snmp orchestrator")
		return err
	}

	rdb, err := infra.NewRedis()
	if err != nil {
		jobLogger.Error().Err(err).Msg("failed to create redis client")
		return err
	}
	defer rdb.Close()

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

	if recovered := wg.WaitAndRecover(); recovered != nil {
		err := fmt.Errorf("solarman alarm panic: %v", recovered.Value)
		jobLogger.Error().Err(err).Msg("alarm recovered from panic")
		return err
	}

	return nil
}

func runClearPerformanceAlarm(jobLogger zerolog.Logger) error {
	defer guardJob(jobLogger, "clear_performance_alarm")

	snmp, err := infra.NewSnmpOrchestrator(infra.TrapTypeClearAlarm, config.GetConfig().SnmpList)
	if err != nil {
		jobLogger.Error().Err(err).Msg("failed to create snmp orchestrator")
		return err
	}

	clearAlarm := alarm.NewClearAlarm(repo.NewSolarRepo(infra.ElasticClient), snmp)
	if err := clearAlarm.ClearPerformanceAlarm(); err != nil {
		jobLogger.Error().Err(err).Msg("failed to clear performance alarm")
		return err
	}

	return nil
}

func runLowPerformanceAlarm(jobLogger zerolog.Logger) error {
	defer guardJob(jobLogger, "low_performance_alarm")

	snmp, err := infra.NewSnmpOrchestrator(infra.TrapTypeClearAlarm, config.GetConfig().SnmpList)
	if err != nil {
		jobLogger.Error().Err(err).Msg("failed to create snmp orchestrator")
		return err
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

	retries := 0
	for retries < lowPerformanceMaxRetries {
		if err := lowAlarm.Run(); err != nil {
			jobLogger.Warn().
				Err(err).
				Int("retry", retries+1).
				Msg("low performance alarm failed, retrying")
			time.Sleep(lowPerformanceRetryDelay)

			retries++
			continue
		}

		jobLogger.Info().Msg("low performance alarm completed successfully")
		return nil
	}

	err = fmt.Errorf("low performance alarm failed after %d retries", lowPerformanceMaxRetries)
	jobLogger.Error().Err(err).Msg("low performance alarm exhausted retries")
	return err
}

func runSumPerformanceAlarm(jobLogger zerolog.Logger) error {
	defer guardJob(jobLogger, "sum_performance_alarm")

	snmp, err := infra.NewSnmpOrchestrator(infra.TrapTypeClearAlarm, config.GetConfig().SnmpList)
	if err != nil {
		jobLogger.Error().Err(err).Msg("failed to create snmp orchestrator")
		return err
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
		jobLogger.Error().Err(err).Msg("failed to run sum performance alarm")
		return err
	}

	return nil
}

func newVendorLogger(file string) zerolog.Logger {
	return zerolog.New(logger.NewWriter(file)).With().Timestamp().Caller().Logger()
}

func guardJob(jobLogger zerolog.Logger, name string) {
	if r := recover(); r != nil {
		jobLogger.Error().
			Str("job", name).
			Any("recover", r).
			Msg("job panicked, recovered to keep scheduler alive")
	}
}

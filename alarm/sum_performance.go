package alarm

import (
	"errors"
	"fmt"
	"strings"
	"time"

	appconfig "github.com/HavvokLab/true-solar/config"
	"github.com/HavvokLab/true-solar/infra"
	"github.com/HavvokLab/true-solar/model"
	"github.com/HavvokLab/true-solar/pkg/logger"
	"github.com/HavvokLab/true-solar/pkg/util"
	"github.com/HavvokLab/true-solar/repo"
	"github.com/rs/zerolog"
	"go.openly.dev/pointy"
)

type SumPerformanceAlarm struct {
	solarRepo                  repo.SolarRepo
	installedCapacityRepo      repo.InstalledCapacityRepo
	performanceAlarmConfigRepo repo.PerformanceAlarmConfigRepo
	snmp                       *infra.SnmpOrchestrator
	logger                     zerolog.Logger
}

func NewSumPerformanceAlarm(
	solarRepo repo.SolarRepo,
	installedCapacityRepo repo.InstalledCapacityRepo,
	performanceAlarmConfigRepo repo.PerformanceAlarmConfigRepo,
	snmp *infra.SnmpOrchestrator,
) *SumPerformanceAlarm {
	return &SumPerformanceAlarm{
		solarRepo:                  solarRepo,
		installedCapacityRepo:      installedCapacityRepo,
		performanceAlarmConfigRepo: performanceAlarmConfigRepo,
		snmp:                       snmp,
		logger:                     zerolog.New(logger.NewWriter("sum_performance_alarm.log")).With().Timestamp().Caller().Logger(),
	}
}

func (p SumPerformanceAlarm) Run() error {
	now := time.Now()
	installedCapacityConfig, err := p.getInstalledCapacity()
	if err != nil {
		p.logger.Error().Err(err).Msg("SumPerformanceAlarm::Run() - failed to get installed capacity")
		return err
	}

	config, err := p.getConfig()
	if err != nil {
		p.logger.Error().Err(err).Msg("SumPerformanceAlarm::Run() - failed to get config")
		return err
	}

	efficiencyFactor := installedCapacityConfig.EfficiencyFactor
	focusHour := installedCapacityConfig.FocusHour
	duration := *config.Duration
	percentage := config.Percentage / 100.0

	p.logger.Info().Int("duration", duration).Msg("start polling sum performance alarm")
	buckets, err := p.solarRepo.GetSumPerformanceLow(duration)
	if err != nil {
		p.logger.Error().Err(err).Msg("SumPerformanceAlarm::Run() - failed to get sum performance low")
		return err
	}
	p.logger.Info().Int("bucket_count", len(buckets)).Msg("Retrieved buckets")

	period := fmt.Sprintf("%s - %s", now.AddDate(0, 0, -duration).Format("02Jan2006"), now.AddDate(0, 0, -1).Format("02Jan2006"))
	filteredBuckets := make(map[string]map[string]interface{})
	for _, bucketPtr := range buckets {
		if bucketPtr != nil {
			bucket := *bucketPtr

			if len(bucket.Key) == 0 {
				continue
			}

			var plantItem *model.PlantItem
			var dailyProduction float64
			var installedCapacity float64
			var key string

			if len(bucket.Key) > 0 {
				if vendorType, ok := bucket.Key["vendor_type"]; ok {
					if id, ok := bucket.Key["id"]; ok {
						key = fmt.Sprintf("%s_%s", vendorType, id)
					}
				}
			}

			if maxDaily, ok := bucket.ValueCount("max_daily"); ok {
				dailyProduction = pointy.Float64Value(maxDaily.Value, 0.0)
			}

			if avgCapacity, ok := bucket.ValueCount("avg_capacity"); ok {
				installedCapacity = pointy.Float64Value(avgCapacity.Value, 0.0)
			}

			if topHits, found := bucket.Aggregations.TopHits("hits"); found {
				if topHits.Hits != nil {
					if len(topHits.Hits.Hits) == 1 {
						searchHitPtr := topHits.Hits.Hits[0]
						if searchHitPtr != nil {
							if err := util.Recast(searchHitPtr.Source, &plantItem); err != nil {
								p.logger.Warn().Err(err).Msg("LowPerformanceAlarm::Run() - failed to recast plant item")
								continue
							}
						}
					}
				}
			}

			if !util.IsEmpty(key) {
				if item, found := filteredBuckets[key]; found {
					if totalProduction, ok := item["totalProduction"].(float64); ok {
						item["totalProduction"] = totalProduction + dailyProduction
					}
					filteredBuckets[key] = item
				} else {
					filteredBuckets[key] = map[string]interface{}{
						"totalProduction":   dailyProduction,
						"installedCapacity": installedCapacity,
						"plantItem":         plantItem,
						"period":            period,
					}
				}
			}
		}
	}

	p.logger.Info().Int("plant_count", len(filteredBuckets)).Msg("start sending sum performance alarm")

	var alarmCount int
	var failedAlarmCount int
	documents := make([]any, 0)
	if len(filteredBuckets) > 0 {
		bucketBatches := p.chunkBy(filteredBuckets, appconfig.PerformanceAlarmSnmpBatchSize)

		var batchAlarmCount int
		var failedBatchAlarmCount int
		for i, batches := range bucketBatches {
			batchAlarmCount = 0
			failedBatchAlarmCount = 0

			for _, batch := range batches {
				for _, data := range batch {
					if installedCapacity, ok := data["installedCapacity"].(float64); ok {
						if totalProduction, ok := data["totalProduction"].(float64); ok {
							threshold := installedCapacity * efficiencyFactor * float64(focusHour) * float64(duration) * percentage
							if totalProduction <= threshold {
								plantName, alarmName, payload, severity, err := p.buildPayload(appconfig.PerformanceAlarmTypeSumPerformanceLow, config, installedCapacityConfig, data)
								if err != nil {
									p.logger.Error().Err(err).Msg("SumPerformanceAlarm::Run() - failed to build payload")
									continue
								}

								document := model.NewSnmpPerformanceAlarmItem("sum", plantName, alarmName, payload, severity, now.Format(time.RFC3339Nano))
								p.snmp.SendTrap(plantName, alarmName, payload, severity, now.Format(time.RFC3339Nano))
								documents = append(documents, document)
								alarmCount++
								batchAlarmCount++
							}
						}
					}
				}
			}

			p.logger.Info().Int("batch", i+1).Int("alarm_count", batchAlarmCount).Msg("batch completed to send alarms")
			p.logger.Info().Int("batch", i+1).Int("failed_alarm_count", failedBatchAlarmCount).Msg("batch failed to send alarms")
			p.logger.Info().Int("batch", i+1).Str("delay", appconfig.PerformanceAlarmSnmpBatchDelay.String()).Msg("batch sleeping for delay")
			time.Sleep(appconfig.PerformanceAlarmSnmpBatchDelay)
		}

		p.logger.Info().Int("alarm_count", alarmCount).Msg("completed to send alarms")
		p.logger.Info().Int("failed_alarm_count", failedAlarmCount).Msg("failed to send alarms")
		p.logger.Info().Str("duration", time.Since(now).String()).Msg("polling finished")
	}

	index := fmt.Sprintf("%s-%s", model.PerformanceAlarmIndex, now.Format("2006.01.02"))
	if err := p.solarRepo.BulkIndex(index, documents); err != nil {
		p.logger.Error().Err(err).Msg("SumPerformanceAlarm::Run() - failed to bulk index")
		return err
	}

	return nil
}

func (s *SumPerformanceAlarm) getConfig() (*model.PerformanceAlarmConfig, error) {
	config, err := s.performanceAlarmConfigRepo.GetSumPerformanceAlarmConfig()
	if err != nil {
		return nil, err
	}

	if config == nil {
		err := errors.New("performance alarm config not found")
		return nil, err
	}

	if pointy.IntValue(config.HitDay, 0) == 0 {
		err := errors.New("hit day must not be zero value")
		return nil, err
	}

	if pointy.IntValue(config.Duration, 0) == 0 {
		err := errors.New("duration must not be zero value")
		return nil, err
	}

	return config, nil
}

func (p SumPerformanceAlarm) getInstalledCapacity() (*model.InstalledCapacity, error) {
	installedCapacity, err := p.installedCapacityRepo.FindOne()
	if err != nil {
		p.logger.Error().Err(err).Msg("SumPerformanceAlarm::getInstalledCapacity() - failed to find installed capacity")
		return nil, err
	}

	if installedCapacity == nil {
		p.logger.Error().Msg("SumPerformanceAlarm::getInstalledCapacity() - installed capacity not found")
		return nil, errors.New("installed capacity not found")
	}

	return installedCapacity, nil
}

func (s *SumPerformanceAlarm) chunkBy(items map[string]map[string]interface{}, chunkSize int) (chunks [][]map[string]map[string]interface{}) {
	slice := make([]map[string]map[string]interface{}, 0)

	for k, v := range items {
		slice = append(slice, map[string]map[string]interface{}{k: v})
	}

	for chunkSize < len(slice) {
		slice, chunks = slice[chunkSize:], append(chunks, slice[0:chunkSize:chunkSize])
	}

	return append(chunks, slice)
}

func (p SumPerformanceAlarm) buildPayload(alarmType int, config *model.PerformanceAlarmConfig, installedCapacity *model.InstalledCapacity, data map[string]any) (string, string, string, string, error) {
	if alarmType != appconfig.PerformanceAlarmTypePerformanceLow && alarmType != appconfig.PerformanceAlarmTypeSumPerformanceLow {
		return "", "", "", "", errors.New("invalid alarm type")
	}

	var capacity float64
	if cap, ok := data["installedCapacity"].(float64); ok {
		capacity = cap
	}

	var plant model.PlantItem
	if item, ok := data["plantItem"].(*model.PlantItem); ok {
		if item != nil {
			plant = *item
		}
	}

	var period string
	if p, ok := data["period"].(string); ok {
		period = p
	}

	var vendorName string
	switch strings.ToLower(plant.VendorType) {
	case model.VendorTypeGrowatt:
		vendorName = "Growatt"
	case model.VendorTypeHuawei:
		vendorName = "HUA"
	case model.VendorTypeKstar:
		vendorName = "Kstar"
	case model.VendorTypeInvt:
		vendorName = "INVT-Ipanda"
	case model.VendorTypeSolarman: // Todo: Remove after change SOLARMAN to INVT in elasticsearch
		vendorName = "INVT-Ipanda"
	default:
		return "", "", "", "", errors.New("invalid vendor type")
	}

	plantName := pointy.StringValue(plant.Name, "")
	alarmName := fmt.Sprintf("SolarCell-%s", strings.ReplaceAll(config.Name, " ", ""))
	alarmNameInDescription := util.AddSpace(config.Name)
	severity := infra.MajorSeverity
	duration := pointy.IntValue(config.Duration, 0)
	hitDay := pointy.IntValue(config.HitDay, 0)
	multipliedCapacity := capacity * installedCapacity.EfficiencyFactor * float64(installedCapacity.FocusHour)

	if alarmType == appconfig.PerformanceAlarmTypePerformanceLow {
		payload := fmt.Sprintf("%s, %s, Less than or equal %.2f%%, Expected Daily Production:%.2f KWH, Actual Production less than:%.2f KWH, Duration:%d days, Period:%s",
			vendorName, alarmNameInDescription, config.Percentage, multipliedCapacity, multipliedCapacity*(config.Percentage/100.0), hitDay, period)
		return plantName, alarmName, payload, severity, nil
	}

	// SumPerformanceLow
	var totalProduction float64
	if x, ok := data["totalProduction"].(float64); ok {
		totalProduction = x
	}

	payload := fmt.Sprintf("%s, %s, Less than or equal %.2f%%, Expected Production:%.2f KWH, Actual Production:%.2f KWH (less than %.2f KWH), Duration:%d days, Period:%s",
		vendorName, alarmNameInDescription, config.Percentage, multipliedCapacity*float64(duration), totalProduction, (multipliedCapacity*float64(duration))*(config.Percentage/100.0), duration, period)
	return plantName, alarmName, payload, severity, nil
}

package alarm

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/HavvokLab/true-solar/infra"
	"github.com/HavvokLab/true-solar/model"
	"github.com/HavvokLab/true-solar/pkg/logger"
	"github.com/HavvokLab/true-solar/pkg/util"
	"github.com/HavvokLab/true-solar/repo"
	"github.com/HavvokLab/true-solar/setting"
	"github.com/rs/zerolog"
	"go.openly.dev/pointy"
)

type LowPerformanceAlarm struct {
	solarRepo                  repo.SolarRepo
	installedCapacityRepo      repo.InstalledCapacityRepo
	performanceAlarmConfigRepo repo.PerformanceAlarmConfigRepo
	snmp                       *infra.SnmpOrchestrator
	logger                     zerolog.Logger
}

func NewLowPerformanceAlarm(
	solarRepo repo.SolarRepo,
	installedCapacityRepo repo.InstalledCapacityRepo,
	performanceAlarmConfigRepo repo.PerformanceAlarmConfigRepo,
	snmp *infra.SnmpOrchestrator,
) *LowPerformanceAlarm {
	return &LowPerformanceAlarm{
		solarRepo:                  solarRepo,
		installedCapacityRepo:      installedCapacityRepo,
		performanceAlarmConfigRepo: performanceAlarmConfigRepo,
		snmp:                       snmp,
		logger:                     zerolog.New(logger.NewWriter("low_performance_alarm.log")).With().Timestamp().Caller().Logger(),
	}
}

func (p LowPerformanceAlarm) Run() error {
	now := time.Now()
	installedCapacity, err := p.getInstalledCapacity()
	if err != nil {
		p.logger.Error().Err(err).Msg("LowPerformanceAlarm::Run() - failed to get installed capacity")
		return err
	}

	config, err := p.getConfig()
	if err != nil {
		p.logger.Error().Err(err).Msg("LowPerformanceAlarm::Run() - failed to get config")
		return err
	}

	efficiencyFactor := installedCapacity.EfficiencyFactor
	focusHour := installedCapacity.FocusHour
	hitDay := *config.HitDay
	duration := *config.Duration
	percentage := config.Percentage / 100.0

	buckets, err := p.solarRepo.GetPerformanceLow(duration, efficiencyFactor, focusHour, percentage)
	if err != nil {
		p.logger.Error().Err(err).Msg("LowPerformanceAlarm::Run() - failed to get performance low")
		return err
	}

	period := fmt.Sprintf("%s - %s", now.AddDate(0, 0, -duration).Format("02Jan2006"), now.AddDate(0, 0, -1).Format("02Jan2006"))
	filteredBuckets := make(map[string]map[string]interface{})
	for _, bucketPtr := range buckets {
		if bucketPtr != nil {
			bucket := *bucketPtr

			if len(bucket.Key) == 0 {
				continue
			}

			var plantItem *model.PlantItem
			var key string
			var installedCapacity float64

			if len(bucket.Key) > 0 {
				if vendorType, ok := bucket.Key["vendor_type"]; ok {
					if id, ok := bucket.Key["id"]; ok {
						key = fmt.Sprintf("%s_%s", vendorType, id)
					}
				}
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
					if count, ok := item["count"].(int); ok {
						item["count"] = count + 1
					}
					filteredBuckets[key] = item
				} else {
					filteredBuckets[key] = map[string]interface{}{
						"count":             1,
						"installedCapacity": installedCapacity,
						"plantItem":         plantItem,
						"period":            period,
					}
				}
			}
		}
	}
	p.logger.Info().Int("plant_count", len(filteredBuckets)).Msg("start sending low performance alarm")

	var alarmCount int
	var failedAlarmCount int
	documents := make([]interface{}, 0)
	if len(filteredBuckets) > 0 {
		bucketBatches := p.chunkBy(filteredBuckets, setting.PerformanceAlarmSnmpBatchSize)

		var batchAlarmCount int
		var failedBatchAlarmCount int
		for i, batches := range bucketBatches {
			batchAlarmCount = 0
			failedBatchAlarmCount = 0

			for _, batch := range batches {
				for _, data := range batch {
					if count, ok := data["count"].(int); ok {
						if count >= hitDay {
							plantName, alarmName, description, severity, err := p.buildPayload(setting.PerformanceAlarmTypePerformanceLow, config, installedCapacity, data)
							if err != nil {
								p.logger.Error().Err(err).Msg("LowPerformanceAlarm::Run() - failed to build payload")
								continue
							}

							document := model.NewSnmpPerformanceAlarmItem("low", plantName, alarmName, description, severity, now.Format(time.RFC3339Nano))
							p.snmp.SendTrap(plantName, alarmName, description, severity, now.Format(time.RFC3339Nano))
							documents = append(documents, document)

							p.logger.Info().Str("plant_name", plantName).Str("alarm_name", alarmName).Str("description", description).Str("severity", severity).Msg("SendAlarmTrap")
							alarmCount++
							batchAlarmCount++
						}
					}
				}
			}

			p.logger.Info().Int("batch", i+1).Int("alarm_count", batchAlarmCount).Msg("batch completed to send alarms")
			p.logger.Info().Int("batch", i+1).Int("failed_alarm_count", failedBatchAlarmCount).Msg("batch failed to send alarms")
			p.logger.Info().Int("batch", i+1).Str("delay", setting.PerformanceAlarmSnmpBatchDelay.String()).Msg("batch sleeping for delay")
			time.Sleep(setting.PerformanceAlarmSnmpBatchDelay)
		}

		p.logger.Info().Int("alarm_count", alarmCount).Msg("completed to send alarms")
		p.logger.Info().Int("failed_alarm_count", failedAlarmCount).Msg("failed to send alarms")
		p.logger.Info().Str("duration", time.Since(now).String()).Msg("polling finished")
	}

	index := fmt.Sprintf("%s-%s", model.PerformanceAlarmIndex, now.Format("2006.01.02"))
	if err := p.solarRepo.BulkIndex(index, documents); err != nil {
		p.logger.Error().Err(err).Msg("LowPerformanceAlarm::Run() - failed to bulk index")
		return err
	}

	return nil
}

func (s *LowPerformanceAlarm) getConfig() (*model.PerformanceAlarmConfig, error) {
	config, err := s.performanceAlarmConfigRepo.GetLowPerformanceAlarmConfig()
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

func (p LowPerformanceAlarm) getInstalledCapacity() (*model.InstalledCapacity, error) {
	installedCapacity, err := p.installedCapacityRepo.FindOne()
	if err != nil {
		p.logger.Error().Err(err).Msg("LowPerformanceAlarm::getInstalledCapacity() - failed to find installed capacity")
		return nil, err
	}

	if installedCapacity == nil {
		p.logger.Error().Msg("LowPerformanceAlarm::getInstalledCapacity() - installed capacity not found")
		return nil, errors.New("installed capacity not found")
	}

	return installedCapacity, nil
}

func (s *LowPerformanceAlarm) chunkBy(items map[string]map[string]interface{}, chunkSize int) (chunks [][]map[string]map[string]interface{}) {
	slice := make([]map[string]map[string]interface{}, 0)

	for k, v := range items {
		slice = append(slice, map[string]map[string]interface{}{k: v})
	}

	for chunkSize < len(slice) {
		slice, chunks = slice[chunkSize:], append(chunks, slice[0:chunkSize:chunkSize])
	}

	return append(chunks, slice)
}

func (p LowPerformanceAlarm) buildPayload(alarmType int, config *model.PerformanceAlarmConfig, installedCapacity *model.InstalledCapacity, data map[string]any) (string, string, string, string, error) {
	if alarmType != setting.PerformanceAlarmTypePerformanceLow && alarmType != setting.PerformanceAlarmTypeSumPerformanceLow {
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

	if alarmType == setting.PerformanceAlarmTypePerformanceLow {
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

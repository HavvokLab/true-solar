package alarm

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/HavvokLab/true-solar/api/kstar"
	"github.com/HavvokLab/true-solar/infra"
	"github.com/HavvokLab/true-solar/model"
	"github.com/HavvokLab/true-solar/pkg/logger"
	"github.com/HavvokLab/true-solar/pkg/util"
	"github.com/HavvokLab/true-solar/repo"
	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog"
	"go.openly.dev/pointy"
)

type KstarAlarm struct {
	vendorType string
	solarRepo  repo.SolarRepo
	snmp       *infra.SnmpOrchestrator
	rdb        *redis.Client
	logger     zerolog.Logger
}

func NewKstarAlarm(solarRepo repo.SolarRepo, snmp *infra.SnmpOrchestrator, rdb *redis.Client) *KstarAlarm {
	return &KstarAlarm{
		vendorType: strings.ToUpper(model.VendorTypeKstar),
		solarRepo:  solarRepo,
		snmp:       snmp,
		rdb:        rdb,
		logger:     zerolog.New(logger.NewWriter("kstar_alarm.log")).With().Timestamp().Caller().Logger(),
	}
}

func (s *KstarAlarm) Run(credential *model.KstarCredential) error {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Warn().Str("username", credential.Username).Any("error", r).Msg("KstarAlarm::Run() - failed to run")
		}
	}()

	ctx := context.Background()
	client := kstar.NewKstarClient(credential.Username, credential.Password, kstar.WithRetryCount(0))
	deviceList, err := client.GetDeviceList()
	if err != nil {
		s.logger.Error().Err(err).Msg("KstarAlarm::Run() - failed to get device list")
		return err
	}

	if deviceList == nil {
		s.logger.Error().Msg("KstarAlarm::Run() - deviceList is nil")
		return errors.New("empty deviceList")
	}

	deviceCount := 1
	deviceSize := len(deviceList)
	documents := make([]interface{}, 0)
	for _, device := range deviceList {
		deviceID := pointy.StringValue(device.ID, "")
		deviceName := pointy.StringValue(device.Name, "")
		plantID := pointy.StringValue(device.PlantID, "")
		plantName := pointy.StringValue(device.PlantName, "")
		saveTime := pointy.StringValue(device.SaveTime, "")

		s.logger.Info().Str("username", credential.Username).Int("device_count", deviceCount).Int("device_size", deviceSize).Str("device_id", deviceID).Str("device_name", deviceName).Str("plant_id", plantID).Str("plant_name", plantName).Str("save_time", saveTime).Msg("KstarAlarm::Run() - device info")
		deviceCount++

		realtimeDeviceDataResp, err := client.GetRealtimeDeviceData(deviceID)
		if err != nil {
			s.logger.Error().Err(err).Msg("KstarAlarm::Run() - failed to get realtime device data")
			return err
		}

		if realtimeDeviceDataResp == nil {
			s.logger.Warn().Msg("KstarAlarm::Run() - realtimeDeviceDataResp is nil")
			continue
		}

		if realtimeDeviceDataResp.Data != nil {
			saveTime = pointy.StringValue(realtimeDeviceDataResp.Data.SaveTime, "")
		}

		if device.Status != nil {
			var document interface{}
			switch *device.Status {
			case 0:
				key := fmt.Sprintf("Kstar,%s,%s,%s,%s", plantID, deviceID, deviceName, "Kstar-Disconnect")
				val := fmt.Sprintf("%s,%s", plantName, saveTime)
				if err := s.rdb.Set(ctx, key, val, 0).Err(); err != nil {
					s.logger.Error().Err(err).Msg("KstarAlarm::Run() - failed to set redis key")
					return err
				}

				alarmName := "Kstar-Disconnect"
				payload := fmt.Sprintf("Kstar,%s,%s,%s", plantID, deviceID, deviceName)
				document = model.NewSnmpAlarmItem(s.vendorType, plantName, alarmName, payload, infra.MajorSeverity, saveTime)
				s.snmp.SendTrap(plantName, alarmName, payload, infra.MajorSeverity, saveTime)
			case 1:
				realtimeAlarmResp, err := client.GetRealtimeAlarmListOfDevice(deviceID)
				if err != nil {
					s.logger.Error().Err(err).Msg("KstarAlarm::Run() - failed to get realtime alarm list of device")
					return err
				}

				if len(realtimeAlarmResp.Data) > 0 {
					alarmName := "Kstar-Disconnect"
					payload := fmt.Sprintf("Kstar,%s,%s,%s", plantID, deviceID, deviceName)
					document = model.NewSnmpAlarmItem(s.vendorType, plantName, alarmName, payload, infra.MajorSeverity, saveTime)
					s.snmp.SendTrap(plantName, alarmName, payload, infra.MajorSeverity, saveTime)

					if err := s.rdb.Del(ctx, alarmName).Err(); err != nil {
						s.logger.Error().Err(err).Msg("KstarAlarm::Run() - failed to delete redis key")
						return err
					}

					for _, alarm := range realtimeAlarmResp.Data {
						alarmTime := pointy.StringValue(alarm.SaveTime, "")
						alarmMessage := strings.ReplaceAll(pointy.StringValue(alarm.Message, ""), " ", "-")

						key := fmt.Sprintf("Kstar,%s,%s,%s,%s", plantID, deviceID, deviceName, alarmMessage)
						val := fmt.Sprintf("%s,%s", plantName, alarmTime)
						if err := s.rdb.Set(ctx, key, val, 0).Err(); err != nil {
							s.logger.Error().Err(err).Msg("KstarAlarm::Run() - failed to set redis key")
							return err
						}

						payload := fmt.Sprintf("Kstar,%s,%s,%s", plantID, deviceID, deviceName)
						document = model.NewSnmpAlarmItem(s.vendorType, plantName, alarmMessage, payload, infra.MajorSeverity, alarmTime)
						s.snmp.SendTrap(plantName, alarmMessage, payload, infra.MajorSeverity, alarmTime)
					}
					continue
				}

				var keys []string
				var cursor uint64
				for {
					var scanKeys []string
					match := fmt.Sprintf("Kstar,%s,%s,%s,*", plantID, deviceID, deviceName)
					scanKeys, cursor, err = s.rdb.Scan(ctx, cursor, match, 10).Result()
					if err != nil {
						s.logger.Error().Err(err).Msg("KstarAlarm::Run() - failed to scan redis keys")
						return err
					}

					keys = append(keys, scanKeys...)
					if cursor == 0 {
						break
					}
				}

				for _, key := range keys {
					val, err := s.rdb.Get(ctx, key).Result()
					if err != nil {
						s.logger.Error().Err(err).Msg("KstarAlarm::Run() - failed to get redis key")
						if err != redis.Nil {
							return err
						}
						continue
					}

					if !util.IsEmpty(val) {
						splitKey := strings.Split(key, ",")
						splitVal := strings.Split(val, ",")

						plantName := splitVal[0]
						alarmName := strings.ReplaceAll(splitKey[4], " ", "-")
						payload := fmt.Sprintf("Kstar,%s,%s,%s", plantID, deviceID, deviceName)
						document = model.NewSnmpAlarmItem(s.vendorType, plantName, alarmName, payload, infra.ClearSeverity, splitVal[1])
						s.snmp.SendTrap(plantName, alarmName, payload, infra.ClearSeverity, splitVal[1])

						if err := s.rdb.Del(ctx, key).Err(); err != nil {
							s.logger.Error().Err(err).Msg("KstarAlarm::Run() - failed to delete redis key")
							return err
						}
					}
				}
			case 2:
				realtimeAlarmResp, err := client.GetRealtimeAlarmListOfDevice(deviceID)
				if err != nil {
					s.logger.Error().Err(err).Msg("KstarAlarm::Run() - failed to get realtime alarm list of device")
					return err
				}

				if len(realtimeAlarmResp.Data) > 0 {
					for _, alarm := range realtimeAlarmResp.Data {
						alarmTime := pointy.StringValue(alarm.SaveTime, "")
						alarmMessage := strings.ReplaceAll(pointy.StringValue(alarm.Message, ""), " ", "-")

						key := fmt.Sprintf("Kstar,%s,%s,%s,%s", plantID, deviceID, deviceName, alarmMessage)
						val := fmt.Sprintf("%s,%s", plantName, alarmTime)
						if err := s.rdb.Set(ctx, key, val, 0).Err(); err != nil {
							s.logger.Error().Err(err).Msg("KstarAlarm::Run() - failed to set redis key")
							return err
						}

						payload := fmt.Sprintf("Kstar,%s,%s,%s", plantID, deviceID, deviceName)
						document = model.NewSnmpAlarmItem(s.vendorType, plantName, alarmMessage, payload, infra.MajorSeverity, alarmTime)
						s.snmp.SendTrap(plantName, alarmMessage, payload, infra.MajorSeverity, alarmTime)
					}
				}
			default:
			}
			documents = append(documents, document)
		}
	}

	index := fmt.Sprintf("%s-%s", model.AlarmIndex, time.Now().Format("2006.01.02"))
	if err := s.solarRepo.BulkIndex(index, documents); err != nil {
		s.logger.Error().Err(err).Msg("KstarAlarm::Run() - failed to bulk index")
		return err
	}
	s.logger.Info().Str("index", index).Int("document_count", len(documents)).Any("documents", documents).Msg("KstarAlarm::Run() - success")

	return nil

}

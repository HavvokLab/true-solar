package alarm

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/HavvokLab/true-solar/api/huawei"
	"github.com/HavvokLab/true-solar/infra"
	"github.com/HavvokLab/true-solar/model"
	"github.com/HavvokLab/true-solar/pkg/logger"
	"github.com/HavvokLab/true-solar/pkg/util"
	"github.com/HavvokLab/true-solar/repo"
	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog"
	"go.openly.dev/pointy"
)

type HuaweiAlarm struct {
	vendorType string
	solarRepo  repo.SolarRepo
	snmp       *infra.SnmpOrchestrator
	rdb        *redis.Client
	logger     zerolog.Logger
}

func NewHuaweiAlarm(solarRepo repo.SolarRepo, snmp *infra.SnmpOrchestrator, rdb *redis.Client) *HuaweiAlarm {
	return &HuaweiAlarm{
		vendorType: strings.ToUpper(model.VendorTypeHuawei),
		solarRepo:  solarRepo,
		snmp:       snmp,
		rdb:        rdb,
		logger:     zerolog.New(logger.NewWriter("huawei_alarm.log")).With().Timestamp().Caller().Logger(),
	}
}

func (s *HuaweiAlarm) Run(credential *model.HuaweiCredential) error {
	s.logger.Info().Str("username", credential.Username).Msg("HuaweiAlarm::Run() - start alarm")

	now := time.Now().UTC()
	ctx := context.Background()
	beginTime := time.Date(now.Year(), now.Month(), now.Day(), 6, 0, 0, 0, time.Local).UnixNano() / 1e6
	endTime := now.UnixNano() / 1e6
	documents := make([]interface{}, 0)

	client, err := huawei.NewHuaweiClient(credential.Username, credential.Password, huawei.WithRetryCount(0))
	if err != nil {
		s.logger.Error().Err(err).Msg("HuaweiAlarm::Run() - failed to create huawei client")
		return err
	}

	plantListResp, err := client.GetPlantList()
	if err != nil {
		s.logger.Error().Err(err).Msg("HuaweiAlarm::Run() - failed to get plant list")
		return err
	}

	var stationCodeList []string
	var stationCodeListString []string
	s.logger.Info().Int("plant_count", len(plantListResp.Data)).Msg("HuaweiAlarm::Run() - get plant list success")
	for _, plant := range plantListResp.Data {
		if len(stationCodeList) == 100 {
			stationCodeListString = append(stationCodeListString, strings.Join(stationCodeList, ","))
			stationCodeList = []string{}
		}

		if plant.Code != nil {
			stationCodeList = append(stationCodeList, *plant.Code)
		}
	}
	stationCodeListString = append(stationCodeListString, strings.Join(stationCodeList, ","))

	var inverterList []huawei.Device
	mapPlantCodeToDevice := make(map[string][]huawei.Device)
	mapDeviceSNToAlarm := make(map[string][]huawei.DeviceAlarm)
	mapInverterIDToRealtimeData := make(map[int]huawei.RealtimeDeviceData)
	s.logger.Info().Int("station_code_count", len(stationCodeListString)).Msg("HuaweiAlarm::Run() - get station code list success")
	for _, stationCode := range stationCodeListString {
		deviceListResp, err := client.GetDeviceList(stationCode)
		if err != nil {
			s.logger.Error().Err(err).Msg("HuaweiAlarm::Run() - failed to get device list")
			return err
		}
		s.logger.Info().Int("device_count", len(deviceListResp.Data)).Msg("HuaweiAlarm::Run() - get device list success")

		for _, device := range deviceListResp.Data {
			if device.PlantCode != nil {
				mapPlantCodeToDevice[*device.PlantCode] = append(mapPlantCodeToDevice[*device.PlantCode], device)
			}

			if device.TypeID != nil && *device.TypeID == 1 {
				inverterList = append(inverterList, device)
			}
		}

		deviceAlarmListResp, err := client.GetDeviceAlarm(stationCode, beginTime, endTime)
		if err != nil {
			s.logger.Error().Err(err).Msg("HuaweiAlarm::Run() - failed to get device alarm list")
			return err
		}
		s.logger.Info().Int("device_alarm_count", len(deviceAlarmListResp.Data)).Msg("HuaweiAlarm::Run() - get device alarm list success")

		for _, alarm := range deviceAlarmListResp.Data {
			doubleAlarm := false
			var deviceSn string

			if alarm.DeviceSN != nil {
				deviceSn = *alarm.DeviceSN
				for i, deviceAlarm := range mapDeviceSNToAlarm[deviceSn] {
					if deviceAlarm.AlarmName == alarm.AlarmName {
						doubleAlarm = true

						if deviceAlarm.RaiseTime != nil && alarm.RaiseTime != nil && *deviceAlarm.RaiseTime < *alarm.RaiseTime {
							mapDeviceSNToAlarm[deviceSn][i] = alarm
							break
						}
					}
				}

				if !doubleAlarm {
					mapDeviceSNToAlarm[deviceSn] = append(mapDeviceSNToAlarm[deviceSn], alarm)
				}
			}
		}
	}

	var inverterIDList []string
	var inverterIDListString []string
	s.logger.Info().Int("inverter_count", len(inverterList)).Msg("HuaweiAlarm::Run() - get inverter list success")
	for _, device := range inverterList {
		if len(inverterIDList) == 100 {
			inverterIDListString = append(inverterIDListString, strings.Join(inverterIDList, ","))
			inverterIDList = []string{}
		}

		if device.ID != nil {
			inverterIDList = append(inverterIDList, strconv.Itoa(*device.ID))
		}
	}
	inverterIDListString = append(inverterIDListString, strings.Join(inverterIDList, ","))

	s.logger.Info().Int("inverter_id_count", len(inverterIDListString)).Msg("HuaweiAlarm::Run() - get inverter id list success")
	for _, inverterID := range inverterIDListString {
		realtimeDeviceResp, err := client.GetRealtimeDeviceData(inverterID, "1")
		if err != nil {
			s.logger.Error().Err(err).Msg("HuaweiAlarm::Run() - failed to get realtime device data")
			return err
		}
		s.logger.Info().Int("realtime_device_count", len(realtimeDeviceResp.Data)).Msg("HuaweiAlarm::Run() - get realtime device data success")

		for _, realtimeDevice := range realtimeDeviceResp.Data {
			if realtimeDevice.ID != nil {
				mapInverterIDToRealtimeData[*realtimeDevice.ID] = realtimeDevice
			}
		}
	}

	for _, plant := range plantListResp.Data {
		plantCode := pointy.StringValue(plant.Code, "")
		plantName := pointy.StringValue(plant.Name, "")

		for _, device := range mapPlantCodeToDevice[plantCode] {
			deviceID := pointy.IntValue(device.ID, 0)
			deviceSN := pointy.StringValue(device.SN, "")
			deviceName := pointy.StringValue(device.Name, "")
			deviceTypeID := pointy.IntValue(device.TypeID, 0)

			if deviceTypeID == 1 {
				realtimeDevice := mapInverterIDToRealtimeData[deviceID].DataItemMap
				if realtimeDevice == nil {
					s.logger.Warn().Any("device_id", device.ID).Msg("realtimeDevice is nil")
					continue
				}

				realtimeDeviceStatus := pointy.IntValue(realtimeDevice.Status, 10)
				if realtimeDeviceStatus == 0 {
					shutdownTime := strconv.Itoa(int(endTime))
					if mapInverterIDToRealtimeData[deviceID].DataItemMap != nil {

						if mapInverterIDToRealtimeData[deviceID].DataItemMap.InverterShutdown != nil {
							inverterShutdown, ok := (*mapInverterIDToRealtimeData[deviceID].DataItemMap.InverterShutdown).(float64)
							if ok {
								shutdownTime = strconv.Itoa(int(inverterShutdown))
							}
						}
					}

					key := fmt.Sprintf("Huawei,%s,%s,%s,%s", plantCode, deviceSN, deviceName, "Disconnect")
					val := fmt.Sprintf("%s,%s,%s", plantName, "Disconnect", shutdownTime)
					err := s.rdb.Set(ctx, key, val, 0).Err()
					if err != nil {
						s.logger.Error().Err(err).Msg("HuaweiAlarm::Run() - failed to set redis")
						return err
					}

					alarmName := fmt.Sprintf("HUW-%s", "Disconnect")
					payload := fmt.Sprintf("Huawei,%s,%s", deviceName, "Disconnect")
					document := model.NewSnmpAlarmItem(s.vendorType, plantName, alarmName, payload, infra.MajorSeverity, shutdownTime)
					s.snmp.SendTrap(plantName, alarmName, payload, infra.MajorSeverity, shutdownTime)
					documents = append(documents, document)
					continue
				}
			}

			if len(mapDeviceSNToAlarm[deviceSN]) > 0 {
				for _, alarm := range mapDeviceSNToAlarm[deviceSN] {
					alarmName := pointy.StringValue(alarm.AlarmName, "")
					alarmCause := pointy.StringValue(alarm.AlarmCause, "")
					alarmTime := strconv.Itoa(int(pointy.Int64Value(alarm.RaiseTime, 0)))

					key := fmt.Sprintf("Huawei,%s,%s,%s,%s", plantCode, deviceSN, deviceName, alarmName)
					val := fmt.Sprintf("%s,%s,%s", plantName, alarmCause, alarmTime)
					err := s.rdb.Set(ctx, key, val, 0).Err()
					if err != nil {
						s.logger.Error().Err(err).Msg("HuaweiAlarm::Run() - failed to set redis")
						return err
					}

					alarmName = strings.ReplaceAll(fmt.Sprintf("HUW-%s", alarmName), " ", "-")
					payload := fmt.Sprintf("Huawei,%s,%s", deviceName, alarmCause)

					document := model.NewSnmpAlarmItem(s.vendorType, plantName, alarmName, payload, infra.MajorSeverity, alarmTime)
					s.snmp.SendTrap(plantName, alarmName, payload, infra.MajorSeverity, alarmTime)
					documents = append(documents, document)
				}

				continue
			}

			var keys []string
			var cursor uint64
			for {
				var scanKeys []string
				match := fmt.Sprintf("Huawei,%s,%s,%s,*", plantCode, deviceSN, deviceName)
				scanKeys, cursor, err = s.rdb.Scan(ctx, cursor, match, 10).Result()
				if err != nil {
					s.logger.Error().Err(err).Msg("HuaweiAlarm::Run() - failed to scan redis")
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
					if err != redis.Nil {
						s.logger.Error().Err(err).Msg("HuaweiAlarm::Run() - failed to get redis")
						return err
					}
					continue
				}

				if !util.IsEmpty(val) {
					splitKey := strings.Split(key, ",")
					splitVal := strings.Split(val, ",")

					alarmName := strings.ReplaceAll(fmt.Sprintf("HUW-%s", splitKey[4]), " ", "-")
					payload := fmt.Sprintf("Huawei,%s,%s", deviceName, splitVal[1])
					document := model.NewSnmpAlarmItem(s.vendorType, plantName, alarmName, payload, infra.ClearSeverity, splitVal[2])
					s.snmp.SendTrap(
						splitVal[0],
						alarmName,
						payload,
						infra.ClearSeverity,
						splitVal[2],
					)
					documents = append(documents, document)

					if err := s.rdb.Del(ctx, key).Err(); err != nil {
						s.logger.Error().Err(err).Msg("HuaweiAlarm::Run() - failed to delete redis")
						return err
					}
				}
			}
		}
	}

	index := fmt.Sprintf("%s-%s", model.AlarmIndex, now.Format("2006.01.02"))
	if err := s.solarRepo.BulkIndex(index, documents); err != nil {
		s.logger.Error().Err(err).Msg("HuaweiAlarm::Run() - failed to bulk index")
		return err
	}

	s.logger.Info().Str("index", index).Any("documents", documents).Int("document_count", len(documents)).Msg("HuaweiAlarm::Run() - success")
	return nil
}

package alarm

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/HavvokLab/true-solar/api/growatt"
	"github.com/HavvokLab/true-solar/infra"
	"github.com/HavvokLab/true-solar/model"
	"github.com/HavvokLab/true-solar/pkg/logger"
	"github.com/HavvokLab/true-solar/pkg/util"
	"github.com/HavvokLab/true-solar/repo"
	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog"
	"go.openly.dev/pointy"
)

type GrowattAlarm struct {
	vendorType string
	solarRepo  repo.SolarRepo
	snmp       *infra.SnmpOrchestrator
	rdb        *redis.Client
	logger     zerolog.Logger
}

func NewGrowattAlarm(solarRepo repo.SolarRepo, snmp *infra.SnmpOrchestrator, rdb *redis.Client) *GrowattAlarm {
	return &GrowattAlarm{
		vendorType: strings.ToUpper(model.VendorTypeGrowatt),
		solarRepo:  solarRepo,
		snmp:       snmp,
		rdb:        rdb,
		logger:     zerolog.New(logger.NewWriter("growatt_alarm.log")).With().Timestamp().Caller().Logger(),
	}
}

func (s *GrowattAlarm) Run(credential *model.GrowattCredential) error {
	now := time.Now()
	documents := make([]interface{}, 0)
	ctx := context.Background()
	client := growatt.NewGrowattClient(credential.Username, credential.Token)
	plants, err := client.GetPlantList()
	if err != nil {
		s.logger.Error().Err(err).Msg("GrowattAlarm::Run() - failed to get plant list")
		return err
	}

	for _, plant := range plants {
		plantID := pointy.IntValue(plant.PlantID, 0)
		plantName := pointy.StringValue(plant.Name, "")
		s.logger.Info().Str("username", credential.Username).Int("plant_id", plantID).Str("plant_name", plantName).Msg("GrowattAlarm::Run() - retrieve alarm")

		devices, err := client.GetPlantDeviceList(plantID)
		if err != nil {
			s.logger.Error().Err(err).Msg("GrowattAlarm::Run() - failed to get plant device list")
			continue
		}

		s.logger.Info().Str("username", credential.Username).Int("device_count", len(devices)).Msg("GrowattAlarm::Run() - retrieve alarm")
		for _, device := range devices {
			deviceSN := pointy.StringValue(device.DeviceSN, "")
			deviceModel := pointy.StringValue(device.Model, "")
			deviceLastUpdateTime := pointy.StringValue(device.LastUpdateTime, "")
			status := pointy.IntValue(device.Status, 0)
			dtype := pointy.IntValue(device.Type, 0)
			deviceStatus := growatt.GrowattInverterStatusMapper[status]
			deviceType := growatt.GrowattEquipmentTypeMapper[dtype]
			deviceName := fmt.Sprintf("%s_%d_%s", plantName, plantID, deviceSN)
			var document interface{}

			s.logger.Info().Str("username", credential.Username).Str("device_sn", deviceSN).Str("device_model", deviceModel).Str("device_status", deviceStatus).Str("device_type", deviceType).Str("device_name", deviceName).Msg("GrowattAlarm::Run() - retrieve alarm")
			switch deviceStatus {
			case "Online":
				key := fmt.Sprintf("%d,%s,%s,%s", plantID, plantName, deviceType, deviceSN)
				val, err := s.rdb.Get(ctx, key).Result()
				if err != nil {
					s.logger.Error().Err(err).Msg("GrowattAlarm::Run() - failed to get redis key")
					continue
				}

				if !util.IsEmpty(val) {
					vals := strings.Split(val, ",")
					alarmName := fmt.Sprintf("Growatt,%s,%s", vals[1], deviceModel)
					payload := fmt.Sprintf("%s-Error-%s", alarmName, vals[0])
					severity := infra.ClearSeverity
					document = model.NewSnmpAlarmItem(s.vendorType, deviceName, payload, alarmName, severity, deviceLastUpdateTime)
					s.snmp.SendTrap(deviceName, alarmName, payload, severity, deviceLastUpdateTime)
				}

				if err := s.rdb.Del(ctx, key).Err(); err != nil {
					s.logger.Error().Err(err).Msg("GrowattAlarm::Run() - failed to delete redis key")
					continue
				}

			case "Disconnect":
				rkey := fmt.Sprintf("%d,%s,%s,%s", plantID, plantName, deviceType, deviceSN)
				val := "0,Disconnect"
				if err := s.rdb.Set(ctx, rkey, val, 0).Err(); err != nil {
					s.logger.Error().Err(err).Msg("GrowattAlarm::Run() - failed to set redis key")
					continue
				}

				alarmName := fmt.Sprintf("Growatt,Disconnect,%s", deviceModel)
				payload := fmt.Sprintf("%s-Error-0", deviceType)
				severity := "4"
				document = model.NewSnmpAlarmItem(s.vendorType, deviceName, payload, alarmName, severity, deviceLastUpdateTime)
				s.snmp.SendTrap(deviceName, alarmName, payload, severity, deviceLastUpdateTime)
			default:
				date := now.AddDate(0, 0, -1).Format("2006-01-02")
				alarms, err := client.GetInverterAlertList(deviceSN)
				if err != nil {
					s.logger.Error().Err(err).Msg("GrowattAlarm::Run() - failed to get inverter alert list")
					continue
				}

				if len(alarms) > 0 {
					alarm := alarms[0]
					rkey := fmt.Sprintf("%d,%s,%s,%s", plantID, plantName, deviceType, deviceSN)
					val := fmt.Sprintf("%d,%s", pointy.IntValue(alarm.AlarmCode, 0), pointy.StringValue(alarm.AlarmMessage, ""))
					if err := s.rdb.Set(ctx, rkey, val, 0).Err(); err != nil {
						s.logger.Error().Err(err).Msg("GrowattAlarm::Run() - failed to set redis key")
						continue
					}

					alarmName := fmt.Sprintf("Growatt,%s,%s", pointy.StringValue(alarm.AlarmMessage, ""), deviceModel)
					payload := fmt.Sprintf("%s-Error-%d", deviceType, pointy.IntValue(alarm.AlarmCode, 0))
					severity := infra.MajorSeverity
					document = model.NewSnmpAlarmItem(s.vendorType, deviceName, payload, alarmName, severity, deviceLastUpdateTime)
					s.snmp.SendTrap(deviceName, alarmName, payload, severity, date)
				}
			}

			documents = append(documents, document)
		}

		time.Sleep(10 * time.Second)
	}

	index := fmt.Sprintf("%s-%s", model.AlarmIndex, now.Format("2006.01.02"))
	if err := s.solarRepo.BulkIndex(index, documents); err != nil {
		s.logger.Error().Err(err).Msg("GrowattAlarm::Run() - failed to bulk index")
		return err
	}
	s.logger.Info().Str("index", index).Int("document_count", len(documents)).Any("documents", documents).Msg("GrowattAlarm::Run() - success")

	return nil

}

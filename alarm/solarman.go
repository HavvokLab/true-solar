package alarm

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/HavvokLab/true-solar/api/solarman"
	"github.com/HavvokLab/true-solar/infra"
	"github.com/HavvokLab/true-solar/model"
	"github.com/HavvokLab/true-solar/pkg/logger"
	"github.com/HavvokLab/true-solar/pkg/util"
	"github.com/HavvokLab/true-solar/repo"
	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog"
	"go.openly.dev/pointy"
)

type SolarmanAlarm struct {
	vendorType string
	solarRepo  repo.SolarRepo
	snmp       *infra.SnmpOrchestrator
	rdb        *redis.Client
	logger     zerolog.Logger
}

func NewSolarmanAlarm(solarRepo repo.SolarRepo, snmp *infra.SnmpOrchestrator, rdb *redis.Client) *SolarmanAlarm {
	return &SolarmanAlarm{
		vendorType: "INVT-Ipanda",
		solarRepo:  solarRepo,
		snmp:       snmp,
		rdb:        rdb,
		logger:     zerolog.New(logger.NewWriter("solarman_alarm.log")).With().Timestamp().Caller().Logger(),
	}
}

func (s *SolarmanAlarm) Run(credential *model.SolarmanCredential) error {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Warn().Str("username", credential.Username).Any("error", r).Msg("SolarmanAlarm::Run() - failed to run")
		}
	}()

	ctx := context.Background()
	now := time.Now().UTC()
	beginningOfDay := time.Date(now.Year(), now.Month(), now.Day(), 6, 0, 0, 0, time.Local)
	documents := make([]interface{}, 0)

	if credential == nil {
		s.logger.Error().Msg("credential should not be empty")
		return errors.New("credential should not be empty")
	}

	client := solarman.NewSolarmanClient(credential.Username, credential.Password, credential.AppID, credential.AppSecret)
	basicTokenResp, err := client.GetBasicToken()
	if err != nil {
		s.logger.Error().Err(err).Msg("SolarmanAlarm::Run() - failed to get basic token")
		return err
	}

	if basicTokenResp.AccessToken == nil {
		s.logger.Error().Msg("basicToken should not be empty")
		return errors.New("basicToken should not be empty")
	}
	client.SetAccessToken(*basicTokenResp.AccessToken)

	userInfoResp, err := client.GetUserInfo()
	if err != nil {
		s.logger.Error().Err(err).Msg("SolarmanAlarm::Run() - failed to get user info")
		return err
	}

	if userInfoResp.OrgInfoList == nil {
		s.logger.Error().Msg("organization should not be empty")
		return errors.New("organization should not be empty")
	}

	companyCount := 1
	companyTotal := len(userInfoResp.OrgInfoList)
	for _, company := range userInfoResp.OrgInfoList {
		s.logger.Info().Str("username", credential.Username).Int("company_count", companyCount).Int("company_total", companyTotal).Msg("SolarmanAlarm::Run() - company info")
		companyCount++

		companyId := pointy.IntValue(company.CompanyID, 0)
		tokenResp, err := client.GetBusinessToken(companyId)
		if err != nil {
			s.logger.Error().Err(err).Msg("SolarmanAlarm::Run() - failed to get business token")
			return err
		}

		if tokenResp.AccessToken == nil {
			s.logger.Error().Msg("accesstoken should not be empty")
			return errors.New("accesstoken should not be empty")
		}
		client.SetAccessToken(*tokenResp.AccessToken)

		plantList, err := client.GetPlantList()
		if err != nil {
			s.logger.Error().Err(err).Msg("SolarmanAlarm::Run() - failed to get plant list")
			return err
		}

		plantCount := 1
		plantTotal := len(plantList)
		for _, plant := range plantList {
			s.logger.Info().Str("username", credential.Username).Int("plant_count", plantCount).Int("plant_total", plantTotal).Msg("SolarmanAlarm::Run() - plant info")
			plantCount++

			stationID := pointy.IntValue(plant.ID, 0)
			stationName := pointy.StringValue(plant.Name, "")

			deviceList, err := client.GetPlantDeviceList(stationID)
			if err != nil {
				s.logger.Error().Err(err).Msg("SolarmanAlarm::Run() - failed to get plant device list")
				return err
			}

			deviceCount := 1
			deviceTotal := len(deviceList)
			for _, device := range deviceList {
				s.logger.Info().Str("username", credential.Username).Int("device_count", deviceCount).Int("device_total", deviceTotal).Msg("SolarmanAlarm::Run() - device info")
				deviceCount++

				deviceID := pointy.IntValue(device.DeviceID, 0)
				deviceSN := pointy.StringValue(device.DeviceSN, "")
				deviceType := pointy.StringValue(device.DeviceType, "")
				deviceCollectionTime := pointy.Int64Value(device.CollectionTime, 0)
				deviceCollectionTimeStr := strconv.FormatInt(deviceCollectionTime, 10)

				if device.ConnectStatus != nil {
					var document interface{}
					connectStatus := pointy.IntValue(device.ConnectStatus, -1)
					switch connectStatus {
					case 0:
						rkey := fmt.Sprintf("%s,%d,%s,%s,%d,%s", s.vendorType, stationID, deviceType, deviceSN, deviceID, "Disconnect")
						val := fmt.Sprintf("%s,%s", stationName, deviceCollectionTimeStr)

						err := s.rdb.Set(ctx, rkey, val, 0).Err()
						if err != nil {
							s.logger.Error().Err(err).Msg("SolarmanAlarm::Run() - failed to set redis")
							return err
						}

						name := fmt.Sprintf("%s-%s", stationName, deviceSN)
						alert := strings.ReplaceAll(fmt.Sprintf("%s-%s", deviceType, "Disconnect"), " ", "-")
						description := fmt.Sprintf("%s,%d,%s,%d", s.vendorType, stationID, deviceSN, deviceID)
						document = model.NewSnmpAlarmItem(s.vendorType, name, alert, description, infra.MajorSeverity, deviceCollectionTimeStr)
						s.snmp.SendTrap(name, alert, description, infra.MajorSeverity, deviceCollectionTimeStr)
					case 1:
						var keys []string
						var cursor uint64

						for {
							var scanKeys []string
							match := fmt.Sprintf("%s,%d,%s,%s,%d,*", s.vendorType, stationID, deviceType, deviceSN, deviceID)
							scanKeys, cursor, err = s.rdb.Scan(ctx, cursor, match, 10).Result()
							if err != nil {
								s.logger.Error().Err(err).Msg("SolarmanAlarm::Run() - failed to scan redis")
								return err
							}

							keys = append(keys, scanKeys...)
							if cursor == 0 {
								break
							}
						}

						for _, key := range keys {
							val, err := s.rdb.Get(ctx, key).Result()
							if err == redis.Nil {
								s.logger.Warn().Msg("SolarmanAlarm::Run() - redis key not found")
								continue
							}

							if err != nil {
								s.logger.Error().Err(err).Msg("SolarmanAlarm::Run() - failed to get redis key")
								return err
							}

							if !util.IsEmpty(val) {
								splitKey := strings.Split(key, ",")
								splitVal := strings.Split(val, ",")

								name := fmt.Sprintf("%s-%s", stationName, deviceSN)
								alert := strings.ReplaceAll(fmt.Sprintf("%s-%s", deviceType, splitKey[5]), " ", "-")
								description := fmt.Sprintf("%s,%d,%s,%d", s.vendorType, stationID, deviceSN, deviceID)
								document = model.NewSnmpAlarmItem(s.vendorType, name, alert, description, infra.ClearSeverity, splitVal[1])
								s.snmp.SendTrap(name, alert, description, infra.ClearSeverity, splitVal[1])
							}

							if err := s.rdb.Del(ctx, key).Err(); err != nil {
								s.logger.Error().Err(err).Msg("SolarmanAlarm::Run() - failed to delete redis key")
								return err
							}
						}
					case 2:
						alertList, err := client.GetDeviceAlertList(deviceSN, beginningOfDay.Unix(), now.Unix())
						if err != nil {
							s.logger.Error().Err(err).Msg("SolarmanAlarm::Run() - failed to get device alert list")
							return err
						}

						for _, alert := range alertList {
							alertName := pointy.StringValue(alert.AlertNameInPAAS, "")
							alertTime := pointy.Int64Value(alert.AlertTime, 0)
							alertTimeStr := strconv.FormatInt(alertTime, 10)

							if alert.AlertNameInPAAS != nil && alert.AlertTime != nil {
								rkey := fmt.Sprintf("%s,%d,%s,%s,%d,%s", s.vendorType, stationID, deviceType, deviceSN, deviceID, alertName)
								val := fmt.Sprintf("%s,%s", stationName, alertTimeStr)

								err := s.rdb.Set(ctx, rkey, val, 0).Err()
								if err != nil {
									s.logger.Error().Err(err).Msg("SolarmanAlarm::Run() - failed to set redis")
									return err
								}

								name := fmt.Sprintf("%s-%s", stationName, deviceSN)
								alert := strings.ReplaceAll(fmt.Sprintf("%s-%s", deviceType, alertName), " ", "-")
								description := fmt.Sprintf("%s,%d,%s,%d", s.vendorType, stationID, deviceSN, deviceID)
								document = model.NewSnmpAlarmItem(s.vendorType, name, alert, description, infra.MajorSeverity, alertTimeStr)
								s.snmp.SendTrap(name, alert, description, infra.MajorSeverity, alertTimeStr)
							}
						}
					default:
					}
					documents = append(documents, document)
				}
			}
		}
	}

	index := fmt.Sprintf("%s-%s", model.AlarmIndex, time.Now().Format("2006.01.02"))
	if err := s.solarRepo.BulkIndex(index, documents); err != nil {
		s.logger.Error().Err(err).Msg("SolarmanAlarm::Run() - failed to bulk index")
		return err
	}
	s.logger.Info().Int("document_count", len(documents)).Any("documents", documents).Msg("SolarmanAlarm::Run() - saved alarms")

	return nil

}

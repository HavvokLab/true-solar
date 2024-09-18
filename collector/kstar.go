package collector

import (
	"fmt"
	"strings"
	"time"

	"github.com/HavvokLab/true-solar/api/kstar"
	"github.com/HavvokLab/true-solar/model"
	"github.com/HavvokLab/true-solar/pkg/logger"
	"github.com/HavvokLab/true-solar/pkg/util"
	"github.com/HavvokLab/true-solar/repo"
	"github.com/rs/zerolog"
	"go.openly.dev/pointy"
)

type KstarCollector struct {
	vendorType     string
	solarRepo      repo.SolarRepo
	siteRegionRepo repo.SiteRegionMappingRepo
	siteRegions    []model.SiteRegionMapping
	logger         zerolog.Logger
}

func NewKstarCollector(
	solarRepo repo.SolarRepo,
	siteRegionRepo repo.SiteRegionMappingRepo,
) *KstarCollector {
	return &KstarCollector{
		vendorType:     strings.ToUpper(model.VendorTypeKstar),
		solarRepo:      solarRepo,
		siteRegionRepo: siteRegionRepo,
		siteRegions:    make([]model.SiteRegionMapping, 0),
		logger:         zerolog.New(logger.NewWriter("kstar_collector.log")).With().Timestamp().Caller().Logger(),
	}
}

func (k *KstarCollector) Execute(credential *model.KstarCredential) {
	siteRegions, err := k.siteRegionRepo.GetSiteRegionMappings()
	if err != nil {
		k.logger.Error().Err(err).Msg("KstarCollector::Execute() - failed to get site region mappings")
		return
	}
	k.siteRegions = siteRegions
	now := time.Now().UTC()
	documents := make([]any, 0)
	siteDocuments := make([]model.SiteItem, 0)
	docCh := make(chan any)
	errCh := make(chan error)
	doneCh := make(chan bool)

	defer func() {
		if r := recover(); r != nil {
			k.logger.Error().Any("recover", r).Msg("KstarCollector::Execute() - panic")
		}
	}()

	go k.Collect(credential, now, docCh, errCh, doneCh)

COLLECT:
	for {
		select {
		case <-doneCh:
			k.logger.Info().Msg("KstarCollector::Execute() - done")
			break COLLECT
		case err := <-errCh:
			k.logger.Error().Err(err).Msg("KstarCollector::Execute() - failed")
			break COLLECT
		case doc := <-docCh:
			documents = append(documents, doc)
			if plantItemDoc, ok := doc.(model.PlantItem); ok {
				siteItemDoc := model.SiteItem{
					Timestamp:   plantItemDoc.Timestamp,
					VendorType:  plantItemDoc.VendorType,
					Area:        plantItemDoc.Area,
					SiteID:      plantItemDoc.SiteID,
					NodeType:    plantItemDoc.NodeType,
					Name:        plantItemDoc.Name,
					Location:    plantItemDoc.Location,
					PlantStatus: plantItemDoc.PlantStatus,
					Owner:       credential.Owner,
				}
				siteDocuments = append(siteDocuments, siteItemDoc)
			}
		}
	}

	index := fmt.Sprintf("%v-%v", model.SolarIndex, time.Now().Format("2006.01.02"))
	if err := k.solarRepo.BulkIndex(index, documents); err != nil {
		k.logger.Error().Err(err).Msg("KstarCollector::Execute() - failed to bulk index documents")
	} else {
		k.logger.Info().Int("count", len(documents)).Msg("KstarCollector::Execute() - bulk index documents success")
	}

	if err := k.solarRepo.UpsertSiteStation(siteDocuments); err != nil {
		k.logger.Error().Err(err).Msg("KstarCollector::Execute() - failed to upsert site station")
	} else {
		k.logger.Info().Int("count", len(siteDocuments)).Msg("KstarCollector::Execute() - upsert site station success")
	}

	close(doneCh)
	close(errCh)
	close(docCh)

}

func (k *KstarCollector) Collect(
	credential *model.KstarCredential,
	now time.Time,
	docCh chan any,
	errCh chan error,
	doneCh chan bool,
) {
	client := kstar.NewKstarClient(credential.Username, credential.Password)

	mapPlantIdToDeviceList := make(map[string][]kstar.DeviceItem)
	devices, err := client.GetDeviceList()
	if err != nil {
		k.logger.Error().
			Err(err).
			Str("username", credential.Username).
			Str("password", credential.Password).
			Msg("KstarCollector::Collect() - failed to get device list")
		errCh <- err
		return
	}

	if len(devices) == 0 {
		k.logger.Error().
			Str("username", credential.Username).
			Str("password", credential.Password).
			Int("device_count", len(devices)).
			Msg("KstarCollector::Collect() - no devices found")
		errCh <- fmt.Errorf("no devices found")
		return
	}

	for _, device := range devices {
		plantId := pointy.StringValue(device.PlantID, "")
		if !util.IsEmpty(plantId) {
			mapPlantIdToDeviceList[plantId] = append(mapPlantIdToDeviceList[plantId], device)
		}
	}

	plantListResp, err := client.GetPlantList()
	if err != nil {
		k.logger.Error().
			Err(err).
			Str("username", credential.Username).
			Str("password", credential.Password).
			Msg("KstarCollector::Collect() - failed to get plant list")
		errCh <- err
		return
	}

	if len(plantListResp.Data) == 0 {
		k.logger.Error().
			Str("username", credential.Username).
			Str("password", credential.Password).
			Int("plant_count", len(plantListResp.Data)).
			Msg("KstarCollector::Collect() - no plants found")
		errCh <- fmt.Errorf("no plants found")
		return
	}

	plantSize := len(plantListResp.Data)
	for i, plant := range plantListResp.Data {
		currentPlant := i + 1
		plantId := pointy.StringValue(plant.ID, "")
		plantName := pointy.StringValue(plant.Name, "")
		plantNameInfo, _ := util.ParsePlantID(plantName)
		cityName, cityCode, cityArea := util.ParseSiteID(k.siteRegions, plantNameInfo.SiteID)

		var plantStatus string
		var currentPower, totalProduction, dailyProduction, monthlyProduction, yearlyProduction float64
		var location *string

		if plant.Latitude != nil && plant.Longitude != nil {
			tmp := fmt.Sprintf("%f,%f", *plant.Latitude, *plant.Longitude)
			location = &tmp
		}

		deviceSize := len(mapPlantIdToDeviceList[plantId])
		for j, device := range mapPlantIdToDeviceList[plantId] {
			currentDevice := j + 1

			deviceId := pointy.StringValue(device.ID, "")
			realtimeAlarmResp, err := client.GetRealtimeAlarmListOfDevice(deviceId)
			if err != nil {
				k.logger.Error().
					Err(err).
					Str("plant_count", fmt.Sprintf("%v/%v", currentPlant, plantSize)).
					Str("device_count", fmt.Sprintf("%v/%v", currentDevice, deviceSize)).
					Str("username", credential.Username).
					Str("password", credential.Password).
					Str("plant_id", plantId).
					Str("device_id", deviceId).
					Msg("KstarCollector::Collect() - failed to get realtime alarm list of device")
				errCh <- err
				return
			}

			var deviceStatus *int = device.Status
			if len(realtimeAlarmResp.Data) == 0 {
				k.logger.Warn().
					Str("plant_count", fmt.Sprintf("%v/%v", currentPlant, plantSize)).
					Str("device_count", fmt.Sprintf("%v/%v", currentDevice, deviceSize)).
					Str("username", credential.Username).
					Str("password", credential.Password).
					Str("plant_id", plantId).
					Str("device_id", deviceId).
					Msg("KstarCollector::Collect() - no alarms found")
				continue
			} else {
				deviceStatus = pointy.Int(2)
				alarmSize := len(realtimeAlarmResp.Data)
				for n, alarm := range realtimeAlarmResp.Data {
					currentAlarm := n + 1
					alarmItem := model.AlarmItem{
						Timestamp:    now,
						Month:        now.Format("01"),
						Year:         now.Format("2006"),
						MonthYear:    now.Format("01-2006"),
						VendorType:   k.vendorType,
						DataType:     model.DataTypeAlarm,
						Area:         cityArea,
						SiteID:       plantNameInfo.SiteID,
						SiteCityCode: cityCode,
						SiteCityName: cityName,
						NodeType:     plantNameInfo.NodeType,
						ACPhase:      plantNameInfo.ACPhase,
						PlantID:      alarm.PlantID,
						PlantName:    alarm.PlantName,
						Latitude:     plant.Latitude,
						Longitude:    plant.Longitude,
						Location:     location,
						DeviceID:     alarm.DeviceID,
						DeviceSN:     device.SN,
						DeviceName:   alarm.DeviceName,
						DeviceStatus: pointy.String(kstar.KstarDeviceStatusAlarm),
						ID:           nil,
						Message:      alarm.Message,
						Owner:        credential.Owner,
					}

					if alarm.SaveTime != nil {
						if alarmTime, err := time.Parse("2006-01-02 15:04:05", *alarm.SaveTime); err == nil {
							alarmItem.Timestamp = alarmTime
						}
					}

					docCh <- alarmItem
					k.logger.Info().
						Str("alarm_count", fmt.Sprintf("%v/%v", currentAlarm, alarmSize)).
						Str("plant_count", fmt.Sprintf("%v/%v", currentPlant, plantSize)).
						Str("device_count", fmt.Sprintf("%v/%v", currentDevice, deviceSize)).
						Str("username", credential.Username).
						Str("password", credential.Password).
						Str("plant_id", plantId).
						Str("device_id", deviceId).
						Any("alarm", alarmItem).
						Msg("KstarCollector::Collect() - alarm item added")
				}
			}

			deviceItem := model.DeviceItem{
				Timestamp:    now,
				Month:        now.Format("01"),
				Year:         now.Format("2006"),
				MonthYear:    now.Format("01-2006"),
				VendorType:   k.vendorType,
				DataType:     model.DataTypeDevice,
				Area:         cityArea,
				SiteID:       plantNameInfo.SiteID,
				SiteCityCode: cityCode,
				SiteCityName: cityName,
				NodeType:     plantNameInfo.NodeType,
				ACPhase:      plantNameInfo.ACPhase,
				PlantID:      device.PlantID,
				PlantName:    device.PlantName,
				Latitude:     plant.Latitude,
				Longitude:    plant.Longitude,
				Location:     location,
				ID:           device.ID,
				SN:           device.SN,
				Name:         device.Name,
				DeviceType:   pointy.String(kstar.KstarDeviceTypeInverter),
				Owner:        credential.Owner,
			}

			deviceInfoResp, err := client.GetRealtimeDeviceData(deviceId)
			if err != nil {
				k.logger.Error().
					Err(err).
					Str("plant_count", fmt.Sprintf("%v/%v", currentPlant, plantSize)).
					Str("device_count", fmt.Sprintf("%v/%v", currentDevice, deviceSize)).
					Str("username", credential.Username).
					Str("password", credential.Password).
					Str("plant_id", plantId).
					Str("device_id", deviceId).
					Msg("KstarCollector::Collect() - failed to get realtime device data")
				errCh <- err
				return
			}

			if deviceInfoResp == nil || deviceInfoResp.Data == nil {
				k.logger.Error().
					Str("plant_count", fmt.Sprintf("%v/%v", currentPlant, plantSize)).
					Str("device_count", fmt.Sprintf("%v/%v", currentDevice, deviceSize)).
					Str("username", credential.Username).
					Str("password", credential.Password).
					Str("plant_id", plantId).
					Str("device_id", deviceId).
					Msg("KstarCollector::Collect() - no device data found")
				errCh <- fmt.Errorf("no device data found")
				continue
			}

			if deviceInfoResp.Data.SaveTime != nil {
				if saveTime, err := time.Parse("2006-01-02 15:04:05", *deviceInfoResp.Data.SaveTime); err == nil {
					deviceItem.Timestamp = saveTime
					if now.Sub(saveTime).Hours() < 12 {
						currentPower += pointy.Float64Value(deviceInfoResp.Data.PowerInter, 0)
						totalProduction += pointy.Float64Value(deviceInfoResp.Data.TotalGeneration, 0)
						dailyProduction += pointy.Float64Value(deviceInfoResp.Data.DayGeneration, 0)
						monthlyProduction += pointy.Float64Value(deviceInfoResp.Data.MonthGeneration, 0)
						yearlyProduction += pointy.Float64Value(deviceInfoResp.Data.YearGeneration, 0)
					} else {
						k.logger.Warn().
							Str("plant_count", fmt.Sprintf("%v/%v", currentPlant, plantSize)).
							Str("device_count", fmt.Sprintf("%v/%v", currentDevice, deviceSize)).
							Str("username", credential.Username).
							Str("password", credential.Password).
							Str("plant_id", plantId).
							Str("device_id", deviceId).
							Str("save_time", pointy.StringValue(deviceInfoResp.Data.SaveTime, "-")).
							Msg("KstarCollector::Collect() - device data is outdated")
						continue
					}
				}
			}

			if deviceStatus != nil {
				switch *deviceStatus {
				case 0: // OFFLINE
					deviceItem.Status = pointy.String(kstar.KstarDeviceStatusOffline)
					if plantStatus != kstar.KstarDeviceStatusAlarm {
						plantStatus = kstar.KstarDeviceStatusOffline
					}
				case 1: // ONLINE
					deviceItem.Status = pointy.String(kstar.KstarDeviceStatusOnline)
					if plantStatus != kstar.KstarDeviceStatusOffline && plantStatus != kstar.KstarDeviceStatusAlarm {
						plantStatus = kstar.KstarDeviceStatusOnline
					}
				case 2: // ALARM
					deviceItem.Status = pointy.String(kstar.KstarDeviceStatusAlarm)
					plantStatus = kstar.KstarDeviceStatusAlarm
				}
			}

			docCh <- deviceItem
			k.logger.Info().
				Str("plant_count", fmt.Sprintf("%v/%v", currentPlant, plantSize)).
				Str("device_count", fmt.Sprintf("%v/%v", currentDevice, deviceSize)).
				Str("username", credential.Username).
				Str("password", credential.Password).
				Str("plant_id", plantId).
				Str("device_id", deviceId).
				Any("device", deviceItem).
				Msg("KstarCollector::Collect() - device item added")
		}

		plantItem := model.PlantItem{
			Timestamp:         now,
			Month:             now.Format("01"),
			Year:              now.Format("2006"),
			MonthYear:         now.Format("01-2006"),
			VendorType:        k.vendorType,
			DataType:          model.DataTypePlant,
			Area:              cityArea,
			SiteID:            plantNameInfo.SiteID,
			SiteCityCode:      cityCode,
			SiteCityName:      cityName,
			NodeType:          plantNameInfo.NodeType,
			ACPhase:           plantNameInfo.ACPhase,
			ID:                &plantId,
			Name:              &plantName,
			Latitude:          plant.Latitude,
			Longitude:         plant.Longitude,
			Location:          location,
			LocationAddress:   plant.Address,
			InstalledCapacity: plant.InstalledCapacity,
			TotalCO2:          nil,
			MonthlyCO2:        nil,
			TotalSavingPrice:  pointy.Float64(totalProduction * pointy.Float64Value(plant.ElectricPrice, 0)),
			Currency:          plant.ElectricUnit,
			CurrentPower:      pointy.Float64(currentPower / 1000), // W to kW
			TotalProduction:   pointy.Float64(totalProduction),
			DailyProduction:   pointy.Float64(dailyProduction),
			MonthlyProduction: pointy.Float64(monthlyProduction),
			YearlyProduction:  pointy.Float64(yearlyProduction),
			PlantStatus:       pointy.String(plantStatus),
			Owner:             credential.Owner,
		}

		if plant.CreatedTime != nil {
			if createdTime, err := time.Parse("2006-01-02 15:04:05", *plant.CreatedTime); err == nil {
				plantItem.CreatedDate = &createdTime
			}
		}

		docCh <- plantItem
		k.logger.Info().
			Str("plant_count", fmt.Sprintf("%v/%v", currentPlant, plantSize)).
			Str("username", credential.Username).
			Str("password", credential.Password).
			Str("plant_id", plantId).
			Any("plant", plantItem).
			Msg("KstarCollector::Collect() - plant item added")
	}

	doneCh <- true
}

package collector

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/HavvokLab/true-solar/api/huawei2"
	"github.com/HavvokLab/true-solar/model"
	"github.com/HavvokLab/true-solar/pkg/logger"
	"github.com/HavvokLab/true-solar/pkg/util"
	"github.com/HavvokLab/true-solar/repo"
	"github.com/rs/zerolog"
	"go.openly.dev/pointy"
)

type Huawei2Collector struct {
	vendorType     string
	solarRepo      repo.SolarRepo
	siteRegionRepo repo.SiteRegionMappingRepo
	siteRegions    []model.SiteRegionMapping
	logger         zerolog.Logger
}

func NewHuawei2Collector(
	solarRepo repo.SolarRepo,
	siteRegionRepo repo.SiteRegionMappingRepo,
) *Huawei2Collector {
	return &Huawei2Collector{
		vendorType:     strings.ToUpper(model.VendorTypeHuawei),
		solarRepo:      solarRepo,
		siteRegionRepo: siteRegionRepo,
		siteRegions:    make([]model.SiteRegionMapping, 0),
		logger:         zerolog.New(logger.NewWriter("huawei2_collector.log")).With().Timestamp().Caller().Logger(),
	}
}

func (h *Huawei2Collector) Execute(credential *model.HuaweiCredential) {
	siteRegions, err := h.siteRegionRepo.GetSiteRegionMappings()
	if err != nil {
		h.logger.Error().Err(err).Msg("Huawei2Collector::Execute() - failed to get site region mappings")
		return
	}
	h.siteRegions = siteRegions
	now := time.Now().UTC()
	documents := make([]any, 0)
	siteDocuments := make([]model.SiteItem, 0)
	docCh := make(chan any)
	errCh := make(chan error)
	doneCh := make(chan bool)

	defer func() {
		if r := recover(); r != nil {
			h.logger.Error().Any("recover", r).Msg("Huawei2Collector::Execute() - panic")
		}
	}()

	go h.Collect(credential, now, docCh, errCh, doneCh)

COLLECT:
	for {
		select {
		case <-doneCh:
			h.logger.Info().Msg("Huawei2Collector::Execute() - done")
			break COLLECT
		case err := <-errCh:
			h.logger.Error().Err(err).Msg("Huawei2Collector::Execute() - failed")
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
	if err := h.solarRepo.BulkIndex(index, documents); err != nil {
		h.logger.Error().Err(err).Msg("Huawei2Collector::Execute() - failed to bulk index documents")
	} else {
		h.logger.Info().Int("count", len(documents)).Msg("Huawei2Collector::Execute() - bulk index documents success")
	}

	if err := h.solarRepo.UpsertSiteStation(siteDocuments); err != nil {
		h.logger.Error().Err(err).Msg("Huawei2Collector::Execute() - failed to upsert site station")
	} else {
		h.logger.Info().Int("count", len(siteDocuments)).Msg("Huawei2Collector::Execute() - upsert site station success")
	}

	close(doneCh)
	close(errCh)
	close(docCh)
}

func (h *Huawei2Collector) Collect(credential *model.HuaweiCredential, now time.Time, docCh chan any, errCh chan error, doneCh chan bool) {
	beginTime := time.Date(now.Year(), now.Month(), now.Day(), 6, 0, 0, 0, time.UTC).UnixMilli()
	collectTime := now.UnixMilli()
	client, err := huawei2.NewHuawei2Client(credential.Username, credential.Password)
	if err != nil {
		h.logger.Error().
			Err(err).
			Msg("Huawei2Collector::Collect() - failed to create huawei2 client")
		errCh <- err
		return
	}

	stations, err := client.GetPlantList()
	if err != nil {
		h.logger.Error().
			Err(err).
			Msg("Huawei2Collector::Collect() - failed to get plant list")
		errCh <- err
		return
	}

	h.logger.Info().
		Str("username", credential.Username).
		Int("station_count", len(stations)).
		Msg("Huawei2Collector::Collect() - got plant list")

	stationCodeList := make([]string, 0)
	stationCodeListString := make([]string, 0)
	for _, station := range stations {
		if len(stationCodeList) == 100 {
			stationCodeListString = append(stationCodeListString, strings.Join(stationCodeList, ","))
			stationCodeList = make([]string, 0)
		}

		stationCodeList = append(stationCodeList, pointy.StringValue(station.PlantCode, ""))
	}
	stationCodeListString = append(stationCodeListString, strings.Join(stationCodeList, ","))

	deviceList := make([]huawei2.Device, 0)
	mapPlantCodeToRealtimeData := make(map[string]huawei2.RealtimePlantData)
	mapPlantCodeToDailyData := make(map[string]huawei2.HistoricalPlantData)
	mapPlantCodeToMonthlyData := make(map[string]huawei2.HistoricalPlantData)
	mapPlantCodeToYearlyPower := make(map[string]float64)
	mapPlantCodeToTotalPower := make(map[string]float64)
	mapPlantCodeToTotalCO2 := make(map[string]float64)
	mapPlantCodeToDevice := make(map[string][]huawei2.Device)
	mapDeviceSNToAlarm := make(map[string][]huawei2.DeviceAlarm)

	h.logger.Info().
		Str("username", credential.Username).
		Msg("Huawei2Collector::Collect() - start to collect plant data")
	stationCodeSize := len(stationCodeListString)
	for i, stationCodes := range stationCodeListString {
		currentRound := i + 1

		realtimePlantDataResp, err := client.GetRealtimePlantData(stationCodes)
		if err != nil {
			h.logger.Error().
				Str("username", credential.Username).
				Int("count", currentRound).
				Int("total", stationCodeSize).
				Err(err).
				Msg("Huawei2Collector::Collect() - failed to get realtime plant data")
			errCh <- err
			return
		}

		//? Checking if the length of the station code list and the realtime plant data list are the same
		stationCodeList := strings.Split(stationCodes, ",")
		if len(stationCodeList) != len(realtimePlantDataResp.Data) {
			for _, code := range stationCodeList {
				resp, err := client.GetRealtimePlantData(code)
				if err != nil {
					h.logger.Error().
						Str("username", credential.Username).
						Int("count", currentRound).
						Int("total", stationCodeSize).
						Err(err).
						Msg("Huawei2Collector::Collect() - failed to get realtime plant data")
					continue
				}

				if len(resp.Data) > 0 {
					mapPlantCodeToRealtimeData[code] = resp.Data[0]
				} else {
					h.logger.Warn().
						Str("username", credential.Username).
						Int("count", currentRound).
						Int("total", stationCodeSize).
						Msg("Huawei2Collector::Collect() - no data for station code: " + code)
				}
			}
		} else {
			for i, code := range stationCodeList {
				mapPlantCodeToRealtimeData[code] = realtimePlantDataResp.Data[i]
			}
		}

		dailyPlantDataResp, err := client.GetHistoricalPlantData(huawei2.IntervalDay, stationCodes, collectTime)
		if err != nil {
			h.logger.Error().
				Str("username", credential.Username).
				Int("count", currentRound).
				Int("total", stationCodeSize).
				Err(err).
				Msg("Huawei2Collector::Collect() - failed to get daily plant data")
			errCh <- err
			return
		}

		for _, item := range dailyPlantDataResp.Data {
			if !util.IsEmpty(pointy.StringValue(item.Code, "")) {
				a := now.Format("2006-01-02")
				b := time.Unix(pointy.Int64Value(item.CollectTime, 0)/1e3, 0).Format("2006-01-02")
				if a == b {
					mapPlantCodeToDailyData[*item.Code] = item
				}
			}
		}

		monthlyPlantDataResp, err := client.GetHistoricalPlantData(huawei2.IntervalMonth, stationCodes, collectTime)
		if err != nil {
			h.logger.Error().
				Str("username", credential.Username).
				Int("count", currentRound).
				Int("total", stationCodeSize).
				Err(err).
				Msg("Huawei2Collector::Collect() - failed to get monthly plant data")
			errCh <- err
			return
		}

		for _, item := range monthlyPlantDataResp.Data {
			if !util.IsEmpty(pointy.StringValue(item.Code, "")) {
				mapPlantCodeToYearlyPower[*item.Code] = mapPlantCodeToYearlyPower[*item.Code] + pointy.Float64Value(item.DataItemMap.InverterPower, 0)

				a := now.Format("2006-01")
				b := time.Unix(pointy.Int64Value(item.CollectTime, 0)/1e3, 0).Format("2006-01")
				if a == b {
					mapPlantCodeToMonthlyData[*item.Code] = item
				}
			}
		}

		yearlyPlantDataResp, err := client.GetHistoricalPlantData(huawei2.IntervalYear, stationCodes, collectTime)
		if err != nil {
			h.logger.Error().
				Str("username", credential.Username).
				Int("count", currentRound).
				Int("total", stationCodeSize).
				Err(err).
				Msg("Huawei2Collector::Collect() - failed to get yearly plant data")
			errCh <- err
			return
		}

		for _, item := range yearlyPlantDataResp.Data {
			if !util.IsEmpty(pointy.StringValue(item.Code, "")) {
				mapPlantCodeToTotalPower[*item.Code] = mapPlantCodeToTotalPower[*item.Code] + pointy.Float64Value(item.DataItemMap.InverterPower, 0)
				mapPlantCodeToTotalCO2[*item.Code] = mapPlantCodeToTotalCO2[*item.Code] + pointy.Float64Value(item.DataItemMap.ReductionTotalCO2, 0)
			}
		}

		deviceResp, err := client.GetDeviceList(stationCodes)
		if err != nil {
			h.logger.Error().
				Str("username", credential.Username).
				Int("count", currentRound).
				Int("total", stationCodeSize).
				Err(err).
				Msg("Huawei2Collector::Collect() - failed to get device list")
			errCh <- err
			return
		}

		for _, device := range deviceResp.Data {
			plantCode := pointy.StringValue(device.PlantCode, "")
			if !util.IsEmpty(plantCode) {
				mapPlantCodeToDevice[plantCode] = append(mapPlantCodeToDevice[plantCode], device)
			}

			if pointy.IntValue(device.TypeID, 0) == 1 {
				deviceList = append(deviceList, device)
			}
		}

		deviceAlarmResp, err := client.GetDeviceAlarm(stationCodes, beginTime, collectTime)
		if err != nil {
			h.logger.Error().
				Str("username", credential.Username).
				Int("count", currentRound).
				Int("total", stationCodeSize).
				Err(err).
				Msg("Huawei2Collector::Collect() - failed to get device alarm")
			errCh <- err
			return
		}

		for _, item := range deviceAlarmResp.Data {
			double := false
			deviceSN := pointy.StringValue(item.DeviceSN, "")
			if !util.IsEmpty(deviceSN) {
				for i, alarm := range mapDeviceSNToAlarm[deviceSN] {
					if alarm.AlarmName == item.AlarmName {
						double = true

						alarmRaiseTime := pointy.Int64Value(alarm.RaiseTime, 0)
						itemRaiseTime := pointy.Int64Value(item.RaiseTime, 0)
						if alarmRaiseTime < itemRaiseTime {
							mapDeviceSNToAlarm[deviceSN][i] = item
							break
						}
					}
				}
			}

			if !double {
				mapDeviceSNToAlarm[deviceSN] = append(mapDeviceSNToAlarm[deviceSN], item)
			}
		}
	}

	h.logger.Info().
		Str("username", credential.Username).
		Int("devices_size", len(deviceList)).
		Int("mapPlantCodeToRealtimeData_size", len(mapPlantCodeToRealtimeData)).
		Int("mapPlantCodeToDailyData_size", len(mapPlantCodeToDailyData)).
		Int("mapPlantCodeToMonthlyData_size", len(mapPlantCodeToMonthlyData)).
		Int("mapPlantCodeToYearlyPower_size", len(mapPlantCodeToYearlyPower)).
		Int("mapPlantCodeToTotalPower_size", len(mapPlantCodeToTotalPower)).
		Int("mapPlantCodeToTotalCO2_size", len(mapPlantCodeToTotalCO2)).
		Int("mapDeviceSNToAlarm_size", len(mapDeviceSNToAlarm)).
		Msg("Huawei2Collector::Collect() - got data")

	h.logger.Info().Msg("Huawei2Collector::Collect() - start to prepare device document")
	deviceIdList := make([]string, 0)
	deviceIdListString := make([]string, 0)
	for _, device := range deviceList {
		if len(deviceIdList) == 100 {
			deviceIdListString = append(deviceIdListString, strings.Join(deviceIdList, ","))
			deviceIdList = make([]string, 0)
		}

		deviceId := pointy.IntValue(device.ID, 0)
		if deviceId > 0 {
			deviceIdList = append(deviceIdList, strconv.Itoa(deviceId))
		}
	}
	deviceIdListString = append(deviceIdListString, strings.Join(deviceIdList, ","))

	mapDeviceToRealtimeData := make(map[int]huawei2.RealtimeDeviceData)
	mapDeviceToDailyData := make(map[int]huawei2.HistoricalDeviceData)
	mapDeviceToMonthlyData := make(map[int]huawei2.HistoricalDeviceData)
	mapDeviceToYearlyPower := make(map[int]float64)

	deviceIdSize := len(deviceIdListString)
	for i, deviceIds := range deviceIdListString {
		currentRound := i + 1
		if util.IsEmpty(deviceIds) {
			continue
		}

		realtimeDeviceResp, err := client.GetRealtimeDeviceData(deviceIds, "1")
		if err != nil {
			h.logger.Error().
				Str("username", credential.Username).
				Int("count", currentRound).
				Int("total", deviceIdSize).
				Err(err).
				Msg("Huawei2Collector::Collect() - failed to get realtime device data")
			errCh <- err
			return
		}

		for _, item := range realtimeDeviceResp.Data {
			id := pointy.IntValue(item.ID, 0)
			if id > 0 {
				mapDeviceToRealtimeData[id] = item
			}
		}

		dailyDeviceDataResp, err := client.GetHistoricalDeviceData(huawei2.IntervalDay, deviceIds, "1", collectTime)
		if err != nil {
			h.logger.Error().
				Str("username", credential.Username).
				Int("count", currentRound).
				Int("total", deviceIdSize).
				Err(err).
				Msg("Huawei2Collector::Collect() - failed to get daily device data")
			errCh <- err
			return
		}

		for _, item := range dailyDeviceDataResp.Data {
			if now.Format("2006-01-02") == time.Unix(pointy.Int64Value(item.CollectTime, 0)/1e3, 0).Format("2006-01-02") {
				switch id := item.ID.(type) {
				case float64:
					parsedId := int(id)
					mapDeviceToDailyData[parsedId] = item
				}
			}
		}

		monthlyDeviceDataResp, err := client.GetHistoricalDeviceData(huawei2.IntervalMonth, deviceIds, "1", collectTime)
		if err != nil {
			h.logger.Error().
				Str("username", credential.Username).
				Int("count", currentRound).
				Int("total", deviceIdSize).
				Err(err).
				Msg("Huawei2Collector::Collect() - failed to get monthly device data")
			errCh <- err
			return
		}

		for _, item := range monthlyDeviceDataResp.Data {
			switch id := item.ID.(type) {
			case float64:
				parsedId := int(id)
				mapDeviceToYearlyPower[parsedId] = mapDeviceToYearlyPower[parsedId] + pointy.Float64Value(item.DataItemMap.ProductPower, 0)
				if now.Format("2006-01") == time.Unix(pointy.Int64Value(item.CollectTime, 0)/1e3, 0).Format("2006-01") {
					mapDeviceToMonthlyData[parsedId] = item
				}
			}
		}
	}

	stationSize := len(stations)
	h.logger.Info().
		Int("station_size", stationSize).
		Msg("Huawei2Collector::Collect() - start to prepare plant document")

	for i, station := range stations {
		stationCode := pointy.StringValue(station.PlantCode, "")
		stationName := pointy.StringValue(station.PlantName, "")
		plantNameInfo, _ := util.ParsePlantID(stationCode)
		cityName, cityCode, cityArea := util.ParseSiteID(h.siteRegions, plantNameInfo.SiteID)

		var latitude, longitude *float64
		var location *string
		if station.Latitude != nil && station.Longitude != nil {
			if lat, err := strconv.ParseFloat(*station.Latitude, 64); err == nil {
				latitude = &lat
			}

			if long, err := strconv.ParseFloat(*station.Longitude, 64); err == nil {
				longitude = &long
			}

			if latitude != nil && longitude != nil {
				location = pointy.String(fmt.Sprintf("%f,%f", *latitude, *longitude))
			}
		}

		currentPower := 0.0
		plantStatus := huawei2.HuaweiMapPlantStatus[pointy.IntValue(mapPlantCodeToRealtimeData[stationCode].Data.RealHealthState, 0)]
		for _, device := range mapPlantCodeToDevice[stationCode] {
			deviceId := pointy.IntValue(device.ID, 0)
			deviceSN := pointy.StringValue(device.SN, "")
			if device.Latitude != nil && device.Longitude != nil {
				location = pointy.String(fmt.Sprintf("%f,%f", *device.Latitude, *device.Longitude))
			}

			var deviceStatus *int
			if mapDeviceToRealtimeData[deviceId].DataItemMap != nil {
				deviceStatus = mapDeviceToRealtimeData[deviceId].DataItemMap.Status
			}

			if len(mapDeviceSNToAlarm[deviceSN]) > 0 {
				deviceStatus = pointy.Int(2)
				for _, deviceAlarm := range mapDeviceSNToAlarm[deviceSN] {
					alarmItem := model.AlarmItem{
						Timestamp:    now,
						Month:        now.Format("01"),
						Year:         now.Format("2006"),
						MonthYear:    now.Format("01-2006"),
						VendorType:   h.vendorType,
						DataType:     model.DataTypeAlarm,
						Area:         cityArea,
						SiteID:       plantNameInfo.SiteID,
						SiteCityCode: cityCode,
						SiteCityName: cityName,
						NodeType:     plantNameInfo.NodeType,
						ACPhase:      plantNameInfo.ACPhase,
						PlantID:      &stationCode,
						PlantName:    &stationName,
						Latitude:     latitude,
						Longitude:    longitude,
						Location:     location,
						DeviceID:     pointy.String(strconv.Itoa(deviceId)),
						DeviceSN:     deviceAlarm.DeviceSN,
						DeviceName:   deviceAlarm.DeviceName,
						DeviceStatus: pointy.String(huawei2.HuaweiStatusAlarm),
						ID:           pointy.String(strconv.Itoa(pointy.IntValue(deviceAlarm.AlarmID, 0))),
						Message:      deviceAlarm.AlarmName,
						Owner:        credential.Owner,
					}

					if deviceAlarm.RaiseTime != nil {
						alarmTime := time.Unix(pointy.Int64Value(deviceAlarm.RaiseTime, 0)/1e3, 0)
						alarmItem.AlarmTime = &alarmTime
					}

					docCh <- alarmItem
					h.logger.Info().
						Str("username", credential.Username).
						Str("station_code", stationCode).
						Str("device_id", strconv.Itoa(deviceId)).
						Str("device_sn", deviceSN).
						Any("alarm", alarmItem).
						Msg("Huawei2Collector::Collect() - collected alarm document")
				}
			}

			deviceItem := model.DeviceItem{
				Timestamp:      now,
				Month:          now.Format("01"),
				Year:           now.Format("2006"),
				MonthYear:      now.Format("01-2006"),
				VendorType:     h.vendorType,
				DataType:       model.DataTypeDevice,
				Area:           cityArea,
				SiteID:         plantNameInfo.SiteID,
				SiteCityCode:   cityCode,
				SiteCityName:   cityName,
				NodeType:       plantNameInfo.NodeType,
				ACPhase:        plantNameInfo.ACPhase,
				PlantID:        &stationCode,
				PlantName:      &stationName,
				Latitude:       latitude,
				Longitude:      longitude,
				Location:       location,
				ID:             pointy.String(strconv.Itoa(deviceId)),
				SN:             &deviceSN,
				Name:           device.Name,
				LastUpdateTime: nil,
				Owner:          credential.Owner,
			}

			if deviceStatus != nil {
				switch *deviceStatus {
				case 0:
					deviceItem.Status = pointy.String(huawei2.HuaweiStatusOffline)
					if plantStatus != huawei2.HuaweiStatusAlarm {
						plantStatus = huawei2.HuaweiStatusOffline
					}
				case 1:
					deviceItem.Status = pointy.String(huawei2.HuaweiStatusOnline)
					if plantStatus != huawei2.HuaweiStatusOffline && plantStatus != huawei2.HuaweiStatusAlarm {
						plantStatus = huawei2.HuaweiStatusOnline
					}
				case 2:
					deviceItem.Status = pointy.String(huawei2.HuaweiStatusAlarm)
					plantStatus = huawei2.HuaweiStatusAlarm
				}
			}

			typeId := pointy.IntValue(device.TypeID, 0)
			if typeId == 1 {
				if mapDeviceToRealtimeData[deviceId].DataItemMap != nil {
					deviceItem.TotalPowerGeneration = mapDeviceToRealtimeData[deviceId].DataItemMap.TotalEnergy
				}

				if mapDeviceToDailyData[deviceId].DataItemMap != nil {
					deviceItem.DailyPowerGeneration = mapDeviceToDailyData[deviceId].DataItemMap.ProductPower
				}

				if mapDeviceToMonthlyData[deviceId].DataItemMap != nil {
					deviceItem.MonthlyPowerGeneration = mapDeviceToMonthlyData[deviceId].DataItemMap.ProductPower
				}

				deviceItem.YearlyPowerGeneration = pointy.Float64(mapDeviceToYearlyPower[deviceId])
				currentPower += pointy.Float64Value(mapDeviceToRealtimeData[deviceId].DataItemMap.ActivePower, 0)
				latitude = device.Latitude
				longitude = device.Longitude
			}

			docCh <- deviceItem
			h.logger.Info().
				Str("username", credential.Username).
				Str("station_code", stationCode).
				Str("device_id", strconv.Itoa(deviceId)).
				Str("device_sn", deviceSN).
				Any("device", deviceItem).
				Msg("Huawei2Collector::Collect() - collected device document")
		}

		var dailyProduction float64
		if mapPlantCodeToDailyData[stationCode].DataItemMap != nil {
			dailyProduction = pointy.Float64Value(mapPlantCodeToDailyData[stationCode].DataItemMap.InverterPower, 0)
		}

		var monthlyProduction float64
		var monthlyCO2 float64
		if data, ok := mapPlantCodeToMonthlyData[stationCode]; ok {
			if data.DataItemMap != nil {
				monthlyCO2 = pointy.Float64Value(data.DataItemMap.ReductionTotalCO2, 0) * 1000
				monthlyProduction = pointy.Float64Value(mapPlantCodeToMonthlyData[stationCode].DataItemMap.InverterPower, 0)
			}
		}

		var capacity float64
		var totalSavingPrice float64
		var totalProduction float64
		if data, ok := mapPlantCodeToRealtimeData[stationCode]; ok {
			totalProduction = pointy.Float64Value(data.Data.TotalPower, 0)
			capacity = pointy.Float64Value(station.Capacity, 0)
			if data.Data != nil {
				totalSavingPrice = pointy.Float64Value(data.Data.TotalIncome, 0)
			}
		}

		yearlyPower, ok := mapPlantCodeToYearlyPower[stationCode]
		if ok {
			if totalProduction < yearlyPower {
				totalProduction = mapPlantCodeToTotalPower[stationCode]
			}
		}

		var totalCO2 float64
		if data, ok := mapPlantCodeToTotalCO2[stationCode]; ok {
			totalCO2 = data
		}

		plantDocument := model.PlantItem{
			Timestamp:         now,
			Month:             now.Format("01"),
			Year:              now.Format("2006"),
			MonthYear:         now.Format("01-2006"),
			VendorType:        strings.ToUpper(model.VendorTypeHuawei),
			DataType:          model.DataTypePlant,
			Area:              cityArea,
			SiteID:            plantNameInfo.SiteID,
			SiteCityName:      cityName,
			SiteCityCode:      cityCode,
			NodeType:          plantNameInfo.NodeType,
			ACPhase:           plantNameInfo.ACPhase,
			ID:                &stationCode,
			Name:              &stationName,
			Latitude:          latitude,
			Longitude:         longitude,
			Location:          location,
			LocationAddress:   station.PlantAddress,
			CreatedDate:       nil,
			InstalledCapacity: &capacity,
			TotalCO2:          &totalCO2,
			MonthlyCO2:        &monthlyCO2,
			TotalSavingPrice:  &totalSavingPrice,
			Currency:          pointy.String(huawei2.CurrencyUSD),
			CurrentPower:      &currentPower,
			DailyProduction:   &dailyProduction,
			MonthlyProduction: &monthlyProduction,
			YearlyProduction:  &yearlyPower,
			PlantStatus:       &plantStatus,
			Owner:             model.OwnerAltervim,
			TotalProduction:   &totalProduction,
		}

		docCh <- plantDocument
		h.logger.Info().
			Str("username", credential.Username).
			Str("station_code", stationCode).
			Int("count", i+1).
			Int("total", stationSize).
			Any("plant", plantDocument).
			Msg("Huawei2Collector::Collect() - collected plant document")
	}

	doneCh <- true
}

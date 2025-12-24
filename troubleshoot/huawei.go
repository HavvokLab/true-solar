package troubleshoot

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/HavvokLab/true-solar/api/huawei"
	"github.com/HavvokLab/true-solar/model"
	"github.com/HavvokLab/true-solar/pkg/logger"
	"github.com/HavvokLab/true-solar/pkg/util"
	"github.com/HavvokLab/true-solar/repo"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.openly.dev/pointy"
)

type HuaweiTroubleshoot struct {
	vendorType     string
	solarRepo      repo.SolarRepo
	siteRegionRepo repo.SiteRegionMappingRepo
	siteRegions    []model.SiteRegionMapping
	logger         zerolog.Logger
}

func NewHuaweiTroubleshoot(solarRepo repo.SolarRepo, siteRegionRepo repo.SiteRegionMappingRepo) *HuaweiTroubleshoot {
	return &HuaweiTroubleshoot{
		vendorType:     strings.ToUpper(model.VendorTypeHuawei),
		solarRepo:      solarRepo,
		siteRegionRepo: siteRegionRepo,
		siteRegions:    make([]model.SiteRegionMapping, 0),
		logger:         zerolog.New(logger.NewWriter("huawei_troubleshoot.log")).With().Timestamp().Caller().Logger(),
	}
}

func (k *HuaweiTroubleshoot) ExecuteByRange(
	credential *model.HuaweiCredential,
	start, end time.Time,
) {
	for date := start; date.Before(end); date = date.AddDate(0, 0, 1) {
		k.Execute(credential, date)
	}
}

func (h *HuaweiTroubleshoot) Execute(
	credential *model.HuaweiCredential,
	date time.Time,
) {
	defer func() {
		if r := recover(); r != nil {
			h.logger.Error().Any("recover", r).Msg("HuaweiTroubleshoot::Execute() - panic")
		}
	}()

	siteRegions, err := h.siteRegionRepo.GetSiteRegionMappings()
	if err != nil {
		h.logger.Error().Err(err).Msg("HuaweiTroubleshoot::Execute() - failed to get site region mappings")
		return
	}

	h.siteRegions = siteRegions
	documents := make([]any, 0)
	docCh := make(chan any)
	errorCh := make(chan error)
	doneCh := make(chan bool)
	go h.collectByDate(credential, date.UTC(), docCh, errorCh, doneCh)

DONE:
	for {
		select {
		case <-doneCh:
			break DONE
		case err := <-errorCh:
			h.logger.Error().Err(err).Msg("HuaweiTroubleshoot::Execute() - failed")
		case doc := <-docCh:
			documents = append(documents, doc)
		}
	}

	collectorIndex := fmt.Sprintf("%s-%s", model.SolarIndex, date.Format("2006.01.02"))
	if err := h.solarRepo.BulkIndex(collectorIndex, documents); err != nil {
		h.logger.Error().Err(err).Msg("HuaweiTroubleshoot::Execute() - failed to bulk index documents")
		return
	}

	h.logger.Info().Int("count", len(documents)).Msg("HuaweiTroubleshoot::Execute() - bulk index documents success")
	h.logger.Info().Msg("HuaweiTroubleshoot::Execute() - all goroutines finished")

	close(docCh)
	close(doneCh)
	close(errorCh)
}

func (h *HuaweiTroubleshoot) collectByDate(
	credential *model.HuaweiCredential,
	date time.Time,
	docCh chan any,
	errCh chan error,
	doneCh chan bool,
) {
	beginTime := time.Date(date.Year(), date.Month(), date.Day(), 6, 0, 0, 0, time.UTC).UnixMilli()
	collectTime := date.UnixMilli()

	client, err := huawei.NewHuaweiClient(credential.Username, credential.Password, huawei.WithRetryCount(0))
	if err != nil {
		h.logger.Error().Err(err).Msg("HuaweiTroubleshoot::collectByDate() - failed to create huawei client")
		errCh <- err
		return
	}

	plantListResp, err := client.GetPlantList()
	if err != nil {
		h.logger.Error().Err(err).Msg("HuaweiTroubleshoot::collectByDate() - failed to get plant list")
		errCh <- err
		return
	}

	var stationCodeList []string
	var stationCodeListString []string
	for _, station := range plantListResp.Data {
		if len(stationCodeList) == 100 {
			stationCodeListString = append(stationCodeListString, strings.Join(stationCodeList, ","))
			stationCodeList = make([]string, 0)
		}

		if station.Code != nil {
			stationCodeList = append(stationCodeList, *station.Code)
		}
	}
	stationCodeListString = append(stationCodeListString, strings.Join(stationCodeList, ","))

	var inverterList []huawei.Device
	// mapPlantCodeToRealtimeData := make(map[string]huawei.RealtimePlantData)
	mapPlantCodeToDailyData := make(map[string]huawei.HistoricalPlantData)
	mapPlantCodeToMonthlyData := make(map[string]huawei.HistoricalPlantData)
	mapPlantCodeToYearlyPower := make(map[string]float64)
	mapPlantCodeToTotalPower := make(map[string]float64)
	mapPlantCodeToTotalCO2 := make(map[string]float64)
	mapPlantCodeToDevice := make(map[string][]huawei.Device)
	mapDeviceSNToAlarm := make(map[string][]huawei.DeviceAlarm)

	for _, stationCode := range stationCodeListString {
		dailyPlantDataResp, err := client.GetHistoricalPlantData(huawei.IntervalDay, stationCode, collectTime)
		if err != nil {
			h.logger.Error().Err(err).Msg("HuaweiTroubleshoot::collectByDate() - failed to get daily plant data")
			errCh <- err
			continue
		}

		for _, item := range dailyPlantDataResp.Data {
			if item.Code != nil {
				collectionTime := pointy.Int64Value(item.CollectTime, 0)
				if date.Format("2006-01-02") == time.Unix(collectionTime/1e3, 0).Format("2006-01-02") {
					mapPlantCodeToDailyData[*item.Code] = item
				}
			}
		}

		monthlyPlantDataResp, err := client.GetHistoricalPlantData(huawei.IntervalMonth, stationCode, collectTime)
		if err != nil {
			h.logger.Error().Err(err).Msg("HuaweiTroubleshoot::collectByDate() - failed to get monthly plant data")
			errCh <- err
			continue
		}

		for _, item := range monthlyPlantDataResp.Data {
			if item.Code != nil {
				collectionTime := pointy.Int64Value(item.CollectTime, 0)
				if date.Format("2006-01") == time.Unix(collectionTime/1e3, 0).Format("2006-01") {
					mapPlantCodeToMonthlyData[*item.Code] = item
				}
				mapPlantCodeToYearlyPower[*item.Code] = mapPlantCodeToYearlyPower[*item.Code] + pointy.Float64Value(item.DataItemMap.InverterPower, 0)
			}
		}

		yearlyPlantDataResp, err := client.GetHistoricalPlantData(huawei.IntervalYear, stationCode, collectTime)
		if err != nil {
			h.logger.Error().Err(err).Msg("HuaweiTroubleshoot::collectByDate() - failed to get yearly plant data")
			errCh <- err
			continue
		}

		for _, item := range yearlyPlantDataResp.Data {
			if item.Code != nil {
				mapPlantCodeToTotalPower[*item.Code] = mapPlantCodeToTotalPower[*item.Code] + pointy.Float64Value(item.DataItemMap.InverterPower, 0)
				mapPlantCodeToTotalCO2[*item.Code] = mapPlantCodeToTotalCO2[*item.Code] + pointy.Float64Value(item.DataItemMap.ReductionTotalCO2, 0)
			}
		}

		deviceListResp, err := client.GetDeviceList(stationCode)
		if err != nil {
			h.logger.Error().Err(err).Msg("HuaweiTroubleshoot::collectByDate() - failed to get device list")
			errCh <- err
			continue
		}

		for _, item := range deviceListResp.Data {
			if item.PlantCode != nil {
				mapPlantCodeToDevice[*item.PlantCode] = append(mapPlantCodeToDevice[*item.PlantCode], item)
			}

			if pointy.IntValue(item.TypeID, 0) == 1 {
				inverterList = append(inverterList, item)
			}
		}

		deviceAlarmResp, err := client.GetDeviceAlarm(stationCode, beginTime, collectTime)
		if err != nil {
			h.logger.Error().Err(err).Msg("HuaweiTroubleshoot::collectByDate() - failed to get device alarm")
			errCh <- err
			continue
		}

		for _, item := range deviceAlarmResp.Data {
			doubleAlarm := false
			if item.DeviceSN != nil {
				for i, alarm := range mapDeviceSNToAlarm[*item.DeviceSN] {
					if pointy.StringValue(alarm.AlarmName, "") == pointy.StringValue(item.AlarmName, "") {
						doubleAlarm = true

						alarmRaiseTime, itemRaiseTime := pointy.Int64Value(alarm.RaiseTime, 0), pointy.Int64Value(item.RaiseTime, 0)
						if alarmRaiseTime < itemRaiseTime {
							mapDeviceSNToAlarm[*item.DeviceSN][i] = item
							break
						}
					}
				}

				if !doubleAlarm {
					mapDeviceSNToAlarm[*item.DeviceSN] = append(mapDeviceSNToAlarm[*item.DeviceSN], item)
				}
			}
		}
	}
	var inverterIDList []string
	var inverterIDListString []string
	for _, device := range inverterList {
		if len(inverterIDList) == 100 {
			inverterIDListString = append(inverterIDListString, strings.Join(inverterIDList, ","))
			inverterIDList = make([]string, 0)
		}

		if device.ID != nil {
			inverterIDList = append(inverterIDList, strconv.Itoa(*device.ID))
		}
	}
	inverterIDListString = append(inverterIDListString, strings.Join(inverterIDList, ","))

	mapDeviceToDailyData := make(map[int]huawei.HistoricalDeviceData)
	mapDeviceToMonthlyData := make(map[int]huawei.HistoricalDeviceData)
	mapDeviceToYearlyPower := make(map[int]float64)

	for _, deviceID := range inverterIDListString {
		dailyDeviceDataResp, err := client.GetHistoricalDeviceData(huawei.IntervalDay, deviceID, "1", collectTime)
		if err != nil {
			h.logger.Error().Err(err).Msg("HuaweiTroubleshoot::collectByDate() - failed to get daily device data")
			errCh <- err
			continue
		}

		for _, item := range dailyDeviceDataResp.Data {
			if item.ID != nil {
				collectionTime := pointy.Int64Value(item.CollectTime, 0)
				if date.Format("2006-01-02") == time.Unix(collectionTime/1e3, 0).Format("2006-01-02") {
					switch deviceID := item.ID.(type) {
					case float64:
						parsedDeviceID := int(deviceID)
						mapDeviceToDailyData[parsedDeviceID] = item
					default:
					}
				}
			}
		}

		monthlyDeviceDataResp, err := client.GetHistoricalDeviceData(huawei.IntervalMonth, deviceID, "1", collectTime)
		if err != nil {
			h.logger.Error().Err(err).Msg("HuaweiTroubleshoot::collectByDate() - failed to get monthly device data")
			errCh <- err
			continue
		}

		for _, item := range monthlyDeviceDataResp.Data {
			if item.ID != nil {
				switch deviceID := item.ID.(type) {
				case float64:
					parsedDeviceID := int(deviceID)
					mapDeviceToYearlyPower[parsedDeviceID] = mapDeviceToYearlyPower[parsedDeviceID] + pointy.Float64Value(item.DataItemMap.ProductPower, 0)
					if date.Format("2006-01") == time.Unix(pointy.Int64Value(item.CollectTime, 0)/1e3, 0).Format("2006-01") {
						mapDeviceToMonthlyData[parsedDeviceID] = item
					}
				default:
				}
			}
		}
	}

	plantSize := len(plantListResp.Data)
	for i, station := range plantListResp.Data {
		plantCount := i + 1
		log.Info().Str("count", fmt.Sprintf("%d/%d", plantCount, plantSize)).Msg("HuaweiTroubleshoot::collectByDate() - processing plant")

		stationCode := pointy.StringValue(station.Code, "")
		stationName := pointy.StringValue(station.Name, "")
		plantNameInfo, _ := util.ParsePlantID(stationName)
		cityName, cityCode, cityArea := util.ParseSiteID(h.siteRegions, plantNameInfo.SiteID)

		var latitude, longitude *float64
		var location *string
		currentPower := 0.0
		var plantStatus string = "UNKNOWN"

		for _, device := range mapPlantCodeToDevice[stationCode] {
			deviceSN := pointy.StringValue(device.SN, "")
			if device.Latitude != nil && device.Longitude != nil {
				location = pointy.String(fmt.Sprintf("%f,%f", *device.Latitude, *device.Longitude))
			}

			var deviceStatus *int
			if len(mapDeviceSNToAlarm[deviceSN]) > 0 {
				deviceStatus = pointy.Int(2)
			}
			if deviceStatus != nil {
				switch *deviceStatus {
				case 0:
					if plantStatus != huawei.HuaweiStatusAlarm {
						plantStatus = huawei.HuaweiStatusOffline
					}
				case 1:
					if plantStatus != huawei.HuaweiStatusOffline && plantStatus != huawei.HuaweiStatusAlarm {
						plantStatus = huawei.HuaweiStatusOnline
					}
				case 2:
					plantStatus = huawei.HuaweiStatusAlarm
				}
			}
		}

		var dailyProduction, monthlyProduction float64
		if mapPlantCodeToDailyData[stationCode].DataItemMap != nil {
			dailyProduction = pointy.Float64Value(mapPlantCodeToDailyData[stationCode].DataItemMap.InverterPower, 0)
		}

		if mapPlantCodeToMonthlyData[stationCode].DataItemMap != nil {
			monthlyProduction = pointy.Float64Value(mapPlantCodeToMonthlyData[stationCode].DataItemMap.InverterPower, 0)
		}

		var monthlyCO2 float64 = 0.0
		if data, ok := mapPlantCodeToMonthlyData[stationCode]; ok {
			monthlyCO2 = pointy.Float64Value(data.DataItemMap.ReductionTotalCO2, 0)
		}

		plantItem := model.PlantItem{
			Timestamp:         date,
			Month:             date.Format("01"),
			Year:              date.Format("2006"),
			MonthYear:         date.Format("01-2006"),
			VendorType:        h.vendorType,
			DataType:          model.DataTypePlant,
			Area:              cityArea,
			SiteID:            plantNameInfo.SiteID,
			SiteCityCode:      cityCode,
			SiteCityName:      cityName,
			NodeType:          plantNameInfo.NodeType,
			ACPhase:           plantNameInfo.ACPhase,
			ID:                station.Code,
			Name:              station.Name,
			Latitude:          latitude,
			Longitude:         longitude,
			Location:          location,
			LocationAddress:   station.Address,
			CreatedDate:       nil,
			InstalledCapacity: pointy.Float64(pointy.Float64Value(station.Capacity, 0) * 1000),
			TotalCO2:          pointy.Float64(mapPlantCodeToTotalCO2[stationCode] * 1000),
			MonthlyCO2:        &monthlyCO2,
			TotalSavingPrice:  nil,
			Currency:          pointy.String(huawei.CurrencyUSD),
			CurrentPower:      pointy.Float64(currentPower),
			DailyProduction:   &dailyProduction,
			MonthlyProduction: &monthlyProduction,
			YearlyProduction:  pointy.Float64(mapPlantCodeToYearlyPower[stationCode]),
			PlantStatus:       pointy.String(plantStatus),
			Owner:             credential.Owner,
			TotalProduction:   pointy.Float64(mapPlantCodeToTotalPower[stationCode]),
		}

		docCh <- plantItem
	}

	doneCh <- true
}

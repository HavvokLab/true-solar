package collector

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/HavvokLab/true-solar/api/solarman"
	"github.com/HavvokLab/true-solar/model"
	"github.com/HavvokLab/true-solar/pkg/logger"
	"github.com/HavvokLab/true-solar/pkg/util"
	"github.com/HavvokLab/true-solar/repo"
	"github.com/rs/zerolog"
	"github.com/sourcegraph/conc"
	"go.openly.dev/pointy"
)

type SolarmanCollector struct {
	vendorType     string
	solarRepo      repo.SolarRepo
	siteRegionRepo repo.SiteRegionMappingRepo
	siteRegions    []model.SiteRegionMapping
	logger         zerolog.Logger
}

func NewSolarmanCollector(
	solarRepo repo.SolarRepo,
	siteRegionRepo repo.SiteRegionMappingRepo,
) *SolarmanCollector {
	return &SolarmanCollector{
		vendorType:     strings.ToUpper(model.VendorTypeInvt),
		solarRepo:      solarRepo,
		siteRegionRepo: siteRegionRepo,
		siteRegions:    make([]model.SiteRegionMapping, 0),
		logger:         zerolog.New(logger.NewWriter("solarman_collector.log")).With().Timestamp().Caller().Logger(),
	}
}

func (c *SolarmanCollector) Execute(now time.Time, credential *model.SolarmanCredential) {

	siteRegions, err := c.siteRegionRepo.GetSiteRegionMappings()
	if err != nil {
		c.logger.Error().Err(err).Msg("SolarmanCollector::Execute() - failed to get site region mappings")
		return
	}
	c.siteRegions = siteRegions
	now = now.UTC()
	documents := make([]any, 0)
	siteDocuments := make([]model.SiteItem, 0)
	docCh := make(chan any)
	errCh := make(chan error)
	doneCh := make(chan bool)

	defer func() {
		if r := recover(); r != nil {
			c.logger.Error().Any("recover", r).Msg("SolarmanCollector::Execute() - panic")
		}
	}()

	go c.Collect(credential, now, docCh, errCh, doneCh)

COLLECT:
	for {
		select {
		case <-doneCh:
			c.logger.Info().Msg("SolarmanCollector::Execute() - done")
			break COLLECT
		case err := <-errCh:
			c.logger.Error().Err(err).Msg("SolarmanCollector::Execute() - failed")
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
	if err := c.solarRepo.BulkIndex(index, documents); err != nil {
		c.logger.Error().Err(err).Msg("SolarmanCollector::Execute() - failed to bulk index documents")
	} else {
		c.logger.Info().Int("count", len(documents)).Msg("SolarmanCollector::Execute() - bulk index documents success")
	}

	if err := c.solarRepo.UpsertSiteStation(siteDocuments); err != nil {
		c.logger.Error().Err(err).Msg("SolarmanCollector::Execute() - failed to upsert site station")
	} else {
		c.logger.Info().Int("count", len(siteDocuments)).Msg("SolarmanCollector::Execute() - upsert site station success")
	}

	close(doneCh)
	close(errCh)
	close(docCh)
}

func (c *SolarmanCollector) Collect(
	credential *model.SolarmanCredential,
	now time.Time,
	docCh chan any,
	errCh chan error,
	doneCh chan bool,
) {
	client := solarman.NewSolarmanClient(credential.Username, credential.Password, credential.AppID, credential.AppSecret)
	beginningOfDay := time.Date(now.Year(), now.Month(), now.Day(), 6, 0, 0, 0, time.Local)

	tokenResp, err := client.GetBasicToken()
	if err != nil {
		c.logger.Error().
			Str("username", credential.Username).
			Err(err).
			Msg("SolarmanCollector::Collect() - failed to get basic token")
		errCh <- err
		return
	}

	if tokenResp.AccessToken == nil {
		c.logger.Error().
			Str("username", credential.Username).
			Msg("SolarmanCollector::Collect() - failed to get basic token")
		errCh <- fmt.Errorf("failed to get basic token")
		return
	}
	client.SetAccessToken(pointy.StringValue(tokenResp.AccessToken, util.EmptyString))

	userInfoResp, err := client.GetUserInfo()
	if err != nil {
		c.logger.Error().
			Str("username", credential.Username).
			Err(err).
			Msg("SolarmanCollector::Collect() - failed to get user info")
		errCh <- err
		return
	}

	wg := conc.NewWaitGroup()
	for _, company := range userInfoResp.OrgInfoList {
		company := company
		credential := credential
		basicToken := pointy.StringValue(tokenResp.AccessToken, util.EmptyString)

		producer := func() {
			client := solarman.NewSolarmanClient(credential.Username, credential.Password, credential.AppID, credential.AppSecret)
			client.SetAccessToken(basicToken)

			businessTokenResp, err := client.GetBusinessToken(pointy.IntValue(company.CompanyID, 0))
			if err != nil {
				c.logger.Error().
					Str("username", credential.Username).
					Any("company_id", company.CompanyID).
					Err(err).
					Msg("SolarmanCollector::Collect() - failed to get business token")
				errCh <- err
				return
			}

			if businessTokenResp.AccessToken == nil {
				c.logger.Error().
					Str("username", credential.Username).
					Any("company_id", company.CompanyID).
					Msg("SolarmanCollector::Collect() - failed to get business token")
				errCh <- fmt.Errorf("failed to get business token")
				return
			}
			client.SetAccessToken(pointy.StringValue(businessTokenResp.AccessToken, util.EmptyString))

			plantList, err := client.GetPlantList()
			if err != nil {
				c.logger.Error().
					Str("username", credential.Username).
					Any("company_id", company.CompanyID).
					Err(err).
					Msg("SolarmanCollector::Collect() - failed to get plant list")
				errCh <- err
				return
			}

			plantSize := len(plantList)
			for i, station := range plantList {
				if station == nil {
					c.logger.Warn().
						Str("username", credential.Username).
						Any("company_id", company.CompanyID).
						Msg("SolarmanCollector::Collect() - station is null")
					continue
				}

				currentPlant := i + 1
				stationId := pointy.IntValue(station.ID, 0)
				plantId, _ := util.ParsePlantID(pointy.StringValue(station.Name, util.EmptyString))
				cityName, cityCode, cityArea := util.ParseSiteID(c.siteRegions, plantId.SiteID)
				plantItem := model.PlantItem{
					Timestamp:         now,
					VendorType:        c.vendorType,
					DataType:          model.DataTypePlant,
					Month:             now.Format("01"),
					Year:              now.Format("2006"),
					MonthYear:         now.Format("01-2006"),
					Area:              cityArea,
					SiteID:            cityCode,
					SiteCityName:      cityName,
					SiteCityCode:      cityCode,
					NodeType:          plantId.NodeType,
					ACPhase:           plantId.ACPhase,
					ID:                pointy.String(strconv.Itoa(stationId)),
					Name:              station.Name,
					Latitude:          station.LocationLat,
					Longitude:         station.LocationLng,
					LocationAddress:   station.LocationAddress,
					InstalledCapacity: station.InstalledCapacity,
					Owner:             credential.Owner,
				}

				var (
					mergedElectricPrice         *float64
					totalPowerGenerationKWh     *float64
					sumYearlyPowerGenerationKWh *float64
				)

				if plantItem.Latitude != nil && plantItem.Longitude != nil {
					plantItem.Location = pointy.String(fmt.Sprintf("%f,%f", *plantItem.Latitude, *plantItem.Longitude))
				}

				if station.CreatedDate != nil {
					parsed := time.Unix(int64(*station.CreatedDate), 0)
					plantItem.CreatedDate = &parsed
				}

				if plantInfoResp, err := client.GetPlantBaseInfo(stationId); err != nil {
					plantItem.Currency = plantInfoResp.Currency
					mergedElectricPrice = plantInfoResp.MergeElectricPrice
				}

				if realtimeDataResp, err := client.GetPlantRealtimeData(stationId); err != nil {
					generationPower := pointy.Float64Value(realtimeDataResp.GenerationPower, 0)
					plantItem.CurrentPower = pointy.Float64(generationPower / 1000.0)
				}

				if resp, err := client.GetHistoricalPlantData(
					stationId,
					solarman.TimeTypeDay,
					now.Unix(),
					now.Unix(),
				); err == nil && resp != nil {
					if resp.StationDataItems != nil {
						if len(resp.StationDataItems) > 0 {
							plantItem.DailyProduction = resp.StationDataItems[0].GenerationValue
						}
					}
				}

				if resp, err := client.GetHistoricalPlantData(
					stationId,
					solarman.TimeTypeMonth,
					now.Unix(),
					now.Unix(),
				); err == nil && len(resp.StationDataItems) > 0 {
					plantItem.MonthlyProduction = resp.StationDataItems[0].GenerationValue
				}

				startTime := time.Date(2015, now.Month(), now.Day(), 0, 0, 0, 0, time.Local)
				if resp, err := client.GetHistoricalPlantData(
					stationId,
					solarman.TimeTypeYear,
					startTime.Unix(),
					now.Unix(),
				); err == nil && len(resp.StationDataItems) > 0 {
					for _, item := range resp.StationDataItems {
						year := pointy.IntValue(item.Year, 0)
						if year == now.Year() {
							plantItem.YearlyProduction = item.GenerationValue
						}

						sumYearlyPowerGenerationKWh = pointy.Float64(
							pointy.Float64Value(sumYearlyPowerGenerationKWh, 0.0) + pointy.Float64Value(item.GenerationValue, 0.0),
						)
					}
				}

				deviceList, err := client.GetPlantDeviceList(stationId)
				if err != nil {
					c.logger.Error().
						Str("username", credential.Username).
						Any("company_id", company.CompanyID).
						Err(err).
						Msg("SolarmanCollector::Collect() - failed to get plant device list")
					errCh <- err
					continue
				}

				deviceSize, deviceStatusArray := len(deviceList), make([]string, 0)
				for i, device := range deviceList {
					currentDevice := i + 1

					if device == nil {
						c.logger.Warn().
							Str("username", credential.Username).
							Any("company_id", company.CompanyID).
							Msg("SolarmanCollector::Collect() - device is null")
						continue
					}

					deviceSn := pointy.StringValue(device.DeviceSN, util.EmptyString)
					deviceId := pointy.IntValue(device.DeviceID, 0)
					deviceItem := model.DeviceItem{
						Timestamp:    now,
						Month:        now.Format("01"),
						Year:         now.Format("2006"),
						MonthYear:    now.Format("01-2006"),
						VendorType:   c.vendorType,
						DataType:     model.DataTypeDevice,
						Area:         cityArea,
						SiteID:       cityCode,
						SiteCityName: cityName,
						SiteCityCode: cityCode,
						NodeType:     plantId.NodeType,
						ACPhase:      plantId.ACPhase,
						PlantID:      pointy.String(strconv.Itoa(stationId)),
						PlantName:    station.Name,
						Latitude:     plantItem.Latitude,
						Longitude:    plantItem.Longitude,
						Location:     plantItem.Location,
						ID:           pointy.String(strconv.Itoa(deviceId)),
						SN:           device.DeviceSN,
						Name:         device.DeviceSN,
						DeviceType:   device.DeviceType,
						Owner:        credential.Owner,
					}

					if resp, err := client.GetDeviceRealtimeData(deviceSn); err == nil {
						if len(resp.DataList) > 0 {
							for _, data := range resp.DataList {
								key := pointy.StringValue(data.Key, util.EmptyString)
								if key == solarman.DataListKeyCumulativeProduction {
									value := pointy.StringValue(data.Value, util.EmptyString)
									if generation, err := strconv.ParseFloat(value, 64); err == nil {
										totalPowerGenerationKWh = pointy.Float64(
											pointy.Float64Value(totalPowerGenerationKWh, 0.0) + generation,
										)
									}
								}
							}
						}
					}

					if resp, err := client.GetHistoricalDeviceData(deviceSn, solarman.TimeTypeDay, now.Unix(), now.Unix()); err == nil && len(resp.ParamDataList) > 0 {
						for _, param := range resp.ParamDataList {
							if param.DataList != nil {
								for _, data := range param.DataList {
									key := pointy.StringValue(data.Key, util.EmptyString)
									if key == solarman.DataListKeyGeneration {
										value := pointy.StringValue(data.Value, util.EmptyString)
										if generation, err := strconv.ParseFloat(value, 64); err == nil {
											deviceItem.DailyPowerGeneration = pointy.Float64(
												pointy.Float64Value(deviceItem.DailyPowerGeneration, 0.0) + generation,
											)
										}
									}
								}
							}
						}
					}

					if resp, err := client.GetHistoricalDeviceData(deviceSn, solarman.TimeTypeMonth, now.Unix(), now.Unix()); err == nil && len(resp.ParamDataList) > 0 {
						for _, param := range resp.ParamDataList {
							if param.DataList != nil {
								for _, data := range param.DataList {
									key := pointy.StringValue(data.Key, util.EmptyString)
									if key == solarman.DataListKeyGeneration {
										value := pointy.StringValue(data.Value, util.EmptyString)
										if generation, err := strconv.ParseFloat(value, 64); err == nil {
											deviceItem.MonthlyPowerGeneration = pointy.Float64(
												pointy.Float64Value(deviceItem.MonthlyPowerGeneration, 0.0) + generation,
											)
										}
									}
								}
							}
						}
					}

					if resp, err := client.GetHistoricalDeviceData(deviceSn, solarman.TimeTypeYear, now.Unix(), now.Unix()); err == nil && len(resp.ParamDataList) > 0 {
						for _, param := range resp.ParamDataList {
							if param.DataList != nil {
								for _, data := range param.DataList {
									key := pointy.StringValue(data.Key, util.EmptyString)
									if key == solarman.DataListKeyGeneration {
										value := pointy.StringValue(data.Value, util.EmptyString)
										if generation, err := strconv.ParseFloat(value, 64); err == nil {
											deviceItem.YearlyPowerGeneration = pointy.Float64(
												pointy.Float64Value(deviceItem.YearlyPowerGeneration, 0.0) + generation,
											)
										}
									}
								}
							}
						}
					}

					if device.CollectionTime != nil {
						collectionTime := pointy.Int64Value(device.CollectionTime, 0)
						parsed := time.Unix(collectionTime, 0)
						deviceItem.LastUpdateTime = &parsed
					}

					if device.ConnectStatus != nil {
						connectStatus := pointy.IntValue(device.ConnectStatus, 0)
						switch connectStatus {
						case 0:
							deviceItem.Status = pointy.String(solarman.DeviceStatusOff)
						case 1:
							deviceItem.Status = pointy.String(solarman.DeviceStatusOn)
						case 2:
							deviceItem.Status = pointy.String(solarman.DeviceStatusFailure)

							if alertList, err := client.GetDeviceAlertList(deviceSn, beginningOfDay.Unix(), now.Unix()); err == nil {
								alertSize := len(alertList)
								for i, alert := range alertList {
									currentAlert := i + 1

									alarmItem := model.AlarmItem{
										Timestamp:    now,
										Month:        now.Format("01"),
										Year:         now.Format("2006"),
										MonthYear:    now.Format("01-2006"),
										VendorType:   c.vendorType,
										DataType:     model.DataTypeAlarm,
										Area:         cityArea,
										SiteID:       plantId.SiteID,
										SiteCityName: cityName,
										SiteCityCode: cityCode,
										NodeType:     plantId.NodeType,
										ACPhase:      plantId.ACPhase,
										PlantID:      pointy.String(strconv.Itoa(stationId)),
										PlantName:    station.Name,
										Latitude:     plantItem.Latitude,
										Longitude:    plantItem.Longitude,
										Location:     plantItem.Location,
										DeviceID:     pointy.String(strconv.Itoa(deviceId)),
										DeviceSN:     device.DeviceSN,
										DeviceName:   device.DeviceSN,
										DeviceType:   device.DeviceType,
										DeviceStatus: deviceItem.Status,
										ID:           pointy.String(strconv.Itoa(pointy.IntValue(alert.AlertID, 0))),
										Message:      alert.AlertNameInPAAS,
										Owner:        credential.Owner,
									}

									if alert.AlertTime != nil {
										alertTime := time.Unix(pointy.Int64Value(alert.AlertTime, 0), 0)
										alarmItem.AlarmTime = &alertTime
									}

									docCh <- alarmItem
									c.logger.Info().
										Str("username", credential.Username).
										Str("alarm_count", fmt.Sprintf("%v/%v", currentAlert, alertSize)).
										Int("company_id", pointy.IntValue(company.CompanyID, 0)).
										Str("plant_id", strconv.Itoa(stationId)).
										Str("device_id", strconv.Itoa(deviceId)).
										Any("alarm", alarmItem).
										Msg("SolarmanCollector::Collect() - alarm item added")
								}
							}
						default:
						}
					}

					if deviceItem.Status != nil {
						deviceStatusArray = append(deviceStatusArray, *deviceItem.Status)
					}

					docCh <- deviceItem
					c.logger.Info().
						Str("username", credential.Username).
						Str("device_count", fmt.Sprintf("%v/%v", currentDevice, deviceSize)).
						Int("company_id", pointy.IntValue(company.CompanyID, 0)).
						Str("plant_id", strconv.Itoa(stationId)).
						Str("device_id", strconv.Itoa(deviceId)).
						Any("device", deviceItem).
						Msg("SolarmanCollector::Collect() - device item added")
				}

				plantStatus := solarman.SolarmanPlantStatusOn
				if len(deviceStatusArray) > 0 {
					offlineCount, alertingCount := 0, 0
					for _, status := range deviceStatusArray {
						switch status {
						case solarman.DeviceStatusOff:
							offlineCount++
						case solarman.DeviceStatusOn:
						default:
							alertingCount++
						}
					}
					if alertingCount > 0 {
						plantStatus = solarman.SolarmanPlantStatusAlarm
					} else if offlineCount > 0 {
						plantStatus = solarman.SolarmanPlantStatusOff
					}
				} else {
					plantStatus = solarman.SolarmanPlantStatusOff
				}

				plantItem.TotalProduction = totalPowerGenerationKWh
				if pointy.Float64Value(plantItem.TotalProduction, 0) < pointy.Float64Value(plantItem.YearlyProduction, 0) {
					plantItem.TotalProduction = plantItem.YearlyProduction
				}

				plantItem.PlantStatus = &plantStatus
				plantItem.TotalSavingPrice = pointy.Float64(
					pointy.Float64Value(mergedElectricPrice, 0) *
						pointy.Float64Value(totalPowerGenerationKWh, 0),
				)
				docCh <- plantItem
				c.logger.Info().
					Str("username", credential.Username).
					Str("plant_count", fmt.Sprintf("%v/%v", currentPlant, plantSize)).
					Int("company_id", pointy.IntValue(company.CompanyID, 0)).
					Str("plant_id", strconv.Itoa(stationId)).
					Any("plant", plantItem).
					Msg("SolarmanCollector::Collect() - plant item added")
			}
		}

		wg.Go(producer)
	}

	if r := wg.WaitAndRecover(); r != nil {
		c.logger.Error().Err(r.AsError()).
			Any("recover", r).
			Str("username", credential.Username).
			Msg("SolarmanCollector::Collect() - panic")
		return
	}

	doneCh <- true
}

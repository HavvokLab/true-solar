package collector

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/HavvokLab/true-solar/api/growatt"
	"github.com/HavvokLab/true-solar/model"
	"github.com/HavvokLab/true-solar/pkg/logger"
	"github.com/HavvokLab/true-solar/pkg/util"
	"github.com/HavvokLab/true-solar/repo"
	"github.com/mitchellh/mapstructure"
	"github.com/rs/zerolog"
	"github.com/sourcegraph/conc"
	"go.openly.dev/pointy"
)

type GrowattCollector struct {
	vendorType     string
	solarRepo      repo.SolarRepo
	siteRegionRepo repo.SiteRegionMappingRepo
	siteRegions    []model.SiteRegionMapping
	logger         zerolog.Logger
}

func NewGrowattCollector(
	solarRepo repo.SolarRepo,
	siteRegionRepo repo.SiteRegionMappingRepo,
) *GrowattCollector {
	return &GrowattCollector{
		vendorType:     strings.ToUpper(model.VendorTypeGrowatt),
		solarRepo:      solarRepo,
		siteRegionRepo: siteRegionRepo,
		siteRegions:    make([]model.SiteRegionMapping, 0),
		logger:         zerolog.New(logger.NewWriter("growatt_collector.log")).With().Timestamp().Caller().Logger(),
	}
}

func (g *GrowattCollector) Execute(now time.Time, credential *model.GrowattCredential) {
	defer func() {
		if r := recover(); r != nil {
			g.logger.Error().Any("recover", r).Msg("GrowattCollector::Execute() - panic")
		}
	}()

	siteRegions, err := g.siteRegionRepo.GetSiteRegionMappings()
	if err != nil {
		g.logger.Error().Err(err).Msg("GrowattCollector::Execute() - failed to get site region mappings")
		return
	}

	g.siteRegions = siteRegions
	plantDeviceStatusMap := make(map[string]string)
	inverterArray := make([]string, 0)
	documents := make([]interface{}, 0)
	siteDocuments := make([]model.SiteItem, 0)

	documentCh := make(chan interface{})
	inverterCh := make(chan string)
	plantDeviceStatusCh := make(chan map[string]string)
	doneCh := make(chan bool)
	errorCh := make(chan error)
	go g.Collect(credential, now, documentCh, inverterCh, plantDeviceStatusCh, errorCh, doneCh)

DONE:
	for {
		select {
		case <-doneCh:
			break DONE
		case err := <-errorCh:
			g.logger.Error().Err(err).Msg("GrowattCollector::Execute() - failed")
			return
		case doc := <-documentCh:
			documents = append(documents, doc)
		case plantDeviceStatus := <-plantDeviceStatusCh:
			if err := mapstructure.Decode(&plantDeviceStatus, &plantDeviceStatusMap); err != nil {
				g.logger.Error().Err(err).Msg("GrowattCollector::Execute() - failed to decode plant device status")
				return
			}
		case sn := <-inverterCh:
			inverterArray = append(inverterArray, sn)
		}
	}

	g.logger.Info().Msg("GrowattCollector::Execute() - calculating inverter productions")
	realtimeDeviceMap, err := g.CalculateInverterProductions(
		credential,
		inverterArray,
	)
	g.logger.Info().Msg("GrowattCollector::Execute() - calculating inverter productions success")

	if err != nil {
		g.logger.Error().Err(err).Msg("GrowattCollector::Execute() - failed to calculate inverter productions")
	}

	for i, doc := range documents {
		fmt.Println(i, "/", len(documents))
		if plantItem, ok := doc.(model.PlantItem); ok {
			if plantItem.ID != nil {
				if plantStatus, found := plantDeviceStatusMap[*plantItem.ID]; found {
					plantItem.PlantStatus = &plantStatus
					documents[i] = plantItem
				}
			}

			siteItem := model.SiteItem{
				Timestamp:   plantItem.Timestamp,
				VendorType:  plantItem.VendorType,
				Area:        plantItem.Area,
				SiteID:      plantItem.SiteID,
				NodeType:    plantItem.NodeType,
				Name:        plantItem.Name,
				Location:    plantItem.Location,
				PlantStatus: plantItem.PlantStatus,
				Owner:       plantItem.Owner,
			}

			siteDocuments = append(siteDocuments, siteItem)
		} else if deviceItem, ok := doc.(model.DeviceItem); ok {
			if deviceItem.SN != nil {
				if data, ok := realtimeDeviceMap[*deviceItem.SN]; ok {
					deviceItem.TotalPowerGeneration = data.Total
					deviceItem.DailyPowerGeneration = data.Today
					documents[i] = deviceItem
				}
			}
		}
	}

	collectorIndex := fmt.Sprintf("%s-%s", model.SolarIndex, time.Now().Format("2006.01.02"))
	if err := g.solarRepo.BulkIndex(collectorIndex, documents); err != nil {
		g.logger.Error().Err(err).Msg("GrowattCollector::Execute() - failed to bulk index documents")
		return
	}
	g.logger.Info().Int("count", len(documents)).Msg("GrowattCollector::Execute() - bulk index documents success")

	if err := g.solarRepo.UpsertSiteStation(siteDocuments); err != nil {
		g.logger.Error().Err(err).Msg("GrowattCollector::Execute() - failed to upsert site station")
		return
	}
	g.logger.Info().Int("count", len(siteDocuments)).Msg("GrowattCollector::Execute() - upsert site station success")

	g.logger.Info().Msg("GrowattCollector::Execute() - all goroutines finished")
	close(documentCh)
	close(doneCh)
	close(errorCh)
	close(inverterCh)
	close(plantDeviceStatusCh)
}

func (g *GrowattCollector) Collect(
	credential *model.GrowattCredential,
	now time.Time,
	docCh chan any,
	inverterCh chan string,
	plantDeviceStatusCh chan map[string]string,
	errCh chan error,
	doneCh chan bool,
) {
	client := growatt.NewGrowattClient(credential.Username, credential.Token)

	plantList, err := client.GetPlantList()
	if err != nil {
		g.logger.Error().Err(err).Msg("GrowattCollector::Collect() - failed to get plant list")
		errCh <- err
		return
	}

	g.logger.Info().Msgf("GrowattCollector::Collect() - got plant list count: %v", len(plantList))

	wg := conc.NewWaitGroup()
	plantSize := len(plantList)
	for i, station := range plantList {
		plantCount := i + 1

		station := station
		producer := func() {
			stationId := pointy.IntValue(station.PlantID, 0)
			stationIdStr := strconv.Itoa(stationId)
			plantId, _ := util.ParsePlantID(pointy.StringValue(station.Name, ""))
			cityName, cityCode, cityArea := util.ParseSiteID(g.siteRegions, plantId.SiteID)

			plantItem := model.PlantItem{
				Timestamp:    now,
				Month:        now.Format("01"),
				Year:         now.Format("2006"),
				MonthYear:    now.Format("01-2006"),
				VendorType:   g.vendorType,
				DataType:     model.DataTypePlant,
				Area:         cityArea,
				SiteID:       plantId.SiteID,
				SiteCityName: cityName,
				SiteCityCode: cityCode,
				NodeType:     plantId.NodeType,
				ACPhase:      plantId.ACPhase,
				ID:           pointy.String(stationIdStr),
				Name:         station.Name,
				PlantStatus:  pointy.String(growatt.GrowattPlantStatusOffline),
				Owner:        credential.Owner,
			}

			var electricPricePerKWh *float64
			var co2WeightPerKWh *float64

			if station.Latitude != nil {
				if parsed, err := strconv.ParseFloat(pointy.StringValue(station.Latitude, "0"), 64); err == nil {
					plantItem.Latitude = &parsed
				}
			}

			if station.Longitude != nil {
				if parsed, err := strconv.ParseFloat(pointy.StringValue(station.Longitude, "0"), 64); err == nil {
					plantItem.Longitude = &parsed
				}
			}

			if plantItem.Latitude != nil && plantItem.Longitude != nil {
				plantItem.Location = pointy.String(fmt.Sprintf("%f,%f", *plantItem.Latitude, *plantItem.Longitude))
			}

			if station.City != nil {
				if *station.City != "" {
					plantItem.LocationAddress = station.City
				}
			}

			if station.Country != nil {
				if *station.Country != "" {
					if plantItem.LocationAddress != nil {
						plantItem.LocationAddress = pointy.String(fmt.Sprintf("%s, %s", *plantItem.LocationAddress, *station.Country))
					} else {
						plantItem.LocationAddress = station.Country
					}
				}
			}

			if dataLoggerResp, err := client.GetPlantDataLoggerInfo(stationId); err == nil {
				if dataLoggerResp.Data != nil {
					if dataLoggerResp.Data.PeakPowerActual != nil {
						actualData := dataLoggerResp.Data.PeakPowerActual
						electricPricePerKWh = actualData.FormulaMoney
						co2WeightPerKWh = actualData.FormulaCo2

						if actualData.NominalPower != nil {
							plantItem.InstalledCapacity = pointy.Float64(pointy.Float64Value(actualData.NominalPower, 0) / 1000.0)
						} else if plantId.Capacity != 0 {
							plantItem.InstalledCapacity = pointy.Float64(plantId.Capacity)
						}

						if actualData.FormulaMoneyUnitID != nil {
							plantItem.Currency = pointy.String(strings.ToUpper(pointy.StringValue(actualData.FormulaMoneyUnitID, "0")))
						}
					}
				}
			}

			if overviewInfoResp, err := client.GetPlantOverviewInfo(stationId); err == nil {
				if overviewInfoResp.Data != nil {
					plantItem.CurrentPower = overviewInfoResp.Data.CurrentPower

					if overviewInfoResp.Data.TodayEnergy != nil {
						if parsed, err := strconv.ParseFloat(pointy.StringValue(overviewInfoResp.Data.TodayEnergy, "0"), 64); err == nil {
							plantItem.DailyProduction = &parsed
						}
					}

					if overviewInfoResp.Data.MonthlyEnergy != nil {
						if parsed, err := strconv.ParseFloat(pointy.StringValue(overviewInfoResp.Data.MonthlyEnergy, "0"), 64); err == nil {
							plantItem.MonthlyProduction = &parsed

							if co2WeightPerKWh != nil {
								plantItem.MonthlyCO2 = pointy.Float64(parsed * pointy.Float64Value(co2WeightPerKWh, 0.0))
							}
						}
					}

					if overviewInfoResp.Data.YearlyEnergy != nil {
						if parsed, err := strconv.ParseFloat(pointy.StringValue(overviewInfoResp.Data.YearlyEnergy, "0"), 64); err == nil {
							plantItem.YearlyProduction = &parsed
						}
					}

					if overviewInfoResp.Data.TotalEnergy != nil {
						if parsed, err := strconv.ParseFloat(pointy.StringValue(overviewInfoResp.Data.TotalEnergy, "0"), 64); err == nil {
							plantItem.TotalProduction = &parsed

							if electricPricePerKWh != nil {
								plantItem.TotalSavingPrice = pointy.Float64(parsed * pointy.Float64Value(electricPricePerKWh, 0.0))
							}

							if co2WeightPerKWh != nil {
								plantItem.TotalCO2 = pointy.Float64(parsed * pointy.Float64Value(co2WeightPerKWh, 0.0))
							}
						}
					}
				}
			}
			docCh <- plantItem
			g.logger.Info().
				Str("plant_count", fmt.Sprintf("%v/%v", plantCount, plantSize)).
				Str("username", credential.Username).
				Str("password", credential.Password).
				Str("plant_id", stationIdStr).
				Any("plant", plantItem).
				Msg("GrowattCollector::Collect() - plant item added")

			deviceList, err := client.GetPlantDeviceList(stationId)
			if err != nil {
				g.logger.Error().Err(err).
					Msg("GrowattCollector::Collect() - failed to get plant device list")
				errCh <- err
				return
			}

			deviceSize := len(deviceList)
			deviceStatusArray := make([]string, 0)
			for i, device := range deviceList {
				deviceCount := i + 1
				g.logger.Info().Msgf("GrowattCollector::Collect() - processing device %v/%v", deviceCount, deviceSize)

				deviceSn := pointy.StringValue(device.DeviceSN, "")
				deviceId := pointy.IntValue(device.DeviceID, 0)
				deviceTypeRaw := pointy.IntValue(device.Type, 0)
				deviceType := growatt.ParseGrowattDeviceType(deviceTypeRaw)

				deviceItem := model.DeviceItem{
					Timestamp:    now,
					Month:        now.Format("01"),
					Year:         now.Format("2006"),
					MonthYear:    now.Format("01-2006"),
					VendorType:   g.vendorType,
					DataType:     model.DataTypeDevice,
					Area:         cityArea,
					SiteID:       plantId.SiteID,
					SiteCityName: cityName,
					SiteCityCode: cityCode,
					NodeType:     plantId.NodeType,
					ACPhase:      plantId.ACPhase,
					PlantID:      pointy.String(stationIdStr),
					PlantName:    station.Name,
					Latitude:     plantItem.Latitude,
					Longitude:    plantItem.Longitude,
					Location:     plantItem.Location,
					ID:           pointy.String(strconv.Itoa(deviceId)),
					SN:           device.DeviceSN,
					Name:         device.DeviceSN,
					DeviceType:   &deviceType,
					Owner:        credential.Owner,
				}

				if device.LastUpdateTime != nil {
					if parsed, err := time.Parse("2006-01-02 15:04:05", pointy.StringValue(device.LastUpdateTime, "0000-00-00 00:00:00")); err == nil {
						deviceItem.LastUpdateTime = &parsed
					}
				}

				switch deviceTypeRaw {
				case growatt.GrowattDeviceTypeInverter:
					if device.Status != nil {
						switch *device.Status {
						case 0: // Offline
							deviceItem.Status = pointy.String(growatt.GrowattDeviceStatusOffline)
						case 1: // Online
							deviceItem.Status = pointy.String(growatt.GrowattDeviceStatusOnline)
						default: // Others
							if *device.Status == 0 || *device.Status == 2 { // stand by
								deviceItem.Status = pointy.String(growatt.GrowattDeviceStatusStandBy)
							} else if *device.Status == 3 { // Failure
								deviceItem.Status = pointy.String(growatt.GrowattDeviceStatusFailure)
							} else {
								deviceItem.Status = pointy.String(growatt.GrowattDeviceStatusOffline)
							}

							if alarms, err := client.GetMaxAlertList(deviceSn, now.Unix()); err == nil {
								if len(alarms) > 0 {
									latestAlert := alarms[0]
									if startTime := latestAlert.StartTime; startTime != nil {
										if pointy.StringValue(device.LastUpdateTime, "0000-00-00")[0:10] == (*startTime)[0:10] {
											alarmItemDoc := model.AlarmItem{
												Timestamp:    now,
												Month:        now.Format("01"),
												Year:         now.Format("2006"),
												MonthYear:    now.Format("01-2006"),
												VendorType:   g.vendorType,
												DataType:     model.DataTypeAlarm,
												Area:         cityArea,
												SiteID:       plantId.SiteID,
												SiteCityName: cityName,
												SiteCityCode: cityCode,
												NodeType:     plantId.NodeType,
												ACPhase:      plantId.ACPhase,
												PlantID:      pointy.String(stationIdStr),
												PlantName:    station.Name,
												Latitude:     plantItem.Latitude,
												Longitude:    plantItem.Longitude,
												Location:     plantItem.Location,
												DeviceID:     pointy.String(strconv.Itoa(deviceId)),
												DeviceSN:     device.DeviceSN,
												DeviceName:   device.DeviceSN,
												DeviceType:   pointy.String(growatt.ParseGrowattDeviceType(growatt.GrowattDeviceTypeMax)),
												DeviceStatus: deviceItem.Status,
												ID:           pointy.String(strconv.Itoa(pointy.IntValue(latestAlert.AlarmCode, 0))),
												Message:      latestAlert.AlarmMessage,
												Owner:        credential.Owner,
											}

											if latestAlert.StartTime != nil {
												if parsed, err := time.Parse("2006-01-02 15:04:05.0", *latestAlert.StartTime); err == nil {
													utcParsed := parsed.UTC()
													alarmItemDoc.AlarmTime = &utcParsed
												}
											}

											docCh <- alarmItemDoc
										}
									}
								}
							}
						}

					}
				case growatt.GrowattDeviceTypeMix:
					if device.Status != nil {
						switch *device.Status {
						case 5, 6, 7, 8: // Normal
							deviceItem.Status = pointy.String(growatt.GrowattDeviceStatusOnline)
						default: // Others
							if *device.Status == 0 { // Waiting
								deviceItem.Status = pointy.String(growatt.GrowattDeviceStatusWaiting)
							} else if *device.Status == 1 { // Self-check
								deviceItem.Status = pointy.String(growatt.GrowattDeviceStatusSelfCheck)
							} else if *device.Status == 3 { // Failure
								deviceItem.Status = pointy.String(growatt.GrowattDeviceStatusFailure)
							} else if *device.Status == 4 { // Upgrading
								deviceItem.Status = pointy.String(growatt.GrowattDeviceStatusUpgrading)
							} else {
								deviceItem.Status = pointy.String(growatt.GrowattDeviceStatusOffline)
							}

							if alarms, err := client.GetMixAlertList(deviceSn, now.Unix()); err == nil {
								if len(alarms) > 0 {
									latestAlert := alarms[0]
									if startTime := latestAlert.StartTime; startTime != nil {
										if pointy.StringValue(device.LastUpdateTime, "0000-00-00")[0:10] == (*startTime)[0:10] {
											alarmItemDoc := model.AlarmItem{
												Timestamp:    now,
												Month:        now.Format("01"),
												Year:         now.Format("2006"),
												MonthYear:    now.Format("01-2006"),
												VendorType:   g.vendorType,
												DataType:     model.DataTypeAlarm,
												Area:         cityArea,
												SiteID:       plantId.SiteID,
												SiteCityName: cityName,
												SiteCityCode: cityCode,
												NodeType:     plantId.NodeType,
												ACPhase:      plantId.ACPhase,
												PlantID:      pointy.String(stationIdStr),
												PlantName:    station.Name,
												Latitude:     plantItem.Latitude,
												Longitude:    plantItem.Longitude,
												Location:     plantItem.Location,
												DeviceID:     pointy.String(strconv.Itoa(deviceId)),
												DeviceSN:     device.DeviceSN,
												DeviceName:   device.DeviceSN,
												DeviceType:   pointy.String(growatt.ParseGrowattDeviceType(growatt.GrowattDeviceTypeMix)),
												DeviceStatus: deviceItem.Status,
												ID:           pointy.String(strconv.Itoa(pointy.IntValue(latestAlert.AlarmCode, 0))),
												Message:      latestAlert.AlarmMessage,
												Owner:        credential.Owner,
											}

											if latestAlert.StartTime != nil {
												if parsed, err := time.Parse("2006-01-02 15:04:05.0", *latestAlert.StartTime); err == nil {
													utcParsed := parsed.UTC()
													alarmItemDoc.AlarmTime = &utcParsed
												}
											}

											docCh <- alarmItemDoc
										}
									}
								}
							}
						}
					}
				case growatt.GrowattDeviceTypeSpA:
					if device.Status != nil {
						switch *device.Status {
						case 5, 6, 7, 8: // Normal
							deviceItem.Status = pointy.String(growatt.GrowattDeviceStatusOnline)
						default: // Others
							if *device.Status == 0 { // Waiting
								deviceItem.Status = pointy.String(growatt.GrowattDeviceStatusWaiting)
							} else if *device.Status == 1 { // Self-check
								deviceItem.Status = pointy.String(growatt.GrowattDeviceStatusSelfCheck)
							} else if *device.Status == 3 { // Failure
								deviceItem.Status = pointy.String(growatt.GrowattDeviceStatusFailure)
							} else if *device.Status == 4 { // Upgrading
								deviceItem.Status = pointy.String(growatt.GrowattDeviceStatusUpgrading)
							} else {
								deviceItem.Status = pointy.String(growatt.GrowattDeviceStatusOffline)
							}

							if alarms, err := client.GetSpaAlertList(deviceSn, now.Unix()); err == nil {
								if len(alarms) > 0 {
									latestAlert := alarms[0]
									if startTime := latestAlert.StartTime; startTime != nil {
										if pointy.StringValue(device.LastUpdateTime, "0000-00-00")[0:10] == (*startTime)[0:10] {
											alarmItemDoc := model.AlarmItem{
												Timestamp:    now,
												Month:        now.Format("01"),
												Year:         now.Format("2006"),
												MonthYear:    now.Format("01-2006"),
												VendorType:   g.vendorType,
												DataType:     model.DataTypeAlarm,
												Area:         cityArea,
												SiteID:       plantId.SiteID,
												SiteCityName: cityName,
												SiteCityCode: cityCode,
												NodeType:     plantId.NodeType,
												ACPhase:      plantId.ACPhase,
												PlantID:      pointy.String(stationIdStr),
												PlantName:    station.Name,
												Latitude:     plantItem.Latitude,
												Longitude:    plantItem.Longitude,
												Location:     plantItem.Location,
												DeviceID:     pointy.String(strconv.Itoa(deviceId)),
												DeviceSN:     device.DeviceSN,
												DeviceName:   device.DeviceSN,
												DeviceType:   pointy.String(growatt.ParseGrowattDeviceType(growatt.GrowattDeviceTypeSpA)),
												DeviceStatus: deviceItem.Status,
												ID:           pointy.String(strconv.Itoa(pointy.IntValue(latestAlert.AlarmCode, 0))),
												Message:      latestAlert.AlarmMessage,
												Owner:        credential.Owner,
											}

											if latestAlert.StartTime != nil {
												if parsed, err := time.Parse("2006-01-02 15:04:05.0", *latestAlert.StartTime); err == nil {
													utcParsed := parsed.UTC()
													alarmItemDoc.AlarmTime = &utcParsed
												}
											}

											docCh <- alarmItemDoc
										}
									}

								}
							}
						}
					}

				case growatt.GrowattDeviceTypeMin:
					if device.Status != nil {
						switch *device.Status {
						case 0: // Offline
							deviceItem.Status = pointy.String(growatt.GrowattDeviceStatusOffline)
						case 1: // Online
							deviceItem.Status = pointy.String(growatt.GrowattDeviceStatusOnline)
						default: // Others
							if *device.Status == 2 { // Stand by
								deviceItem.Status = pointy.String(growatt.GrowattDeviceStatusStandBy)
							} else if *device.Status == 3 { // Failure
								deviceItem.Status = pointy.String(growatt.GrowattDeviceStatusFailure)
							} else {
								deviceItem.Status = pointy.String(growatt.GrowattDeviceStatusOffline)
							}

							if alarms, err := client.GetMinAlertList(deviceSn, now.Unix()); err == nil {
								if len(alarms) > 0 {
									latestAlert := alarms[0]
									if startTime := latestAlert.StartTime; startTime != nil {
										if pointy.StringValue(device.LastUpdateTime, "0000-00-00")[0:10] == (*startTime)[0:10] {
											alarmItemDoc := model.AlarmItem{
												Timestamp:    now,
												Month:        now.Format("01"),
												Year:         now.Format("2006"),
												MonthYear:    now.Format("01-2006"),
												VendorType:   g.vendorType,
												DataType:     model.DataTypeAlarm,
												Area:         cityArea,
												SiteID:       plantId.SiteID,
												SiteCityName: cityName,
												SiteCityCode: cityCode,
												NodeType:     plantId.NodeType,
												ACPhase:      plantId.ACPhase,
												PlantID:      pointy.String(stationIdStr),
												PlantName:    station.Name,
												Latitude:     plantItem.Latitude,
												Longitude:    plantItem.Longitude,
												Location:     plantItem.Location,
												DeviceID:     pointy.String(strconv.Itoa(deviceId)),
												DeviceSN:     device.DeviceSN,
												DeviceName:   device.DeviceSN,
												DeviceType:   pointy.String(growatt.ParseGrowattDeviceType(growatt.GrowattDeviceTypeMin)),
												DeviceStatus: deviceItem.Status,
												ID:           pointy.String(strconv.Itoa(pointy.IntValue(latestAlert.AlarmCode, 0))),
												Message:      latestAlert.AlarmMessage,
												Owner:        credential.Owner,
											}

											if latestAlert.StartTime != nil {
												if parsed, err := time.Parse("2006-01-02 15:04:05.0", *latestAlert.StartTime); err == nil {
													utcParsed := parsed.UTC()
													alarmItemDoc.AlarmTime = &utcParsed
												}
											}

											docCh <- alarmItemDoc
										}
									}
								}
							}
						}
					}
				case growatt.GrowattDeviceTypePcs:
					if device.Status != nil {
						switch *device.Status {
						case 0: // Offline
							deviceItem.Status = pointy.String(growatt.GrowattDeviceStatusOffline)
						case 1: // Online
							deviceItem.Status = pointy.String(growatt.GrowattDeviceStatusOnline)
						default: // Others
							if *device.Status == 2 { // Stand by
								deviceItem.Status = pointy.String(growatt.GrowattDeviceStatusStandBy)
							} else if *device.Status == 3 { // Failure
								deviceItem.Status = pointy.String(growatt.GrowattDeviceStatusFailure)
							} else {
								deviceItem.Status = pointy.String(growatt.GrowattDeviceStatusOffline)
							}

							if alarms, err := client.GetPcsAlertList(deviceSn, now.Unix()); err == nil {
								if len(alarms) > 0 {
									latestAlert := alarms[0]
									if startTime := latestAlert.StartTime; startTime != nil {
										if pointy.StringValue(device.LastUpdateTime, "0000-00-00")[0:10] == (*startTime)[0:10] {
											alarmItemDoc := model.AlarmItem{
												Timestamp:    now,
												Month:        now.Format("01"),
												Year:         now.Format("2006"),
												MonthYear:    now.Format("01-2006"),
												VendorType:   g.vendorType,
												DataType:     model.DataTypeAlarm,
												Area:         cityArea,
												SiteID:       plantId.SiteID,
												SiteCityName: cityName,
												SiteCityCode: cityCode,
												NodeType:     plantId.NodeType,
												ACPhase:      plantId.ACPhase,
												PlantID:      pointy.String(stationIdStr),
												PlantName:    station.Name,
												Latitude:     plantItem.Latitude,
												Longitude:    plantItem.Longitude,
												Location:     plantItem.Location,
												DeviceID:     pointy.String(strconv.Itoa(deviceId)),
												DeviceSN:     device.DeviceSN,
												DeviceName:   device.DeviceSN,
												DeviceType:   pointy.String(growatt.ParseGrowattDeviceType(growatt.GrowattDeviceTypePcs)),
												DeviceStatus: deviceItem.Status,
												ID:           pointy.String(strconv.Itoa(pointy.IntValue(latestAlert.AlarmCode, 0))),
												Message:      latestAlert.AlarmMessage,
												Owner:        credential.Owner,
											}

											if latestAlert.StartTime != nil {
												if parsed, err := time.Parse("2006-01-02 15:04:05.0", *latestAlert.StartTime); err == nil {
													utcParsed := parsed.UTC()
													alarmItemDoc.AlarmTime = &utcParsed
												}
											}

											docCh <- alarmItemDoc
										}
									}
								}
							}
						}

					}
				case growatt.GrowattDeviceTypePbd:
					if device.Status != nil {
						switch *device.Status {
						case 0: // Offline
							deviceItem.Status = pointy.String(growatt.GrowattDeviceStatusOffline)
						case 1: // Online
							deviceItem.Status = pointy.String(growatt.GrowattDeviceStatusOnline)
						default: // Others
							if *device.Status == 2 { // Stand by
								deviceItem.Status = pointy.String(growatt.GrowattDeviceStatusStandBy)
							} else if *device.Status == 3 { // Failure
								deviceItem.Status = pointy.String(growatt.GrowattDeviceStatusFailure)
							} else {
								deviceItem.Status = pointy.String(growatt.GrowattDeviceStatusOffline)
							}

							if alarms, err := client.GetPbdAlertList(deviceSn, now.Unix()); err == nil {
								if len(alarms) > 0 {
									latestAlert := alarms[0]
									if startTime := latestAlert.StartTime; startTime != nil {
										if pointy.StringValue(device.LastUpdateTime, "0000-00-00")[0:10] == (*startTime)[0:10] {
											alarmItemDoc := model.AlarmItem{
												Timestamp:    now,
												Month:        now.Format("01"),
												Year:         now.Format("2006"),
												MonthYear:    now.Format("01-2006"),
												VendorType:   g.vendorType,
												DataType:     model.DataTypeAlarm,
												Area:         cityArea,
												SiteID:       plantId.SiteID,
												SiteCityName: cityName,
												SiteCityCode: cityCode,
												NodeType:     plantId.NodeType,
												ACPhase:      plantId.ACPhase,
												PlantID:      pointy.String(stationIdStr),
												PlantName:    station.Name,
												Latitude:     plantItem.Latitude,
												Longitude:    plantItem.Longitude,
												Location:     plantItem.Location,
												DeviceID:     pointy.String(strconv.Itoa(deviceId)),
												DeviceSN:     device.DeviceSN,
												DeviceName:   device.DeviceSN,
												DeviceType:   pointy.String(growatt.ParseGrowattDeviceType(growatt.GrowattDeviceTypePbd)),
												DeviceStatus: deviceItem.Status,
												ID:           pointy.String(strconv.Itoa(pointy.IntValue(latestAlert.AlarmCode, 0))),
												Message:      latestAlert.AlarmMessage,
												Owner:        credential.Owner,
											}

											if latestAlert.StartTime != nil {
												if parsed, err := time.Parse("2006-01-02 15:04:05.0", *latestAlert.StartTime); err == nil {
													utcParsed := parsed.UTC()
													alarmItemDoc.AlarmTime = &utcParsed
												}
											}

											docCh <- alarmItemDoc
										}
									}
								}
							}
						}

					}
				default:
				}

				if deviceItem.Status != nil {
					deviceStatusArray = append(deviceStatusArray, *deviceItem.Status)
				}

				docCh <- deviceItem
				g.logger.Info().
					Str("plant_count", fmt.Sprintf("%v/%v", plantCount, plantSize)).
					Str("device_count", fmt.Sprintf("%v/%v", deviceCount, deviceSize)).
					Str("username", credential.Username).
					Str("password", credential.Password).
					Str("plant_id", stationIdStr).
					Str("device_id", deviceSn).
					Any("device", deviceItem).
					Msg("GrowattCollector::Collect() - device item added")

				if deviceTypeRaw == growatt.GrowattDeviceTypeInverter {
					inverterCh <- deviceSn
				}
			}

			plantStatus := growatt.GrowattPlantStatusOnline
			if len(deviceStatusArray) > 0 {
				var offlineCount int
				var alertingCount int

				for _, status := range deviceStatusArray {
					switch status {
					case growatt.GrowattDeviceStatusOffline:
						offlineCount++
					case growatt.GrowattDeviceStatusOnline:
					default:
						alertingCount++
					}
				}

				if alertingCount > 0 {
					plantStatus = growatt.GrowattPlantStatusAlarm
				} else if offlineCount > 0 {
					plantStatus = growatt.GrowattPlantStatusOffline
				}
			} else {
				plantStatus = growatt.GrowattPlantStatusOffline
			}

			plantDeviceStatusCh <- map[string]string{stationIdStr: plantStatus}
			g.logger.Info().
				Str("plant_count", fmt.Sprintf("%v/%v", plantCount, plantSize)).
				Str("username", credential.Username).
				Str("password", credential.Password).
				Str("plant_id", stationIdStr).
				Any("plant", plantItem).
				Msg("GrowattCollector::Collect() - finished ✅")
		}

		wg.Go(producer)
	}

	if r := wg.WaitAndRecover(); r != nil {
		g.logger.Error().Err(r.AsError()).Msg("GrowattCollector::Collect() - failed to collect data")
		return
	}

	doneCh <- true
}

type GrowattInverterProduction struct {
	Total *float64
	Today *float64
}

func (g *GrowattCollector) CalculateInverterProductions(credential *model.GrowattCredential, inverterSNs []string) (map[string]GrowattInverterProduction, error) {
	client := growatt.NewGrowattClient(credential.Username, credential.Token)
	g.logger.Info().Msg("GrowattCollector::CalculateInverterProductions() - getting realtime device batches data")
	resp, err := client.GetRealtimeDeviceBatchesData(inverterSNs)
	if err != nil {
		return nil, err
	}

	if resp.Data == nil {
		return nil, fmt.Errorf("empty response data")
	}

	deviceMap := make(map[string]GrowattInverterProduction)
	g.logger.Info().Int("count", len(resp.Data)).Msg("GrowattCollector::CalculateInverterProductions() - decoding realtime device data")
	for sn, data := range resp.Data {
		if mappedData, ok := data[sn].(map[string]interface{}); ok {
			var decoded growatt.RealtimeDeviceData
			if err := mapstructure.Decode(&mappedData, &decoded); err == nil {
				deviceMap[sn] = GrowattInverterProduction{
					Total: decoded.PowerTotal,
					Today: decoded.PowerToday,
				}
			}
		}
	}

	return deviceMap, nil
}

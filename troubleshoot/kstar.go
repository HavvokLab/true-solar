package troubleshoot

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

type KstarTroubleshoot struct {
	vendorType     string
	solarRepo      repo.SolarRepo
	siteRegionRepo repo.SiteRegionMappingRepo
	siteRegions    []model.SiteRegionMapping
	logger         zerolog.Logger
}

func NewKstarTroubleshoot(solarRepo repo.SolarRepo, siteRegionRepo repo.SiteRegionMappingRepo) *KstarTroubleshoot {
	return &KstarTroubleshoot{
		vendorType:     strings.ToUpper(model.VendorTypeKstar),
		solarRepo:      solarRepo,
		siteRegionRepo: siteRegionRepo,
		siteRegions:    make([]model.SiteRegionMapping, 0),
		logger:         zerolog.New(logger.NewWriter("kstar_troubleshoot.log")).With().Timestamp().Caller().Logger(),
	}
}

func (k *KstarTroubleshoot) ExecuteByRange(
	credential *model.KstarCredential,
	start, end time.Time,
) {
	for date := start; date.Before(end); date = date.AddDate(0, 0, 1) {
		k.Execute(credential, date)
	}
}

func (k *KstarTroubleshoot) Execute(
	credential *model.KstarCredential,
	date time.Time,
) {
	defer func() {
		if r := recover(); r != nil {
			k.logger.Error().Any("recover", r).Msg("KstarTroubleshoot::Execute() - panic")
		}
	}()

	siteRegions, err := k.siteRegionRepo.GetSiteRegionMappings()
	if err != nil {
		k.logger.Error().Err(err).Msg("KstarTroubleshoot::Execute() - failed to get site region mappings")
		return
	}

	k.siteRegions = siteRegions
	documents := make([]any, 0)
	docCh := make(chan any)
	errorCh := make(chan error)
	doneCh := make(chan bool)
	go k.collectByDate(credential, date.UTC(), docCh, errorCh, doneCh)

DONE:
	for {
		select {
		case <-doneCh:
			break DONE
		case err := <-errorCh:
			k.logger.Error().Err(err).Msg("KstarTroubleshoot::Execute() - failed")
		case doc := <-docCh:
			documents = append(documents, doc)
		}
	}

	collectorIndex := fmt.Sprintf("%s-%s", model.SolarIndex, time.Now().Format("2006.01.02"))
	if err := k.solarRepo.BulkIndex(collectorIndex, documents); err != nil {
		k.logger.Error().Err(err).Msg("KstarTroubleshoot::Execute() - failed to bulk index documents")
		return
	}

	k.logger.Info().Int("count", len(documents)).Msg("KstarTroubleshoot::Execute() - bulk index documents success")
	k.logger.Info().Msg("KstarTroubleshoot::Execute() - all goroutines finished")

	close(docCh)
	close(doneCh)
	close(errorCh)
}

func (k *KstarTroubleshoot) collectByDate(
	credential *model.KstarCredential,
	date time.Time,
	docCh chan any,
	errCh chan error,
	doneCh chan bool,
) {
	client := kstar.NewKstarClient(credential.Username, credential.Password)
	plantListResp, err := client.GetPlantList()
	if err != nil {
		errCh <- err
		return
	}

	if len(plantListResp.Data) == 0 {
		errCh <- fmt.Errorf("KstarTroubleshoot::collectByDate() - no plant found")
		return
	}

	plants := plantListResp.Data
	mapPlantIdToDeviceList := make(map[string][]kstar.DeviceItem)
	devices, err := client.GetDeviceList()
	if err != nil {
		errCh <- err
		return
	}

	for _, device := range devices {
		for _, plant := range plants {
			if plant.ID != nil && *plant.ID == *device.PlantID {
				mapPlantIdToDeviceList[*plant.ID] = append(mapPlantIdToDeviceList[*plant.ID], device)
			}
		}
	}

	for _, station := range plants {
		stationId := pointy.StringValue(station.ID, "")
		stationName := pointy.StringValue(station.Name, "")
		plantNameInfo, _ := util.ParsePlantID(stationName)
		cityName, cityCode, cityArea := util.ParseSiteID(k.siteRegions, plantNameInfo.SiteID)

		var currentPower float64
		var totalProduction float64
		var dailyProduction float64
		var monthlyProduction float64
		var yearlyProduction float64
		var location *string

		if station.Latitude != nil && station.Longitude != nil {
			location = pointy.String(fmt.Sprintf("%f,%f", *station.Latitude, *station.Longitude))
		}

		for _, device := range mapPlantIdToDeviceList[stationId] {
			deviceId := pointy.StringValue(device.ID, "")
			deviceInfoResp, err := client.GetHistoricalDeviceData(deviceId, &date)
			if err != nil {
				k.logger.Error().Err(err).Msg("KstarTroubleshoot::collectByDate() - failed to get historical device data")
				errCh <- err
				continue
			}

			if len(deviceInfoResp.Data) == 0 {
				k.logger.Error().Msg("KstarTroubleshoot::collectByDate() - no data found")
				errCh <- fmt.Errorf("KstarTroubleshoot::collectByDate() - no data found")
				continue
			}

			for _, data := range deviceInfoResp.Data {
				currentPower += pointy.Float64Value(data.PowerInter, 0)
				totalProduction += pointy.Float64Value(data.TotalGeneration, 0)
				dailyProduction += pointy.Float64Value(data.DayGeneration, 0)
				monthlyProduction += pointy.Float64Value(data.MonthGeneration, 0)
				yearlyProduction += pointy.Float64Value(data.YearGeneration, 0)
			}

			plantItem := model.PlantItem{
				Timestamp:         date,
				Month:             date.Format("01"),
				Year:              date.Format("2006"),
				MonthYear:         date.Format("01-2006"),
				VendorType:        k.vendorType,
				DataType:          model.DataTypePlant,
				Area:              cityArea,
				SiteID:            plantNameInfo.SiteID,
				SiteCityCode:      cityCode,
				SiteCityName:      cityName,
				NodeType:          plantNameInfo.NodeType,
				ACPhase:           plantNameInfo.ACPhase,
				ID:                station.ID,
				Name:              station.Name,
				Latitude:          station.Latitude,
				Longitude:         station.Longitude,
				Location:          location,
				LocationAddress:   station.Address,
				InstalledCapacity: station.InstalledCapacity,
				TotalCO2:          nil,
				MonthlyCO2:        nil,
				TotalSavingPrice:  pointy.Float64(totalProduction * pointy.Float64Value(station.ElectricPrice, 0)),
				Currency:          station.ElectricUnit,
				CurrentPower:      pointy.Float64(currentPower / 1000), // W to kW
				TotalProduction:   pointy.Float64(totalProduction),
				DailyProduction:   pointy.Float64(dailyProduction),
				MonthlyProduction: pointy.Float64(monthlyProduction),
				YearlyProduction:  pointy.Float64(yearlyProduction),
				PlantStatus:       pointy.String("UNKNOWN"),
				Owner:             credential.Owner,
			}

			docCh <- plantItem
		}
	}

	doneCh <- true
}

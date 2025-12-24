package troubleshoot

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
	"github.com/rs/zerolog"
	"go.openly.dev/pointy"
)

type GrowattTroubleshoot struct {
	vendorType     string
	solarRepo      repo.SolarRepo
	siteRegionRepo repo.SiteRegionMappingRepo
	siteRegions    []model.SiteRegionMapping
	logger         zerolog.Logger
}

func NewGrowattTroubleshoot(solarRepo repo.SolarRepo, siteRegionRepo repo.SiteRegionMappingRepo) *GrowattTroubleshoot {
	return &GrowattTroubleshoot{
		vendorType:     strings.ToUpper(model.VendorTypeGrowatt),
		solarRepo:      solarRepo,
		siteRegionRepo: siteRegionRepo,
		siteRegions:    make([]model.SiteRegionMapping, 0),
		logger:         zerolog.New(logger.NewWriter("growatt_troubleshoot.log")).With().Timestamp().Caller().Logger(),
	}
}

func (g *GrowattTroubleshoot) ExecuteByRange(
	credential *model.GrowattCredential,
	start, end time.Time,
) {
	for date := start; date.Before(end); date = date.AddDate(0, 0, 1) {
		g.Execute(credential, date)
	}
}

func (g *GrowattTroubleshoot) Execute(
	credential *model.GrowattCredential,
	date time.Time,
) {
	defer func() {
		if r := recover(); r != nil {
			g.logger.Error().Any("recover", r).Msg("GrowattTroubleshoot::Execute() - panic")
		}
	}()

	siteRegions, err := g.siteRegionRepo.GetSiteRegionMappings()
	if err != nil {
		g.logger.Error().Err(err).Msg("GrowattTroubleshoot::Execute() - failed to get site region mappings")
		return
	}

	g.siteRegions = siteRegions
	documents := make([]any, 0)
	docCh := make(chan any)
	errorCh := make(chan error)
	doneCh := make(chan bool)
	go g.collectByDate(credential, date.UTC(), docCh, errorCh, doneCh)

DONE:
	for {
		select {
		case <-doneCh:
			break DONE
		case err := <-errorCh:
			g.logger.Error().Err(err).Msg("GrowattTroubleshoot::Execute() - failed")
			return
		case doc := <-docCh:
			documents = append(documents, doc)
		}
	}

	collectorIndex := fmt.Sprintf("%s-%s", model.SolarIndex, date.Format("2006.01.02"))
	if err := g.solarRepo.BulkIndex(collectorIndex, documents); err != nil {
		g.logger.Error().Err(err).Msg("GrowattTroubleshoot::Execute() - failed to bulk index documents")
		return
	}

	g.logger.Info().Int("count", len(documents)).Msg("GrowattTroubleshoot::Execute() - bulk index documents success")
	g.logger.Info().Msg("GrowattTroubleshoot::Execute() - all goroutines finished")

	close(docCh)
	close(doneCh)
	close(errorCh)
}

func (g *GrowattTroubleshoot) collectByDate(
	credential *model.GrowattCredential,
	date time.Time,
	docCh chan any,
	errCh chan error,
	doneCh chan bool,
) {
	startCollectTime := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, date.Location())
	endCollectTime := startCollectTime.AddDate(0, 0, 1).Add(-time.Second)

	client := growatt.NewGrowattClient(credential.Username, credential.Token)
	plantList, err := client.GetPlantList()
	if err != nil {
		g.logger.Error().Err(err).Msg("GrowattTroubleshoot::collectByDate() - failed to get plant list")
		return
	}

	plantSize := len(plantList)
	for i, plant := range plantList {
		currentPlant := i + 1

		if plant.PlantID == nil {
			g.logger.Warn().Msg("GrowattTroubleshoot::collectByDate() - plant id is nil")
			continue
		}

		plantId := pointy.IntValue(plant.PlantID, 0)
		basicInfo, err := client.GetPlantBasicInfo(plantId)
		if err != nil {
			g.logger.Warn().Err(err).Msg("GrowattTroubleshoot::collectByDate() - failed to get plant basic info")
			continue
		}

		if basicInfo == nil || basicInfo.Data == nil {
			g.logger.Warn().Msg("GrowattTroubleshoot::collectByDate() - plant basic info is nil")
			continue
		}

		plantIdStr := strconv.Itoa(plantId)
		plantName := pointy.StringValue(plant.Name, util.EmptyString)
		plantIdentity, _ := util.ParsePlantID(plantName)
		cityName, cityCode, cityArea := util.ParseSiteID(g.siteRegions, plantIdentity.SiteID)

		var monthlyProduction *float64
		var yearlyProduction *float64
		yearlyEnergies, err := client.GetHistoricalPlantPowerGeneration(plantId, startCollectTime.Unix(), endCollectTime.Unix(), "year")
		if err != nil {
			g.logger.Error().Err(err).Msg("GrowattTroubleshoot::collectByDate() - failed to get historical plant power generation")
			errCh <- err
			continue
		}

		if len(yearlyEnergies) > 0 {
			yearlyEnergy := pointy.StringValue(yearlyEnergies[0].Energy, "0")
			parsed, _ := strconv.ParseFloat(yearlyEnergy, 64)
			yearlyProduction = &parsed
		}

		monthlyEnergies, err := client.GetHistoricalPlantPowerGeneration(plantId, startCollectTime.Unix(), endCollectTime.Unix(), "month")
		if err != nil {
			g.logger.Error().Err(err).Msg("GrowattTroubleshoot::collectByDate() - failed to get historical plant power generation")
			errCh <- err
			continue
		}

		if len(monthlyEnergies) > 0 {
			monthlyEnergy := pointy.StringValue(monthlyEnergies[0].Energy, "0")
			parsed, _ := strconv.ParseFloat(monthlyEnergy, 64)
			monthlyProduction = &parsed
		}

		dailyEnergies, err := client.GetHistoricalPlantPowerGeneration(plantId, startCollectTime.Unix(), endCollectTime.Unix(), "day")
		if err != nil {
			g.logger.Error().Err(err).Msg("GrowattTroubleshoot::collectByDate() - failed to get historical plant power generation")
			errCh <- err
			continue
		}

		if len(dailyEnergies) == 0 {
			continue
		}

		var location string
		if plant.Latitude != nil && plant.Longitude != nil {
			location = fmt.Sprintf("%v,%v", *plant.Latitude, *plant.Longitude)
		}

		var lat, long *float64
		if plant.Latitude != nil {
			parsed, _ := strconv.ParseFloat(*plant.Latitude, 64)
			lat = &parsed
		}

		if plant.Longitude != nil {
			parsed, _ := strconv.ParseFloat(*plant.Longitude, 64)
			long = &parsed
		}

		var installedCapacity *float64
		var currency *string
		if dataLoggerResp, err := client.GetPlantDataLoggerInfo(plantId); err == nil {
			if dataLoggerResp.Data != nil {
				if dataLoggerResp.Data.PeakPowerActual != nil {
					actualData := dataLoggerResp.Data.PeakPowerActual

					if actualData.NominalPower != nil {
						installedCapacity = pointy.Float64(pointy.Float64Value(actualData.NominalPower, 0) / 1000.0)
					} else if plantIdentity.Capacity != 0 {
						installedCapacity = pointy.Float64(plantIdentity.Capacity)
					}

					if actualData.FormulaMoneyUnitID != nil {
						currency = pointy.String(strings.ToUpper(pointy.StringValue(actualData.FormulaMoneyUnitID, "0")))
					}
				}
			}
		}

		dailySize := len(dailyEnergies)
		for j, daily := range dailyEnergies {
			dailyCount := j + 1
			g.logger.Info().Str("daily_count", fmt.Sprintf("%d/%d", dailyCount, dailySize)).Any("daily", daily).Msg("GrowattTroubleshoot::collectByDate() - start loop")
			dailyEnergy := pointy.StringValue(daily.Energy, "0")
			parsed, _ := strconv.ParseFloat(dailyEnergy, 64)
			plantItem := model.PlantItem{
				Timestamp:         date,
				Month:             date.Format("01"),
				Year:              date.Format("2006"),
				MonthYear:         date.Format("01-2006"),
				VendorType:        g.vendorType,
				DataType:          model.DataTypePlant,
				Area:              cityArea,
				SiteID:            plantIdentity.SiteID,
				SiteCityName:      cityName,
				SiteCityCode:      cityCode,
				NodeType:          plantIdentity.NodeType,
				ACPhase:           plantIdentity.ACPhase,
				ID:                pointy.String(plantIdStr),
				Name:              &plantName,
				PlantStatus:       pointy.String("UNKNOWN"),
				Owner:             credential.Owner,
				Latitude:          lat,
				Longitude:         long,
				Location:          &location,
				LocationAddress:   basicInfo.Data.City,
				YearlyProduction:  yearlyProduction,
				MonthlyProduction: monthlyProduction,
				DailyProduction:   &parsed,
				InstalledCapacity: installedCapacity,
				Currency:          currency,
				MonthlyCO2:        pointy.Float64(pointy.Float64Value(monthlyProduction, 0) * 2.079),
			}

			docCh <- plantItem
			g.logger.Info().
				Str("username", credential.Username).
				Int("plant_id", plantId).
				Str("plant_count", fmt.Sprintf("%d/%d", currentPlant, plantSize)).
				Str("daily_count", fmt.Sprintf("%d/%d", dailyCount, dailySize)).
				Any("plant_item", plantItem).
				Msg("GrowattTroubleshoot::collectByDate() - collect plant item")
		}
	}

	doneCh <- true
}

package troubleshoot

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

type SolarmanTroubleshoot struct {
	vendorType     string
	solarRepo      repo.SolarRepo
	siteRegionRepo repo.SiteRegionMappingRepo
	siteRegions    []model.SiteRegionMapping
	logger         zerolog.Logger
}

func NewSolarmanTroubleshoot(solarRepo repo.SolarRepo, siteRegionRepo repo.SiteRegionMappingRepo) *SolarmanTroubleshoot {
	return &SolarmanTroubleshoot{
		vendorType:     strings.ToUpper(model.VendorTypeInvt),
		solarRepo:      solarRepo,
		siteRegionRepo: siteRegionRepo,
		siteRegions:    make([]model.SiteRegionMapping, 0),
		logger:         zerolog.New(logger.NewWriter("solarman_troubleshoot.log")).With().Timestamp().Caller().Logger(),
	}
}

func (s *SolarmanTroubleshoot) ExecuteByRange(
	credential *model.SolarmanCredential,
	start, end time.Time,
) {
	for date := start; date.Before(end); date = date.AddDate(0, 0, 1) {
		s.Execute(credential, date)
	}
}

func (s *SolarmanTroubleshoot) Execute(
	credential *model.SolarmanCredential,
	date time.Time,
) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.Error().Any("recover", r).Msg("SolarmanTroubleshoot::Execute() - panic")
		}
	}()

	siteRegions, err := s.siteRegionRepo.GetSiteRegionMappings()
	if err != nil {
		s.logger.Error().Err(err).Msg("SolarmanTroubleshoot::Execute() - failed to get site region mappings")
		return
	}

	s.siteRegions = siteRegions
	documents := make([]interface{}, 0)
	documentCh := make(chan interface{})
	errorCh := make(chan error)
	doneCh := make(chan bool)
	go s.collectByDate(credential, date, documentCh, errorCh, doneCh)

DONE:
	for {
		select {
		case <-doneCh:
			break DONE
		case err := <-errorCh:
			s.logger.Error().Err(err).Msg("SolarmanTroubleshoot::Execute() - failed")
			return
		case doc := <-documentCh:
			documents = append(documents, doc)
		}
	}

	collectorIndex := fmt.Sprintf("%s-%s", model.SolarIndex, time.Now().Format("2006.01.02"))
	if err := s.solarRepo.BulkIndex(collectorIndex, documents); err != nil {
		s.logger.Error().Err(err).Msg("SolarmanTroubleshoot::Execute() - failed to bulk index documents")
		return
	}

	s.logger.Info().Int("count", len(documents)).Msg("SolarmanTroubleshoot::Execute() - bulk index documents success")
	s.logger.Info().Msg("SolarmanTroubleshoot::Execute() - all goroutines finished")

	close(documentCh)
	close(doneCh)
	close(errorCh)
}

func (s *SolarmanTroubleshoot) collectByDate(
	credential *model.SolarmanCredential,
	date time.Time,
	docCh chan any,
	errCh chan error,
	doneCh chan bool,
) {
	client := solarman.NewSolarmanClient(credential.Username, credential.Password, credential.AppID, credential.AppSecret)
	tokenResp, err := client.GetBasicToken()
	if err != nil {
		s.logger.Error().Err(err).Msg("SolarmanTroubleshoot::collectByDate() - failed to get basic token")
		return
	}

	if tokenResp.AccessToken == nil {
		s.logger.Error().
			Str("username", credential.Username).
			Msg("SolarmanTroubleshoot::collectByDate() - failed to get basic token")
		errCh <- fmt.Errorf("failed to get basic token")
		return
	}
	client.SetAccessToken(pointy.StringValue(tokenResp.AccessToken, util.EmptyString))

	userInfoResp, err := client.GetUserInfo()
	if err != nil {
		s.logger.Error().
			Str("username", credential.Username).
			Err(err).
			Msg("SolarmanTroubleshoot::collectByDate() - failed to get user info")
		errCh <- err
		return
	}

	wg := conc.NewWaitGroup()
	for _, company := range userInfoResp.OrgInfoList {
		company := company
		credential := credential

		producer := func() {
			client := solarman.NewSolarmanClient(credential.Username, credential.Password, credential.AppID, credential.AppSecret)
			companyId := pointy.IntValue(company.CompanyID, 0)
			tokenResp, err := client.GetBusinessToken(companyId)
			if err != nil {
				s.logger.Error().
					Str("username", credential.Username).
					Err(err).
					Msg("SolarmanTroubleshoot::collectByDate() - failed to get business token")
			}

			if tokenResp.AccessToken == nil {
				s.logger.Error().
					Str("username", credential.Username).
					Msg("SolarmanTroubleshoot::collectByDate() - failed to get business token")
			}
			accessToken := pointy.StringValue(tokenResp.AccessToken, util.EmptyString)
			if accessToken == util.EmptyString {
				s.logger.Warn().
					Str("username", credential.Username).
					Msg("SolarmanTroubleshoot::collectByDate() - failed to get business token")
				return
			}
			client.SetAccessToken(accessToken)

			plantList, err := client.GetPlantList()
			if err != nil {
				s.logger.Error().
					Str("username", credential.Username).
					Err(err).
					Msg("SolarmanTroubleshoot::collectByDate() - failed to get plant list")
				return
			}

			plantSize := len(plantList)
			for i, station := range plantList {
				currentPlant := i + 1
				if station == nil {
					s.logger.Warn().
						Str("username", credential.Username).
						Msgf("SolarmanTroubleshoot::collectByDate() - plant %d is nil", currentPlant)
					continue
				}

				stationId := pointy.IntValue(station.ID, 0)
				stationName := pointy.StringValue(station.Name, util.EmptyString)
				plantId, _ := util.ParsePlantID(stationName)
				cityName, cityCode, cityArea := util.ParseSiteID(s.siteRegions, plantId.SiteID)
				plantItem := model.PlantItem{
					Timestamp:         date,
					Month:             date.Format("01"),
					Year:              date.Format("2006"),
					MonthYear:         date.Format("01-2006"),
					VendorType:        s.vendorType,
					DataType:          model.DataTypePlant,
					Area:              cityArea,
					SiteID:            plantId.SiteID,
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
					CurrentPower:      pointy.Float64(0),
				}

				var (
					// mergedElectricPrice         *float64
					// totalPowerGenerationKWh     *float64
					sumYearlyPowerGenerationKWh *float64
				)

				if plantItem.Latitude != nil && plantItem.Longitude != nil {
					plantItem.Location = pointy.String(fmt.Sprintf("%f,%f", *plantItem.Latitude, *plantItem.Longitude))
				}

				if station.CreatedDate != nil {
					parsed := time.Unix(int64(*station.CreatedDate), 0)
					plantItem.CreatedDate = &parsed
				}

				if plantInfoResp, err := client.GetPlantBaseInfo(stationId); err == nil {
					plantItem.Currency = plantInfoResp.Currency
					// mergedElectricPrice = plantInfoResp.MergeElectricPrice
				}

				if resp, err := client.GetHistoricalPlantData(
					stationId,
					solarman.TimeTypeDay,
					date.Unix(),
					date.Unix(),
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
					date.Unix(),
					date.Unix(),
				); err == nil && len(resp.StationDataItems) > 0 {
					plantItem.MonthlyProduction = resp.StationDataItems[0].GenerationValue
				}

				startTime := time.Date(2015, date.Month(), date.Day(), 0, 0, 0, 0, time.Local)
				if resp, err := client.GetHistoricalPlantData(
					stationId,
					solarman.TimeTypeYear,
					startTime.Unix(),
					date.Unix(),
				); err == nil && len(resp.StationDataItems) > 0 {
					for _, item := range resp.StationDataItems {
						year := pointy.IntValue(item.Year, 0)
						if year == date.Year() {
							plantItem.YearlyProduction = item.GenerationValue
						}

						generationValue := pointy.Float64Value(item.GenerationValue, 0.0)
						sumYearlyPowerGenerationKWh = pointy.Float64(
							pointy.Float64Value(sumYearlyPowerGenerationKWh, 0.0) + generationValue,
						)
					}
				}

				docCh <- plantItem
				s.logger.Info().
					Str("username", credential.Username).
					Str("plant_count", fmt.Sprintf("%d/%d", currentPlant, plantSize)).
					Str("plant_id", strconv.Itoa(stationId)).
					Str("plant_name", stationName).
					Any("plant_item", plantItem).
					Msg("SolarmanTroubleshoot::collectByDate() - collected plant item")
			}
		}

		wg.Go(producer)
	}

	if r := wg.WaitAndRecover(); r != nil {
		s.logger.Error().
			Str("username", credential.Username).
			Err(r.AsError()).
			Msg("SolarmanTroubleshoot::collectByDate() - failed to collect plant item")
		return
	}

	doneCh <- true
}

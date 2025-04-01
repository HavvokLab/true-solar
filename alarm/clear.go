package alarm

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/HavvokLab/true-solar/infra"
	"github.com/HavvokLab/true-solar/model"
	"github.com/HavvokLab/true-solar/pkg/logger"
	"github.com/HavvokLab/true-solar/pkg/util"
	"github.com/HavvokLab/true-solar/repo"
	"github.com/rs/zerolog"
	"go.openly.dev/pointy"
)

type ClearAlarm struct {
	solarRepo repo.SolarRepo
	snmp      *infra.SnmpOrchestrator
	logger    zerolog.Logger
}

func NewClearAlarm(solarRepo repo.SolarRepo, snmp *infra.SnmpOrchestrator) *ClearAlarm {
	return &ClearAlarm{
		solarRepo: solarRepo,
		snmp:      snmp,
		logger:    zerolog.New(logger.NewWriter("clear_alarm.log")).With().Timestamp().Caller().Logger(),
	}
}

func (s *ClearAlarm) Run() error {
	now := time.Now()
	index := fmt.Sprintf("solarcell-%v", now.AddDate(0, 0, -1).Format("2006.01.02"))
	data, err := s.solarRepo.GetUniquePlantByIndex(index)
	if err != nil {
		s.logger.Error().Err(err).Msg("error getting unique plant by index")
		return err
	}

	for _, item := range data {
		if item == nil {
			continue
		}

		mapData := map[string]interface{}{}
		if topHits, found := item.Aggregations.TopHits("data"); found {
			if topHits.Hits != nil {
				hit := topHits.Hits.Hits[0]
				if hit != nil {
					if err := util.Recast(hit.Source, &mapData); err != nil {
						s.logger.Error().Err(err).Msg("error recasting hit source")
						return err
					}
				}
			}
		}

		plant := new(model.PlantItem)
		if val, ok := mapData["name"]; ok {
			if parsed, ok := val.(string); ok {
				plant.Name = &parsed
			} else {
				continue
			}
		} else {
			continue
		}

		if val, ok := mapData["area"]; ok {
			if parsed, ok := val.(string); ok {
				plant.Area = parsed
			}
		}

		if val, ok := mapData["vendor_type"]; ok {
			if parsed, ok := val.(string); ok {
				plant.VendorType = parsed
			} else {
				continue
			}
		} else {
			continue
		}

		if val, ok := mapData["installed_capacity"]; ok {
			if parsed, ok := val.(float64); ok {
				plant.InstalledCapacity = &parsed
			}
		}

		if val, ok := mapData["owner"]; ok {
			if parsed, ok := val.(string); ok {
				plant.Owner = parsed
			} else {
				plant.Owner = string(model.OwnerTrue)
			}
		} else {
			plant.Owner = string(model.OwnerTrue)
		}

		if val, ok := mapData["location"]; ok {
			if parsed, ok := val.(string); ok {
				if len(parsed) > 0 {
					parts := strings.Split(parsed, ",")
					if len(parts) == 2 {
						lat, err := strconv.ParseFloat(parts[0], 64)
						if err == nil {
							plant.Latitude = &lat
						}

						long, err := strconv.ParseFloat(parts[0], 64)
						if err == nil {
							plant.Longitude = &long
						}
					}
				}
			}
		}

		payloads, err := s.Payload(&now, plant)
		if err != nil {
			s.logger.Error().Err(err).Msg("error getting payload")
			continue
		}

		date := now.Format("2006-01-02 15:04:05")
		severity := infra.ClearSeverity
		for _, payload := range payloads {
			s.snmp.SendTrap(payload.PlantName, payload.AlarmName, payload.Payload, severity, date)
		}

	}
	return nil
}

type ClearAlarmPayload struct {
	PlantName  string
	AlarmName  string
	Payload    string
	Date       string
	VendorType string
}

func (s *ClearAlarm) Payload(date *time.Time, plant *model.PlantItem) ([]*ClearAlarmPayload, error) {
	var vendorType string
	alarmNames := make([]string, 0)
	switch strings.ToLower(plant.VendorType) {
	case model.VendorTypeGrowatt:
		vendorType = "Growatt"
		alarmNames = append(alarmNames, "Growatt-Solarcell-Inverter_Error_0")
	case model.VendorTypeHuawei:
		vendorType = "HUA"
		alarmNames = append(alarmNames, "Huawei-Solarcell-HUW_Disconnect")
	case model.VendorTypeKstar:
		vendorType = "Kstar"
		alarmNames = append(alarmNames, "Huawei-Solarcell-Disconnect")
	case model.VendorTypeInvt:
		vendorType = "INVT-Ipanda"
		alarmNames = append(alarmNames, "INVT-Solarcell-INVERTER_Disconnect")
		alarmNames = append(alarmNames, "INVT-Solarcell-COLLECTOR_Disconnect")
	case model.VendorTypeSolarman:
		vendorType = "INVT-Ipanda"
		alarmNames = append(alarmNames, "INVT-Solarcell-INVERTER_Disconnect")
		alarmNames = append(alarmNames, "INVT-Solarcell-COLLECTOR_Disconnect")
	default:
	}

	if util.IsEmpty(vendorType) {
		err := fmt.Errorf("vendor type (%s) not supported", plant.VendorType)
		return nil, err
	}

	plantName := pointy.StringValue(plant.Name, "")
	if strings.Contains(plantName, "ATV") {
		err := fmt.Errorf("plant name (%s) not supported", plantName)
		return nil, err
	}

	payloads := make([]*ClearAlarmPayload, 0)
	for _, alarmName := range alarmNames {
		payload := &ClearAlarmPayload{
			PlantName: plantName,
			AlarmName: alarmName,
			Payload: fmt.Sprintf("%v, %v, Clear all alarms, Date:%v",
				vendorType,
				plantName,
				date.Format("2006-01-02 15:04:05"),
			),
		}

		payloads = append(payloads, payload)
	}

	return payloads, nil
}

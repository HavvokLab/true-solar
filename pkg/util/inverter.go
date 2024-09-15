package util

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/HavvokLab/true-solar/model"
	"go.openly.dev/pointy"
)

var (
	acPhaseRegexp        = regexp.MustCompile(`\d+`)
	capacityRegexp       = regexp.MustCompile(`(\d*\.?\d*)([^\d]*)$`)
	capacityNumberRegexp = regexp.MustCompile(`(\d*\.?\d*)`)
	cityCodeRegexp       = regexp.MustCompile(`^[A-Z]{3}`)
)

type ID struct {
	Original string
	SiteID   string
	NodeType string
	ACPhase  int // 1 or 3
	DC       string
	Capacity float64
}

// ParsePlantID returns plant id object parsed from formatted string
func ParsePlantID(raw string) (ID, error) {
	plantID := strings.TrimSpace(raw)
	id := ID{Original: plantID}
	plantIDSplit := strings.Split(plantID, "-")

	// Format: MHS7143
	if len(plantIDSplit) == 1 {
		id.SiteID = plantIDSplit[0]
		return id, nil
	}

	// Format: BBO05-AN-3P-19.68
	if len(plantIDSplit) == 4 {
		id.SiteID = plantIDSplit[0]
		id.NodeType = plantIDSplit[1]
		if len(id.NodeType) > 2 {
			id.NodeType = id.NodeType[0:2]
		}

		if acPhaseResults := acPhaseRegexp.FindAllString(plantIDSplit[2], 1); len(acPhaseResults) == 1 {
			acPhase, err := strconv.Atoi(acPhaseResults[0])
			if err != nil {
				return id, err
			}
			id.ACPhase = acPhase
		}

		if dcSplit := acPhaseRegexp.Split(plantIDSplit[2], 2); len(dcSplit) == 2 {
			id.DC = dcSplit[1]
		}

		rawCapacity := plantIDSplit[3]

		// Find only numbers if it is in format: BBO05-AN-3P-19.68kw
		if capacityNumberResults := capacityNumberRegexp.FindAllString(rawCapacity, 1); len(capacityNumberResults) == 1 {
			rawCapacity = capacityNumberResults[0]
		}

		capacity, err := strconv.ParseFloat(rawCapacity, 64)
		if err != nil {
			return id, err
		}
		id.Capacity = capacity

		return id, nil
	}

	// Format: BBO05-AN-3P-19.68-A
	if len(plantIDSplit) == 5 {
		id.SiteID = plantIDSplit[0]
		id.NodeType = plantIDSplit[1]
		if len(id.NodeType) > 2 {
			id.NodeType = id.NodeType[0:2]
		}

		if acPhaseResults := acPhaseRegexp.FindAllString(plantIDSplit[2], 1); len(acPhaseResults) == 1 {
			acPhase, err := strconv.Atoi(acPhaseResults[0])
			if err != nil {
				return id, err
			}
			id.ACPhase = acPhase
		}

		if dcSplit := acPhaseRegexp.Split(plantIDSplit[2], 2); len(dcSplit) == 2 {
			id.DC = dcSplit[1]
		}

		rawCapacity := plantIDSplit[3]

		// Find only numbers if it is in format: BBO05-AN-3P-19.68kw-A
		if capacityNumberResults := capacityNumberRegexp.FindAllString(rawCapacity, 1); len(capacityNumberResults) == 1 {
			rawCapacity = capacityNumberResults[0]
		}

		capacity, err := strconv.ParseFloat(rawCapacity, 64)
		if err != nil {
			return id, err
		}
		id.Capacity = capacity

		return id, nil
	}

	// Format: BBO05-AN3P19.68 Or PKK11-AGN
	if len(plantIDSplit) == 2 {
		id.SiteID = plantIDSplit[0]
		if acPhaseResults := acPhaseRegexp.FindAllString(plantIDSplit[1], 1); len(acPhaseResults) == 1 {
			acPhase, err := strconv.Atoi(acPhaseResults[0])
			if err != nil {
				return id, err
			}
			id.ACPhase = acPhase
		}

		acPhaseSplit := acPhaseRegexp.Split(plantIDSplit[1], 2)

		// Format: PKK11-AGN
		if len(acPhaseSplit) == 1 {
			id.NodeType = acPhaseSplit[0]
			if len(id.NodeType) > 2 {
				id.NodeType = id.NodeType[0:2]
			}
			return id, nil
		}

		// Format: BBO05-AN3P19.68
		if len(acPhaseSplit) == 2 {
			id.NodeType = acPhaseSplit[0]
			if len(id.NodeType) > 2 {
				id.NodeType = id.NodeType[0:2]
			}

			if capacityResults := capacityRegexp.FindAllString(acPhaseSplit[1], 1); len(capacityResults) == 1 {
				rawCapacity := capacityResults[0]

				// Find only numbers if it is in format: BBO05-AN3P19.68kw
				if capacityNumberResults := capacityNumberRegexp.FindAllString(rawCapacity, 1); len(capacityNumberResults) == 1 {
					rawCapacity = capacityNumberResults[0]
				}

				capacity, err := strconv.ParseFloat(rawCapacity, 64)
				if err != nil {
					return id, err
				}
				id.Capacity = capacity
			}

			if dcSplit := capacityRegexp.Split(acPhaseSplit[1], 2); len(dcSplit) == 2 {
				id.DC = dcSplit[0]
			}

			return id, nil
		}
	}

	// Format: BBO05-AN3P19.68-A Or PKK11-AGN-A
	if len(plantIDSplit) == 3 {
		id.SiteID = plantIDSplit[0]

		if acPhaseResults := acPhaseRegexp.FindAllString(plantIDSplit[1], 1); len(acPhaseResults) == 1 {
			acPhase, err := strconv.Atoi(acPhaseResults[0])
			if err != nil {
				return id, err
			}
			id.ACPhase = acPhase
		}

		acPhaseSplit := acPhaseRegexp.Split(plantIDSplit[1], 2)

		// Format: PKK11-AGN-A
		if len(acPhaseSplit) == 1 {
			id.NodeType = acPhaseSplit[0]
			if len(id.NodeType) > 2 {
				id.NodeType = id.NodeType[0:2]
			}
			return id, nil
		}

		// Format: BBO05-AN3P19.68-A
		if len(acPhaseSplit) == 2 {
			id.NodeType = acPhaseSplit[0]
			if len(id.NodeType) > 2 {
				id.NodeType = id.NodeType[0:2]
			}

			if capacityResults := capacityRegexp.FindAllString(acPhaseSplit[1], 1); len(capacityResults) == 1 {
				rawCapacity := capacityResults[0]

				// Find only numbers if it is in format: BBO05-AN3P19.68kw-A
				if capacityNumberResults := capacityNumberRegexp.FindAllString(rawCapacity, 1); len(capacityNumberResults) == 1 {
					rawCapacity = capacityNumberResults[0]
				}

				capacity, err := strconv.ParseFloat(rawCapacity, 64)
				if err != nil {
					return id, err
				}
				id.Capacity = capacity
			}

			if dcSplit := capacityRegexp.Split(acPhaseSplit[1], 2); len(dcSplit) == 2 {
				id.DC = dcSplit[0]
			}

			return id, nil
		}
	}

	return id, nil
}

// ParseSiteID returns city name, code, area from site id
func ParseSiteID(siteRegions []model.SiteRegionMapping, siteID string) (cityName string, cityCode string, area string) {
	upperedSiteID := strings.ToUpper(siteID)

	// Whole site id, e.g. BKK00001
	for _, siteRegion := range siteRegions {
		if strings.ToUpper(siteRegion.Code) == upperedSiteID {
			return siteRegion.Name, siteRegion.Code, pointy.StringValue(siteRegion.Area, "")
		}
	}

	// First three letters, e.g. BKK
	if code := cityCodeRegexp.Find([]byte(upperedSiteID)); code != nil {
		codeStr := strings.ToUpper(string(code))
		for _, siteRegion := range siteRegions {
			if strings.ToUpper(siteRegion.Code) == codeStr {
				return siteRegion.Name, siteRegion.Code, pointy.StringValue(siteRegion.Area, "")
			}
		}
	}

	return "", "", ""
}

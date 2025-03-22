package model

import "time"

const (
	SolarIndex            = "solarcell"
	SiteStationIndex      = "site-station"
	AlarmIndex            = "alarm"
	PerformanceAlarmIndex = "performance-alarm"
)

const (
	DataTypePlant  = "PLANT"
	DataTypeDevice = "DEVICE"
	DataTypeAlarm  = "ALARM"
)

const (
	OwnerTrue     = "TRUE"
	OwnerAltervim = "ALTERVIM"
)

const (
	VendorTypeHuawei   = "huawei"
	VendorTypeKstar    = "kstar"
	VendorTypeGrowatt  = "growatt"
	VendorTypeInvt     = "invt"
	VendorTypeSolarman = "solarman"
)

type PlantItem struct {
	Timestamp         time.Time  `json:"@timestamp"`
	Month             string     `json:"month"`
	Year              string     `json:"year"`
	MonthYear         string     `json:"month_year"`
	VendorType        string     `json:"vendor_type"`
	DataType          string     `json:"data_type"`
	Area              string     `json:"area"`
	SiteID            string     `json:"site_id"`
	SiteCityName      string     `json:"site_city_name"`
	SiteCityCode      string     `json:"site_city_code"`
	NodeType          string     `json:"node_type"`
	ACPhase           int        `json:"ac_phase"`
	ID                *string    `json:"id"`
	Name              *string    `json:"name"`
	Latitude          *float64   `json:"lat"`
	Longitude         *float64   `json:"lng"`
	Location          *string    `json:"location"`
	LocationAddress   *string    `json:"location_address"`
	CreatedDate       *time.Time `json:"created_date"`
	InstalledCapacity *float64   `json:"installed_capacity"`
	TotalCO2          *float64   `json:"total_co2"`
	MonthlyCO2        *float64   `json:"monthly_co2"`
	TotalSavingPrice  *float64   `json:"total_saving_price"`
	Currency          *string    `json:"currency"`
	CurrentPower      *float64   `json:"current_power"`
	TotalProduction   *float64   `json:"total_production"`
	DailyProduction   *float64   `json:"daily_production"`
	MonthlyProduction *float64   `json:"monthly_production"`
	YearlyProduction  *float64   `json:"yearly_production"`
	PlantStatus       *string    `json:"plant_status"`
	Owner             string     `json:"owner"`
}

type DeviceItem struct {
	Timestamp              time.Time  `json:"@timestamp"`
	Month                  string     `json:"month"`
	Year                   string     `json:"year"`
	MonthYear              string     `json:"month_year"`
	VendorType             string     `json:"vendor_type"`
	DataType               string     `json:"data_type"`
	Area                   string     `json:"area"`
	SiteID                 string     `json:"site_id"`
	SiteCityName           string     `json:"site_city_name"`
	SiteCityCode           string     `json:"site_city_code"`
	NodeType               string     `json:"node_type"`
	ACPhase                int        `json:"ac_phase"`
	PlantID                *string    `json:"plant_id"`
	PlantName              *string    `json:"plant_name"`
	Latitude               *float64   `json:"lat"`
	Longitude              *float64   `json:"lng"`
	Location               *string    `json:"location"`
	ID                     *string    `json:"id"`
	SN                     *string    `json:"sn"`
	Name                   *string    `json:"name"`
	DeviceType             *string    `json:"device_type"`
	Status                 *string    `json:"status"`
	TotalPowerGeneration   *float64   `json:"total_power_generation"`
	DailyPowerGeneration   *float64   `json:"daily_power_generation"`
	MonthlyPowerGeneration *float64   `json:"monthly_power_generation"`
	YearlyPowerGeneration  *float64   `json:"yearly_power_generation"`
	LastUpdateTime         *time.Time `json:"last_update_time"`
	Owner                  string     `json:"owner"`
}

type AlarmItem struct {
	Timestamp    time.Time  `json:"@timestamp"`
	Month        string     `json:"month"`
	Year         string     `json:"year"`
	MonthYear    string     `json:"month_year"`
	VendorType   string     `json:"vendor_type"`
	DataType     string     `json:"data_type"`
	Area         string     `json:"area"`
	SiteID       string     `json:"site_id"`
	SiteCityName string     `json:"site_city_name"`
	SiteCityCode string     `json:"site_city_code"`
	NodeType     string     `json:"node_type"`
	ACPhase      int        `json:"ac_phase"`
	PlantID      *string    `json:"plant_id"`
	PlantName    *string    `json:"plant_name"`
	Latitude     *float64   `json:"lat"`
	Longitude    *float64   `json:"lng"`
	Location     *string    `json:"location"`
	DeviceID     *string    `json:"device_id"`
	DeviceSN     *string    `json:"device_sn"`
	DeviceName   *string    `json:"device_name"`
	DeviceType   *string    `json:"device_type"`
	DeviceStatus *string    `json:"device_status"`
	ID           *string    `json:"id"`
	Message      *string    `json:"message"`
	AlarmTime    *time.Time `json:"alarm_time"`
	Owner        string     `json:"owner"`
}

type SiteItem struct {
	Timestamp   time.Time `json:"@timestamp"`
	VendorType  string    `json:"vendor_type"`
	Area        string    `json:"area"`
	SiteID      string    `json:"site_id"`
	NodeType    string    `json:"node_type"`
	Name        *string   `json:"name"`
	Location    *string   `json:"location"`
	PlantStatus *string   `json:"plant_status"`
	Owner       string    `json:"owner"`
}

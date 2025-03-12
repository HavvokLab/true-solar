package huawei

type Response[T any] struct {
	Success    bool   `json:"success"`
	FailCode   int    `json:"failCode"`
	Parameters any    `json:"params,omitempty"`
	Message    string `json:"message,omitempty"`
	Data       T      `json:"data,omitempty"`
}

type GetTokenResponse Response[string]

type Plant struct {
	Code           *string  `json:"stationCode,omitempty"`
	Name           *string  `json:"stationName,omitempty"`
	Address        *string  `json:"stationAddr,omitempty"`
	Capacity       *float64 `json:"capacity,omitempty"`
	BuildState     *string  `json:"buildState,omitempty"`
	CombineType    *string  `json:"combineType,omitempty"`
	AIDType        *int     `json:"aidType,omitempty"`
	StationLinkMan *string  `json:"stationLinkman,omitempty"`
	LinkManPhone   *string  `json:"linkmanPho,omitempty"`
}

type GetPlantListResponse Response[[]Plant]

type RealtimePlantData struct {
	Code string             `json:"code,omitempty"`
	Data *RealtimePlantItem `json:"dataItemMap,omitempty"`
}

type RealtimePlantItem struct {
	TotalIncome     *float64 `json:"total_income,omitempty"`
	TotalPower      *float64 `json:"total_power,omitempty"`
	DayPower        *float64 `json:"day_power,omitempty"`
	DayIncome       *float64 `json:"day_income,omitempty"`
	RealHealthState *int     `json:"real_health_state,omitempty"`
	MonthPower      *float64 `json:"month_power,omitempty"`
}

type GetRealtimePlantDataResponse Response[[]RealtimePlantData]

type Interval string

const (
	IntervalDay   Interval = "day"
	IntervalMonth Interval = "month"
	IntervalYear  Interval = "year"
)

type HistoricalPlantData struct {
	Code        *string              `json:"stationCode,omitempty"`
	CollectTime *int64               `json:"collectTime,omitempty"`
	DataItemMap *HistoricalPlantItem `json:"dataItemMap,omitempty"`
}

type HistoricalPlantItem struct {
	RadiationIntensity *float64 `json:"radiation_intensity,omitempty"`
	InstalledCapacity  *float64 `json:"installed_capacity,omitempty"`
	UsePower           *float64 `json:"use_power,omitempty"`
	InverterPower      *float64 `json:"inverter_power,omitempty"`
	PowerProfit        *float64 `json:"power_profit,omitempty"`
	TheoryPower        *float64 `json:"theory_power,omitempty"`
	PerPowerRatio      *float64 `json:"perpower_ratio,omitempty"`
	OnGridPower        *float64 `json:"ongrid_power,omitempty"`
	PerformanceRatio   *float64 `json:"performance_ratio,omitempty"`
	ReductionTotalCO2  *float64 `json:"reduction_total_co2,omitempty"`
	ReductionTotalCoal *float64 `json:"reduction_total_coal,omitempty"`
	ReductionTotalTree *float64 `json:"reduction_total_tree,omitempty"`
}

type GetHistoricalPlantDataResponse Response[[]HistoricalPlantData]

type Device struct {
	ID              *int     `json:"id,omitempty"`
	SN              *string  `json:"esnCode,omitempty"`
	Name            *string  `json:"devName,omitempty"`
	TypeID          *int     `json:"devTypeId,omitempty"`
	InverterModel   *string  `json:"invType,omitempty"`
	Latitude        *float64 `json:"latitude,omitempty"`
	Longitude       *float64 `json:"longitude,omitempty"`
	SoftwareVersion *string  `json:"softwareVersion,omitempty"`
	PlantCode       *string  `json:"stationCode,omitempty"`
}

type GetDeviceListResponse Response[[]Device]

type RealtimeDeviceData struct {
	ID          *int                    `json:"devId,omitempty"`
	DataItemMap *RealtimeDeviceDataItem `json:"dataItemMap,omitempty"`
}

type RealtimeDeviceDataItem struct {
	TotalEnergy *float64 `json:"total_cap,omitempty"`
	ActivePower *float64 `json:"active_power,omitempty"`
	Status      *int     `json:"run_state,omitempty"`
}

type GetRealtimeDeviceDataResponse Response[[]RealtimeDeviceData]

type HistoricalDeviceData struct {
	ID          any                       `json:"devId,omitempty"`
	CollectTime *int64                    `json:"collectTime,omitempty"`
	DataItemMap *HistoricalDeviceDataItem `json:"dataItemMap,omitempty"`
}

type HistoricalDeviceDataItem struct {
	InstalledCapacity *float64 `json:"installed_capacity,omitempty"`
	ProductPower      *float64 `json:"product_power,omitempty"`
	PerPowerRatio     *float64 `json:"perpower_ratio,omitempty"`
}

type GetHistoricalDeviceDataResponse Response[[]HistoricalDeviceData]

type DeviceAlarm struct {
	PlantCode        *string `json:"stationCode,omitempty"`
	PlantName        *string `json:"stationName,omitempty"`
	DeviceSN         *string `json:"esnCode,omitempty"`
	DeviceName       *string `json:"devName,omitempty"`
	DeviceTypeID     *int    `json:"devTypeId,omitempty"`
	AlarmID          *int    `json:"alarmId,omitempty"`
	AlarmName        *string `json:"alarmName,omitempty"`
	AlarmCause       *string `json:"alarmCause,omitempty"`
	AlarmType        *int    `json:"alarmType,omitempty"`
	RepairSuggestion *string `json:"repairSuggestion,omitempty"`
	CauseID          *int    `json:"causeId,omitempty"`
	RaiseTime        *int64  `json:"raiseTime,omitempty"`
	Level            *int    `json:"lev,omitempty"`
	Status           *int    `json:"status,omitempty"`
}

type GetDeviceAlarmResponse Response[[]DeviceAlarm]

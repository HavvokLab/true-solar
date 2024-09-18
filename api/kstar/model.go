package kstar

type Meta struct {
	Success bool    `json:"success,omitempty"`
	Code    *string `json:"code,omitempty"`
}

type Response[T any] struct {
	Meta *Meta `json:"meta,omitempty"`
	Data T     `json:"data,omitempty"`
}

type PlantInfoItem struct {
	ID                *string  `json:"powerId,omitempty"`
	Name              *string  `json:"powerName,omitempty"`
	InstalledCapacity *float64 `json:"powerCap,omitempty"`
	Longitude         *float64 `json:"longitude,omitempty"`
	Latitude          *float64 `json:"latitude,omitempty"`
	CityCode          *string  `json:"cityCode,omitempty"`
	CityName          *string  `json:"cityName,omitempty"`
	DistrictCode      *string  `json:"districtCode,omitempty"`
	Address           *string  `json:"powerArea,omitempty"`
	CreatedTime       *string  `json:"createTime,omitempty"`
	DealerCode        *string  `json:"dealerCode,omitempty"`
	ElectricPrice     *float64 `json:"elecPrice,omitempty"`
	ElectricUnit      *string  `json:"elecUnit,omitempty"`
	TimeZone          *string  `json:"timeZone,omitempty"`
}

type GetPlantListResponse Response[[]PlantInfoItem]

type DeviceItem struct {
	ID        *string `json:"deviceId,omitempty"`
	SN        *string `json:"inverterId,omitempty"`
	Name      *string `json:"deviceName,omitempty"`
	Status    *int    `json:"deviceStatus,omitempty"`
	PlantID   *string `json:"powerId,omitempty"`
	PlantName *string `json:"powerName,omitempty"`
	SaveTime  *string `json:"saveTime,omitempty"`
}

type GetDeviceListData struct {
	Code  *int         `json:"code,omitempty"`
	Count *int         `json:"count,omitempty"`
	List  []DeviceItem `json:"list,omitempty"`
}

type GetDeviceListResponse Response[*GetDeviceListData]

type DeviceInfoItem struct {
	ID                 *string  `json:"device_id,omitempty"`
	SN                 *string  `json:"inverter_id,omitempty"`
	Status             *int     `json:"status,omitempty"`
	Version            *int     `json:"version,omitempty"`
	SaveTime           *string  `json:"save_time,omitempty"`
	VoltagePV1         *float64 `json:"voltage_pv1,omitempty"`
	VoltagePV2         *float64 `json:"voltage_pv2,omitempty"`
	VoltagePV3         *float64 `json:"voltage_pv3,omitempty"`
	CurrentPV1         *float64 `json:"current_pv1,omitempty"`
	CurrentPV2         *float64 `json:"current_pv2,omitempty"`
	CurrentPV3         *float64 `json:"current_pv3,omitempty"`
	PowerPV1           *float64 `json:"power_pv1,omitempty"`
	PowerPV2           *float64 `json:"power_pv2,omitempty"`
	PowerPV3           *float64 `json:"power_pv3,omitempty"`
	VoltagePBUS        *float64 `json:"voltage_pbus,omitempty"`
	VoltageNBUS        *float64 `json:"voltage_nbus,omitempty"`
	VoltageRS          *float64 `json:"voltage_rs,omitempty"`
	VoltageST          *float64 `json:"voltage_st,omitempty"`
	VoltageTR          *float64 `json:"voltage_tr,omitempty"`
	FrequencyRS        *float64 `json:"frequency_rs,omitempty"`
	FrequencyST        *float64 `json:"frequency_st,omitempty"`
	FrequencyTR        *float64 `json:"frequency_tr,omitempty"`
	CurrentR           *float64 `json:"current_r,omitempty"`
	CurrentS           *float64 `json:"current_s,omitempty"`
	CurrentT           *float64 `json:"current_t,omitempty"`
	PowerInter         *float64 `json:"power_inter,omitempty"`
	RadiatorTemp       *float64 `json:"radiator_temp,omitempty"`
	ModuleTemp         *float64 `json:"module_temp,omitempty"`
	DSPAlarmCode       *string  `json:"dsp_alarm_code,omitempty"`
	DSPErrorCode       *string  `json:"dsp_error_code,omitempty"`
	InverterType       *int     `json:"inverter_type,omitempty"`
	DeviceModel        *int     `json:"device_model,omitempty"`
	FanA               *float64 `json:"fan_A,omitempty"`
	FanB               *float64 `json:"fan_B,omitempty"`
	FanC               *float64 `json:"fan_C,omitempty"`
	ARMAlarmCode       *string  `json:"arm_alarm_code,omitempty"`
	ARMErrorCode       *string  `json:"arm_error_code,omitempty"`
	InputType          *int     `json:"input_type,omitempty"`
	GenerationStandard *int     `json:"generation_standard,omitempty"`
	TotalGeneration    *float64 `json:"total_generation,omitempty"`
	YearGeneration     *float64 `json:"year_generation,omitempty"`
	DayGeneration      *float64 `json:"day_generation,omitempty"`
	MonthGeneration    *float64 `json:"month_generation,omitempty"`
	VoltageOpen        *float64 `json:"voltage_open,omitempty"`
	DelayTime          *float64 `json:"delay_time,omitempty"`
	VoltageLower       *float64 `json:"voltage_lower,omitempty"`
	VoltageCeiling     *float64 `json:"voltage_ceiling,omitempty"`
	PowerLower         *float64 `json:"power_lower,omitempty"`
	PowerCeiling       *float64 `json:"power_ceiling,omitempty"`
	ReactSet           *float64 `json:"react_set,omitempty"`
	ActiveSet          *float64 `json:"active_set,omitempty"`
	ReactiveSet        *float64 `json:"reactive_set,omitempty"`
	ReactiveType       *int     `json:"reactive_type,omitempty"`
	PowerApparent      *float64 `json:"power_apparent,omitempty"`
	PowerReactive      *float64 `json:"power_reactive,omitempty"`
	PowerFactors       *float64 `json:"power_factors,omitempty"`
	CurrentImpedance   *float64 `json:"current_impedance,omitempty"`
	OverPower          *int     `json:"over_power,omitempty"`
	OverPowerSet       *float64 `json:"over_power_set,omitempty"`
	QVHighSet          *float64 `json:"qv_high_set,omitempty"`
	QVHighPercentage   *float64 `json:"qv_high_percentage,omitempty"`
	QVLowerSet         *float64 `json:"qv_lower_set,omitempty"`
	QVLowerPercentage  *float64 `json:"qv_lower_percentage,omitempty"`
	WeakGeneration     *float64 `json:"weak_generation,omitempty"`
	InverterPower      *float64 `json:"inverter_power,omitempty"`
	InverterMake       *int     `json:"inverter_make,omitempty"`
	DREDMake           *int     `json:"dred_make,omitempty"`
}

type GetRealtimeDeviceDataResponse Response[*DeviceInfoItem]

type DeviceAlarmInfoItem struct {
	DeviceID   *string `json:"deviceId,omitempty"`
	DeviceName *string `json:"deviceName,omitempty"`
	SaveTime   *string `json:"saveTime,omitempty"`
	Message    *string `json:"message,omitempty"`
	ErrorLevel *int    `json:"errorLevel,omitempty"`
	RemoveTime *string `json:"removeTime,omitempty"`
	PlantID    *string `json:"powerId,omitempty"`
	PlantName  *string `json:"powerName,omitempty"`
}

type GetRealtimeAlarmListOfDeviceResponse Response[[]DeviceAlarmInfoItem]

type DeviceHistory struct {
	Status             *int     `json:"status,omitempty"`
	SaveTime           *string  `json:"save_time,omitempty"`
	VoltagePV1         *float64 `json:"voltage_pv1,omitempty"`
	VoltagePV2         *float64 `json:"voltage_pv2,omitempty"`
	VoltagePV3         *float64 `json:"voltage_pv3,omitempty"`
	CurrentPV1         *float64 `json:"current_pv1,omitempty"`
	CurrentPV2         *float64 `json:"current_pv2,omitempty"`
	CurrentPV3         *float64 `json:"current_pv3,omitempty"`
	PowerPV1           *float64 `json:"power_pv1,omitempty"`
	PowerPV2           *float64 `json:"power_pv2,omitempty"`
	PowerPV3           *float64 `json:"power_pv3,omitempty"`
	VoltagePBUS        *float64 `json:"voltage_pbus,omitempty"`
	VoltageNBUS        *float64 `json:"voltage_nbus,omitempty"`
	VoltageRS          *float64 `json:"voltage_rs,omitempty"`
	VoltageST          *float64 `json:"voltage_st,omitempty"`
	VoltageTR          *float64 `json:"voltage_tr,omitempty"`
	FrequencyRS        *float64 `json:"frequency_rs,omitempty"`
	FrequencyST        *float64 `json:"frequency_st,omitempty"`
	FrequencyTR        *float64 `json:"frequency_tr,omitempty"`
	CurrentR           *float64 `json:"current_r,omitempty"`
	CurrentS           *float64 `json:"current_s,omitempty"`
	CurrentT           *float64 `json:"current_t,omitempty"`
	PowerInter         *float64 `json:"power_inter,omitempty"`
	RadiatorTemp       *float64 `json:"radiator_temp,omitempty"`
	ModuleTemp         *float64 `json:"module_temp,omitempty"`
	DSPAlarmCode       *string  `json:"dsp_alarm_code,omitempty"`
	DSPErrorCode       *string  `json:"dsp_error_code,omitempty"`
	InverterType       *int     `json:"inverter_type,omitempty"`
	DeviceModel        *int     `json:"device_model,omitempty"`
	FanA               *float64 `json:"fan_A,omitempty"`
	FanB               *float64 `json:"fan_B,omitempty"`
	FanC               *float64 `json:"fan_C,omitempty"`
	ARMAlarmCode       *string  `json:"arm_alarm_code,omitempty"`
	ARMErrorCode       *string  `json:"arm_error_code,omitempty"`
	InputType          *int     `json:"input_type,omitempty"`
	GenerationStandard *int     `json:"generation_standard,omitempty"`
	TotalGeneration    *float64 `json:"total_generation,omitempty"`
	YearGeneration     *float64 `json:"year_generation,omitempty"`
	DayGeneration      *float64 `json:"day_generation,omitempty"`
	MonthGeneration    *float64 `json:"month_generation,omitempty"`
	VoltageOpen        *float64 `json:"voltage_open,omitempty"`
	DelayTime          *float64 `json:"delay_time,omitempty"`
	VoltageLower       *float64 `json:"voltage_lower,omitempty"`
	VoltageCeiling     *float64 `json:"voltage_ceiling,omitempty"`
	PowerLower         *float64 `json:"power_lower,omitempty"`
	PowerCeiling       *float64 `json:"power_ceiling,omitempty"`
	ReactSet           *float64 `json:"react_set,omitempty"`
	ActiveSet          *float64 `json:"active_set,omitempty"`
	ReactiveSet        *float64 `json:"reactive_set,omitempty"`
	ReactiveType       *int     `json:"reactive_type,omitempty"`
	PowerApparent      *float64 `json:"power_apparent,omitempty"`
	PowerReactive      *float64 `json:"power_reactive,omitempty"`
	PowerFactors       *float64 `json:"power_factors,omitempty"`
	CurrentImpedance   *float64 `json:"current_impedance,omitempty"`
	OverPower          *int     `json:"over_power,omitempty"`
	OverPowerSet       *float64 `json:"over_power_set,omitempty"`
	QVHighSet          *float64 `json:"qv_high_set,omitempty"`
	QVHighPercentage   *float64 `json:"qv_high_percentage,omitempty"`
	QVLowerSet         *float64 `json:"qv_lower_set,omitempty"`
	QVLowerPercentage  *float64 `json:"qv_lower_percentage,omitempty"`
	WeakGeneration     *float64 `json:"weak_generation,omitempty"`
	InverterPower      *float64 `json:"inverter_power,omitempty"`
	InverterMake       *int     `json:"inverter_make,omitempty"`
	DREDMake           *int     `json:"dred_make,omitempty"`
}

type GetHistoricalDeviceDataResponse Response[[]DeviceHistory]

package growatt

type DefaultResponse struct {
	ErrorCode *int    `json:"error_code,omitempty"`
	ErrorMsg  *string `json:"error_msg,omitempty"`
}

// |=> GetPlantList
type PlantItem struct {
	Status       *int     `json:"status,omitempty"`
	Locale       *string  `json:"locale,omitempty"`
	TotalEnergy  *string  `json:"total_energy,omitempty"`
	Operator     *string  `json:"operator,omitempty"`
	Country      *string  `json:"country,omitempty"`
	City         *string  `json:"city,omitempty"`
	CurrentPower *string  `json:"current_power,omitempty"`
	CreateDate   *string  `json:"create_date,omitempty"`
	ImageURL     *string  `json:"image_url,omitempty"`
	PlantID      *int     `json:"plant_id,omitempty"`
	Name         *string  `json:"name,omitempty"`
	Installer    *string  `json:"installer,omitempty"`
	UserID       *int     `json:"user_id,omitempty"`
	Longitude    *string  `json:"longitude,omitempty"`
	Latitude     *string  `json:"latitude,omitempty"`
	PeakPower    *float64 `json:"peak_power,omitempty"`
	LatitudeD    any      `json:"latitude_d,omitempty"`
	LatitudeF    any      `json:"latitude_f,omitempty"`
}

type PlantData struct {
	Plants []PlantItem `json:"plants,omitempty"`
	Count  *int        `json:"count,omitempty"`
}

type GetPlantListResponse struct {
	DefaultResponse
	Data *PlantData `json:"data,omitempty"`
}

// |=> GetPlantOverviewInfo
type PlantOverviewInfo struct {
	PeakPowerActual *float64 `json:"peak_power_actual,omitempty"`
	MonthlyEnergy   *string  `json:"monthly_energy,omitempty"`
	LastUpdateTime  *string  `json:"last_update_time,omitempty"`
	CurrentPower    *float64 `json:"current_power,omitempty"`
	Timezone        *string  `json:"timezone,omitempty"`
	YearlyEnergy    *string  `json:"yearly_energy,omitempty"`
	TodayEnergy     *string  `json:"today_energy,omitempty"`
	CarbonOffset    *string  `json:"carbon_offset,omitempty"`
	Efficiency      *string  `json:"efficiency,omitempty"`
	TotalEnergy     *string  `json:"total_energy,omitempty"`
}

type GetPlantOverviewInfoResponse struct {
	DefaultResponse
	Data *PlantOverviewInfo `json:"data,omitempty"`
}

// |=> GetPlantDataLoggerInfo
type Date struct {
	Time           *int64 `json:"time,omitempty"`
	Minutes        *int   `json:"minutes,omitempty"`
	Seconds        *int   `json:"seconds,omitempty"`
	Hours          *int   `json:"hours,omitempty"`
	Month          *int   `json:"month,omitempty"`
	TimezoneOffset *int   `json:"timezoneOffset,omitempty"`
	Year           *int   `json:"year,omitempty"`
	Day            *int   `json:"day,omitempty"`
	Date           *int   `json:"date,omitempty"`
}

type PeakPowerActual struct {
	MapCityID                *int     `json:"map_cityId,omitempty"`
	MapCountryID             *int     `json:"map_countryId,omitempty"`
	StorageBattoryPercentage *float64 `json:"storage_BattoryPercentage,omitempty"`
	DefaultPlant             *bool    `json:"defaultPlant,omitempty"`
	PeakPeriodPrice          *float64 `json:"peakPeriodPrice,omitempty"`
	City                     *string  `json:"city,omitempty"`
	NominalPower             *float64 `json:"nominalPower,omitempty"`
	AlarmValue               *float64 `json:"alarmValue,omitempty"`
	CurrentPacTxt            *string  `json:"currentPacTxt,omitempty"`
	FixedPowerPrice          *float64 `json:"fixedPowerPrice,omitempty"`
	PlantFromBean            any      `json:"plantFromBean,omitempty"`
	DeviceCount              *int     `json:"deviceCount,omitempty"`
	PlantImgName             *string  `json:"plantImgName,omitempty"`
	EtodaySo2Text            *string  `json:"etodaySo2Text,omitempty"`
	CompanyName              *string  `json:"companyName,omitempty"`
	EmonthMoneyText          *string  `json:"emonthMoneyText,omitempty"`
	FormulaMoney             *float64 `json:"formulaMoney,omitempty"`
	UserAccount              *string  `json:"userAccount,omitempty"`
	MapLat                   *string  `json:"mapLat,omitempty"`
	CreateDateTextA          *string  `json:"createDateTextA,omitempty"`
	MapLng                   *string  `json:"mapLng,omitempty"`
	OnLineEnvCount           *int     `json:"onLineEnvCount,omitempty"`
	EventMessBeanList        []any    `json:"eventMessBeanList,omitempty"`
	LatitudeText             *string  `json:"latitudeText,omitempty"`
	PlantAddress             *string  `json:"plantAddress,omitempty"`
	HasDeviceOnLine          *int     `json:"hasDeviceOnLine,omitempty"`
	FormulaMoneyStr          *string  `json:"formulaMoneyStr,omitempty"`
	EtodayMoney              *float64 `json:"etodayMoney,omitempty"`
	CreateDate               *Date    `json:"createDate,omitempty"`
	MapCity                  *string  `json:"mapCity,omitempty"`
	PrMonth                  *string  `json:"prMonth,omitempty"`
	StorageTodayToGrid       *float64 `json:"storage_TodayToGrid,omitempty"`
	FormulaCo2               *float64 `json:"formulaCo2,omitempty"`
	ETotal                   *float64 `json:"eTotal,omitempty"`
	EmonthSo2Text            *string  `json:"emonthSo2Text,omitempty"`
	WindAngle                *float64 `json:"windAngle,omitempty"`
	EtotalCoalText           *string  `json:"etotalCoalText,omitempty"`
	WindSpeed                *float64 `json:"windSpeed,omitempty"`
	EmonthCoalText           *string  `json:"emonthCoalText,omitempty"`
	EtodayMoneyText          *string  `json:"etodayMoneyText,omitempty"`
	EYearMoneyText           *string  `json:"EYearMoneyText,omitempty"`
	PlantLng                 *string  `json:"plant_lng,omitempty"`
	LatitudeM                *string  `json:"latitude_m,omitempty"`
	PairViewUserAccount      *string  `json:"pairViewUserAccount,omitempty"`
	StorageTotalToUser       *float64 `json:"storage_TotalToUser,omitempty"`
	LatitudeD                *string  `json:"latitude_d,omitempty"`
	LatitudeF                *string  `json:"latitude_f,omitempty"`
	Remark                   *string  `json:"remark,omitempty"`
	TreeID                   *string  `json:"treeID,omitempty"`
	FlatPeriodPrice          *float64 `json:"flatPeriodPrice,omitempty"`
	LongitudeText            *string  `json:"longitudeText,omitempty"`
	StorageEChargeToday      *float64 `json:"storage_eChargeToday,omitempty"`
	DataLogList              []any    `json:"dataLogList,omitempty"`
	DesignCompany            *string  `json:"designCompany,omitempty"`
	TimezoneText             *string  `json:"timezoneText,omitempty"`
	FormulaCoal              *float64 `json:"formulaCoal,omitempty"`
	StorageEDisChargeToday   *float64 `json:"storage_eDisChargeToday,omitempty"`
	UnitMap                  any      `json:"unitMap,omitempty"`
	Timezone                 *int     `json:"timezone,omitempty"`
	PhoneNum                 *string  `json:"phoneNum,omitempty"`
	Level                    *int     `json:"level,omitempty"`
	FormulaMoneyUnitID       *string  `json:"formulaMoneyUnitId,omitempty"`
	ImgPath                  *string  `json:"imgPath,omitempty"`
	PanelTemp                *float64 `json:"panelTemp,omitempty"`
	LocationImgName          *string  `json:"locationImgName,omitempty"`
	MoneyUnitText            *string  `json:"moneyUnitText,omitempty"`
	StorageTotalToGrid       *float64 `json:"storage_TotalToGrid,omitempty"`
	PrToday                  *string  `json:"prToday,omitempty"`
	EnergyMonth              *float64 `json:"energyMonth,omitempty"`
	PlantName                *string  `json:"plantName,omitempty"`
	EToday                   *float64 `json:"eToday,omitempty"`
	Status                   *int     `json:"status,omitempty"`
	PlantType                *int     `json:"plantType,omitempty"`
	Country                  *string  `json:"country,omitempty"`
	LongitudeD               *string  `json:"longitude_d,omitempty"`
	MapAreaID                *int     `json:"map_areaId,omitempty"`
	LongitudeF               *string  `json:"longitude_f,omitempty"`
	CreateDateText           *string  `json:"createDateText,omitempty"`
	LongitudeM               *string  `json:"longitude_m,omitempty"`
	FormulaSo2               *float64 `json:"formulaSo2,omitempty"`
	ValleyPeriodPrice        *float64 `json:"valleyPeriodPrice,omitempty"`
	EnergyYear               *float64 `json:"energyYear,omitempty"`
	TreeName                 *string  `json:"treeName,omitempty"`
	PlantLat                 *string  `json:"plant_lat,omitempty"`
	EtodayCo2Text            *string  `json:"etodayCo2Text,omitempty"`
	NominalPowerStr          *string  `json:"nominalPowerStr,omitempty"`
	FormulaTree              *float64 `json:"formulaTree,omitempty"`
	EtotalSo2Text            *string  `json:"etotalSo2Text,omitempty"`
	Children                 []any    `json:"children,omitempty"`
	ID                       *int     `json:"id,omitempty"`
	EtodayCoalText           *string  `json:"etodayCoalText,omitempty"`
	ParamBean                any      `json:"paramBean,omitempty"`
	EtotalMoney              *int     `json:"etotalMoney,omitempty"`
	EnvTemp                  *int     `json:"envTemp,omitempty"`
	LogoImgName              *string  `json:"logoImgName,omitempty"`
	Alias                    *string  `json:"alias,omitempty"`
	EtotalCo2Text            *string  `json:"etotalCo2Text,omitempty"`
	CurrentPacStr            *string  `json:"currentPacStr,omitempty"`
	MapProvinceID            *int     `json:"map_provinceId,omitempty"`
	EtotalMoneyText          *string  `json:"etotalMoneyText,omitempty"`
	EmonthCo2Text            *string  `json:"emonthCo2Text,omitempty"`
	Irradiance               *int     `json:"irradiance,omitempty"`
	HasStorage               *int     `json:"hasStorage,omitempty"`
	ParentID                 *string  `json:"parentID,omitempty"`
	UserBean                 any      `json:"userBean,omitempty"`
	StorageTodayToUser       *int     `json:"storage_TodayToUser,omitempty"`
	IsShare                  *bool    `json:"isShare,omitempty"`
	CurrentPac               *int     `json:"currentPac,omitempty"`
}

type DataLogger struct {
	LastUpdateTime *Date   `json:"last_update_time,omitempty"`
	Model          *string `json:"model,omitempty"`
	SN             *string `json:"sn,omitempty"`
	Lost           *bool   `json:"lost,omitempty"`
	Manufacturer   *string `json:"manufacturer,omitempty"`
	Type           *int    `json:"type,omitempty"`
}

type PlantDataLoggerInfo struct {
	Count           *int             `json:"count,omitempty"`
	PeakPowerActual *PeakPowerActual `json:"peak_power_actual,omitempty"`
	DataLoggers     []DataLogger     `json:"dataloggers,omitempty"`
}

type GetPlantDataLoggerInfoResponse struct {
	DefaultResponse
	Data *PlantDataLoggerInfo `json:"data,omitempty"`
}

// |=> GetPlantDeviceList
type DeviceItem struct {
	DeviceSN       *string `json:"device_sn,omitempty"`
	LastUpdateTime *string `json:"last_update_time,omitempty"`
	Model          *string `json:"model,omitempty"`
	Lost           *bool   `json:"lost,omitempty"`
	Status         *int    `json:"status,omitempty"`
	Manufacturer   *string `json:"manufacturer,omitempty"`
	DeviceID       *int    `json:"device_id,omitempty"`
	DataLoggerSN   *string `json:"datalogger_sn,omitempty"`
	Type           *int    `json:"type,omitempty"`
}

type DeviceData struct {
	Count   *int         `json:"count,omitempty"`
	Devices []DeviceItem `json:"devices,omitempty"`
}

type GetPlantDeviceListResponse struct {
	DefaultResponse
	Data *DeviceData `json:"data,omitempty"`
}

// GetRealtimeDeviceData
type TimeZone struct {
	LastRuleInstance any     `json:"lastRuleInstance,omitempty"`
	RawOffset        *int    `json:"rawOffset,omitempty"`
	DSTSavings       *int    `json:"DSTSavings,omitempty"`
	Dirty            *bool   `json:"dirty,omitempty"`
	ID               *string `json:"ID,omitempty"`
	DisplayName      *string `json:"displayName,omitempty"`
}

type TimeCalendar struct {
	MinimalDaysInFirstWeek *int      `json:"minimalDaysInFirstWeek,omitempty"`
	WeekYear               *int      `json:"weekYear,omitempty"`
	Time                   *Date     `json:"time,omitempty"`
	WeeksInWeekYear        *int      `json:"weeksInWeekYear,omitempty"`
	GregorianChange        *Date     `json:"gregorianChange,omitempty"`
	TimeZone               *TimeZone `json:"timeZone,omitempty"`
	TimeInMillis           *int64    `json:"timeInMillis,omitempty"`
	Lenient                *bool     `json:"lenient,omitempty"`
	FirstDayOfWeek         *int      `json:"firstDayOfWeek,omitempty"`
	WeekDateSupported      *bool     `json:"weekDateSupported,omitempty"`
}

type RealtimeDeviceData struct {
	IPidPvcpe             *float64      `json:"iPidPvcpe,omitempty"`
	Epv4Total             *float64      `json:"epv4Total,omitempty"`
	RealOPPercent         *float64      `json:"realOPPercent,omitempty"`
	PidBus                *float64      `json:"pidBus,omitempty"`
	Ppv7                  *float64      `json:"ppv7,omitempty"`
	Ctharis               *float64      `json:"ctharis,omitempty"`
	Ctir                  *float64      `json:"ctir,omitempty"`
	VacTr                 *float64      `json:"vacTr,omitempty"`
	Ppv6                  *float64      `json:"ppv6,omitempty"`
	ERacTotal             *float64      `json:"eRacTotal,omitempty"`
	Ctharit               *float64      `json:"ctharit,omitempty"`
	Ctis                  *float64      `json:"ctis,omitempty"`
	Ppv9                  *float64      `json:"ppv9,omitempty"`
	Epv1Total             *float64      `json:"epv1Total,omitempty"`
	WStringStatusValue    *float64      `json:"wStringStatusValue,omitempty"`
	Ppv8                  *float64      `json:"ppv8,omitempty"`
	Ctharir               *float64      `json:"ctharir,omitempty"`
	WarningValue3         *float64      `json:"warningValue3,omitempty"`
	VPidPvape             *float64      `json:"vPidPvape,omitempty"`
	WarningValue1         *float64      `json:"warningValue1,omitempty"`
	Ctit                  *float64      `json:"ctit,omitempty"`
	FaultCode1            *float64      `json:"faultCode1,omitempty"`
	WarningValue2         *float64      `json:"warningValue2,omitempty"`
	Temperature           *float64      `json:"temperature,omitempty"`
	FaultCode2            *float64      `json:"faultCode2,omitempty"`
	Time                  *string       `json:"time,omitempty"`
	IPidPvbpe             *float64      `json:"iPidPvbpe,omitempty"`
	IPidPvdpe             *float64      `json:"iPidPvdpe,omitempty"`
	Epv2Total             *float64      `json:"epv2Total,omitempty"`
	WarnBit               *float64      `json:"warnBit,omitempty"`
	IPidPvepe             *float64      `json:"iPidPvepe,omitempty"`
	VacSt                 *float64      `json:"vacSt,omitempty"`
	VPidPvcpe             *float64      `json:"vPidPvcpe,omitempty"`
	Epv8Total             *float64      `json:"epv8Total,omitempty"`
	Ipv10                 *float64      `json:"ipv10,omitempty"`
	Again                 *bool         `json:"again,omitempty"`
	Vpv10                 *float64      `json:"vpv10,omitempty"`
	StrBreak              *float64      `json:"strBreak,omitempty"`
	Compqt                *float64      `json:"compqt,omitempty"`
	IpmTemperature        *float64      `json:"ipmTemperature,omitempty"`
	Compqs                *float64      `json:"compqs,omitempty"`
	Ppv                   *float64      `json:"ppv,omitempty"`
	Compqr                *float64      `json:"compqr,omitempty"`
	Ctqt                  *float64      `json:"ctqt,omitempty"`
	Pf                    *float64      `json:"pf,omitempty"`
	Epv7Total             *float64      `json:"epv7Total,omitempty"`
	Vpv1                  *float64      `json:"vpv1,omitempty"`
	Epv10Today            *float64      `json:"epv10Today,omitempty"`
	IPidPvape             *float64      `json:"iPidPvape,omitempty"`
	Vpv3                  *float64      `json:"vpv3,omitempty"`
	Ctqr                  *float64      `json:"ctqr,omitempty"`
	Vpv2                  *float64      `json:"vpv2,omitempty"`
	Ctqs                  *float64      `json:"ctqs,omitempty"`
	Vpv5                  *float64      `json:"vpv5,omitempty"`
	Vpv4                  *float64      `json:"vpv4,omitempty"`
	Vpv7                  *float64      `json:"vpv7,omitempty"`
	Vpv6                  *float64      `json:"vpv6,omitempty"`
	PowerTotal            *float64      `json:"powerTotal,omitempty"`
	Vpv9                  *float64      `json:"vpv9,omitempty"`
	TDci                  *float64      `json:"tDci,omitempty"`
	Vpv8                  *float64      `json:"vpv8,omitempty"`
	Epv2Today             *float64      `json:"epv2Today,omitempty"`
	TimeTotal             *float64      `json:"timeTotal,omitempty"`
	Epv1Today             *float64      `json:"epv1Today,omitempty"`
	Epv6Today             *float64      `json:"epv6Today,omitempty"`
	Epv9Today             *float64      `json:"epv9Today,omitempty"`
	TimeTotalText         *string       `json:"timeTotalText,omitempty"`
	DwStringWarningValue1 *float64      `json:"dwStringWarningValue1,omitempty"`
	VPidPvepe             *float64      `json:"vPidPvepe,omitempty"`
	EpvTotal              *float64      `json:"epvTotal,omitempty"`
	VPidPvgpe             *float64      `json:"vPidPvgpe,omitempty"`
	FaultType             *float64      `json:"faultType,omitempty"`
	CurrentString12       *float64      `json:"currentString12,omitempty"`
	CurrentString11       *float64      `json:"currentString11,omitempty"`
	CurrentString10       *float64      `json:"currentString10,omitempty"`
	ERacToday             *float64      `json:"eRacToday,omitempty"`
	CurrentString16       *float64      `json:"currentString16,omitempty"`
	Epv5Today             *float64      `json:"epv5Today,omitempty"`
	CurrentString15       *float64      `json:"currentString15,omitempty"`
	CurrentString14       *float64      `json:"currentString14,omitempty"`
	CurrentString13       *float64      `json:"currentString13,omitempty"`
	WPIDFaultValue        *float64      `json:"wPIDFaultValue,omitempty"`
	VString11             *float64      `json:"vString11,omitempty"`
	VString10             *float64      `json:"vString10,omitempty"`
	PowerToday            *float64      `json:"powerToday,omitempty"`
	VString16             *float64      `json:"vString16,omitempty"`
	VString13             *float64      `json:"vString13,omitempty"`
	VString12             *float64      `json:"vString12,omitempty"`
	VString15             *float64      `json:"vString15,omitempty"`
	VString14             *float64      `json:"vString14,omitempty"`
	BigDevice             *bool         `json:"bigDevice,omitempty"`
	Epv9Total             *float64      `json:"epv9Total,omitempty"`
	WarnCode              *float64      `json:"warnCode,omitempty"`
	PvIso                 *float64      `json:"pvIso,omitempty"`
	Epv6Total             *float64      `json:"epv6Total,omitempty"`
	InverterID            *string       `json:"inverterId,omitempty"`
	Temperature3          *float64      `json:"temperature3,omitempty"`
	Temperature2          *float64      `json:"temperature2,omitempty"`
	TimeCalendar          *TimeCalendar `json:"timeCalendar,omitempty"`
	PBusVoltage           *float64      `json:"pBusVoltage,omitempty"`
	CurrentString5        *float64      `json:"currentString5,omitempty"`
	StrFault              *float64      `json:"strFault,omitempty"`
	CurrentString4        *float64      `json:"currentString4,omitempty"`
	VPidPvdpe             *float64      `json:"vPidPvdpe,omitempty"`
	CurrentString3        *float64      `json:"currentString3,omitempty"`
	Epv3Today             *float64      `json:"epv3Today,omitempty"`
	CurrentString2        *float64      `json:"currentString2,omitempty"`
	CurrentString9        *float64      `json:"currentString9,omitempty"`
	Status                *float64      `json:"status,omitempty"`
	CurrentString8        *float64      `json:"currentString8,omitempty"`
	CurrentString7        *float64      `json:"currentString7,omitempty"`
	CurrentString6        *float64      `json:"currentString6,omitempty"`
	NBusVoltage           *float64      `json:"nBusVoltage,omitempty"`
	CurrentString1        *float64      `json:"currentString1,omitempty"`
	Pacs                  *float64      `json:"pacs,omitempty"`
	Pacr                  *float64      `json:"pacr,omitempty"`
	StrUnblance           *float64      `json:"strUnblance,omitempty"`
	StrUnmatch            *float64      `json:"strUnmatch,omitempty"`
	SDci                  *float64      `json:"sDci,omitempty"`
	Pact                  *float64      `json:"pact,omitempty"`
	Fac                   *float64      `json:"fac,omitempty"`
	VPidPvbpe             *float64      `json:"vPidPvbpe,omitempty"`
	FaultValue            *float64      `json:"faultValue,omitempty"`
	Epv5Total             *float64      `json:"epv5Total,omitempty"`
	Ipv6                  *float64      `json:"ipv6,omitempty"`
	Ipv5                  *float64      `json:"ipv5,omitempty"`
	Ipv4                  *float64      `json:"ipv4,omitempty"`
	Epv4Today             *float64      `json:"epv4Today,omitempty"`
	Ipv3                  *float64      `json:"ipv3,omitempty"`
	Ipv2                  *float64      `json:"ipv2,omitempty"`
	Ipv1                  *float64      `json:"ipv1,omitempty"`
	IPidPvfpe             *float64      `json:"iPidPvfpe,omitempty"`
	StatusText            *string       `json:"statusText,omitempty"`
	VacRs                 *float64      `json:"vacRs,omitempty"`
	IPidPvgpe             *float64      `json:"iPidPvgpe,omitempty"`
	Ipv9                  *float64      `json:"ipv9,omitempty"`
	Ipv8                  *float64      `json:"ipv8,omitempty"`
	Ipv7                  *float64      `json:"ipv7,omitempty"`
	ID                    *float64      `json:"id,omitempty"`
	Epv8Today             *float64      `json:"epv8Today,omitempty"`
	Gfci                  *float64      `json:"gfci,omitempty"`
	IPidPvhpe             *float64      `json:"iPidPvhpe,omitempty"`
	Ppv10                 *float64      `json:"ppv10,omitempty"`
	Epv3Total             *float64      `json:"epv3Total,omitempty"`
	ApfStatus             *float64      `json:"apfStatus,omitempty"`
	Temperature4          *float64      `json:"temperature4,omitempty"`
	RDci                  *float64      `json:"rDci,omitempty"`
	Pac                   *float64      `json:"pac,omitempty"`
	Temperature5          *float64      `json:"temperature5,omitempty"`
	Vact                  *float64      `json:"vact,omitempty"`
	Compharir             *float64      `json:"compharir,omitempty"`
	Vacr                  *float64      `json:"vacr,omitempty"`
	Compharis             *float64      `json:"compharis,omitempty"`
	Vacs                  *float64      `json:"vacs,omitempty"`
	PidFaultCode          *float64      `json:"pidFaultCode,omitempty"`
	Compharit             *float64      `json:"compharit,omitempty"`
	DeratingMode          *float64      `json:"deratingMode,omitempty"`
	VString1              *float64      `json:"vString1,omitempty"`
	Epv7Today             *float64      `json:"epv7Today,omitempty"`
	VString2              *float64      `json:"vString2,omitempty"`
	VString3              *float64      `json:"vString3,omitempty"`
	VPidPvhpe             *float64      `json:"vPidPvhpe,omitempty"`
	VString4              *float64      `json:"vString4,omitempty"`
	VString5              *float64      `json:"vString5,omitempty"`
	VString6              *float64      `json:"vString6,omitempty"`
	VString8              *float64      `json:"vString8,omitempty"`
	PidStatus             *float64      `json:"pidStatus,omitempty"`
	Iacs                  *float64      `json:"iacs,omitempty"`
	OpFullwatt            *float64      `json:"opFullwatt,omitempty"`
	VString7              *float64      `json:"vString7,omitempty"`
	Iact                  *float64      `json:"iact,omitempty"`
	VString9              *float64      `json:"vString9,omitempty"`
	Epv10Total            *float64      `json:"epv10Total,omitempty"`
	VPidPvfpe             *float64      `json:"vPidPvfpe,omitempty"`
	Ppv5                  *float64      `json:"ppv5,omitempty"`
	Debug1                *string       `json:"debug1,omitempty"`
	Ppv4                  *float64      `json:"ppv4,omitempty"`
	Debug2                *string       `json:"debug2,omitempty"`
	Ppv3                  *float64      `json:"ppv3,omitempty"`
	Ppv2                  *float64      `json:"ppv2,omitempty"`
	Ppv1                  *float64      `json:"ppv1,omitempty"`
	Rac                   *float64      `json:"rac,omitempty"`
	Iacr                  *float64      `json:"iacr,omitempty"`
}

type GetRealtimeDeviceDataResponse struct {
	DefaultResponse
	DeviceSN     *string             `json:"device_sn,omitempty"`
	DataLoggerSN *string             `json:"dataloggerSn,omitempty"`
	Data         *RealtimeDeviceData `json:"data,omitempty"`
}

// GetRealtimeDeviceBatchesData
type GetRealtimeDeviceBatchesDataResponse struct {
	DefaultResponse
	Inverters []string                          `json:"inverters,omitempty"`
	Data      map[string]map[string]interface{} `json:"data,omitempty"`
	PageNum   *int                              `json:"pageNum,omitempty"`
}

// GetInverterAlertList
type AlarmItem struct {
	AlarmCode    *int    `json:"alarm_code,omitempty"`
	Status       *int    `json:"status,omitempty"`
	EndTime      *string `json:"end_time,omitempty"`
	StartTime    *string `json:"start_time,omitempty"`
	AlarmMessage *string `json:"alarm_message,omitempty"`
}

type InverterAlertData struct {
	Count  *int        `json:"count,omitempty"`
	SN     *string     `json:"sn,omitempty"`
	Alarms []AlarmItem `json:"alarms,omitempty"`
}

type GetInverterAlertListResponse struct {
	DefaultResponse
	Data *InverterAlertData `json:"data,omitempty"`
}

// GetEnergyStorageMachineAlertList
type EnergyStorageMachineAlertData struct {
	Count     *int        `json:"count,omitempty"`
	StorageSN *string     `json:"storage_sn,omitempty"`
	Alarms    []AlarmItem `json:"alarms,omitempty"`
}

type GetEnergyStorageMachineAlertListResponse struct {
	DefaultResponse
	Data *EnergyStorageMachineAlertData `json:"data,omitempty"`
}

// GetMaxAlertList
type MaxAlertData struct {
	Count  *int        `json:"count,omitempty"`
	MaxSN  *string     `json:"max_sn,omitempty"`
	Alarms []AlarmItem `json:"alarms,omitempty"`
}

type GetMaxAlertListResponse struct {
	DefaultResponse
	Data *MaxAlertData `json:"data,omitempty"`
}

// GetMixAlertList
type MixAlertData struct {
	Count  *int        `json:"count,omitempty"`
	MixSN  *string     `json:"mix_sn,omitempty"`
	Alarms []AlarmItem `json:"alarms,omitempty"`
}

type GetMixAlertListResponse struct {
	DefaultResponse
	Data *MixAlertData `json:"data,omitempty"`
}

// GetMinAlertList
type MinAlertData struct {
	Count  *int        `json:"count,omitempty"`
	MinSN  *string     `json:"min_sn,omitempty"`
	Alarms []AlarmItem `json:"alarms,omitempty"`
}

type GetMinAlertListResponse struct {
	DefaultResponse
	Data *MinAlertData `json:"data,omitempty"`
}

// GetSpaAlertList
type SpaAlertData struct {
	Count  *int        `json:"count,omitempty"`
	SpaSN  *string     `json:"spa_sn,omitempty"`
	Alarms []AlarmItem `json:"alarms,omitempty"`
}

type GetSpaAlertListResponse struct {
	DefaultResponse
	Data *SpaAlertData `json:"data,omitempty"`
}

// GetPcsAlertList
type PcsAlertData struct {
	Count  *int        `json:"count,omitempty"`
	PcsSN  *string     `json:"pcs_sn,omitempty"`
	Alarms []AlarmItem `json:"alarms,omitempty"`
}

type GetPcsAlertListResponse struct {
	DefaultResponse
	Data *PcsAlertData `json:"data,omitempty"`
}

// GetHpsAlertList
type HpsAlertData struct {
	Count  *int        `json:"count,omitempty"`
	HpsSN  *string     `json:"hps_sn,omitempty"`
	Alarms []AlarmItem `json:"alarms,omitempty"`
}

type GetHpsAlertListResponse struct {
	DefaultResponse
	Data *HpsAlertData `json:"data,omitempty"`
}

// GetPbdAlertList
type PbdAlertData struct {
	Count  *int        `json:"count,omitempty"`
	PbdSN  *string     `json:"pbd_sn,omitempty"`
	Alarms []AlarmItem `json:"alarms,omitempty"`
}

type GetPbdAlertListResponse struct {
	DefaultResponse
	Data *PbdAlertData `json:"data,omitempty"`
}

// |=> GetHistoricalPlantPowerGeneration
type GetHistoricalPlantPowerGenerationResponse struct {
	DefaultResponse
	Data *GetHistoricalPlantPowerGenerationData `json:"data,omitempty"`
}

type GetHistoricalPlantPowerGenerationData struct {
	Count    *int                                   `json:"count,omitempty"`
	TimeUnit *string                                `json:"time_unit,omitempty"`
	Energys  []HistoricalPlantPowerGenerationEnergy `json:"energys,omitempty"`
}

type HistoricalPlantPowerGenerationEnergy struct {
	Date   any     `json:"date"`
	Energy *string `json:"energy"`
}

// |=> GetPlantBasicInfo
type GetPlantBasicInfoResponse struct {
	DefaultResponse
	Data *GetPlantBasicInfoData `json:"data,omitempty"`
}

type GetPlantBasicInfoData struct {
	Address1            *string  `json:"address1,omitempty"`
	InstalledDcCapacity *string  `json:"installed_dc_capacity,omitempty"`
	City                *string  `json:"city,omitempty"`
	Longitude           *string  `json:"longitude,omitempty"`
	Country             *string  `json:"country,omitempty"`
	Latitude            *string  `json:"latitude,omitempty"`
	Locale              *string  `json:"locale,omitempty"`
	Currency            *string  `json:"currency,omitempty"`
	Name                *string  `json:"name,omitempty"`
	PeakPower           *float64 `json:"peak_power,omitempty"`
}

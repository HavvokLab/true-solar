package solarman

type PaginationRequestBody struct {
	Page int `json:"page"`
	Size int `json:"size"`
}

type StationRequestBody struct {
	StationID int `json:"stationId"`
}

type DeviceRequestBody struct {
	DeviceSN string `json:"deviceSn"`
}

type GetTokenRequestBody struct {
	Username    string `json:"username,omitempty"`
	Password    string `json:"password"`
	AppSecret   string `json:"appSecret"`
	CountryCode string `json:"countryCode,omitempty"`
	Email       string `json:"email,omitempty"`
	Mobile      string `json:"mobile,omitempty"`
	OrgID       int    `json:"orgId,omitempty"`
}

type GetTokenResponse struct {
	Code         *string `json:"code,omitempty"`
	Message      *string `json:"msg,omitempty"`
	Success      *bool   `json:"success,omitempty"`
	RequestID    *string `json:"requestId,omitempty"`
	Uid          *int    `json:"uid,omitempty"`
	TokenType    *string `json:"token_type,omitempty"`
	Scope        *string `json:"scope,omitempty"`
	AccessToken  *string `json:"access_token,omitempty"`
	RefreshToken *string `json:"refresh_token,omitempty"`
	ExpiresIn    *string `json:"expires_in,omitempty"`
}

type GetUserInfoResponse struct {
	Code        *string                 `json:"code,omitempty"`
	Message     *string                 `json:"msg,omitempty"`
	Success     *bool                   `json:"success,omitempty"`
	RequestID   *string                 `json:"requestId,omitempty"`
	OrgInfoList []*OrganizationInfoItem `json:"orgInfoList,omitempty"`
}

type OrganizationInfoItem struct {
	CompanyID   *int    `json:"companyId,omitempty"`
	CompanyName *string `json:"companyName,omitempty"`
	RoleName    *string `json:"roleName,omitempty"`
}

type GetPlantListResponse struct {
	Code        *string      `json:"code,omitempty"`
	Message     *string      `json:"msg,omitempty"`
	Success     *bool        `json:"success,omitempty"`
	RequestID   *string      `json:"requestId,omitempty"`
	Total       *int         `json:"total,omitempty"`
	StationList []*PlantItem `json:"stationList,omitempty"`
}

type PlantItem struct {
	ID                      *int     `json:"id,omitempty"`
	Name                    *string  `json:"name,omitempty"`
	LocationLat             *float64 `json:"locationLat,omitempty"`
	LocationLng             *float64 `json:"locationLng,omitempty"`
	LocationAddress         *string  `json:"locationAddress,omitempty"`
	RegionNationID          *int     `json:"regionNationId,omitempty"`
	RegionLevel1            *int     `json:"regionLevel1,omitempty"`
	RegionLevel2            *int     `json:"regionLevel2,omitempty"`
	RegionLevel3            *int     `json:"regionLevel3,omitempty"`
	RegionLevel4            *int     `json:"regionLevel4,omitempty"`
	RegionLevel5            *int     `json:"regionLevel5,omitempty"`
	RegionTimezone          *string  `json:"regionTimezone,omitempty"`
	Type                    *string  `json:"type,omitempty"`
	GridInterconnectionType *string  `json:"gridInterconnectionType,omitempty"`
	InstalledCapacity       *float64 `json:"installedCapacity,omitempty"`
	StartOperatingTime      *float64 `json:"startOperatingTime,omitempty"`
	StationImage            *string  `json:"stationImage,omitempty"`
	CreatedDate             *float64 `json:"createdDate,omitempty"`
	BatterySoc              *float64 `json:"batterySoc,omitempty"`
	NetworkStatus           *string  `json:"networkStatus,omitempty"`
	GenerationPower         *float64 `json:"generationPower,omitempty"`
	LastUpdateTime          *float64 `json:"lastUpdateTime,omitempty"`
}

type GetPlantDeviceListRequestBody struct {
	StationID  int    `json:"stationId"`
	DeviceType string `json:"deviceType,omitempty"`
	Page       int    `json:"page,omitempty"`
	Size       int    `json:"size,omitempty"`
}

type GetPlantDeviceListResponse struct {
	Code            *string            `json:"code,omitempty"`
	Message         *string            `json:"msg,omitempty"`
	Success         *bool              `json:"success,omitempty"`
	RequestID       *string            `json:"requestId,omitempty"`
	Total           *int               `json:"total,omitempty"`
	DeviceListItems []*PlantDeviceItem `json:"deviceListItems,omitempty"`
}

type PlantDeviceItem struct {
	DeviceSN       *string `json:"deviceSn,omitempty"`
	DeviceID       *int    `json:"deviceId,omitempty"`
	DeviceType     *string `json:"deviceType,omitempty"`
	ConnectStatus  *int    `json:"connectStatus,omitempty"`
	CollectionTime *int64  `json:"collectionTime,omitempty"`
}

type GetDeviceAlertListRequestBody struct {
	DeviceSN       string `json:"deviceSn"`
	DeviceID       int    `json:"deviceId,omitempty"`
	StartTimestamp int64  `json:"startTimestamp"`
	EndTimestamp   int64  `json:"endTimestamp"`
	Page           int    `json:"page,omitempty"`
	Size           int    `json:"size,omitempty"`
}

type GetDeviceAlertListResponse struct {
	Code       *string            `json:"code,omitempty"`
	Message    *string            `json:"msg,omitempty"`
	Success    *bool              `json:"success,omitempty"`
	RequestID  *string            `json:"requestId,omitempty"`
	DeviceSN   *string            `json:"deviceSn,omitempty"`
	DeviceID   *int               `json:"deviceId,omitempty"`
	DeviceType *string            `json:"deviceType,omitempty"`
	Total      *int               `json:"total,omitempty"`
	AlertList  []*DeviceAlertItem `json:"alertList,omitempty"`
}

type DeviceAlertItem struct {
	AlertID         *int    `json:"alertId,omitempty"`
	AlertName       *string `json:"addr,omitempty"`
	AlertNameInPAAS *string `json:"alertName,omitempty"`
	Code            *string `json:"code,omitempty"`
	Level           *int    `json:"level,omitempty"`
	Influence       *int    `json:"influence,omitempty"`
	AlertTime       *int64  `json:"alertTime,omitempty"`
}

type GetPlantBaseInfoResponse struct {
	Code                     *string             `json:"code,omitempty"`
	Message                  *string             `json:"msg,omitempty"`
	Success                  *bool               `json:"success,omitempty"`
	RequestID                *string             `json:"requestId,omitempty"`
	ID                       *int                `json:"id,omitempty"`
	Name                     *string             `json:"name,omitempty"`
	LocationLat              *float64            `json:"locationLat,omitempty"`
	LocationLng              *float64            `json:"locationLng,omitempty"`
	LocationAddress          *string             `json:"locationAddress,omitempty"`
	Region                   Region              `json:"region,omitempty"`
	Type                     *string             `json:"type,omitempty"`
	GridInterconnectionType  *string             `json:"gridInterconnectionType,omitempty"`
	InstalledCapacity        *float64            `json:"installedCapacity,omitempty"`
	InstallationAzimuthAngle *float64            `json:"installationAzimuthAngle,omitempty"`
	InstallationTiltAngle    *float64            `json:"installationTiltAngle,omitempty"`
	StartOperatingTime       *float64            `json:"startOperatingTime,omitempty"`
	Currency                 *string             `json:"currency,omitempty"`
	OwnerName                *string             `json:"ownerName,omitempty"`
	OwnerCompany             *string             `json:"ownerCompany,omitempty"`
	ContactPhone             *string             `json:"contactPhone,omitempty"`
	MergeElectricPrice       *float64            `json:"mergeElectricPrice,omitempty"`
	ConstructionCost         *float64            `json:"constructionCost,omitempty"`
	StationImage             *string             `json:"stationImage,omitempty"`
	StationImages            []*StationImageItem `json:"stationImages,omitempty"`
	CreatedDate              *float64            `json:"createdDate,omitempty"`
}

type Region struct {
	NationID *int    `json:"nationId,omitempty"`
	Level1   *int    `json:"level1,omitempty"`
	Level2   *int    `json:"level2,omitempty"`
	Level3   *int    `json:"level3,omitempty"`
	Level4   *int    `json:"level4,omitempty"`
	Level5   *int    `json:"level5,omitempty"`
	Timezone *string `json:"timezone,omitempty"`
}

type StationImageItem struct {
	ID          *int    `json:"id,omitempty"`
	Name        *string `json:"name,omitempty"`
	Description *string `json:"description,omitempty"`
	Url         *string `json:"url,omitempty"`
}

type GetRealtimePlantDataResponse struct {
	Code               *string  `json:"code,omitempty"`
	Message            *string  `json:"msg,omitempty"`
	Success            *bool    `json:"success,omitempty"`
	RequestID          *string  `json:"requestId,omitempty"`
	GenerationPower    *float64 `json:"generationPower,omitempty"`
	UsePower           *float64 `json:"usePower,omitempty"`
	GridPower          *float64 `json:"gridPower,omitempty"`
	PurchasePower      *float64 `json:"purchasePower,omitempty"`
	WirePower          *float64 `json:"wirePower,omitempty"`
	ChargePower        *float64 `json:"chargePower,omitempty"`
	DischargePower     *float64 `json:"dischargePower,omitempty"`
	BatteryPower       *float64 `json:"batteryPower,omitempty"`
	BatterySoc         *float64 `json:"batterySoc,omitempty"`
	IrradiateIntensity *float64 `json:"irradiateIntensity,omitempty"`
	LastUpdateTime     *float64 `json:"lastUpdateTime,omitempty"`
}

type GetHistoricalPlantDataRequestBody struct {
	StationID int    `json:"stationId"`
	StartTime string `json:"startTime"`
	EndTime   string `json:"endTime"`
	TimeType  int    `json:"timeType"`
}

type GetHistoricalPlantDataResponse struct {
	Code             *string                      `json:"code,omitempty"`
	Message          *string                      `json:"msg,omitempty"`
	Success          *bool                        `json:"success,omitempty"`
	RequestID        *string                      `json:"requestId,omitempty"`
	Total            *int                         `json:"total,omitempty"`
	StationDataItems []*HistoricalStationDataItem `json:"stationDataItems,omitempty"`
}

type HistoricalStationDataItem struct {
	GenerationPower       *float64 `json:"generationPower,omitempty"`
	UsePower              *float64 `json:"usePower,omitempty"`
	GridPower             *float64 `json:"gridPower,omitempty"`
	PurchasePower         *float64 `json:"purchasePower,omitempty"`
	WirePower             *float64 `json:"wirePower,omitempty"`
	ChargePower           *float64 `json:"chargePower,omitempty"`
	DischargePower        *float64 `json:"dischargePower,omitempty"`
	BatterPower           *float64 `json:"batteryPower,omitempty"`
	BatterSoc             *float64 `json:"batterySoc,omitempty"`
	IrradiateIntensity    *float64 `json:"irradiateIntensity,omitempty"`
	GenerationValue       *float64 `json:"generationValue,omitempty"`
	GenerationRation      *float64 `json:"generationRatio,omitempty"`
	GridRatio             *float64 `json:"gridRatio,omitempty"`
	ChargeRatio           *float64 `json:"chargeRatio,omitempty"`
	UseValue              *float64 `json:"useValue,omitempty"`
	UseRatio              *float64 `json:"useRatio,omitempty"`
	BuyRatio              *float64 `json:"buyRatio,omitempty"`
	UseDischargeRatio     *float64 `json:"useDischargeRatio,omitempty"`
	GridValue             *float64 `json:"gridValue,omitempty"`
	BuyValue              *float64 `json:"buyValue,omitempty"`
	ChargeValue           *float64 `json:"chargeValue,omitempty"`
	DischargeValue        *float64 `json:"dischargeValue,omitempty"`
	FullPowerHours        *float64 `json:"fullPowerHours,omitempty"`
	Irradiate             *float64 `json:"irradiate,omitempty"`
	TheoreticalGeneration *float64 `json:"theoreticalGeneration,omitempty"`
	PR                    *float64 `json:"pr,omitempty"`
	CPR                   *float64 `json:"cpr,omitempty"`
	DateTime              *float64 `json:"dateTime,omitempty"`
	Year                  *int     `json:"year,omitempty"`
	Month                 *int     `json:"month,omitempty"`
	Day                   *int     `json:"day,omitempty"`
}

type GetRealtimeDeviceDataResponse struct {
	Code        *string     `json:"code,omitempty"`
	Message     *string     `json:"msg,omitempty"`
	Success     *bool       `json:"success,omitempty"`
	RequestID   *string     `json:"requestId,omitempty"`
	DeviceSN    *string     `json:"deviceSn,omitempty"`
	DeviceID    *int        `json:"deviceId,omitempty"`
	DeviceType  *string     `json:"deviceType,omitempty"`
	DeviceState *int        `json:"deviceState,omitempty"`
	DataList    []*DataItem `json:"dataList,omitempty"`
}

type GetHistoricalDeviceDataRequestBody struct {
	DeviceSN  string `json:"deviceSn"`
	StartTime string `json:"startTime"`
	EndTime   string `json:"endTime"`
	TimeType  int    `json:"timeType"`
}

type GetHistoricalDeviceDataResponse struct {
	Code          *string          `json:"code,omitempty"`
	Message       *string          `json:"msg,omitempty"`
	Success       *bool            `json:"success,omitempty"`
	RequestID     *string          `json:"requestId,omitempty"`
	DeviceSN      *string          `json:"deviceSn,omitempty"`
	DeviceID      *int             `json:"deviceId,omitempty"`
	DeviceType    *string          `json:"deviceType,omitempty"`
	TimeType      *int             `json:"timeType,omitempty"`
	ParamDataList []*ParamDataItem `json:"paramDataList,omitempty"`
}

type ParamDataItem struct {
	CollectTime *string     `json:"collectTime,omitempty"`
	DataList    []*DataItem `json:"dataList,omitempty"`
}

type DataItem struct {
	Key   *string `json:"key,omitempty"`
	Value *string `json:"value,omitempty"`
	Unit  *string `json:"unit,omitempty"`
	Name  *string `json:"name,omitempty"`
}

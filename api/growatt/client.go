package growatt

// TODO - validate API path from document
import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"dario.cat/mergo"
	"github.com/HavvokLab/true-solar/model"
	"github.com/HavvokLab/true-solar/pkg/logger"
	"github.com/imroc/req/v3"
	"github.com/rs/zerolog"
	"go.openly.dev/pointy"
)

const (
	AuthHeader  = "Token"
	MaxPageSize = 100
	BatchSize   = 50
)

const (
	GrowattPlantStatusOnline  = "ONLINE"
	GrowattPlantStatusOffline = "OFFLINE"
	GrowattPlantStatusAlarm   = "ALARM"
)

const (
	GrowattDeviceStatusOnline      = "ONLINE"
	GrowattDeviceStatusOffline     = "OFFLINE"
	GrowattDeviceStatusDisconnect  = "DISCONNECT"
	GrowattDeviceStatusStandBy     = "STAND BY"
	GrowattDeviceStatusFailure     = "FAILURE"
	GrowattDeviceStatusCharging    = "CHARGING"
	GrowattDeviceStatusDischarging = "DISCHARGING"
	GrowattDeviceStatusBurning     = "BURNING"
	GrowattDeviceStatusWaiting     = "WAITING"
	GrowattDeviceStatusSelfCheck   = "SELF CHECK"
	GrowattDeviceStatusUpgrading   = "UPGRADING"
)

const (
	GrowattDeviceTypeInverter = iota + 1
	GrowattDeviceTypeEnergyStorageMachine
	GrowattDeviceTypeOtherEquipment
	GrowattDeviceTypeMax
	GrowattDeviceTypeMix
	GrowattDeviceTypeSpA
	GrowattDeviceTypeMin
	GrowattDeviceTypePcs
	GrowattDeviceTypeHps
	GrowattDeviceTypePbd
)

func ParseGrowattDeviceType(deviceType int) string {
	switch deviceType {
	case GrowattDeviceTypeInverter:
		return "INVERTER"
	case GrowattDeviceTypeEnergyStorageMachine:
		return "ENERGY STORAGE MACHINE"
	case GrowattDeviceTypeOtherEquipment:
		return "OTHER EQUIPMENT"
	case GrowattDeviceTypeMax:
		return "MAX"
	case GrowattDeviceTypeMix:
		return "MIX"
	case GrowattDeviceTypeSpA:
		return "SPA"
	case GrowattDeviceTypeMin:
		return "MIN"
	case GrowattDeviceTypePcs:
		return "PCS"
	case GrowattDeviceTypeHps:
		return "HPS"
	case GrowattDeviceTypePbd:
		return "PBD"
	default:
		return ""
	}
}

type GrowattClient struct {
	reqClient *req.Client
	username  string
	token     string
	url       string
	headers   map[string]string
	logger    zerolog.Logger
}

func NewGrowattClient(username, token string) *GrowattClient {
	logger := zerolog.New(logger.NewWriter("growatt_api.log")).With().Caller().Timestamp().Logger()
	g := &GrowattClient{
		reqClient: req.C().
			SetCommonRetryCount(3).
			SetCommonRetryFixedInterval(5 * time.Minute),
		url:      "https://openapi.growatt.com/v1",
		username: username,
		token:    token,
		headers:  map[string]string{AuthHeader: token},
		logger:   logger,
	}

	return g
}

func (g *GrowattClient) GetPlantListWithPagination(page, size int) (*GetPlantListResponse, error) {
	query := map[string]string{
		"user_name": g.username,
		"page":      strconv.Itoa(page),
		"perpage":   strconv.Itoa(size),
	}

	url := g.url + "/plant/user_plant_list"
	result := GetPlantListResponse{}
	errorResult := model.ApiErrorResponse{}
	resp, err := g.reqClient.R().
		SetHeaders(g.headers).
		SetQueryParams(query).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Post(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		g.logger.Error().
			Err(err).
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Str("raw", string(raw)).
			Any("query", query).
			Msg("failed to get plant list")
		return nil, err
	}

	if resp.IsErrorState() {
		g.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("error_response", errorResult).
			Any("query", query).
			Msg("failed to get plant list")
		return nil, fmt.Errorf("GrowattClient::GetPlantListWithPagination() - failed")
	}

	g.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("result", result).
		Any("query", query).
		Msg("GrowattClient::GetPlantListWithPagination() - success")
	return &result, nil
}

func (g *GrowattClient) GetPlantList() ([]PlantItem, error) {
	plants := make([]PlantItem, 0)
	page := 1
	for {
		res, err := g.GetPlantListWithPagination(page, MaxPageSize)
		if err != nil {
			return nil, err
		}

		plants = append(plants, res.Data.Plants...)

		if len(plants) >= pointy.IntValue(res.Data.Count, 0) {
			break
		}

		page += 1
	}

	return plants, nil
}

func (g *GrowattClient) GetPlantOverviewInfo(plantId int) (*GetPlantOverviewInfoResponse, error) {
	url := g.url + "/plant/data"
	query := map[string]string{
		"plant_id": strconv.Itoa(plantId),
	}

	result := GetPlantOverviewInfoResponse{}
	errorResult := model.ApiErrorResponse{}
	resp, err := g.reqClient.R().
		SetHeaders(g.headers).
		SetQueryParams(query).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Get(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		g.logger.Error().
			Err(err).
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Str("raw", string(raw)).
			Any("query", query).
			Msg("failed to get plant overview info")
		return nil, err
	}

	if resp.IsErrorState() {
		g.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("error_response", errorResult).
			Any("query", query).
			Msg("failed to get plant overview info")
		return nil, fmt.Errorf("GrowattClient::GetPlantOverviewInfo() - failed")
	}

	g.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("result", result).
		Any("query", query).
		Msg("GrowattClient::GetPlantOverviewInfo() - success")
	return &result, nil
}

func (g *GrowattClient) GetPlantDataLoggerInfo(plantId int) (*GetPlantDataLoggerInfoResponse, error) {
	url := g.url + "/device/datalogger/list"
	query := map[string]string{
		"plant_id": strconv.Itoa(plantId),
	}

	result := GetPlantDataLoggerInfoResponse{}
	errorResult := model.ApiErrorResponse{}
	resp, err := g.reqClient.R().
		SetHeaders(g.headers).
		SetQueryParams(query).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Get(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		g.logger.Error().
			Err(err).
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Str("raw", string(raw)).
			Any("query", query).
			Msg("failed to get plant data logger info")
		return nil, err
	}

	if resp.IsErrorState() {
		g.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("error_response", errorResult).
			Any("query", query).
			Msg("failed to get plant data logger info")
		return nil, fmt.Errorf("GrowattClient::GetPlantDataLoggerInfo() - failed")
	}

	g.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("result", result).
		Any("query", query).
		Msg("GrowattClient::GetPlantDataLoggerInfo() - success")
	return &result, nil
}

func (g *GrowattClient) GetPlantDeviceListWithPagination(plantId, page, size int) (*GetPlantDeviceListResponse, error) {
	url := g.url + "/device/list"
	query := map[string]string{
		"plant_id": strconv.Itoa(plantId),
		"page":     strconv.Itoa(page),
		"perpage":  strconv.Itoa(size),
	}

	result := GetPlantDeviceListResponse{}
	errorResult := model.ApiErrorResponse{}
	resp, err := g.reqClient.R().
		SetHeaders(g.headers).
		SetQueryParams(query).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Get(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		g.logger.Error().
			Err(err).
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Str("raw", string(raw)).
			Any("query", query).
			Msg("failed to get plant device list")
		return nil, err
	}

	if resp.IsErrorState() {
		g.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("error_response", errorResult).
			Any("query", query).
			Msg("failed to get plant device list")
		return nil, fmt.Errorf("GrowattClient::GetPlantDeviceListWithPagination() - failed")
	}

	g.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("result", result).
		Any("query", query).
		Msg("GrowattClient::GetPlantDeviceListWithPagination() - success")
	return &result, nil
}

func (r *GrowattClient) GetPlantDeviceList(plantId int) ([]DeviceItem, error) {
	devices := make([]DeviceItem, 0)
	page := 1
	for {
		res, err := r.GetPlantDeviceListWithPagination(plantId, page, MaxPageSize)
		if err != nil {
			return nil, err
		}

		devices = append(devices, res.Data.Devices...)

		if len(devices) >= pointy.IntValue(res.Data.Count, 0) {
			break
		}

		page += 1
	}

	return devices, nil
}

func (g *GrowattClient) GetRealtimeDeviceBatchDataWithPagination(deviceSNs []string, page int) (*GetRealtimeDeviceBatchesDataResponse, error) {
	query := map[string]string{
		"inverter": strings.Join(deviceSNs, ","),
		"pageNum":  strconv.Itoa(page),
	}

	url := g.url + "/device/inverter/invs_data"
	result := GetRealtimeDeviceBatchesDataResponse{}
	errorResult := model.ApiErrorResponse{}
	resp, err := g.reqClient.R().
		SetHeaders(g.headers).
		SetHeader("Accept", "application/json").
		SetQueryParams(query).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Post(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		g.logger.Error().
			Err(err).
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Str("raw", string(raw)).
			Any("query", query).
			Msg("failed to get realtime device batch data")
		return nil, err
	}

	if resp.IsErrorState() {
		g.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("error_response", errorResult).
			Any("query", query).
			Msg("failed to get realtime device batch data")
		return nil, fmt.Errorf("GrowattClient::GetRealtimeDeviceBatchDataWithPagination() - failed")
	}

	g.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("result", result).
		Any("query", query).
		Msg("GrowattClient::GetRealtimeDeviceBatchDataWithPagination() - success")
	return &result, nil
}

func (g *GrowattClient) GetRealtimeDeviceBatchesData(deviceSNs []string) (*GetRealtimeDeviceBatchesDataResponse, error) {
	batches := make([][]string, 0)
	var j int
	for i := 0; i < len(deviceSNs); i += BatchSize {
		j += BatchSize
		if j > len(deviceSNs) {
			j = len(deviceSNs)
		}

		batches = append(batches, deviceSNs[i:j])
	}
	g.logger.Info().Int("count", len(batches)).Msg("GrowattClient::GetRealtimeDeviceBatchesData() - splitting device SNs into batches")

	result := GetRealtimeDeviceBatchesDataResponse{
		Inverters: make([]string, 0),
		Data:      make(map[string]map[string]interface{}, 0),
		PageNum:   pointy.Int(1),
	}

	for _, batch := range batches {
		resp, err := g.GetRealtimeDeviceBatchDataWithPagination(batch, 1)
		if err != nil {
			return nil, err
		}

		err = mergo.Merge(&result.Data, resp.Data)
		if err != nil {
			return nil, err
		}
	}

	return &result, nil
}

func (g *GrowattClient) GetInverterAlertListWithPagination(deviceSN string, page, size int) (*GetInverterAlertListResponse, error) {
	url := g.url + "/device/inverter/alarm"
	query := map[string]string{
		"device_sn": deviceSN,
		"page":      strconv.Itoa(page),
		"perpage":   strconv.Itoa(size),
	}

	result := GetInverterAlertListResponse{}
	errorResult := model.ApiErrorResponse{}
	resp, err := g.reqClient.R().
		SetHeaders(g.headers).
		SetHeader("Accept", "application/json").
		SetQueryParams(query).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Get(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		g.logger.Error().
			Err(err).
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Str("raw", string(raw)).
			Any("query", query).
			Msg("failed to get inverter alert list")

		return nil, err
	}

	if resp.IsErrorState() {
		g.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("error_response", errorResult).
			Any("query", query).
			Msg("failed to get inverter alert list")

		return nil, fmt.Errorf("GrowattClient::GetInverterAlertListWithPagination() - failed")
	}

	g.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("result", result).
		Any("query", query).
		Msg("GrowattClient::GetInverterAlertListWithPagination() - success")

	return &result, nil
}

func (g *GrowattClient) GetInverterAlertList(deviceSN string) ([]AlarmItem, error) {
	alerts := make([]AlarmItem, 0)
	page := 1
	for {
		res, err := g.GetInverterAlertListWithPagination(deviceSN, page, MaxPageSize)
		if err != nil {
			return nil, err
		}

		alerts = append(alerts, res.Data.Alarms...)

		if len(alerts) >= pointy.IntValue(res.Data.Count, 0) {
			break
		}

		page += 1
	}

	return alerts, nil
}

func (g *GrowattClient) GetEnergyStorageMachineAlertList(deviceSN string, timestamp int64) (*GetEnergyStorageMachineAlertListResponse, error) {
	url := g.url + "/device/storage/alarm_data"
	query := map[string]string{
		"device_sn": deviceSN,
		"date":      time.Unix(timestamp, 0).Format("2006-01-02"),
	}

	result := GetEnergyStorageMachineAlertListResponse{}
	errorResult := model.ApiErrorResponse{}
	resp, err := g.reqClient.R().
		SetHeaders(g.headers).
		SetQueryParams(query).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Get(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		g.logger.Error().
			Err(err).
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Str("raw", string(raw)).
			Any("query", query).
			Msg("failed to get energy storage machine alert list")

		return nil, err
	}

	if resp.IsErrorState() {
		g.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("error_response", errorResult).
			Any("query", query).
			Msg("failed to get energy storage machine alert list")

		return nil, fmt.Errorf("GrowattClient::GetEnergyStorageMachineAlertList() - failed")
	}

	g.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("result", result).
		Any("query", query).
		Msg("GrowattClient::GetEnergyStorageMachineAlertList() - success")

	return &result, nil
}

func (g *GrowattClient) GetMaxAlertListWithPagination(deviceSN string, timestamp int64, page, size int) (*GetMaxAlertListResponse, error) {
	url := g.url + "/device/max/alarm_data"
	query := map[string]string{
		"max_sn":  deviceSN,
		"date":    time.Unix(timestamp, 0).Format("2006-01-02"),
		"page":    strconv.Itoa(page),
		"perpage": strconv.Itoa(size),
	}

	result := GetMaxAlertListResponse{}
	errorResult := model.ApiErrorResponse{}
	resp, err := g.reqClient.R().
		SetHeaders(g.headers).
		SetQueryParams(query).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Get(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		g.logger.Error().
			Err(err).
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Str("raw", string(raw)).
			Any("query", query).
			Msg("failed to get max alert list")

		return nil, err
	}

	if resp.IsErrorState() {
		g.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("error_response", errorResult).
			Any("query", query).
			Msg("failed to get max alert list")

		return nil, fmt.Errorf("GrowattClient::GetMaxAlertListWithPagination() - failed")
	}

	g.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("result", result).
		Any("query", query).
		Msg("GrowattClient::GetMaxAlertListWithPagination() - success")

	return &result, nil
}

func (g *GrowattClient) GetMaxAlertList(deviceSN string, timestamp int64) ([]AlarmItem, error) {
	alerts := make([]AlarmItem, 0)
	page := 1
	for {
		res, err := g.GetMaxAlertListWithPagination(deviceSN, timestamp, page, MaxPageSize)
		if err != nil {
			return nil, err
		}

		alerts = append(alerts, res.Data.Alarms...)

		if len(alerts) >= pointy.IntValue(res.Data.Count, 0) {
			break
		}

		page += 1
	}

	return alerts, nil
}

func (g *GrowattClient) GetMixAlertListWithPagination(deviceSN string, timestamp int64, page, size int) (*GetMixAlertListResponse, error) {
	url := g.url + "/device/mix/alarm_data"
	query := map[string]string{
		"mix_sn":  deviceSN,
		"date":    time.Unix(timestamp, 0).Format("2006-01-02"),
		"page":    strconv.Itoa(page),
		"perpage": strconv.Itoa(size),
	}

	result := GetMixAlertListResponse{}
	errorResult := model.ApiErrorResponse{}
	resp, err := g.reqClient.R().
		SetHeaders(g.headers).
		SetQueryParams(query).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Get(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		g.logger.Error().
			Err(err).
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Str("raw", string(raw)).
			Any("query", query).
			Msg("failed to get mix alert list")

		return nil, err
	}

	if resp.IsErrorState() {
		g.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("error_response", errorResult).
			Any("query", query).
			Msg("failed to get mix alert list")

		return nil, fmt.Errorf("GrowattClient::GetMixAlertListWithPagination() - failed")
	}

	g.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("result", result).
		Any("query", query).
		Msg("GrowattClient::GetMixAlertListWithPagination() - success")

	return &result, nil
}

func (g *GrowattClient) GetMixAlertList(deviceSN string, timestamp int64) ([]AlarmItem, error) {
	alerts := make([]AlarmItem, 0)
	page := 1
	for {
		res, err := g.GetMixAlertListWithPagination(deviceSN, timestamp, page, MaxPageSize)
		if err != nil {
			return nil, err
		}

		alerts = append(alerts, res.Data.Alarms...)

		if len(alerts) >= pointy.IntValue(res.Data.Count, 0) {
			break
		}

		page += 1
	}

	return alerts, nil
}

func (g *GrowattClient) GetMinAlertListWithPagination(deviceSN string, timestamp int64, page, size int) (*GetMinAlertListResponse, error) {
	url := g.url + "/device/min/alarm_data"
	query := map[string]string{
		"min_sn":  deviceSN,
		"date":    time.Unix(timestamp, 0).Format("2006-01-02"),
		"page":    strconv.Itoa(page),
		"perpage": strconv.Itoa(size),
	}

	result := GetMinAlertListResponse{}
	errorResult := model.ApiErrorResponse{}
	resp, err := g.reqClient.R().
		SetHeaders(g.headers).
		SetQueryParams(query).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Get(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		g.logger.Error().
			Err(err).
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Str("raw", string(raw)).
			Any("query", query).
			Msg("failed to get min alert list")

		return nil, err
	}

	if resp.IsErrorState() {
		g.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("error_response", errorResult).
			Any("query", query).
			Msg("failed to get min alert list")

		return nil, fmt.Errorf("GrowattClient::GetMinAlertListWithPagination() - failed")
	}

	g.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("result", result).
		Any("query", query).
		Msg("GrowattClient::GetMinAlertListWithPagination() - success")

	return &result, nil
}

func (g *GrowattClient) GetMinAlertList(deviceSN string, timestamp int64) ([]AlarmItem, error) {
	alerts := make([]AlarmItem, 0)
	page := 1
	for {
		res, err := g.GetMinAlertListWithPagination(deviceSN, timestamp, page, MaxPageSize)
		if err != nil {
			return nil, err
		}

		alerts = append(alerts, res.Data.Alarms...)

		if len(alerts) >= pointy.IntValue(res.Data.Count, 0) {
			break
		}

		page += 1
	}

	return alerts, nil
}

func (g *GrowattClient) GetSpaAlertListWithPagination(deviceSN string, timestamp int64, page, size int) (*GetSpaAlertListResponse, error) {
	url := g.url + "/device/spa/alarm_data"
	query := map[string]string{
		"spa_sn":  deviceSN,
		"date":    time.Unix(timestamp, 0).Format("2006-01-02"),
		"page":    strconv.Itoa(page),
		"perpage": strconv.Itoa(size),
	}

	result := GetSpaAlertListResponse{}
	errorResult := model.ApiErrorResponse{}
	resp, err := g.reqClient.R().
		SetHeaders(g.headers).
		SetQueryParams(query).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Get(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		g.logger.Error().
			Err(err).
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Str("raw", string(raw)).
			Any("query", query).
			Msg("failed to get spa alert list")

		return nil, err
	}

	if resp.IsErrorState() {
		g.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("error_response", errorResult).
			Any("query", query).
			Msg("failed to get spa alert list")

		return nil, fmt.Errorf("GrowattClient::GetSpaAlertListWithPagination() - failed")
	}

	g.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("result", result).
		Any("query", query).
		Msg("GrowattClient::GetSpaAlertListWithPagination() - success")

	return &result, nil
}

func (g *GrowattClient) GetSpaAlertList(deviceSN string, timestamp int64) ([]AlarmItem, error) {
	alerts := make([]AlarmItem, 0)
	page := 1
	for {
		res, err := g.GetSpaAlertListWithPagination(deviceSN, timestamp, page, MaxPageSize)
		if err != nil {
			return nil, err
		}

		alerts = append(alerts, res.Data.Alarms...)

		if len(alerts) >= pointy.IntValue(res.Data.Count, 0) {
			break
		}

		page += 1
	}

	return alerts, nil
}

func (g *GrowattClient) GetPcsAlertListWithPagination(deviceSN string, timestamp int64, page, size int) (*GetPcsAlertListResponse, error) {
	url := g.url + "/device/pcs/alarm_data"
	query := map[string]string{
		"pcs_sn":  deviceSN,
		"date":    time.Unix(timestamp, 0).Format("2006-01-02"),
		"page":    strconv.Itoa(page),
		"perpage": strconv.Itoa(size),
	}

	result := GetPcsAlertListResponse{}
	errorResult := model.ApiErrorResponse{}
	resp, err := g.reqClient.R().
		SetHeaders(g.headers).
		SetQueryParams(query).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Get(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		g.logger.Error().
			Err(err).
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Str("raw", string(raw)).
			Any("query", query).
			Msg("failed to get pcs alert list")

		return nil, err
	}

	if resp.IsErrorState() {
		g.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("error_response", errorResult).
			Any("query", query).
			Msg("failed to get pcs alert list")

		return nil, fmt.Errorf("GrowattClient::GetPcsAlertListWithPagination() - failed")
	}

	g.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("result", result).
		Any("query", query).
		Msg("GrowattClient::GetPcsAlertListWithPagination() - success")

	return &result, nil
}

func (g *GrowattClient) GetPcsAlertList(deviceSN string, timestamp int64) ([]AlarmItem, error) {
	alerts := make([]AlarmItem, 0)
	page := 1
	for {
		res, err := g.GetPcsAlertListWithPagination(deviceSN, timestamp, page, MaxPageSize)
		if err != nil {
			return nil, err
		}

		alerts = append(alerts, res.Data.Alarms...)

		if len(alerts) >= pointy.IntValue(res.Data.Count, 0) {
			break
		}

		page += 1
	}

	return alerts, nil
}

func (g *GrowattClient) GetHpsAlertListWithPagination(deviceSN string, timestamp int64, page, size int) (*GetHpsAlertListResponse, error) {
	url := g.url + "/device/hps/alarm_data"
	query := map[string]string{
		"hps_sn":  deviceSN,
		"date":    time.Unix(timestamp, 0).Format("2006-01-02"),
		"page":    strconv.Itoa(page),
		"perpage": strconv.Itoa(size),
	}

	result := GetHpsAlertListResponse{}
	errorResult := model.ApiErrorResponse{}
	resp, err := g.reqClient.R().
		SetHeaders(g.headers).
		SetQueryParams(query).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Get(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		g.logger.Error().
			Err(err).
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Str("raw", string(raw)).
			Any("query", query).
			Msg("failed to get hps alert list")

		return nil, err
	}

	if resp.IsErrorState() {
		g.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("error_response", errorResult).
			Any("query", query).
			Msg("failed to get hps alert list")

		return nil, fmt.Errorf("GrowattClient::GetHpsAlertListWithPagination() - failed")
	}

	g.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("result", result).
		Any("query", query).
		Msg("GrowattClient::GetHpsAlertListWithPagination() - success")

	return &result, nil
}

func (g *GrowattClient) GetHpsAlertList(deviceSN string, timestamp int64) ([]AlarmItem, error) {
	alerts := make([]AlarmItem, 0)
	page := 1
	for {
		res, err := g.GetHpsAlertListWithPagination(deviceSN, timestamp, page, MaxPageSize)
		if err != nil {
			return nil, err
		}

		alerts = append(alerts, res.Data.Alarms...)

		if len(alerts) >= pointy.IntValue(res.Data.Count, 0) {
			break
		}

		page += 1
	}

	return alerts, nil
}

func (g *GrowattClient) GetPbdAlertListWithPagination(deviceSN string, timestamp int64, page, size int) (*GetPbdAlertListResponse, error) {
	url := g.url + "/device/pbd/alarm_data"
	query := map[string]string{
		"pbd_sn":  deviceSN,
		"date":    time.Unix(timestamp, 0).Format("2006-01-02"),
		"page":    strconv.Itoa(page),
		"perpage": strconv.Itoa(size),
	}

	result := GetPbdAlertListResponse{}
	errorResult := model.ApiErrorResponse{}
	resp, err := g.reqClient.R().
		SetHeaders(g.headers).
		SetQueryParams(query).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Get(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		g.logger.Error().
			Err(err).
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Str("raw", string(raw)).
			Any("query", query).
			Msg("failed to get pbd alert list")

		return nil, err
	}

	if resp.IsErrorState() {
		g.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("error_response", errorResult).
			Any("query", query).
			Msg("failed to get pbd alert list")

		return nil, fmt.Errorf("GrowattClient::GetPbdAlertListWithPagination() - failed")
	}

	g.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("result", result).
		Any("query", query).
		Msg("GrowattClient::GetPbdAlertListWithPagination() - success")

	return &result, nil
}

func (g *GrowattClient) GetPbdAlertList(deviceSN string, timestamp int64) ([]AlarmItem, error) {
	alerts := make([]AlarmItem, 0)
	page := 1
	for {
		res, err := g.GetPbdAlertListWithPagination(deviceSN, timestamp, page, MaxPageSize)
		if err != nil {
			return nil, err
		}

		alerts = append(alerts, res.Data.Alarms...)

		if len(alerts) >= pointy.IntValue(res.Data.Count, 0) {
			break
		}

		page += 1
	}

	return alerts, nil
}

func (g *GrowattClient) GetHistoricalPlantPowerGenerationWithPagination(plantId int, start, end int64, unit string, page, size int) (*GetHistoricalPlantPowerGenerationResponse, error) {
	query := map[string]string{
		"plant_id":   strconv.Itoa(plantId),
		"start_date": time.Unix(start, 0).Format("2006-01-02"),
		"end_date":   time.Unix(end, 0).Format("2006-01-02"),
		"time_unit":  unit,
		"page":       strconv.Itoa(page),
		"perpage":    strconv.Itoa(size),
	}

	url := g.url + "/plant/energy"
	result := GetHistoricalPlantPowerGenerationResponse{}
	errorResult := model.ApiErrorResponse{}
	resp, err := g.reqClient.R().
		SetHeaders(g.headers).
		SetQueryParams(query).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Get(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		g.logger.Error().
			Err(err).
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Str("raw", string(raw)).
			Any("query", query).
			Msg("failed to get historical plant power generation")

		return nil, err
	}

	if resp.IsErrorState() {
		g.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("error_response", errorResult).
			Any("query", query).
			Msg("failed to get historical plant power generation")

		return nil, fmt.Errorf("GrowattClient::GetHistoricalPlantPowerGenerationWithPagination() - failed")
	}

	g.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("result", result).
		Any("query", query).
		Msg("GrowattClient::GetHistoricalPlantPowerGenerationWithPagination() - success")

	return &result, nil
}

func (g *GrowattClient) GetHistoricalPlantPowerGeneration(plantId int, start, end int64, unit string) ([]HistoricalPlantPowerGenerationEnergy, error) {
	result := make([]HistoricalPlantPowerGenerationEnergy, 0)

	page := 1
	for {
		res, err := g.GetHistoricalPlantPowerGenerationWithPagination(plantId, start, end, unit, page, MaxPageSize)
		if err != nil {
			return nil, err
		}

		result = append(result, res.Data.Energys...)
		if len(result) >= pointy.IntValue(res.Data.Count, 0) {
			break
		}

		page += 1
	}

	return result, nil
}

func (g *GrowattClient) GetPlantBasicInfo(plantId int) (*GetPlantBasicInfoResponse, error) {
	url := g.url + "/plant/details"
	query := map[string]string{
		"plant_id": strconv.Itoa(plantId),
	}

	result := GetPlantBasicInfoResponse{}
	errorResult := model.ApiErrorResponse{}
	resp, err := g.reqClient.R().
		SetHeaders(g.headers).
		SetQueryParams(query).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Get(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		g.logger.Error().
			Err(err).
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Str("raw", string(raw)).
			Any("query", query).
			Msg("failed to get plant basic info")

		return nil, err
	}

	if resp.IsErrorState() {
		g.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("error_response", errorResult).
			Any("query", query).
			Msg("failed to get plant basic info")

		return nil, fmt.Errorf("GrowattClient::GetPlantBasicInfo() - failed")
	}

	g.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("result", result).
		Any("query", query).
		Msg("GrowattClient::GetPlantBasicInfo() - success")

	return &result, nil
}

package huawei2

import (
	"fmt"
	"io"

	"github.com/HavvokLab/true-solar/model"
	"github.com/HavvokLab/true-solar/pkg/logger"
	"github.com/HavvokLab/true-solar/pkg/util"
	"github.com/imroc/req/v3"
	"github.com/rs/zerolog"
)

const (
	AuthHeader      = "XSRF-TOKEN"
	CurrencyUSD     = "USD"
	LanguageEnglish = "en_UK"
)

const (
	HuaweiStatusOnline  = "ONLINE"
	HuaweiStatusOffline = "OFFLINE"
	HuaweiStatusAlarm   = "ALARM"
)

var HuaweiMapPlantStatus = map[int]string{
	1: HuaweiStatusOffline,
	2: HuaweiStatusAlarm,
	3: HuaweiStatusOnline,
}

var HuaweiMapDeviceStatus = map[int]string{
	0: HuaweiStatusOffline,
	1: HuaweiStatusOnline,
}

type Huawei2Client struct {
	reqClient *req.Client
	username  string
	password  string
	url       string
	headers   map[string]string
	logger    zerolog.Logger
}

func NewHuawei2Client(username, password string) (*Huawei2Client, error) {
	h := &Huawei2Client{
		reqClient: req.C(),
		url:       "https://sg5.fusionsolar.huawei.com",
		username:  username,
		password:  password,
		headers:   make(map[string]string),
		logger:    zerolog.New(logger.NewWriter("huawei2_api.log")).With().Timestamp().Logger(),
	}

	token, err := h.GetToken(username, password)
	if err != nil {
		return nil, err
	}

	h.headers[AuthHeader] = token
	return h, nil
}

func (h *Huawei2Client) GetToken(username, password string) (string, error) {
	url := h.url + "/thirdData/login"
	body := map[string]any{
		"userName":   username,
		"systemCode": password,
	}

	result := GetTokenResponse{}
	errorResult := model.ApiErrorResponse{}
	resp, err := h.reqClient.R().
		SetBody(body).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Post(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		h.logger.Error().
			Err(err).
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Str("raw", string(raw)).
			Any("body", body).
			Msg("failed to get token")
		return "", err
	}

	if resp.IsErrorState() {
		h.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("error_response", errorResult).
			Any("body", body).
			Msg("failed to get token")
		return "", fmt.Errorf("Huawei2Client::GetToken() - failed")
	}

	var token string
	if result.Success {
		for _, c := range resp.Cookies() {
			if c.Name == AuthHeader {
				token = c.Value
				break
			}
		}
	}

	if util.IsEmpty(token) {
		h.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("result", result).
			Any("body", body).
			Msg("empty token")
		return "", fmt.Errorf("Huawei2Client::GetToken() - failed")
	}

	h.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("result", result).
		Any("body", body).
		Msg("Huawei2Client::GetToken() - success")
	return token, nil
}

func (h *Huawei2Client) GetPlantListWithPagination(page int) (*GetPlantListResponse, error) {
	url := h.url + "/thirdData/stations"
	body := map[string]any{
		"pageNo": page,
	}

	var result GetPlantListResponse
	var errorResult model.ApiErrorResponse
	resp, err := h.reqClient.R().
		SetHeaders(h.headers).
		SetBody(body).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Post(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		h.logger.Error().
			Err(err).
			Int("status_code", resp.StatusCode).
			Str("url", url).
			Str("raw", string(raw)).
			Any("body", body).
			Msg("failed to get plant list")
		return nil, err
	}

	if resp.IsErrorState() {
		h.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("error_response", errorResult).
			Any("body", body).
			Msg("failed to get plant list")
		return nil, fmt.Errorf("Huawei2Client::GetPlantList() - failed")
	}

	if !result.Success {
		h.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("result", result).
			Any("body", body).
			Msg("failed to get plant list")
		return nil, fmt.Errorf("Huawei2Client::GetPlantList() - failed")
	}

	h.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("body", body).
		Msg("Huawei2Client::GetPlantListWithPagination() - success")
	return &result, nil
}

func (h *Huawei2Client) GetPlantList() ([]*Plant, error) {
	plants := make([]*Plant, 0)
	page := 1

	for {
		result, err := h.GetPlantListWithPagination(page)
		if err != nil {
			return nil, err
		}

		if result.Data == nil || len(result.Data.List) == 0 {
			break
		}

		page++
		plants = append(plants, result.Data.List...)
		h.logger.Info().
			Int("page", page).
			Int("collected", len(plants)).
			Int("total", *result.Data.Total).
			Any("plants", plants).
			Msg("Huawei2Client::GetPlantList() - success")

		if len(plants) >= *result.Data.Total {
			break
		}
	}

	return plants, nil
}

func (h *Huawei2Client) GetRealtimePlantData(stationCodes string) (*GetRealtimePlantDataResponse, error) {
	url := h.url + "/thirdData/getStationRealKpi"
	body := map[string]any{"stationCodes": stationCodes}

	var result GetRealtimePlantDataResponse
	tmp := map[string]any{}
	var errorResult model.ApiErrorResponse
	resp, err := h.reqClient.R().
		SetHeaders(h.headers).
		SetBody(body).
		SetSuccessResult(&tmp).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Post(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		h.logger.Error().
			Err(err).
			Int("status_code", resp.StatusCode).
			Str("url", url).
			Str("raw", string(raw)).
			Any("body", body).
			Msg("failed to get realtime plant data")
		return nil, err
	}

	if resp.IsErrorState() {
		h.logger.Error().
			Int("status_code", resp.StatusCode).
			Str("url", url).
			Any("body", body).
			Any("error_response", errorResult).
			Msg("failed to get realtime plant data")
		return nil, fmt.Errorf("Huawei2Client::GetRealtimePlantData() - failed")
	}

	if !result.Success {
		h.logger.Error().
			Int("status_code", resp.StatusCode).
			Str("url", url).
			Any("result", result).
			Any("body", body).
			Msg("failed to get realtime plant data")
		return nil, fmt.Errorf("Huawei2Client::GetRealtimePlantData() - failed")
	}

	h.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("result", result).
		Any("body", body).
		Msg("Huawei2Client::GetRealtimePlantData() - success")

	return &result, nil
}

func (h *Huawei2Client) GetHistoricalPlantData(interval Interval, stationCodes string, collectTime int64) (*GetHistoricalPlantDataResponse, error) {
	var url string
	switch interval {
	case IntervalMonth:
		url = h.url + "/thirdData/getKpiStationMonth"
	case IntervalYear:
		url = h.url + "/thirdData/getKpiStationYear"
	default:
		url = h.url + "/thirdData/getKpiStationDay"
	}

	body := map[string]any{
		"stationCodes": stationCodes,
		"collectTime":  collectTime,
	}

	var result GetHistoricalPlantDataResponse
	var errorResult model.ApiErrorResponse
	resp, err := h.reqClient.R().
		SetHeaders(h.headers).
		SetBody(body).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Post(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		h.logger.Error().
			Err(err).
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Str("raw", string(raw)).
			Any("interval", interval).
			Any("body", body).
			Msg("failed to get historical plant data")
		return nil, err
	}

	if resp.IsErrorState() {
		h.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("interval", interval).
			Any("body", body).
			Any("error_response", errorResult).
			Msg("failed to get historical plant data")
		return nil, fmt.Errorf("Huawei2Client::GetHistoricalPlantData() - failed")
	}

	if !result.Success {
		h.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("interval", interval).
			Any("result", result).
			Any("body", body).
			Msg("failed to get historical plant data")
		return nil, fmt.Errorf("Huawei2Client::GetHistoricalPlantData() - failed")
	}

	h.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("interval", interval).
		Any("result", result).
		Any("body", body).
		Msg("Huawei2Client::GetHistoricalPlantData() - success")

	return &result, nil
}

func (h *Huawei2Client) GetDeviceList(stationCodes string) (*GetDeviceListResponse, error) {
	url := h.url + "/thirdData/getDevList"
	body := map[string]any{"stationCodes": stationCodes}

	var result GetDeviceListResponse
	var errorResult model.ApiErrorResponse
	resp, err := h.reqClient.R().
		SetHeaders(h.headers).
		SetBody(body).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Post(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		h.logger.Error().
			Err(err).
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Str("raw", string(raw)).
			Any("body", body).
			Msg("failed to get device list")
		return nil, err
	}

	if resp.IsErrorState() {
		h.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("body", body).
			Any("error_response", errorResult).
			Msg("failed to get device list")
		return nil, fmt.Errorf("Huawei2Client::GetDeviceList() - failed")
	}

	if !result.Success {
		h.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("body", body).
			Any("result", result).
			Msg("failed to get device list")
		return nil, fmt.Errorf("Huawei2Client::GetDeviceList() - failed")
	}

	h.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("body", body).
		Any("result", result).
		Msg("Huawei2Client::GetDeviceList() - success")

	return &result, nil
}

func (h *Huawei2Client) GetRealtimeDeviceData(deviceIds, deviceTypeId string) (*GetRealtimeDeviceDataResponse, error) {
	url := h.url + "/thirdData/getDevRealKpi"
	data := map[string]any{
		"devIds":    deviceIds,
		"devTypeId": deviceTypeId,
	}

	var result GetRealtimeDeviceDataResponse
	var errorResult model.ApiErrorResponse
	resp, err := h.reqClient.R().
		SetHeaders(h.headers).
		SetBody(data).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Post(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		h.logger.Error().
			Err(err).
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Str("raw", string(raw)).
			Any("body", data).
			Msg("failed to get realtime device data")
		return nil, err
	}

	if resp.IsErrorState() {
		h.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("body", data).
			Any("error_response", errorResult).
			Msg("failed to get realtime device data")
		return nil, fmt.Errorf("Huawei2Client::GetRealtimeDeviceData() - failed")
	}

	if !result.Success {
		h.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("body", data).
			Any("result", result).
			Msg("failed to get realtime device data")
		return nil, fmt.Errorf("Huawei2Client::GetRealtimeDeviceData() - failed")
	}

	h.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("body", data).
		Any("result", result).
		Msg("Huawei2Client::GetRealtimeDeviceData() - success")

	return &result, nil
}

func (h *Huawei2Client) GetHistoricalDeviceData(interval Interval, deviceId, deviceTypeId string, collectTime int64) (*GetHistoricalDeviceDataResponse, error) {
	var url string
	switch interval {
	case IntervalMonth:
		url = h.url + "/thirdData/getKpiDevMonth"
	case IntervalYear:
		url = h.url + "/thirdData/getKpiDevYear"
	default:
		url = h.url + "/thirdData/getKpiDevDay"
	}

	body := map[string]any{
		"devIds":      deviceId,
		"devTypeId":   deviceTypeId,
		"collectTime": collectTime,
	}

	var result GetHistoricalDeviceDataResponse
	var errorResult model.ApiErrorResponse
	resp, err := h.reqClient.R().
		SetHeaders(h.headers).
		SetBody(body).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Post(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		h.logger.Error().
			Err(err).
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Str("raw", string(raw)).
			Any("body", body).
			Msg("failed to get historical device data")
		return nil, err
	}

	if resp.IsErrorState() {
		h.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("body", body).
			Any("error_response", errorResult).
			Msg("failed to get historical device data")
		return nil, fmt.Errorf("Huawei2Client::GetHistoricalDeviceData() - failed")
	}

	if !result.Success {
		h.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("body", body).
			Any("result", result).
			Msg("failed to get historical device data")
		return nil, fmt.Errorf("Huawei2Client::GetHistoricalDeviceData() - failed")
	}

	h.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("body", body).
		Any("result", result).
		Msg("Huawei2Client::GetHistoricalDeviceData() - success")

	return &result, nil
}

func (h *Huawei2Client) GetDeviceAlarm(stationCodes string, from, to int64) (*GetDeviceAlarmResponse, error) {
	url := h.url + "/thirdData/getAlarmList"
	body := map[string]any{
		"stationCodes": stationCodes,
		"from":         from,
		"to":           to,
		"language":     LanguageEnglish,
	}

	var result GetDeviceAlarmResponse
	var errorResult model.ApiErrorResponse

	resp, err := h.reqClient.R().
		SetHeaders(h.headers).
		SetBody(body).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Post(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		h.logger.Error().
			Err(err).
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Str("raw", string(raw)).
			Any("body", body).
			Msg("failed to get device alarm")
		return nil, err
	}

	if resp.IsErrorState() {
		h.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("body", body).
			Any("error_response", errorResult).
			Msg("failed to get device alarm")
		return nil, fmt.Errorf("Huawei2Client::GetDeviceAlarm() - failed")
	}

	if !result.Success {
		h.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("body", body).
			Any("result", result).
			Msg("failed to get device alarm")
		return nil, fmt.Errorf("Huawei2Client::GetDeviceAlarm() - failed")
	}

	h.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("body", body).
		Any("result", result).
		Msg("Huawei2Client::GetDeviceAlarm() - success")

	return &result, nil
}

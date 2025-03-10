package solarman

import (
	"fmt"
	"io"
	"time"

	"github.com/HavvokLab/true-solar/model"
	"github.com/HavvokLab/true-solar/pkg/logger"
	"github.com/imroc/req/v3"
	"github.com/rs/zerolog"
	"go.openly.dev/pointy"
)

const (
	DataListKeyCumulativeProduction = "Et_ge0"
	DataListKeyGeneration           = "generation"
)

const (
	SolarmanPlantStatusOn    = "ONLINE"
	SolarmanPlantStatusOff   = "OFFLINE"
	SolarmanPlantStatusAlarm = "ALARM"
)

const (
	DeviceStatusOn      = "ONLINE"
	DeviceStatusOff     = "OFFLINE"
	DeviceStatusFailure = "FAILURE"
)

const (
	AuthorizationHeader = "Authorization"
	MaxPageSize         = 200
)

type TimeType int

const (
	TimeTypeTimeframe = iota + 1
	TimeTypeDay
	TimeTypeMonth
	TimeTypeYear
)

func (t TimeType) Int() int {
	return int(t)
}

func (t TimeType) Build(timestamp int64) string {
	switch t {
	case TimeTypeYear:
		return time.Unix(timestamp, 0).Format("2006")
	case TimeTypeMonth:
		return time.Unix(timestamp, 0).Format("2006-01")
	default:
		return time.Unix(timestamp, 0).Format("2006-01-02")
	}
}

type SolarmanClient struct {
	reqClient *req.Client
	logger    zerolog.Logger
	url       string
	username  string
	password  string
	appId     string
	appSecret string
	headers   map[string]string
}

func NewSolarmanClient(username, password, appId, appSecret string) *SolarmanClient {
	logger := zerolog.New(logger.NewWriter("solarman_api.log")).With().Caller().Timestamp().Logger()
	client := &SolarmanClient{
		reqClient: req.C().
			SetTimeout(10 * time.Second).
			SetCommonRetryCount(3).
			SetCommonRetryFixedInterval(5 * time.Minute).
			OnBeforeRequest(func(client *req.Client, req *req.Request) error {
				logger.Debug().
					Any("request", req.RawURL).
					Msg("SolarmanClient::NewSolarmanClient() - requesting")
				return nil
			}),
		url:       "https://globalapi.solarmanpv.com",
		username:  username,
		password:  DecodePassword(password),
		appId:     appId,
		appSecret: appSecret,
		logger:    logger,
		headers:   make(map[string]string),
	}

	return client
}

func (c *SolarmanClient) SetAccessToken(accessToken string) {
	c.headers[AuthorizationHeader] = fmt.Sprintf("Bearer %s", accessToken)
}

func (c *SolarmanClient) GetBasicToken() (*GetTokenResponse, error) {
	url := c.url + "/account/v1.0/token"
	body := GetTokenRequestBody{
		Username:  c.username,
		Password:  c.password,
		AppSecret: c.appSecret,
	}
	query := map[string]string{
		"appId": c.appId,
	}

	var result GetTokenResponse
	var errorResult model.ApiErrorResponse
	resp, err := c.reqClient.R().
		SetQueryParams(query).
		SetBody(body).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Post(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		c.logger.Error().
			Err(err).
			Int("status_code", resp.StatusCode).
			Str("url", url).
			Str("raw", string(raw)).
			Any("query", query).
			Msg("failed to get basic token")
		return nil, err
	}

	if resp.IsErrorState() {
		c.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("error_response", errorResult).
			Any("query", query).
			Msg("failed to get basic token")
		return nil, fmt.Errorf("failed to get basic token")
	}

	c.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("result", result).
		Any("query", query).
		Msg("get basic token successfully")

	return &result, nil
}

func (c *SolarmanClient) GetBusinessToken(orgId int) (*GetTokenResponse, error) {
	url := c.url + "/account/v1.0/token"
	body := GetTokenRequestBody{
		Username:  c.username,
		Password:  c.password,
		AppSecret: c.appSecret,
		OrgID:     orgId,
	}
	query := map[string]string{
		"appId": c.appId,
	}

	var result GetTokenResponse
	var errorResult model.ApiErrorResponse
	resp, err := c.reqClient.R().
		SetQueryParams(query).
		SetBody(body).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Post(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		c.logger.Error().
			Err(err).
			Int("status_code", resp.StatusCode).
			Str("url", url).
			Str("raw", string(raw)).
			Any("query", query).
			Msg("failed to get business token")
		return nil, err
	}

	if resp.IsErrorState() {
		c.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("error_response", errorResult).
			Any("query", query).
			Msg("failed to get business token")
		return nil, fmt.Errorf("failed to get business token")
	}

	c.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("result", result).
		Any("query", query).
		Msg("get business token successfully")

	return &result, nil
}

func (c *SolarmanClient) GetUserInfo() (*GetUserInfoResponse, error) {
	url := c.url + "/account/v1.0/info"
	query := map[string]string{
		"language": "en",
	}

	var result GetUserInfoResponse
	var errorResult model.ApiErrorResponse
	resp, err := c.reqClient.R().
		SetHeaders(c.headers).
		SetQueryParams(query).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Post(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		c.logger.Error().
			Err(err).
			Int("status_code", resp.StatusCode).
			Str("url", url).
			Str("raw", string(raw)).
			Any("query", query).
			Msg("failed to get user info")
		return nil, err
	}

	if resp.IsErrorState() {
		c.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("error_response", errorResult).
			Any("query", query).
			Msg("failed to get user info")
		return nil, fmt.Errorf("failed to get user info")
	}

	c.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("result", result).
		Any("query", query).
		Msg("get user info successfully")

	return &result, nil
}

func (c *SolarmanClient) GetPlantListWithPagination(page, size int) (*GetPlantListResponse, error) {
	url := c.url + "/station/v1.0/list"
	query := map[string]string{
		"language": "en",
	}
	body := map[string]any{
		"page": page,
		"size": size,
	}

	var result GetPlantListResponse
	var errorResult model.ApiErrorResponse
	resp, err := c.reqClient.R().
		SetHeaders(c.headers).
		SetQueryParams(query).
		SetBody(body).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Post(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		c.logger.Error().
			Err(err).
			Int("status_code", resp.StatusCode).
			Str("url", url).
			Str("raw", string(raw)).
			Any("body", body).
			Msg("failed to get plant list with pagination")
		return nil, err
	}

	if resp.IsErrorState() {
		c.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("error_response", errorResult).
			Any("body", body).
			Msg("failed to get plant list with pagination")
		return nil, fmt.Errorf("failed to get plant list with pagination")
	}

	c.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("result", result).
		Any("body", body).
		Msg("get plant list with pagination successfully")

	return &result, nil
}

func (c *SolarmanClient) GetPlantList() ([]*PlantItem, error) {
	result := make([]*PlantItem, 0)
	page := 1

	for {
		response, err := c.GetPlantListWithPagination(page, MaxPageSize)
		if err != nil {
			return nil, err
		}

		result = append(result, response.StationList...)
		page += 1

		if len(result) >= pointy.IntValue(response.Total, 0) {
			break
		}
	}

	return result, nil
}

func (c *SolarmanClient) GetPlantBaseInfo(stationId int) (*GetPlantBaseInfoResponse, error) {
	url := c.url + "/station/v1.0/base"
	query := map[string]string{
		"language": "en",
	}
	body := map[string]any{
		"stationId": stationId,
	}

	var result GetPlantBaseInfoResponse
	var errorResult model.ApiErrorResponse
	resp, err := c.reqClient.R().
		SetHeaders(c.headers).
		SetQueryParams(query).
		SetBody(body).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Post(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		c.logger.Error().
			Err(err).
			Int("status_code", resp.StatusCode).
			Str("url", url).
			Str("raw", string(raw)).
			Any("body", body).
			Msg("failed to get plant base info")
		return nil, err
	}

	if resp.IsErrorState() {
		c.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("error_response", errorResult).
			Any("body", body).
			Msg("failed to get plant base info")
		return nil, fmt.Errorf("failed to get plant base info")
	}

	c.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("result", result).
		Any("body", body).
		Msg("get plant base info successfully")

	return &result, nil
}

func (c *SolarmanClient) GetPlantRealtimeData(stationId int) (*GetRealtimePlantDataResponse, error) {
	url := c.url + "/station/v1.0/realTime"
	query := map[string]string{
		"language": "en",
	}
	body := map[string]any{
		"stationId": stationId,
	}

	var result GetRealtimePlantDataResponse
	var errorResult model.ApiErrorResponse
	resp, err := c.reqClient.R().
		SetHeaders(c.headers).
		SetQueryParams(query).
		SetBody(body).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Post(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		c.logger.Error().
			Err(err).
			Int("status_code", resp.StatusCode).
			Str("url", url).
			Str("raw", string(raw)).
			Any("body", body).
			Msg("failed to get plant realtime data")
		return nil, err
	}

	if resp.IsErrorState() {
		c.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("error_response", errorResult).
			Any("body", body).
			Msg("failed to get plant realtime data")
		return nil, fmt.Errorf("failed to get plant realtime data")
	}

	c.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("result", result).
		Any("body", body).
		Msg("get plant realtime data successfully")

	return &result, nil
}

func (c *SolarmanClient) GetHistoricalPlantData(stationId int, timeType TimeType, from, to int64) (*GetHistoricalPlantDataResponse, error) {
	url := c.url + "/station/v1.0/history"
	query := map[string]string{
		"language": "en",
	}
	body := map[string]any{
		"stationId": stationId,
		"startTime": timeType.Build(from),
		"endTime":   timeType.Build(to),
		"timeType":  timeType.Int(),
	}

	var result GetHistoricalPlantDataResponse
	var errorResult model.ApiErrorResponse
	resp, err := c.reqClient.R().
		SetHeaders(c.headers).
		SetQueryParams(query).
		SetBody(body).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Post(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		c.logger.Error().
			Err(err).
			Int("status_code", resp.StatusCode).
			Str("url", url).
			Str("raw", string(raw)).
			Any("body", body).
			Msg("failed to get historical plant data")
		return nil, err
	}

	if resp.IsErrorState() {
		c.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("error_response", errorResult).
			Any("body", body).
			Msg("failed to get historical plant data")
		return nil, fmt.Errorf("failed to get historical plant data")
	}

	c.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("result", result).
		Any("body", body).
		Msg("get historical plant data successfully")

	return &result, nil
}

func (c *SolarmanClient) GetPlantDeviceListWithPagination(stationId, page, size int) (*GetPlantDeviceListResponse, error) {
	url := c.url + "/station/v1.0/device"
	query := map[string]string{
		"language": "en",
	}
	body := map[string]any{
		"stationId": stationId,
		"page":      page,
		"size":      size,
	}

	var result GetPlantDeviceListResponse
	var errorResult model.ApiErrorResponse
	resp, err := c.reqClient.R().
		SetHeaders(c.headers).
		SetQueryParams(query).
		SetBody(body).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Post(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		c.logger.Error().
			Err(err).
			Int("status_code", resp.StatusCode).
			Str("url", url).
			Str("raw", string(raw)).
			Any("body", body).
			Msg("failed to get plant device list with pagination")
		return nil, err
	}

	if resp.IsErrorState() {
		c.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("error_response", errorResult).
			Any("body", body).
			Msg("failed to get plant device list with pagination")
		return nil, fmt.Errorf("failed to get plant device list with pagination")
	}

	c.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("result", result).
		Any("body", body).
		Msg("get plant device list with pagination successfully")

	return &result, nil
}

func (c *SolarmanClient) GetPlantDeviceList(stationId int) ([]*PlantDeviceItem, error) {
	result := make([]*PlantDeviceItem, 0)
	page := 1

	for {
		response, err := c.GetPlantDeviceListWithPagination(stationId, page, MaxPageSize)
		if err != nil {
			return nil, err
		}

		result = append(result, response.DeviceListItems...)
		page += 1

		if len(result) >= pointy.IntValue(response.Total, 0) {
			break
		}
	}

	return result, nil
}

func (c *SolarmanClient) GetDeviceRealtimeData(deviceSn string) (*GetRealtimeDeviceDataResponse, error) {
	url := c.url + "/device/v1.0/currentData"
	query := map[string]string{
		"language": "en",
	}
	body := map[string]any{
		"deviceSn": deviceSn,
	}

	var result GetRealtimeDeviceDataResponse
	var errorResult model.ApiErrorResponse
	resp, err := c.reqClient.R().
		SetHeaders(c.headers).
		SetQueryParams(query).
		SetBody(body).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Post(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		c.logger.Error().
			Err(err).
			Int("status_code", resp.StatusCode).
			Str("url", url).
			Str("raw", string(raw)).
			Any("body", body).
			Msg("failed to get device realtime data")
		return nil, err
	}

	if resp.IsErrorState() {
		c.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("error_response", errorResult).
			Any("body", body).
			Msg("failed to get device realtime data")
		return nil, fmt.Errorf("failed to get device realtime data")
	}

	c.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("result", result).
		Any("body", body).
		Msg("get device realtime data successfully")

	return &result, nil
}

func (c *SolarmanClient) GetHistoricalDeviceData(deviceSn string, timeType TimeType, from, to int64) (*GetHistoricalDeviceDataResponse, error) {
	url := c.url + "/device/v1.0/historical"
	query := map[string]string{
		"language": "en",
	}
	body := map[string]any{
		"deviceSn":  deviceSn,
		"startTime": timeType.Build(from),
		"endTime":   timeType.Build(to),
		"timeType":  timeType.Int(),
	}

	var result GetHistoricalDeviceDataResponse
	var errorResult model.ApiErrorResponse
	resp, err := c.reqClient.R().
		SetHeaders(c.headers).
		SetQueryParams(query).
		SetBody(body).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Post(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		c.logger.Error().
			Err(err).
			Int("status_code", resp.StatusCode).
			Str("url", url).
			Str("raw", string(raw)).
			Any("body", body).
			Msg("failed to get historical device data")
		return nil, err
	}

	if resp.IsErrorState() {
		c.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("error_response", errorResult).
			Any("body", body).
			Msg("failed to get historical device data")
		return nil, fmt.Errorf("failed to get historical device data")
	}

	c.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("result", result).
		Any("body", body).
		Msg("get historical device data successfully")

	return &result, nil
}

func (c *SolarmanClient) GetDeviceAlertListWithPagination(deviceSn string, from, to int64, page, size int) (*GetDeviceAlertListResponse, error) {
	url := c.url + "/device/v1.0/alertList"
	query := map[string]string{
		"language": "en",
	}
	body := map[string]any{
		"deviceSn":       deviceSn,
		"startTimestamp": from,
		"endTimestamp":   to,
		"page":           page,
		"size":           size,
	}

	var result GetDeviceAlertListResponse
	var errorResult model.ApiErrorResponse
	resp, err := c.reqClient.R().
		SetHeaders(c.headers).
		SetQueryParams(query).
		SetBody(body).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Post(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		c.logger.Error().
			Err(err).
			Int("status_code", resp.StatusCode).
			Str("url", url).
			Str("raw", string(raw)).
			Any("body", body).
			Msg("failed to get device alert list with pagination")
		return nil, err
	}

	if resp.IsErrorState() {
		c.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("error_response", errorResult).
			Any("body", body).
			Msg("failed to get device alert list with pagination")
		return nil, fmt.Errorf("failed to get device alert list with pagination")
	}

	c.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("result", result).
		Any("body", body).
		Msg("get device alert list with pagination successfully")

	return &result, nil
}

func (c *SolarmanClient) GetDeviceAlertList(deviceSn string, from, to int64) ([]*DeviceAlertItem, error) {
	result := make([]*DeviceAlertItem, 0)
	page := 1

	for {
		response, err := c.GetDeviceAlertListWithPagination(deviceSn, from, to, page, MaxPageSize)
		if err != nil {
			return nil, err
		}

		result = append(result, response.AlertList...)
		page += 1

		if len(result) >= pointy.IntValue(response.Total, 0) {
			break
		}
	}

	return result, nil
}

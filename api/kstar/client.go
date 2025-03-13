package kstar

import (
	"crypto/md5"
	"crypto/sha1"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/HavvokLab/true-solar/model"
	"github.com/HavvokLab/true-solar/pkg/logger"
	"github.com/imroc/req/v3"
	"github.com/rs/zerolog"
	"go.openly.dev/pointy"
)

const (
	MaxPageSize = 100
)

const (
	KstarDeviceTypeInverter = "INVERTER"
)

const (
	KstarDeviceStatusOnline  = "ONLINE"
	KstarDeviceStatusOffline = "OFFLINE"
	KstarDeviceStatusAlarm   = "ALARM"
)

type KstarClient struct {
	reqClient *req.Client
	username  string
	password  string
	url       string
	logger    zerolog.Logger
}

type Option func(*KstarClient)

func WithRetryCount(count int) Option {
	return func(k *KstarClient) {
		k.reqClient.SetCommonRetryCount(count)
	}
}

func NewKstarClient(username, password string, opts ...Option) *KstarClient {
	logger := zerolog.New(logger.NewWriter("kstar_api.log")).With().Caller().Timestamp().Logger()
	k := &KstarClient{
		reqClient: req.C().
			SetCommonRetryCount(3).
			SetCommonRetryFixedInterval(5 * time.Minute).
			SetTimeout(10 * time.Second).
			OnBeforeRequest(func(client *req.Client, req *req.Request) error {
				logger.Debug().
					Any("request", req.RawURL).
					Msg("KstarClient::NewKstarClient() - requesting")
				return nil
			}),
		url:      "http://solar.kstar.com:9000/public",
		username: username,
		password: password,
		logger:   logger,
	}
	k.password = k.EncodePassword(k.password)

	for _, opt := range opts {
		opt(k)
	}

	return k
}

func (k KstarClient) EncodePassword(password string) string {
	hashPassword := md5.Sum([]byte(password))
	return strings.ToUpper(fmt.Sprintf("%x", hashPassword))
}

func (k KstarClient) EncodeParameter(params map[string]string) string {
	params["userCode"] = k.username
	params["password"] = k.password

	query := url.Values{}
	for k, v := range params {
		query.Add(k, v)
	}

	hash := sha1.New()
	hash.Write([]byte(query.Encode()))
	result := fmt.Sprintf("%x", hash.Sum(nil))
	return result
}

func (k *KstarClient) GetPlantList() (*GetPlantListResponse, error) {
	url := k.url + "/power/info"
	sign := k.EncodeParameter(make(map[string]string))
	query := map[string]string{
		"userCode": k.username,
		"password": k.password,
		"sign":     sign,
	}

	var result GetPlantListResponse
	var errorResult model.ApiErrorResponse
	resp, err := k.reqClient.R().
		SetQueryParams(query).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Get(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		k.logger.Error().
			Err(err).
			Int("status_code", resp.StatusCode).
			Str("url", url).
			Str("raw", string(raw)).
			Any("query", query).
			Msg("failed to get plant list")
		return nil, err
	}

	if resp.IsErrorState() {
		k.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("error_response", errorResult).
			Any("query", query).
			Msg("failed to get plant list")
		return nil, fmt.Errorf("failed to get plant list")
	}

	if result.Meta != nil && !result.Meta.Success {
		k.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("result", result).
			Any("query", query).
			Msg("failed to get plant list")
		return nil, fmt.Errorf("failed to get plant list with error: %s", pointy.StringValue(result.Meta.Code, ""))
	}

	k.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("result", result).
		Any("query", query).
		Msg("get plant list successfully")
	return &result, nil
}

func (k *KstarClient) GetDeviceListWithPagination(page, size int) (*GetDeviceListResponse, error) {
	url := k.url + "/inverter/list"
	sign := k.EncodeParameter(map[string]string{
		"PageNum":  strconv.Itoa(page),
		"PageSize": strconv.Itoa(size),
	})

	query := map[string]string{
		"userCode": k.username,
		"password": k.password,
		"PageNum":  strconv.Itoa(page),
		"PageSize": strconv.Itoa(size),
		"sign":     sign,
	}

	var result GetDeviceListResponse
	var errorResult model.ApiErrorResponse
	resp, err := k.reqClient.R().
		SetQueryParams(query).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Get(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		k.logger.Error().
			Err(err).
			Int("status_code", resp.StatusCode).
			Str("url", url).
			Str("raw", string(raw)).
			Any("query", query).
			Msg("failed to get device list with pagination")
		return nil, err
	}

	if resp.IsErrorState() {
		k.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("error_response", errorResult).
			Any("query", query).
			Msg("failed to get device list with pagination")
		return nil, fmt.Errorf("failed to get device list with pagination")
	}

	if result.Meta != nil && !result.Meta.Success {
		k.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("result", result).
			Any("query", query).
			Msg("failed to get device list with pagination")
		return nil, fmt.Errorf("failed to get device list with pagination with error: %s", pointy.StringValue(result.Meta.Code, ""))
	}

	k.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("result", result).
		Any("query", query).
		Msg("get device list with pagination successfully")
	return &result, nil
}

func (k *KstarClient) GetDeviceList() ([]DeviceItem, error) {
	result := []DeviceItem{}
	page := 1
	for {
		resp, err := k.GetDeviceListWithPagination(page, MaxPageSize)
		if err != nil {
			return nil, err
		}

		if len(resp.Data.List) == 0 {
			break
		}

		result = append(result, resp.Data.List...)
		if len(result) >= pointy.IntValue(resp.Data.Count, 0) {
			break
		}

		page++
	}

	return result, nil
}

func (k *KstarClient) GetRealtimeDeviceData(deviceId string) (*GetRealtimeDeviceDataResponse, error) {
	url := k.url + "/device/real"
	sign := k.EncodeParameter(map[string]string{
		"deviceId": deviceId,
	})

	query := map[string]string{
		"userCode": k.username,
		"password": k.password,
		"deviceId": deviceId,
		"sign":     sign,
	}

	var result GetRealtimeDeviceDataResponse
	var errorResult model.ApiErrorResponse
	resp, err := k.reqClient.R().
		SetQueryParams(query).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Get(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		k.logger.Error().
			Err(err).
			Int("status_code", resp.StatusCode).
			Str("url", url).
			Str("raw", string(raw)).
			Any("query", query).
			Msg("failed to get realtime device data")
	}

	if resp.IsErrorState() {
		k.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("error_response", errorResult).
			Any("query", query).
			Msg("failed to get realtime device data")
	}

	if result.Meta != nil && !result.Meta.Success {
		k.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("result", result).
			Any("query", query).
			Msg("failed to get realtime device data")
	}

	k.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("result", result).
		Any("query", query).
		Msg("get realtime device data successfully")

	return &result, nil
}

func (k *KstarClient) GetRealtimeAlarmListOfDevice(deviceId string) (*GetRealtimeAlarmListOfDeviceResponse, error) {
	url := k.url + "/alarm/device/list"
	sign := k.EncodeParameter(map[string]string{
		"deviceId": deviceId,
	})

	query := map[string]string{
		"userCode": k.username,
		"password": k.password,
		"deviceId": deviceId,
		"sign":     sign,
	}

	var result GetRealtimeAlarmListOfDeviceResponse
	var errorResult model.ApiErrorResponse
	resp, err := k.reqClient.R().
		SetQueryParams(query).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Get(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		k.logger.Error().
			Err(err).
			Int("status_code", resp.StatusCode).
			Str("url", url).
			Str("raw", string(raw)).
			Any("query", query).
			Msg("failed to get realtime alarm list of device")
		return nil, err
	}

	if resp.IsErrorState() {
		k.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("error_response", errorResult).
			Any("query", query).
			Msg("failed to get realtime alarm list of device")
		return nil, fmt.Errorf("failed to get realtime alarm list of device")
	}

	if result.Meta != nil && !result.Meta.Success {
		k.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("result", result).
			Any("query", query).
			Msg("failed to get realtime alarm list of device")
	}

	k.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("result", result).
		Any("query", query).
		Msg("get realtime alarm list of device successfully")

	return &result, nil
}

func (k *KstarClient) GetHistoricalDeviceData(deviceId string, collectTime *time.Time) (*GetHistoricalDeviceDataResponse, error) {
	url := k.url + "/device/history"
	stime := collectTime.Format("2006-01-02")
	sign := k.EncodeParameter(map[string]string{
		"deviceId": deviceId,
		"stime":    stime,
	})

	query := map[string]string{
		"userCode": k.username,
		"password": k.password,
		"deviceId": deviceId,
		"stime":    stime,
		"sign":     sign,
	}

	var result GetHistoricalDeviceDataResponse
	var errorResult model.ApiErrorResponse
	resp, err := k.reqClient.R().
		SetQueryParams(query).
		SetSuccessResult(&result).
		SetErrorResult(&errorResult).
		Get(url)

	if err != nil {
		raw, _ := io.ReadAll(resp.Body)
		k.logger.Error().
			Err(err).
			Int("status_code", resp.StatusCode).
			Str("url", url).
			Str("raw", string(raw)).
			Any("query", query).
			Msg("failed to get historical device data")
		return nil, err
	}

	if resp.IsErrorState() {
		k.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("error_response", errorResult).
			Any("query", query).
			Msg("failed to get historical device data")
		return nil, fmt.Errorf("failed to get historical device data")
	}

	if result.Meta != nil && !result.Meta.Success {
		k.logger.Error().
			Str("url", url).
			Int("status_code", resp.StatusCode).
			Any("result", result).
			Any("query", query).
			Msg("failed to get historical device data")
	}

	k.logger.Info().
		Str("url", url).
		Int("status_code", resp.StatusCode).
		Any("result", result).
		Any("query", query).
		Msg("get historical device data successfully")

	return &result, nil
}

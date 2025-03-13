package model

import "time"

type SnmpAlarmItem struct {
	Timestamp        time.Time `json:"@timestamp"`
	VendorType       string    `json:"vendor_type"`
	DeviceName       string    `json:"device_name"`
	AlertName        string    `json:"alert_name"`
	Description      string    `json:"description"`
	Severity         string    `json:"severity"`
	LastedUpdateTime string    `json:"lasted_update_time"`
}

func NewSnmpAlarmItem(vendorType, deviceName, alertName, description, severity, lastedUpdateTime string) SnmpAlarmItem {
	return SnmpAlarmItem{
		Timestamp:        time.Now(),
		VendorType:       vendorType,
		DeviceName:       deviceName,
		Description:      description,
		Severity:         severity,
		LastedUpdateTime: lastedUpdateTime,
	}
}

type SnmpPerformanceAlarmItem struct {
	Timestamp        time.Time `json:"@timestamp"`
	Type             string    `json:"type"`
	DeviceName       string    `json:"device_name"`
	AlertName        string    `json:"alert_name"`
	Description      string    `json:"description"`
	Severity         string    `json:"severity"`
	LastedUpdateTime string    `json:"lasted_update_time"`
}

func NewSnmpPerformanceAlarmItem(t, deviceName, alertName, description, severity, lastedUpdateTime string) SnmpPerformanceAlarmItem {
	return SnmpPerformanceAlarmItem{
		Timestamp:        time.Now(),
		Type:             t,
		DeviceName:       deviceName,
		Description:      description,
		Severity:         severity,
		LastedUpdateTime: lastedUpdateTime,
	}
}

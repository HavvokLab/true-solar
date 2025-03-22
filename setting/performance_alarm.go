package setting

import "time"

const (
	LowPerformanceAlarm = "PerformanceLow"
	SumPerformanceAlarm = "SumPerformanceLow"
)

const (
	PerformanceAlarmSnmpBatchSize  = 25
	PerformanceAlarmSnmpBatchDelay = 5 * time.Second
)

const (
	PerformanceAlarmTypePerformanceLow = iota + 1
	PerformanceAlarmTypeSumPerformanceLow
)

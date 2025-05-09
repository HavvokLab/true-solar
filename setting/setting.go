package setting

const (
	CrontabCollectDayTime            = "*/16 7-19 * * *"
	CrontabCollectNightTime          = "1 1-6 * * *"
	CrontabSolarmanCollectDayTime    = "0 7-19 * * *"
	CrontabSolarmanCollectNightTime  = "0 1-6 * * *"
	CrontabHuawei2DayTime            = "0 17 * * *"
	CrontabHuawei2NightTime          = "0 19 * * *"
	CrontabAlarmTime                 = "*/15 7-18 * * *"
	CrontabClearAlarmTime            = "0 6 * * *"
	CrontabClearPerformanceAlarmTime = "0 6 * * *"
	CrontabLowPerformanceAlarmTime   = "0 8 * * *"
	CrontabSumPerformanceAlarmTime   = "*/10 * * * *"
)

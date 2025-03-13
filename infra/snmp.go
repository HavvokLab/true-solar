package infra

import (
	"time"

	"github.com/HavvokLab/true-solar/config"
	"github.com/HavvokLab/true-solar/pkg/logger"
	"github.com/gosnmp/gosnmp"
	"github.com/rs/zerolog"
)

type TrapType string

func (t TrapType) String() string {
	return string(t)
}

const (
	TrapTypeGrowattAlarm        TrapType = "growatt_alarm"
	TrapTypeHuaweiAlarm         TrapType = "huawei_alarm"
	TrapTypeSolarmanAlarm       TrapType = "solarman_alarm"
	TrapTypeKstarAlarm          TrapType = "kstar_alarm"
	TrapTypePerformanceAlarm    TrapType = "performance_alarm"
	TrapTypeSumPerformanceAlarm TrapType = "sum_performance_alarm"
	TrapTypeClearAlarm          TrapType = "clear_alarm"
)
const (
	CriticalSeverity      = "6"
	MajorSeverity         = "5"
	MinorSeverity         = "4"
	WarningSeverity       = "3"
	IndeterminateSeverity = "2"
	ClearSeverity         = "0"
)

type SnmpOrchestrator struct {
	clients  []*SnmpClient
	trapType TrapType
	logger   *zerolog.Logger
}

func NewSnmpOrchestrator(trapType TrapType, snmpList []config.SnmpConfig) (*SnmpOrchestrator, error) {
	logger := zerolog.New(logger.NewWriter("snmp.log")).With().Timestamp().Caller().Logger()

	clients := make([]*SnmpClient, 0, len(snmpList))
	for _, c := range snmpList {
		client, err := NewSnmpClient(c)
		if err != nil {
			return nil, err
		}

		clients = append(clients, client)
	}

	return &SnmpOrchestrator{clients: clients, trapType: trapType, logger: &logger}, nil
}

func (s *SnmpOrchestrator) SendTrap(deviceName, alertName, description, severity, lastedUpdateTime string) {
	for _, client := range s.clients {
		if err := client.SendTrap(deviceName, alertName, description, severity, lastedUpdateTime); err != nil {
			s.logger.Error().Err(err).
				Str("agent_host", client.agentHost).
				Str("target_host", client.client.Target).
				Int("target_port", int(client.client.Port)).
				Str("trap_type", s.trapType.String()).
				Str("device_name", deviceName).
				Str("alert_name", alertName).
				Str("description", description).
				Str("severity", severity).
				Str("lasted_update_time", lastedUpdateTime).
				Msg("failed to send trap")
		} else {
			s.logger.Info().
				Str("agent_host", client.agentHost).
				Str("target_host", client.client.Target).
				Int("target_port", int(client.client.Port)).
				Str("trap_type", s.trapType.String()).
				Str("device_name", deviceName).
				Str("alert_name", alertName).
				Str("description", description).
				Str("severity", severity).
				Str("lasted_update_time", lastedUpdateTime).
				Msg("send trap success")
		}
	}
}

type SnmpClient struct {
	agentHost string
	client    *gosnmp.GoSNMP
}

func NewSnmpClient(config config.SnmpConfig) (*SnmpClient, error) {
	client := &gosnmp.GoSNMP{
		Target:             config.TargetHost,
		Port:               uint16(config.TargetPort),
		Transport:          "udp",
		Community:          "public",
		Version:            gosnmp.Version1,
		Timeout:            time.Duration(300) * time.Second,
		Retries:            20,
		ExponentialTimeout: true,
		MaxOids:            gosnmp.MaxOids,
	}

	if err := client.Connect(); err != nil {
		return nil, err
	}

	return &SnmpClient{agentHost: config.AgentHost, client: client}, nil
}

func (c *SnmpClient) SendTrap(deviceName, alertName, description, severity, lastedUpdateTime string) error {
	pduClass := gosnmp.SnmpPDU{
		Name:  "1.3.6.1.4.1.30378.2.1",
		Type:  gosnmp.OctetString,
		Value: "HPOVComponent",
	}
	pduName := gosnmp.SnmpPDU{
		Name:  "1.3.6.1.4.1.30378.2.2",
		Type:  gosnmp.OctetString,
		Value: deviceName,
	}
	pduAlert := gosnmp.SnmpPDU{
		Name:  "1.3.6.1.4.1.30378.2.3",
		Type:  gosnmp.OctetString,
		Value: alertName,
	}
	pduDesc := gosnmp.SnmpPDU{
		Name:  "1.3.6.1.4.1.30378.2.4",
		Type:  gosnmp.OctetString,
		Value: description,
	}
	pduSeverity := gosnmp.SnmpPDU{
		Name:  "1.3.6.1.4.1.30378.2.5",
		Type:  gosnmp.OctetString,
		Value: severity,
	}
	pduLastedUpdateTime := gosnmp.SnmpPDU{
		Name:  "1.3.6.1.4.1.30378.2.6",
		Type:  gosnmp.OctetString,
		Value: lastedUpdateTime,
	}
	trap := gosnmp.SnmpTrap{
		Enterprise:   "1.3.6.1.4.1.30378.1.1",
		AgentAddress: c.agentHost,
		GenericTrap:  6,
		SpecificTrap: 1,
		Variables:    []gosnmp.SnmpPDU{pduClass, pduName, pduAlert, pduDesc, pduSeverity, pduLastedUpdateTime},
	}

	_, err := c.client.SendTrap(trap)
	if err != nil {
		return err
	}

	return nil
}

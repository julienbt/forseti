package xml

import (
	"encoding/xml"
	"fmt"
	"strings"
	"time"

	"github.com/hove-io/forseti/internal/sirism/directionname"
)

type Envelope struct {
	XMLName      xml.Name     `xml:"Envelope"`
	Notification Notification `xml:"Body>NotifyStopMonitoring>Notification"`
}

func (e *Envelope) GetTotalNumberOfMonitoredStopVisits() int {
	return e.Notification.getTotalNumberOfMonitoredStopVisits()
}

type Notification struct {
	XMLName                  xml.Name                 `xml:"Notification"`
	StopMonitoringDeliveries []StopMonitoringDelivery `xml:"StopMonitoringDelivery"`
}

func (n *Notification) getTotalNumberOfMonitoredStopVisits() int {
	result := 0
	for _, stopMonitoringDelivery := range n.StopMonitoringDeliveries {
		result += len(stopMonitoringDelivery.MonitoredStopVisits)
	}
	return result
}

type StopMonitoringDelivery struct {
	XMLName             xml.Name             `xml:"StopMonitoringDelivery"`
	MonitoringRef       StopPointRef         `xml:"MonitoringRef"`
	MonitoredStopVisits []MonitoredStopVisit `xml:"MonitoredStopVisit"`
}

type StopPointRef string

func (mr *StopPointRef) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var innerText string
	err := d.DecodeElement(&innerText, &start)
	if err != nil {
		return err
	}

	splittedInnerText := strings.Split(innerText, ":")
	const EXPECTED_NUM_OF_PARTS int = 5
	if len(splittedInnerText) != EXPECTED_NUM_OF_PARTS {
		return fmt.Errorf("the `MonitoringRef` is not well formatted: %s", innerText)
	}
	if splittedInnerText[1] != "StopPoint" {
		return fmt.Errorf("the `MonitoringRef` is not well formatted: %s", innerText)
	}
	*mr = StopPointRef(splittedInnerText[3])
	return nil
}

type MonitoredStopVisit struct {
	XMLName                 xml.Name                `xml:"MonitoredStopVisit"`
	MonitoringRef           StopPointRef            `xml:"MonitoringRef"`
	MonitoredVehicleJourney MonitoredVehicleJourney `xml:"MonitoredVehicleJourney"`
}

type MonitoredVehicleJourney struct {
	XMLName         xml.Name                    `xml:"MonitoredVehicleJourney"`
	LineRef         LineRef                     `xml:"LineRef"`
	DirectionName   directionname.DirectionName `xml:"DirectionName"`
	DestinationRef  StopPointRef                `xml:"DestinationRef"`
	DestinationName string                      `xml:"DestinationName"`
	MonitoredCall   MonitoredCall               `xml:"MonitoredCall"`
}

type LineRef string

func (lr *LineRef) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var innerText string
	err := d.DecodeElement(&innerText, &start)
	if err != nil {
		return err
	}

	splittedInnerText := strings.Split(innerText, ":")
	const EXPECTED_NUM_OF_PARTS int = 5
	if len(splittedInnerText) != EXPECTED_NUM_OF_PARTS {
		return fmt.Errorf("the `LineRef` is not well formatted: %s", innerText)
	}
	if splittedInnerText[1] != "Line" {
		return fmt.Errorf("the `LineRef` is not well formatted: %s", innerText)
	}
	*lr = LineRef(splittedInnerText[3])
	return nil
}

type MonitoredCall struct {
	XMLName               xml.Name     `xml:"MonitoredCall"`
	StopPointRef          StopPointRef `xml:"StopPointRef"`
	AimedDepartureTime    customTime   `xml:"AimedDepartureTime"`
	ExpectedDepartureTime customTime   `xml:"ExpectedDepartureTime"`
}

type customTime time.Time

func (ct *customTime) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	const CUSTUM_TIME_LAYOUT string = "2006-01-02T15:04:05.000Z07:00"
	var s string
	err := d.DecodeElement(&s, &start)
	if err != nil {
		return err
	}

	t, err := time.Parse(CUSTUM_TIME_LAYOUT, s)
	if err != nil {
		return err
	}
	*ct = customTime(t)
	return nil
}

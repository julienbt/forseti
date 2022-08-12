package departure

import (
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/hove-io/forseti/internal/departures"

	sirism_directionname "github.com/hove-io/forseti/internal/sirism/directionname"
	sirism_xml "github.com/hove-io/forseti/internal/sirism/xml"
)

type Departure struct {
	LineRef               string
	StopPointRef          string
	DirectionType         departures.DirectionType
	DestinationRef        string
	DestinationName       string
	AimedDepartureTime    time.Time
	ExpectedDepartureTime time.Time
}

func (d *Departure) DepartureTimeIsTheoretical() bool {
	return d.AimedDepartureTime.Equal(d.ExpectedDepartureTime)
}

func (d *Departure) GetDepartureTime() time.Time {
	if d.DepartureTimeIsTheoretical() {
		return d.AimedDepartureTime
	}
	return d.ExpectedDepartureTime
}

func LoadDeparturesFromFilePath(xmlFilePath string) ([]Departure, error) {
	xmlFile, err := os.Open(xmlFilePath)
	if err != nil {
		return nil, err
	}
	return LoadDeparturesFromOpenedFile(xmlFile)
}

func LoadDeparturesFromOpenedFile(xmlFile *os.File) ([]Departure, error) {
	xmlBytes, err := ioutil.ReadAll(xmlFile)
	if err != nil {
		return nil, err
	}
	return LoadDeparturesFromByteArray(xmlBytes)
}

func LoadDeparturesFromByteArray(xmlBytes []byte) ([]Departure, error) {
	var envelope sirism_xml.Envelope
	err := xml.Unmarshal(xmlBytes, &envelope)
	if err != nil {
		return nil, err
	}
	departuresList := make([]Departure, 0)
	for _, smDelivery := range envelope.Notification.StopMonitoringDeliveries {
		monitoringRef := string(smDelivery.MonitoringRef)
		for _, monitoredStopVisit := range smDelivery.MonitoredStopVisits {
			monitoredVehicleJourney := &monitoredStopVisit.MonitoredVehicleJourney
			innerMonitoringRef := string(monitoredStopVisit.MonitoringRef)
			if monitoringRef != innerMonitoringRef {
				err := fmt.Errorf(
					"the both XML tags <MonitoringRef> mismatch: %s != %s",
					monitoringRef,
					innerMonitoringRef,
				)
				return nil, err
			}
			var loadedDeparture Departure
			{
				lineRef := string(monitoredVehicleJourney.LineRef)
				directionName := monitoredVehicleJourney.DirectionName
				destinationRef := string(monitoredVehicleJourney.DestinationRef)
				destinationName := string(monitoredVehicleJourney.DestinationName)
				monitoredCall := &monitoredVehicleJourney.MonitoredCall
				stopPointRef := string(monitoredCall.StopPointRef)
				if monitoringRef != stopPointRef {
					err := fmt.Errorf(
						"the XML tags <MonitoringRef> and <StopPOintRef> mismatch: %s != %s",
						monitoringRef,
						stopPointRef,
					)
					return nil, err
				}
				aimedDepartureTime := time.Time(monitoredCall.AimedDepartureTime)
				expectedDepartureTime := time.Time(monitoredCall.ExpectedDepartureTime)
				var directionType departures.DirectionType
				if directionName == sirism_directionname.DirectionNameAller {
					directionType = departures.DirectionTypeForward
				} else {
					directionType = departures.DirectionTypeBackward
				}
				loadedDeparture = Departure{
					LineRef:               lineRef,
					StopPointRef:          monitoringRef,
					DirectionType:         directionType,
					DestinationRef:        destinationRef,
					DestinationName:       destinationName,
					AimedDepartureTime:    aimedDepartureTime,
					ExpectedDepartureTime: expectedDepartureTime,
				}
			}
			departuresList = append(departuresList, loadedDeparture)
		}
	}
	return departuresList, nil
}

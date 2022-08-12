package xml

import (
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hove-io/forseti/internal/sirism/directionname"
)

var fixtureDir string

const SECONDS_PER_HOUR int = 3_600

func TestMain(m *testing.M) {

	fixtureDir = os.Getenv("FIXTUREDIR")
	if fixtureDir == "" {
		panic("$FIXTUREDIR isn't set")
	}

	os.Exit(m.Run())
}

func parseNotificationFromXmlFile(assert *assert.Assertions, envelope *Envelope) {
	uri, err := url.Parse(fmt.Sprintf("file://%s/data_sirism/notif_siri_lille.xml", fixtureDir))
	assert.Nil(err)
	_, err = os.Stat(uri.Path)
	assert.Nilf(err, "The file '%s' cannot be found", uri.Path)
	xmlFile, err := os.Open(uri.Path)
	assert.Nilf(err, "The file '%s' cannot be opened", uri.Path)
	xmlBytes, _ := ioutil.ReadAll(xmlFile)

	err = xml.Unmarshal(xmlBytes, &envelope)
	assert.Nilf(err, "The parsing of '%s' has been failed", uri.Path)
}

func TestEnvelopeGetTotalNumberOfMonitoredStopVisits(t *testing.T) {
	const EXPECTED_TOTAL_NUMBER_OF_MONITORED_STOP_VISITS int = 138
	assert := assert.New(t)
	var envelope Envelope
	time.Local = time.UTC
	parseNotificationFromXmlFile(assert, &envelope)
	assert.Equal(
		EXPECTED_TOTAL_NUMBER_OF_MONITORED_STOP_VISITS,
		envelope.GetTotalNumberOfMonitoredStopVisits(),
	)
}

func TestNotificationXmlUnmarshall(t *testing.T) {
	assert := assert.New(t)
	var envelope Envelope

	// Force the variable `time.Local` of the server while the run
	time.Local = time.UTC
	parseNotificationFromXmlFile(assert, &envelope)

	const EXPECTED_NUMBER_OF_STOP_MONITORING_DELIVERY int = 44

	assert.Len(
		envelope.Notification.StopMonitoringDeliveries,
		EXPECTED_NUMBER_OF_STOP_MONITORING_DELIVERY,
	)

	EXPECTED_LOCATION := time.FixedZone("", 2*SECONDS_PER_HOUR)
	// Check the first element
	{
		EXPECTED_STOP_MONITORING_DELIVERY := StopMonitoringDelivery{
			XMLName: xml.Name{
				Space: "http://www.siri.org.uk/siri",
				Local: "StopMonitoringDelivery",
			},
			MonitoringRef: StopPointRef("CAS001"),
			MonitoredStopVisits: []MonitoredStopVisit{
				{
					XMLName: xml.Name{
						Space: "http://www.siri.org.uk/siri",
						Local: "MonitoredStopVisit",
					},
					MonitoringRef: StopPointRef("CAS001"),
					MonitoredVehicleJourney: MonitoredVehicleJourney{
						XMLName: xml.Name{
							Space: "http://www.siri.org.uk/siri",
							Local: "MonitoredVehicleJourney",
						},
						LineRef:         LineRef("50"),
						DirectionName:   directionname.DirectionNameRetour,
						DestinationRef:  StopPointRef("LIG114"),
						DestinationName: "GARE LILLE FLANDRES",
						MonitoredCall: MonitoredCall{
							XMLName: xml.Name{
								Space: "http://www.siri.org.uk/siri",
								Local: "MonitoredCall",
							},
							StopPointRef: StopPointRef("CAS001"),
							AimedDepartureTime: customTime(time.Date(
								2022, time.June, 15,
								5, 32, 0, 0,
								EXPECTED_LOCATION,
							)),
							ExpectedDepartureTime: customTime(time.Date(
								2022, time.June, 15,
								5, 32, 0, 0,
								EXPECTED_LOCATION,
							)),
						},
					},
				},
				{
					XMLName: xml.Name{
						Space: "http://www.siri.org.uk/siri",
						Local: "MonitoredStopVisit",
					},
					MonitoringRef: StopPointRef("CAS001"),
					MonitoredVehicleJourney: MonitoredVehicleJourney{
						XMLName: xml.Name{
							Space: "http://www.siri.org.uk/siri",
							Local: "MonitoredVehicleJourney",
						},
						LineRef:         LineRef("50"),
						DirectionName:   directionname.DirectionNameRetour,
						DestinationRef:  StopPointRef("LIG114"),
						DestinationName: "GARE LILLE FLANDRES",
						MonitoredCall: MonitoredCall{
							XMLName: xml.Name{
								Space: "http://www.siri.org.uk/siri",
								Local: "MonitoredCall",
							},
							StopPointRef: StopPointRef("CAS001"),
							AimedDepartureTime: customTime(time.Date(
								2022, time.June, 15,
								6, 2, 0, 0,
								EXPECTED_LOCATION,
							)),
							ExpectedDepartureTime: customTime(time.Date(
								2022, time.June, 15,
								6, 2, 0, 0,
								EXPECTED_LOCATION,
							)),
						},
					},
				},
			},
		}
		gotSMDelivery := envelope.Notification.StopMonitoringDeliveries[0]

		assert.Equal(
			EXPECTED_STOP_MONITORING_DELIVERY,
			gotSMDelivery,
		)
	}
	// Check the last element
	{
		EXPECTED_STOP_MONITORING_DELIVERY := StopMonitoringDelivery{
			XMLName: xml.Name{
				Space: "http://www.siri.org.uk/siri",
				Local: "StopMonitoringDelivery",
			},
			MonitoringRef: StopPointRef("CER001"),
			MonitoredStopVisits: []MonitoredStopVisit{
				{
					XMLName: xml.Name{
						Space: "http://www.siri.org.uk/siri",
						Local: "MonitoredStopVisit",
					},
					MonitoringRef: StopPointRef("CER001"),
					MonitoredVehicleJourney: MonitoredVehicleJourney{
						XMLName: xml.Name{
							Space: "http://www.siri.org.uk/siri",
							Local: "MonitoredVehicleJourney",
						},
						LineRef:         LineRef("L1"),
						DirectionName:   directionname.DirectionNameAller,
						DestinationRef:  StopPointRef("OCC001"),
						DestinationName: "FACHES CENTRE COMMERCIAL",
						MonitoredCall: MonitoredCall{
							XMLName: xml.Name{
								Space: "http://www.siri.org.uk/siri",
								Local: "MonitoredCall",
							},
							StopPointRef: StopPointRef("CER001"),
							AimedDepartureTime: customTime(time.Date(
								2022, time.June, 15,
								5, 44, 25, 0,
								EXPECTED_LOCATION,
							)),
							ExpectedDepartureTime: customTime(time.Date(
								2022, time.June, 15,
								5, 44, 25, 0,
								EXPECTED_LOCATION,
							)),
						},
					},
				},
				{
					XMLName: xml.Name{
						Space: "http://www.siri.org.uk/siri",
						Local: "MonitoredStopVisit",
					},
					MonitoringRef: StopPointRef("CER001"),
					MonitoredVehicleJourney: MonitoredVehicleJourney{
						XMLName: xml.Name{
							Space: "http://www.siri.org.uk/siri",
							Local: "MonitoredVehicleJourney",
						},
						LineRef:         LineRef("L1"),
						DirectionName:   directionname.DirectionNameAller,
						DestinationRef:  StopPointRef("OCC001"),
						DestinationName: "FACHES CENTRE COMMERCIAL",
						MonitoredCall: MonitoredCall{
							XMLName: xml.Name{
								Space: "http://www.siri.org.uk/siri",
								Local: "MonitoredCall",
							},
							StopPointRef: StopPointRef("CER001"),
							AimedDepartureTime: customTime(time.Date(
								2022, time.June, 15,
								6, 12, 25, 0,
								EXPECTED_LOCATION,
							)),
							ExpectedDepartureTime: customTime(time.Date(
								2022, time.June, 15,
								6, 12, 25, 0,
								EXPECTED_LOCATION,
							)),
						},
					},
				},
				{
					XMLName: xml.Name{
						Space: "http://www.siri.org.uk/siri",
						Local: "MonitoredStopVisit",
					},
					MonitoringRef: StopPointRef("CER001"),
					MonitoredVehicleJourney: MonitoredVehicleJourney{
						XMLName: xml.Name{
							Space: "http://www.siri.org.uk/siri",
							Local: "MonitoredVehicleJourney",
						},
						LineRef:         LineRef("CO1"),
						DirectionName:   directionname.DirectionNameAller,
						DestinationRef:  StopPointRef("CAL007"),
						DestinationName: "CHU-EURASANTE",
						MonitoredCall: MonitoredCall{
							XMLName: xml.Name{
								Space: "http://www.siri.org.uk/siri",
								Local: "MonitoredCall",
							},
							StopPointRef: StopPointRef("CER001"),
							AimedDepartureTime: customTime(time.Date(
								2022, time.June, 15,
								6, 14, 34, 0,
								EXPECTED_LOCATION,
							)),
							ExpectedDepartureTime: customTime(time.Date(
								2022, time.June, 15,
								6, 14, 34, 0,
								EXPECTED_LOCATION,
							)),
						},
					},
				},
				{
					XMLName: xml.Name{
						Space: "http://www.siri.org.uk/siri",
						Local: "MonitoredStopVisit",
					},
					MonitoringRef: StopPointRef("CER001"),
					MonitoredVehicleJourney: MonitoredVehicleJourney{
						XMLName: xml.Name{
							Space: "http://www.siri.org.uk/siri",
							Local: "MonitoredVehicleJourney",
						},
						LineRef:         LineRef("CO1"),
						DirectionName:   directionname.DirectionNameAller,
						DestinationRef:  StopPointRef("CAL007"),
						DestinationName: "CHU-EURASANTE",
						MonitoredCall: MonitoredCall{
							XMLName: xml.Name{
								Space: "http://www.siri.org.uk/siri",
								Local: "MonitoredCall",
							},
							StopPointRef: StopPointRef("CER001"),
							AimedDepartureTime: customTime(time.Date(
								2022, time.June, 15,
								6, 44, 34, 0,
								EXPECTED_LOCATION,
							)),
							ExpectedDepartureTime: customTime(time.Date(
								2022, time.June, 15,
								6, 44, 34, 0,
								EXPECTED_LOCATION,
							)),
						},
					},
				},
			},
		}
		gotSMDelivery := envelope.Notification.StopMonitoringDeliveries[EXPECTED_NUMBER_OF_STOP_MONITORING_DELIVERY-1]

		assert.Equal(
			EXPECTED_STOP_MONITORING_DELIVERY,
			gotSMDelivery,
		)
	}

}

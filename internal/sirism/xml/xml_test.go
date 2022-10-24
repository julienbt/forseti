package xml

import (
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/hove-io/forseti/internal/sirism/directionname"
	"github.com/stretchr/testify/assert"
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

func parseNotificationFromXmlFile(assert *assert.Assertions, xmlFileName string, envelope *Envelope) {
	uri, err := url.Parse(fmt.Sprintf("file://%s/data_sirism/%s", fixtureDir, xmlFileName))
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
	assert := assert.New(t)

	var tests = []struct {
		xmlFileName                              string
		expectedTotalNumberOfMonitoredStopVisits int
	}{
		{
			xmlFileName:                              "notif_siri_lille.xml",
			expectedTotalNumberOfMonitoredStopVisits: 138,
		},
		{
			xmlFileName:                              "notif_siri_lille_cancellation.xml",
			expectedTotalNumberOfMonitoredStopVisits: 0,
		},
	}

	for _, test := range tests {
		var envelope Envelope
		// Force the variable `time.Local` of the server while the run
		time.Local = time.UTC
		parseNotificationFromXmlFile(assert, test.xmlFileName, &envelope)
		assert.Equal(
			test.expectedTotalNumberOfMonitoredStopVisits,
			envelope.GetTotalNumberOfMonitoredStopVisits(),
		)
	}
}

func TestEnvelopeGetTotalNumberOfMonitoredStopVisitCancellations(t *testing.T) {
	assert := assert.New(t)

	var tests = []struct {
		xmlFileName                                          string
		expectedTotalNumberOfMonitoredStopVisitCancellations int
	}{
		{
			xmlFileName: "notif_siri_lille.xml",
			expectedTotalNumberOfMonitoredStopVisitCancellations: 0,
		},
		{
			xmlFileName: "notif_siri_lille_cancellation.xml",
			expectedTotalNumberOfMonitoredStopVisitCancellations: 1,
		},
	}

	for _, test := range tests {
		var envelope Envelope
		// Force the variable `time.Local` of the server while the run
		time.Local = time.UTC
		parseNotificationFromXmlFile(assert, test.xmlFileName, &envelope)
		assert.Equal(
			test.expectedTotalNumberOfMonitoredStopVisitCancellations,
			envelope.GetTotalNumberOfMonitoredStopVisitCancellations(),
		)
	}
}

func TestNotificationXmlUnmarshall(t *testing.T) {
	EXPECTED_LOCATION := time.FixedZone("", 2*SECONDS_PER_HOUR)
	_ = EXPECTED_LOCATION
	assert := assert.New(t)

	var tests = []struct {
		xmlFileName                            string
		expectedNumberOfStopMonitoringDelivery int
		expectedFirstStopMonitoringDelivery    *StopMonitoringDelivery
		expectedLastStopMonitoringDelivery     *StopMonitoringDelivery
	}{
		{
			xmlFileName:                            "notif_siri_lille.xml",
			expectedNumberOfStopMonitoringDelivery: 44,
			expectedFirstStopMonitoringDelivery: &StopMonitoringDelivery{
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
						ItemIdentifier: "SIRI:130784050",
						MonitoringRef:  StopPointRef("CAS001"),
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
						ItemIdentifier: "SIRI:130784224",
						MonitoringRef:  StopPointRef("CAS001"),
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
				MonitoredStopVisitCancellations: nil,
			},
			expectedLastStopMonitoringDelivery: &StopMonitoringDelivery{
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
						ItemIdentifier: "SIRI:130754436",
						MonitoringRef:  StopPointRef("CER001"),
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
						ItemIdentifier: "SIRI:130794475",
						MonitoringRef:  StopPointRef("CER001"),
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
						ItemIdentifier: "SIRI:130827058",
						MonitoringRef:  StopPointRef("CER001"),
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
						ItemIdentifier: "SIRI:130827335",
						MonitoringRef:  StopPointRef("CER001"),
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
				MonitoredStopVisitCancellations: nil,
			},
		},
		{
			xmlFileName:                            "notif_siri_lille_cancellation.xml",
			expectedNumberOfStopMonitoringDelivery: 1,
			expectedFirstStopMonitoringDelivery: &StopMonitoringDelivery{
				XMLName: xml.Name{
					Space: "http://www.siri.org.uk/siri",
					Local: "StopMonitoringDelivery",
				},
				MonitoringRef:       StopPointRef("CAS001"),
				MonitoredStopVisits: nil,
				MonitoredStopVisitCancellations: []MonitoredStopVisitCancellation{
					{
						XMLName: xml.Name{
							Space: "http://www.siri.org.uk/siri",
							Local: "MonitoredStopVisitCancellation",
						},
						ItemRef:       "SIRI:130784050",
						MonitoringRef: StopPointRef("CAS001"),
					},
				},
			},
			expectedLastStopMonitoringDelivery: nil,
		},
	}

	for _, test := range tests {
		var gotEnveloppe Envelope
		// Force the variable `time.Local` of the server while the run
		time.Local = time.UTC
		parseNotificationFromXmlFile(assert, test.xmlFileName, &gotEnveloppe)
		gotStopMonitoringDeliveries := gotEnveloppe.Notification.StopMonitoringDeliveries
		expectedNumberOfStopMonitoringDelivery := test.expectedNumberOfStopMonitoringDelivery
		assert.Len(
			gotStopMonitoringDeliveries,
			expectedNumberOfStopMonitoringDelivery,
		)
		if test.expectedNumberOfStopMonitoringDelivery > 0 {
			// Check the first element
			assert.Equal(
				*(test.expectedFirstStopMonitoringDelivery),
				gotStopMonitoringDeliveries[0],
			)

			// Check the first element
			if test.expectedNumberOfStopMonitoringDelivery > 1 {
				assert.Equal(
					*(test.expectedLastStopMonitoringDelivery),
					gotStopMonitoringDeliveries[expectedNumberOfStopMonitoringDelivery-1],
				)
			}
		}
	}
}

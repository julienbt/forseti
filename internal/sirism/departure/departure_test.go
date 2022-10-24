package departure

import (
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/hove-io/forseti/internal/departures"
	"github.com/stretchr/testify/assert"
)

var fixtureDir string

const SECONDS_PER_HOUR int = 3_600

var EXPECTED_LOCATION *time.Location = time.FixedZone("", 2*SECONDS_PER_HOUR)

var DEPARTURE_TIME_TESTS = []struct {
	name                               string
	departure                          Departure
	expectedDepartureTimeIsTheoretical bool
	expectedDepartureTime              time.Time
}{
	{
		name: "check theoretical departure time",
		departure: Departure{
			Id:              "unused",
			LineRef:         "unused",
			StopPointRef:    "unused",
			DirectionType:   departures.DirectionTypeBackward,
			DestinationRef:  "unused",
			DestinationName: "unused",
			AimedDepartureTime: time.Date(
				2022, time.June, 15,
				5, 32, 0, 0,
				EXPECTED_LOCATION,
			),
			ExpectedDepartureTime: time.Date(
				2022, time.June, 15,
				5, 32, 0, 0,
				EXPECTED_LOCATION,
			),
		},
		expectedDepartureTimeIsTheoretical: true,
		expectedDepartureTime: time.Date(
			2022, time.June, 15,
			5, 32, 0, 0,
			EXPECTED_LOCATION,
		),
	},
	{
		name: "check estimated departure time 01",
		departure: Departure{
			Id:              "unused",
			LineRef:         "unused",
			StopPointRef:    "unused",
			DirectionType:   departures.DirectionTypeBackward,
			DestinationRef:  "unused",
			DestinationName: "unused",
			AimedDepartureTime: time.Date(
				2022, time.June, 15,
				5, 32, 0, 0,
				EXPECTED_LOCATION,
			),
			ExpectedDepartureTime: time.Date(
				2022, time.June, 15,
				5, 31, 59, 999_999_999,
				EXPECTED_LOCATION,
			),
		},
		expectedDepartureTimeIsTheoretical: false,
		expectedDepartureTime: time.Date(
			2022, time.June, 15,
			5, 31, 59, 999_999_999,
			EXPECTED_LOCATION,
		),
	},
	{
		name: "check estimated departure time 02",
		departure: Departure{
			Id:              "unused",
			LineRef:         "unused",
			StopPointRef:    "unused",
			DirectionType:   departures.DirectionTypeBackward,
			DestinationRef:  "unused",
			DestinationName: "unused",
			AimedDepartureTime: time.Date(
				2022, time.June, 15,
				5, 32, 0, 0,
				EXPECTED_LOCATION,
			),
			ExpectedDepartureTime: time.Date(
				2022, time.June, 15,
				5, 32, 0, 1,
				EXPECTED_LOCATION,
			),
		},
		expectedDepartureTimeIsTheoretical: false,
		expectedDepartureTime: time.Date(
			2022, time.June, 15,
			5, 32, 0, 1,
			EXPECTED_LOCATION,
		),
	},
	{
		name: "check estimated departure time 03 (timezones differ)",
		departure: Departure{
			Id:              "unused",
			LineRef:         "unused",
			StopPointRef:    "unused",
			DirectionType:   departures.DirectionTypeBackward,
			DestinationRef:  "unused",
			DestinationName: "unused",
			AimedDepartureTime: time.Date(
				2022, time.June, 15,
				5, 32, 0, 0,
				EXPECTED_LOCATION,
			),
			ExpectedDepartureTime: time.Date(
				2022, time.June, 15,
				5, 32, 0, 0,
				time.UTC,
			),
		},
		expectedDepartureTimeIsTheoretical: false,
		expectedDepartureTime: time.Date(
			2022, time.June, 15,
			5, 32, 0, 0,
			time.UTC,
		),
	},
}

func TestMain(m *testing.M) {

	fixtureDir = os.Getenv("FIXTUREDIR")
	if fixtureDir == "" {
		panic("$FIXTUREDIR isn't set")
	}

	os.Exit(m.Run())
}

func TestDepartureTimeIsTheoretical(t *testing.T) {
	assert := assert.New(t)

	for _, test := range DEPARTURE_TIME_TESTS {
		assert.Equalf(
			test.expectedDepartureTimeIsTheoretical,
			test.departure.DepartureTimeIsTheoretical(),
			"the unit test '%s' failed", test.name,
		)
	}
}

func TestGetDepartureTime(t *testing.T) {
	assert := assert.New(t)

	for _, test := range DEPARTURE_TIME_TESTS {
		assert.Equalf(
			test.expectedDepartureTime,
			test.departure.GetDepartureTime(),
			"the unit test '%s' failed", test.name,
		)
	}
}

func TestExtractDeparturesFromFilePath(t *testing.T) {
	assert := assert.New(t)
	EXPECTED_LOCATION := time.FixedZone("", 2*SECONDS_PER_HOUR)
	var tests = []struct {
		xmlFileName                         string
		expectedNumberOfUpdatedDepartures   int
		expectedFirstUpdatedDeparture       *Departure
		expectedLastUpdatedDeparture        *Departure
		expectedNumberOfCancelledDepartures int
		expectedFirstCancelledDeparture     *CancelledDeparture
		expectedLastCancelledDeparture      *CancelledDeparture
	}{
		{
			xmlFileName:                       "notif_siri_lille.xml",
			expectedNumberOfUpdatedDepartures: 138,
			expectedFirstUpdatedDeparture: &Departure{
				Id:              "SIRI:130784050",
				LineRef:         "50",
				StopPointRef:    "CAS001",
				DirectionType:   departures.DirectionTypeBackward,
				DestinationRef:  "LIG114",
				DestinationName: "GARE LILLE FLANDRES",
				AimedDepartureTime: time.Date(
					2022, time.June, 15,
					5, 32, 0, 0,
					EXPECTED_LOCATION,
				),
				ExpectedDepartureTime: time.Date(
					2022, time.June, 15,
					5, 32, 0, 0,
					EXPECTED_LOCATION,
				),
			},
			expectedLastUpdatedDeparture: &Departure{
				Id:              "SIRI:130827335",
				LineRef:         "CO1",
				StopPointRef:    "CER001",
				DirectionType:   departures.DirectionTypeForward,
				DestinationRef:  "CAL007",
				DestinationName: "CHU-EURASANTE",
				AimedDepartureTime: time.Date(
					2022, time.June, 15,
					6, 44, 34, 0,
					EXPECTED_LOCATION,
				),
				ExpectedDepartureTime: time.Date(
					2022, time.June, 15,
					6, 44, 34, 0,
					EXPECTED_LOCATION,
				),
			},
			expectedNumberOfCancelledDepartures: 0,
			expectedFirstCancelledDeparture:     nil,
			expectedLastCancelledDeparture:      nil,
		},
		{
			xmlFileName:                         "notif_siri_lille_cancellation.xml",
			expectedNumberOfUpdatedDepartures:   0,
			expectedFirstUpdatedDeparture:       nil,
			expectedLastUpdatedDeparture:        nil,
			expectedNumberOfCancelledDepartures: 1,
			expectedFirstCancelledDeparture: &CancelledDeparture{
				Id:           "SIRI:130784050",
				StopPointRef: "CAS001",
			},
			expectedLastCancelledDeparture: nil,
		},
	}
	_ = tests
	for _, test := range tests {
		uri, err := url.Parse(fmt.Sprintf("file://%s/data_sirism/%s", fixtureDir, test.xmlFileName))
		assert.Nil(err)

		// Force the variable `time.Local` of the server while the run
		time.Local = time.UTC

		gotUpdatedDepartures, gotCancelledDepartures, err := LoadDeparturesFromFilePath(uri.Path)
		assert.Nil(err)

		// Check updated departures
		{
			expectedNumberOfUpdatedDepartures := test.expectedNumberOfUpdatedDepartures
			assert.Len(
				gotUpdatedDepartures,
				expectedNumberOfUpdatedDepartures,
			)
			// Check the first element
			if expectedNumberOfUpdatedDepartures > 0 {
				assert.Equal(
					*(test.expectedFirstUpdatedDeparture),
					gotUpdatedDepartures[0],
				)
				// Check the last element
				if expectedNumberOfUpdatedDepartures > 1 {
					assert.Equal(
						*(test.expectedLastUpdatedDeparture),
						gotUpdatedDepartures[expectedNumberOfUpdatedDepartures-1],
					)
				}
			}
		}

		// Check cancelled departures
		{
			expectedNumberOfCancelledDepartures := test.expectedNumberOfCancelledDepartures
			assert.Len(
				gotCancelledDepartures,
				expectedNumberOfCancelledDepartures,
			)
			// Check the first element
			if expectedNumberOfCancelledDepartures > 0 {
				assert.Equal(
					*(test.expectedFirstCancelledDeparture),
					gotCancelledDepartures[0],
				)
				// Check the last element
				if expectedNumberOfCancelledDepartures > 1 {
					assert.Equal(
						*(test.expectedLastCancelledDeparture),
						gotCancelledDepartures[expectedNumberOfCancelledDepartures-1],
					)
				}
			}
		}
	}
}

func BenchmarkLoadDeparturesFromFilePath(b *testing.B) {
	uri, _ := url.Parse(fmt.Sprintf("file://%s/data_sirism/notif_siri_lille.xml", fixtureDir))
	for n := 0; n < b.N; n++ {
		updatedDepartures, cancelledDepartures, err := LoadDeparturesFromFilePath(uri.Path)
		_ = updatedDepartures
		_ = cancelledDepartures
		_ = err
	}
}

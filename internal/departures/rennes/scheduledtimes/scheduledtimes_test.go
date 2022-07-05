package scheduledtimes

import (
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var fixtureDir string

const defaultTimeout time.Duration = time.Second * 10

func TestMain(m *testing.M) {

	fixtureDir = os.Getenv("FIXTUREDIR")
	if fixtureDir == "" {
		panic("$FIXTUREDIR isn't set")
	}

	os.Exit(m.Run())
}

func TestLoadScheduledStopPoints(t *testing.T) {
	const EXPECTED_NUM_OF_SCHEDULED_STOP_POINTS int = 109_817
	var PROCESSING_DATE time.Time
	var PROCESSING_LOCATION *time.Location
	PROCESSING_LOCATION, _ = time.LoadLocation("Europe/Paris")
	PROCESSING_DATE = time.Date(2012, time.February, 29, 0, 0, 0, 0, PROCESSING_LOCATION)

	assert := assert.New(t)
	require := require.New(t)
	uri, err := url.Parse(fmt.Sprintf("file://%s/data_rennes/referentiel", fixtureDir))
	require.Nil(err)

	loadedScheduledStopPoints, err := LoadScheduledTimes(*uri, defaultTimeout, &PROCESSING_DATE)
	require.Nil(err)
	assert.Len(loadedScheduledStopPoints, EXPECTED_NUM_OF_SCHEDULED_STOP_POINTS)

	// Check the values read from the first line of the CSV
	{
		const EXPECTED_ID string = "268548470"
		var EXPECTED_TIME time.Time = time.Date(
			PROCESSING_DATE.Year(),
			PROCESSING_DATE.Month(),
			PROCESSING_DATE.Day(),
			12, 56, 0, 0,
			PROCESSING_DATE.Location(),
		)
		const EXPECTED_DB_INTERNAL_LINK_ID string = "274605064"

		assert.Contains(loadedScheduledStopPoints, EXPECTED_ID)
		assert.Equal(
			loadedScheduledStopPoints[EXPECTED_ID],
			ScheduledTime{
				Id:               EXPECTED_ID,
				Time:             EXPECTED_TIME,
				DbInternalLinkId: EXPECTED_DB_INTERNAL_LINK_ID,
			},
		)
	}

	// Check the values read from the last line of the CSV
	{
		const EXPECTED_ID string = "268435458"
		var EXPECTED_TIME time.Time = time.Date(
			PROCESSING_DATE.Year(), PROCESSING_DATE.Month(), PROCESSING_DATE.Day(),
			4, 46, 1, 0,
			PROCESSING_DATE.Location(),
		)
		const EXPECTED_DB_INTERNAL_LINK_ID string = "274137857"

		assert.Contains(loadedScheduledStopPoints, EXPECTED_ID)
		assert.Equal(
			loadedScheduledStopPoints[EXPECTED_ID],
			ScheduledTime{
				Id:               EXPECTED_ID,
				Time:             EXPECTED_TIME,
				DbInternalLinkId: EXPECTED_DB_INTERNAL_LINK_ID,
			},
		)
	}

}

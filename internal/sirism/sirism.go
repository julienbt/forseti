package sirism

import (
	"fmt"
	"net/url"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/hove-io/forseti/internal/connectors"
	"github.com/hove-io/forseti/internal/departures"
	sirism_departure "github.com/hove-io/forseti/internal/sirism/departure"
)

type SiriSmContext struct {
	connector  *connectors.Connector
	lastUpdate *time.Time
	departures []sirism_departure.Departure
	mutex      sync.RWMutex
}

func (s *SiriSmContext) GetConnector() *connectors.Connector {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.connector
}

func (s *SiriSmContext) SetConnector(connector *connectors.Connector) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	s.connector = connector
}

func (s *SiriSmContext) GetLastUpdate() *time.Time {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.lastUpdate
}

func (s *SiriSmContext) SetLastUpdate(lastUpdate *time.Time) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.lastUpdate = lastUpdate
}

func (s *SiriSmContext) UpdateDepartures(departures []sirism_departure.Departure) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.departures = departures
	lastUpdateInUTC := time.Now().In(time.UTC)
	s.lastUpdate = &lastUpdateInUTC
}

func (s *SiriSmContext) GetDepartures() []sirism_departure.Departure {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.departures
}

func (s *SiriSmContext) InitContext(connectionTimeout time.Duration) {
	_, _ = loadNotificationFileExample()

	s.SetConnector(connectors.NewConnector(
		url.URL{}, // files URL
		url.URL{}, // service URL
		"",
		time.Duration(-1),
		time.Duration(-1),
		connectionTimeout,
	))
}

func (d *SiriSmContext) RefereshDeparturesLoop(context *departures.DeparturesContext) {
	context.SetPackageName(reflect.TypeOf(SiriSmContext{}).PkgPath())
	context.SetFilesRefeshTime(d.connector.GetFilesRefreshTime())
	context.SetWsRefeshTime(d.connector.GetWsRefreshTime())

	// Wait 10 seconds before reloading external departures informations
	time.Sleep(10 * time.Second)
	for {
		loadedDepartures, err := loadNotificationFileExample()
		d.UpdateDepartures(loadedDepartures)
		if err != nil {
			logrus.Error(err)
		} else {
			mappedLoadedDepartures := mapDeparturesByStopPointId(loadedDepartures)
			context.UpdateDepartures(mappedLoadedDepartures)
			logrus.Info("Departures are updated")
		}
		time.Sleep(10 * time.Second)
	}
}

func loadNotificationFileExample() ([]sirism_departure.Departure, error) {
	fixtureDir := os.Getenv("FIXTUREDIR")
	if fixtureDir == "" {
		panic("$FIXTUREDIR isn't set")
	}
	uri, err := url.Parse(fmt.Sprintf("file://%s/data_sirism/notif_siri_lille.xml", fixtureDir))
	if err != nil {
		return nil, err
	}
	return sirism_departure.LoadDeparturesFromFilePath(uri.Path)
}

func mapDeparturesByStopPointId(siriSmDepartures []sirism_departure.Departure) map[string][]departures.Departure {
	result := make(map[string][]departures.Departure)
	for _, siriSmDeparture := range siriSmDepartures {
		departureType := departures.DepartureTypeEstimated
		if siriSmDeparture.DepartureTimeIsTheoretical() {
			departureType = departures.DepartureTypeTheoretical
		}
		appendedDeparture := departures.Departure{
			Line:          siriSmDeparture.LineRef,
			Stop:          siriSmDeparture.StopPointRef,
			Type:          fmt.Sprint(departureType),
			Direction:     siriSmDeparture.DestinationRef,
			DirectionName: siriSmDeparture.DestinationName,
			Datetime:      siriSmDeparture.GetDepartureTime(),
			DirectionType: siriSmDeparture.DirectionType,
		}
		// Initilize a new list of departures if necessary
		if _, ok := result[appendedDeparture.Stop]; !ok {
			result[appendedDeparture.Stop] = make([]departures.Departure, 0)
		}
		result[appendedDeparture.Stop] = append(
			result[appendedDeparture.Stop],
			appendedDeparture,
		)
	}
	return result
}

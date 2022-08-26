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
	departures map[string]sirism_departure.Departure
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

func (s *SiriSmContext) UpdateDepartures(
	updatedDepartures []sirism_departure.Departure,
	cancelledDepartures []sirism_departure.CancelledDeparture,
) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	oldDeparturesList := s.departures
	newDeparturesList := make(map[string]sirism_departure.Departure, len(oldDeparturesList))
	// Deep copy of the departures map
	for departureId, departure := range oldDeparturesList {
		newDeparturesList[departureId] = departure
	}

	// Delete cancelled departures
	for _, cancelledDeparture := range cancelledDepartures {
		cancelledDepartureId := cancelledDeparture.Id
		if _, ok := newDeparturesList[cancelledDepartureId]; ok {
			delete(newDeparturesList, cancelledDepartureId)
			logrus.Debugf("The departure '%s' is cancelled", cancelledDepartureId)
		} else {
			logrus.Warnf("The departure '%s' cannot be cancelled, it is not exist", cancelledDepartureId)
		}
	}

	// Add/update departures
	for _, updatedDeparture := range updatedDepartures {
		updatedDepartureId := updatedDeparture.Id
		if _, ok := newDeparturesList[updatedDepartureId]; ok {
			newDeparturesList[updatedDepartureId] = updatedDeparture
			logrus.Debugf("The departure '%s' is updated", updatedDepartureId)
		} else {
			newDeparturesList[updatedDepartureId] = updatedDeparture
			logrus.Debugf("The departure '%s' is added", updatedDepartureId)
		}
	}

	s.departures = newDeparturesList
	lastUpdateInUTC := time.Now().In(time.UTC)
	s.lastUpdate = &lastUpdateInUTC
}

func (s *SiriSmContext) GetDepartures() map[string]sirism_departure.Departure {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.departures
}

func (s *SiriSmContext) InitContext(connectionTimeout time.Duration) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.connector = connectors.NewConnector(
		url.URL{}, // files URL
		url.URL{}, // service URL
		"",
		time.Duration(-1),
		time.Duration(-1),
		connectionTimeout,
	)
	s.departures = make(map[string]sirism_departure.Departure)
	s.lastUpdate = nil
}

func (d *SiriSmContext) RefereshDeparturesLoop(context *departures.DeparturesContext) {
	context.SetPackageName(reflect.TypeOf(SiriSmContext{}).PkgPath())
	context.SetFilesRefeshTime(d.connector.GetFilesRefreshTime())
	context.SetWsRefeshTime(d.connector.GetWsRefreshTime())

	// Wait 10 seconds before reloading external departures informations
	time.Sleep(10 * time.Second)
	for {
		updatedDepartures, cancelledDepartures, err := loadNotificationFileExample()
		d.UpdateDepartures(updatedDepartures, cancelledDepartures)
		if err != nil {
			logrus.Error(err)
		} else {
			mappedLoadedDepartures := mapDeparturesByStopPointId(updatedDepartures)
			context.UpdateDepartures(mappedLoadedDepartures)
			logrus.Info("Departures are updated")
		}
		time.Sleep(10 * time.Second)
	}
}

func loadNotificationFileExample() ([]sirism_departure.Departure, []sirism_departure.CancelledDeparture, error) {
	fixtureDir := os.Getenv("FIXTUREDIR")
	if fixtureDir == "" {
		panic("$FIXTUREDIR isn't set")
	}
	uri, err := url.Parse(fmt.Sprintf("file://%s/data_sirism/notif_siri_lille.xml", fixtureDir))
	if err != nil {
		return nil, nil, err
	}
	updatedDepartures, cancelledDepartures, err := sirism_departure.LoadDeparturesFromFilePath(uri.Path)
	return updatedDepartures, cancelledDepartures, err
}

func mapDeparturesByStopPointId(
	siriSmDepartures []sirism_departure.Departure,
) map[string][]departures.Departure {
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
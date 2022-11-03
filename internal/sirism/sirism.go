package sirism

import (
	"fmt"
	"net/url"
	"reflect"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/hove-io/forseti/internal/connectors"
	"github.com/hove-io/forseti/internal/departures"
	sirism_departure "github.com/hove-io/forseti/internal/sirism/departure"
	sirism_kinesis "github.com/hove-io/forseti/internal/sirism/kinesis"
)

// Type aliases
type ItemId = sirism_departure.ItemId
type StopPointRef = sirism_departure.StopPointRef

type SiriSmContext struct {
	connector               *connectors.Connector
	lastUpdate              *time.Time
	departures              map[ItemId]sirism_departure.Departure
	notificationsStream     chan []byte
	notificationsStreamName string
	mutex                   sync.RWMutex
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
	newDeparturesList := make(map[ItemId]sirism_departure.Departure, len(oldDeparturesList))
	// Deep copy of the departures map
	for departureId, departure := range oldDeparturesList {
		newDeparturesList[departureId] = departure
	}

	// Delete cancelled departures
	for _, cancelledDeparture := range cancelledDepartures {
		cancelledDepartureId := cancelledDeparture.Id
		if _, ok := newDeparturesList[cancelledDepartureId]; ok {
			delete(newDeparturesList, cancelledDepartureId)
			logrus.Debugf(
				"The departure '%s' of the stop point '%s' is cancelled",
				string(cancelledDepartureId),
				string(cancelledDeparture.StopPointRef),
			)
		} else {
			logrus.Warnf(
				"The departure '%s' of the stop point '%s' cannot be cancelled, it does not exist",
				string(cancelledDepartureId),
				string(cancelledDeparture.StopPointRef),
			)
		}
	}

	// Add/update departures
	for _, updatedDeparture := range updatedDepartures {
		updatedDepartureId := updatedDeparture.Id
		if _, ok := newDeparturesList[updatedDepartureId]; ok {
			newDeparturesList[updatedDepartureId] = updatedDeparture
			logrus.Debugf(
				"The departure '%s' of the stop point '%s' is updated",
				string(updatedDepartureId),
				string(updatedDeparture.StopPointRef),
			)
		} else {
			newDeparturesList[updatedDepartureId] = updatedDeparture
			logrus.Debugf(
				"The departure '%s' of the stop point '%s' is added",
				string(updatedDepartureId),
				string(updatedDeparture.StopPointRef),
			)
		}
	}

	s.departures = newDeparturesList
	lastUpdateInUTC := time.Now().In(time.UTC)
	s.lastUpdate = &lastUpdateInUTC
}

func (s *SiriSmContext) GetDepartures() map[ItemId]sirism_departure.Departure {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.departures
}

func (s *SiriSmContext) InitContext(
	notificationsStreamName string,
	connectionTimeout time.Duration,
) {
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
	s.departures = make(map[ItemId]sirism_departure.Departure)
	s.notificationsStream = make(chan []byte, 1)
	s.notificationsStreamName = notificationsStreamName
	sirism_kinesis.InitKinesisConsumer(
		s.notificationsStreamName,
		s.notificationsStream,
	)
	s.lastUpdate = nil
}

func (d *SiriSmContext) RefereshDeparturesLoop(context *departures.DeparturesContext) {
	context.SetPackageName(reflect.TypeOf(SiriSmContext{}).PkgPath())
	context.SetFilesRefeshTime(d.connector.GetFilesRefreshTime())
	context.SetWsRefeshTime(d.connector.GetWsRefreshTime())

	// Wait 10 seconds before reloading external departures informations
	time.Sleep(10 * time.Second)
	for {
		// Received a notification
		var notifBytes []byte = <-d.notificationsStream
		logrus.Infof("notification received (%d bytes)", len(notifBytes))
		updatedDepartures, cancelledDepartures, err := sirism_departure.LoadDeparturesFromByteArray(notifBytes)
		if err != nil {
			logrus.Errorf("record parsing error: %v", err)
			continue
		}
		d.UpdateDepartures(updatedDepartures, cancelledDepartures)
		if err != nil {
			logrus.Errorf("departures updating error: %v", err)
			continue
		}
		mappedLoadedDepartures := mapDeparturesByStopPointId(d.departures)
		context.UpdateDepartures(mappedLoadedDepartures)
		logrus.Info("departures are updated")
	}
}

func mapDeparturesByStopPointId(
	siriSmDepartures map[ItemId]sirism_departure.Departure,
) map[string][]departures.Departure {
	result := make(map[string][]departures.Departure)
	for _, siriSmDeparture := range siriSmDepartures {
		departureType := departures.DepartureTypeEstimated
		if siriSmDeparture.DepartureTimeIsTheoretical() {
			departureType = departures.DepartureTypeTheoretical
		}
		appendedDeparture := departures.Departure{
			Line:          siriSmDeparture.LineRef,
			Stop:          string(siriSmDeparture.StopPointRef),
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

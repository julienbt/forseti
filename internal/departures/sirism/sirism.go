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
	rennes_stoptimes "github.com/hove-io/forseti/internal/departures/rennes/stoptimes"
	sirism_kinesis "github.com/hove-io/forseti/internal/sirism/kinesis"
)

type SiriSmContext struct {
	connector                 *connectors.Connector
	lastUpdate                *time.Time
	departures                map[ItemId]Departure
	notificationsStream       chan []byte
	notificationsStreamName   string
	notificationsReloadPeriod time.Duration
	roleARN                   string
	dailyServiceSwitchTime    *time.Time
	location                  *time.Location
	mutex                     sync.RWMutex
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

func (s *SiriSmContext) updateDepartures(
	updatedDepartures []Departure,
	cancelledDepartures []CancelledDeparture,
) {
	oldDeparturesList := s.departures
	newDeparturesList := make(map[ItemId]Departure, len(oldDeparturesList))
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

func (s *SiriSmContext) cleanDepartures(context *departures.DeparturesContext) {
	s.departures = make(map[ItemId]Departure)
	context.UpdateDepartures(make(map[string][]departures.Departure))
}

func (s *SiriSmContext) GetDepartures() map[ItemId]Departure {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.departures
}

func (s *SiriSmContext) InitContext(
	roleARN string,
	notificationsStreamName string,
	notificationsReloadPeriod time.Duration,
	connectionTimeout time.Duration,
	dailyServiceSwitchTime *time.Time,
	location *time.Location,
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
	s.departures = make(map[ItemId]Departure)
	s.notificationsStream = make(chan []byte, 1)
	s.notificationsStreamName = notificationsStreamName
	s.notificationsReloadPeriod = notificationsReloadPeriod
	s.roleARN = roleARN
	s.dailyServiceSwitchTime = dailyServiceSwitchTime
	s.location = location
	sirism_kinesis.InitKinesisConsumer(
		s.roleARN,
		s.notificationsStreamName,
		s.notificationsStream,
		s.notificationsReloadPeriod,
	)
	s.lastUpdate = nil
}

func (d *SiriSmContext) processNotificationsLoop(context *departures.DeparturesContext) {
	cpt := 0
	for {
		// Received a notification
		// TODO: memory optimization
		var recordBytes []byte = <-d.notificationsStream
		logrus.Infof("record #%d received (%d bytes)", cpt, len(recordBytes))
		cpt++
		// var notificationBytes []byte
		// // uncompress the gzip record
		// {
		// 	// TODO: memory optimization
		// 	inputBuffer := bytes.NewBuffer(recordBytes)
		// 	gzipReader, err := gzip.NewReader(inputBuffer)
		// 	if err != nil {
		// 		logrus.Errorf(
		// 			"unexpected error occurred while the gzip decompression of the record, the current record is skipped: %v",
		// 			err,
		// 		)
		// 		continue
		// 	}
		// 	// TODO: initialize with 10 MB
		// 	outputBuffer := bytes.Buffer{}
		// 	_, err = outputBuffer.ReadFrom(gzipReader)
		// 	if err != nil {
		// 		logrus.Errorf(
		// 			"unexpected error occurred while the gzip decompression of the record, the current record is skipped: %v",
		// 			err,
		// 		)
		// 		continue
		// 	}
		// 	notificationBytes = outputBuffer.Bytes()
		// }
		// {
		// 	d.mutex.Lock()
		// 	logrus.Infof("notification received (%d bytes)", len(notificationBytes))
		// 	updatedDepartures, cancelledDepartures, err := LoadDeparturesFromByteArray(notificationBytes)
		// 	if err != nil {
		// 		logrus.Errorf("record parsing error: %v", err)
		// 		d.mutex.Unlock()
		// 		continue
		// 	}
		// 	d.updateDepartures(updatedDepartures, cancelledDepartures)
		// 	mappedLoadedDepartures := mapDeparturesByStopPointId(d.departures)
		// 	context.UpdateDepartures(mappedLoadedDepartures)
		// 	logrus.Info("departures are updated")
		// 	d.mutex.Unlock()
		// }
	}
}

func (d *SiriSmContext) getNextResetDeparturesTimerFrom(t *time.Time) (time.Time, *time.Timer) {
	todayServiceSwitchTime, _ := rennes_stoptimes.CombineDateAndTimeInLocation(
		t, d.dailyServiceSwitchTime, d.location,
	)
	nextServiceSwitchTimeDelay := todayServiceSwitchTime.Sub(*t)
	if nextServiceSwitchTimeDelay <= time.Duration(0) {
		nextServiceSwitchTimeDelay += 24 * time.Hour
	}
	nextServiceSwitchTime := t.Add(nextServiceSwitchTimeDelay)
	nextServiceSwitchTimer := time.NewTimer(nextServiceSwitchTimeDelay)
	return nextServiceSwitchTime, nextServiceSwitchTimer
}

func (d *SiriSmContext) processCleanDeparturesLoop(context *departures.DeparturesContext) {
	now := time.Now().In(d.location)
	nextServiceSwitchTime, serviceSwitchTimer := d.getNextResetDeparturesTimerFrom(&now)
	logrus.Infof("the next departures delete is scheduled at %v", nextServiceSwitchTime.Format(time.RFC3339))
	for {
		<-serviceSwitchTimer.C
		{
			d.mutex.Lock()
			{
				initialNumberOfDepartures := len(d.departures)
				d.cleanDepartures(context)
				logrus.Infof("all departures (%d departures) have been deleted", initialNumberOfDepartures)
				now := time.Now().In(d.location)
				var nextServiceSwitchTime time.Time
				nextServiceSwitchTime, serviceSwitchTimer = d.getNextResetDeparturesTimerFrom(&now)
				logrus.Infof("the next departures delete is scheduled at %v", nextServiceSwitchTime.Format(time.RFC3339))
			}
			d.mutex.Unlock()
		}
	}
}

func (d *SiriSmContext) RefereshDeparturesLoop(context *departures.DeparturesContext) {
	context.SetPackageName(reflect.TypeOf(SiriSmContext{}).PkgPath())
	context.SetFilesRefeshTime(d.connector.GetFilesRefreshTime())
	context.SetWsRefeshTime(d.connector.GetWsRefreshTime())

	// Wait 10 seconds before reloading external departures informations
	time.Sleep(10 * time.Second)

	// Launch the goroutine that allow consume the input SIRI-SM notifications
	go d.processNotificationsLoop(context)
	// Launch the goroutine that allow delete all departures daily
	go d.processCleanDeparturesLoop(context)
}

func mapDeparturesByStopPointId(
	siriSmDepartures map[ItemId]Departure,
) map[string][]departures.Departure {
	result := make(map[string][]departures.Departure)
	for _, siriSmDeparture := range siriSmDepartures {
		appendedDeparture := departures.Departure{
			Line:          siriSmDeparture.LineRef,
			Stop:          string(siriSmDeparture.StopPointRef),
			Type:          fmt.Sprint(siriSmDeparture.GetDepartureType()),
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

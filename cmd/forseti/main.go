package main

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/hove-io/forseti/api"
	"github.com/hove-io/forseti/internal/connectors"
	"github.com/hove-io/forseti/internal/departures"
	"github.com/hove-io/forseti/internal/departures/rennes"
	"github.com/hove-io/forseti/internal/departures/sytralrt"
	"github.com/hove-io/forseti/internal/equipments"
	"github.com/hove-io/forseti/internal/freefloatings"
	"github.com/hove-io/forseti/internal/freefloatings/citiz"
	"github.com/hove-io/forseti/internal/freefloatings/fluctuo"
	"github.com/hove-io/forseti/internal/manager"
	"github.com/hove-io/forseti/internal/parkings"
	"github.com/hove-io/forseti/internal/sirism"
	"github.com/hove-io/forseti/internal/vehicleoccupancies"
	"github.com/hove-io/forseti/internal/vehiclepositions"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type Config struct {
	DeparturesFilesURIStr    string `mapstructure:"departures-files-uri"`
	DeparturesFilesURI       url.URL
	DeparturesFilesRefresh   time.Duration `mapstructure:"departures-files-refresh"`
	DeparturesServiceURIStr  string        `mapstructure:"departures-service-uri"`
	DeparturesServiceURI     url.URL
	DeparturesServiceRefresh time.Duration `mapstructure:"departures-service-refresh"`
	DeparturesToken          string        `mapstructure:"departures-token"`
	DeparturesType           string        `mapstructure:"departures-type"`
	DeparturesServiceSwitch  string        `mapstructure:"departures-service-switch"`

	ParkingsURIStr  string        `mapstructure:"parkings-uri"`
	ParkingsRefresh time.Duration `mapstructure:"parkings-refresh"`
	ParkingsURI     url.URL

	EquipmentsURIStr  string        `mapstructure:"equipments-uri"`
	EquipmentsRefresh time.Duration `mapstructure:"equipments-refresh"`
	EquipmentsURI     url.URL

	FreeFloatingsFilesURIStr string `mapstructure:"free-floatings-files-uri"`
	FreeFloatingsFilesURI    url.URL
	FreeFloatingsURIStr      string `mapstructure:"free-floatings-uri"`
	FreeFloatingsURI         url.URL
	FreeFloatingsRefresh     time.Duration `mapstructure:"free-floatings-refresh"`
	FreeFloatingsToken       string        `mapstructure:"free-floatings-token"`
	FreeFloatingsType        string        `mapstructure:"free-floatings-type"`
	FreeFloatingsUserName    string        `mapstructure:"free-floatings-username"`
	FreeFloatingsPassword    string        `mapstructure:"free-floatings-password"`

	OccupancyFilesURIStr   string `mapstructure:"occupancy-files-uri"`
	OccupancyFilesURI      url.URL
	OccupancyNavitiaURIStr string `mapstructure:"occupancy-navitia-uri"`
	OccupancyNavitiaURI    url.URL
	OccupancyServiceURIStr string `mapstructure:"occupancy-service-uri"`
	OccupancyServiceURI    url.URL
	OccupancyNavitiaToken  string        `mapstructure:"occupancy-navitia-token"`
	OccupancyServiceToken  string        `mapstructure:"occupancy-service-token"`
	OccupancyRefresh       time.Duration `mapstructure:"occupancy-refresh"`
	OccupancyCleanVJ       time.Duration `mapstructure:"occupancy-clean-vj"`
	OccupancyCleanVO       time.Duration `mapstructure:"occupancy-clean-vo"`
	RouteScheduleRefresh   time.Duration `mapstructure:"routeschedule-refresh"`
	TimeZoneLocationStr    string        `mapstructure:"timezone-location"`
	TimeZoneLocation       *time.Location

	PositionsFilesURIStr     string `mapstructure:"positions-files-uri"`
	PositionsFilesURI        url.URL
	PositionsFilesRefresh    time.Duration `mapstructure:"positions-files-refresh"`
	PositionsServiceURIStr   string        `mapstructure:"positions-service-uri"`
	PositionsServiceURI      url.URL
	PositionsServiceToken    string        `mapstructure:"positions-service-token"`
	PositionsServiceRefresh  time.Duration `mapstructure:"positions-service-refresh"`
	PositionsCleanVP         time.Duration `mapstructure:"positions-clean-vp"`
	PositionsNavitiaURIStr   string        `mapstructure:"positions-navitia-uri"`
	PositionsNavitiaURI      url.URL
	PositionsNavitiaCoverage string `mapstructure:"positions-navitia-coverage"`
	PositionsNavitiaToken    string `mapstructure:"positions-navitia-token"`

	LogLevel            string        `mapstructure:"log-level"`
	ConnectionTimeout   time.Duration `mapstructure:"connection-timeout"`
	JSONLog             bool          `mapstructure:"json-log"`
	FreeFloatingsActive bool          `mapstructure:"free-floatings-refresh-active"`
	OccupancyActive     bool          `mapstructure:"occupancy-service-refresh-active"`
	PositionsActive     bool          `mapstructure:"positions-service-refresh-active"`
	Connector           string        `mapstructure:"connector-type"`
}

func noneOf(args ...string) bool {
	for _, a := range args {
		if a != "" {
			return false
		}
	}
	return true
}

func GetConfig() (Config, error) {
	//Passing configurations for departures
	pflag.String("departures-type", "sytralrt", "connector type to load data source")
	pflag.String("departures-files-uri", "", "format: [scheme:][//[userinfo@]host][/]path")
	pflag.Duration("departures-files-refresh", 300*time.Second, "time between refresh of departures data")
	pflag.String("departures-service-uri", "", "format: [scheme:][//[userinfo@]host][/]path")
	pflag.Duration("departures-service-refresh", 30*time.Second, "time between refresh of departures data")
	pflag.String("departures-token", "", "token for departures service source")
	pflag.String("departures-service-switch", "03:30:00", "Service switch time (format: HH:MM:SS)")

	//Passing configurations for parkings
	pflag.String("parkings-uri", "", "format: [scheme:][//[userinfo@]host][/]path")
	pflag.Duration("parkings-refresh", 30*time.Second, "time between refresh of parkings data")

	//Passing configurations for equipments
	pflag.String("equipments-uri", "", "format: [scheme:][//[userinfo@]host][/]path")
	pflag.Duration("equipments-refresh", 30*time.Second, "time between refresh of equipments data")

	//Passing configurations for free-floatings
	pflag.String("free-floatings-files-uri", "", "format: [scheme:][//[userinfo@]host][/]path")
	pflag.String("free-floatings-uri", "", "format: [scheme:][//[userinfo@]host][/]path")
	pflag.String("free-floatings-token", "", "token for free floating source")
	pflag.Bool("free-floatings-refresh-active", false, "activate the periodic refresh of Fluctuo data")
	pflag.Duration("free-floatings-refresh", 30*time.Second, "time between refresh of vehicles in Fluctuo data")
	pflag.String("free-floatings-type", "fluctuo", "connector type to load data source")
	pflag.String("free-floatings-username", "", "username for getting API access tokens")
	pflag.String("free-floatings-password", "", "password for getting API access tokens")

	//Passing configurations for vehicle_occupancies
	pflag.String("occupancy-files-uri", "", "format: [scheme:][//[userinfo@]host][/]path")
	pflag.String("occupancy-navitia-uri", "", "format: [scheme:][//[userinfo@]host][/]path")
	pflag.String("occupancy-navitia-token", "", "token for navitia")
	pflag.String("occupancy-service-uri", "", "format: [scheme:][//[userinfo@]host][/]path")
	pflag.String("occupancy-service-token", "", "token for prediction source")
	pflag.Bool("occupancy-service-refresh-active", false, "activate the periodic refresh of vehicle occupancy data")
	pflag.Duration("occupancy-refresh", 5*time.Minute, "time between refresh of predictions")
	pflag.Duration("routeschedule-refresh", 24*time.Hour, "time between refresh of RouteSchedules from navitia")
	pflag.Duration("occupancy-clean-vj", 24*time.Hour, "time between clean list of VehicleJourneys")
	pflag.Duration("occupancy-clean-vo", 2*time.Hour, "time between clean list of VehicleOccupancies")

	//Passing configurations for vehicle_positions
	pflag.String("positions-files-uri", "", "format: [scheme:][//[userinfo@]host][/]path")
	pflag.Duration(
		"positions-files-refresh", 5*time.Minute,
		"time between refresh of positions (only used by the connector rennes)",
	)
	pflag.String("positions-service-uri", "", "format: [scheme:][//[userinfo@]host][/]path")
	pflag.String("positions-service-token", "", "token for positions source")
	pflag.Duration("positions-service-refresh", 5*time.Minute, "time between refresh of positions")
	pflag.String("positions-navitia-uri", "", "format: [scheme:][//[userinfo@]host][/]path")
	pflag.String("positions-navitia-token", "", "token for the navitia source")
	pflag.String("positions-navitia-coverage", "", "name of a navitia coverage")
	pflag.Duration("positions-clean-vp", 2*time.Hour, "time between clean list of vehiclePositions")
	pflag.Bool("positions-service-refresh-active", false, "activate the periodic refresh of vehicle positions data")

	//Passing configurations for vehicle_occupancies and vehicle_positions
	pflag.String("timezone-location", "Europe/Paris", "timezone location")
	pflag.String("connector-type", "oditi", "connector type to load data source")

	//Passing globals configurations
	pflag.Duration("connection-timeout", 10*time.Second, "timeout to establish the ssh connection")
	pflag.Bool("json-log", false, "enable json logging")
	pflag.String("log-level", "debug", "log level: debug, info, warn, error")
	pflag.Parse()

	var config Config
	if err := viper.BindPFlags(pflag.CommandLine); err != nil {
		return config, errors.Wrap(err, "Impossible to parse flags")
	}
	viper.SetEnvPrefix("FORSETI")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))

	if err := viper.Unmarshal(&config); err != nil {
		return config, errors.Wrap(err, "Unmarshalling of flag failed")
	}

	if noneOf(
		config.DeparturesFilesURIStr, config.DeparturesServiceURIStr,
		config.ParkingsURIStr,
		config.EquipmentsURIStr,
		config.FreeFloatingsURIStr, config.FreeFloatingsFilesURIStr,
		config.OccupancyFilesURIStr, config.OccupancyNavitiaURIStr,
		config.OccupancyServiceURIStr, config.PositionsServiceURIStr,
	) {
		return config, errors.New("no data provided at all. Please provide at lease one type of data")
	}

	type ConfigUri struct {
		configURIStr string
		configURI    *url.URL
	}

	for _, uri := range []ConfigUri{
		{config.DeparturesFilesURIStr, &config.DeparturesFilesURI},
		{config.DeparturesServiceURIStr, &config.DeparturesServiceURI},
		{config.ParkingsURIStr, &config.ParkingsURI},
		{config.EquipmentsURIStr, &config.EquipmentsURI},
		{config.FreeFloatingsFilesURIStr, &config.FreeFloatingsFilesURI},
		{config.FreeFloatingsURIStr, &config.FreeFloatingsURI},
		{config.OccupancyFilesURIStr, &config.OccupancyFilesURI},
		{config.OccupancyNavitiaURIStr, &config.OccupancyNavitiaURI},
		{config.OccupancyServiceURIStr, &config.OccupancyServiceURI},
		{config.PositionsFilesURIStr, &config.PositionsFilesURI},
		{config.PositionsServiceURIStr, &config.PositionsServiceURI},
		{config.PositionsNavitiaURIStr, &config.PositionsNavitiaURI},
	} {
		if url, err := url.Parse(uri.configURIStr); err != nil {
			logrus.Errorf("Unable to parse data url: %s", uri.configURIStr)
		} else {
			*uri.configURI = *url
		}
	}

	// Parse the timezone location
	{
		timeZoneLocation, err := time.LoadLocation(config.TimeZoneLocationStr)
		if err != nil {
			err = fmt.Errorf("Unable to parse the time zone location: %s", config.TimeZoneLocationStr)
			return config, err
		}
		config.TimeZoneLocation = timeZoneLocation
	}

	return config, nil
}

func parseServiceSwitchTime(serviceSwitchTimeStr string, location *time.Location) (time.Time, error) {
	t, err := time.ParseInLocation(
		"15:04:05",
		serviceSwitchTimeStr,
		location,
	)
	if err != nil {
		invlidTime := time.Date(
			0, time.January, 1,
			0, 0, 0, 000_000_000,
			time.UTC,
		)
		return invlidTime, fmt.Errorf("error parse service switch time: %v", err)
	}
	return time.Date(
		0, time.January, 1,
		t.Hour(), t.Minute(), t.Second(), t.Nanosecond(),
		location,
	), nil
}

func main() {
	config, err := GetConfig()
	if err != nil {
		logrus.Fatalf("Impossible to load data at startup: %s", err)
	}

	initLog(config.JSONLog, config.LogLevel)
	manager := &manager.DataManager{}

	location := config.TimeZoneLocation

	// create API router
	router := api.SetupRouter(manager, nil)

	// With equipments
	Equipments(manager, &config, router)

	// With freefloating
	FreeFloating(manager, &config, router)

	// With departures
	var departuresServiceSwitchTime time.Time
	{
		var err error
		departuresServiceSwitchTime, err = parseServiceSwitchTime(config.DeparturesServiceSwitch, location)
		if err != nil {
			if err != nil {
				logrus.Fatalf("%v", err)
			}
		}
	}
	Departures(manager, &config, router, location, &departuresServiceSwitchTime)

	// With parkings
	Parkings(manager, &config, router)

	// With vehicle occupancies
	VehicleOccupancies(manager, &config, router, location)

	// With vehicle positions
	VehiclePositions(manager, &config, router)

	// start router
	err = router.Run()
	if err != nil {
		logrus.Fatalf("Impossible to start gin: %s", err)
	}
}

func Equipments(manager *manager.DataManager, config *Config, router *gin.Engine) {
	if len(config.EquipmentsURI.String()) == 0 || config.EquipmentsRefresh.Seconds() <= 0 {
		logrus.Debug("Equipments is disabled")
		return
	}
	equipmentsContext := &equipments.EquipmentsContext{}
	manager.SetEquipmentsContext(equipmentsContext)
	go equipments.RefreshEquipmentLoop(equipmentsContext, config.EquipmentsURI,
		config.EquipmentsRefresh, config.ConnectionTimeout)
	equipments.AddEquipmentsEntryPoint(router, equipmentsContext)
}

func FreeFloating(manager *manager.DataManager, config *Config, router *gin.Engine) {
	if len(config.FreeFloatingsURI.String()) == 0 || config.FreeFloatingsRefresh.Seconds() <= 0 {
		logrus.Debug("FreeFloating is disabled")
		return
	}

	freeFloatingsContext := &freefloatings.FreeFloatingsContext{}
	manager.SetFreeFloatingsContext(freeFloatingsContext)
	freefloatings.AddFreeFloatingsEntryPoint(router, freeFloatingsContext)
	freeFloatingsContext.ManageFreeFloatingsStatus(config.FreeFloatingsActive)

	if config.FreeFloatingsType == string(connectors.Connector_CITIZ) {
		var c = citiz.CitizContext{}

		c.InitContext(config.FreeFloatingsFilesURI, config.FreeFloatingsURI, config.FreeFloatingsRefresh,
			config.ConnectionTimeout, config.FreeFloatingsUserName, config.FreeFloatingsPassword)

		go c.RefreshFreeFloatingLoop(freeFloatingsContext)

	} else if config.FreeFloatingsType == string(connectors.Connector_FLUCTUO) {
		var f = fluctuo.FluctuoContext{}

		f.InitContext(config.FreeFloatingsURI, config.FreeFloatingsRefresh, config.FreeFloatingsToken,
			config.ConnectionTimeout)

		go f.RefreshFreeFloatingLoop(freeFloatingsContext)
	}
}

func Departures(
	manager *manager.DataManager,
	config *Config,
	router *gin.Engine,
	location *time.Location,
	serviceSwitchTime *time.Time,
) {

	// This argument is mandatory for all departures connectors
	if len(config.DeparturesFilesURI.String()) == 0 {
		logrus.Debug("Departures is disabled")
		return
	}

	departuresContext := &departures.DeparturesContext{}
	manager.SetDeparturesContext(departuresContext)
	departures.AddDeparturesEntryPoint(router, departuresContext)

	// Enable the SytralRT connector
	if config.DeparturesType == string(connectors.Connector_SYTRALRT) {

		var sytralContext sytralrt.SytralRTContext = sytralrt.SytralRTContext{}
		sytralContext.InitContext(config.DeparturesFilesURI, config.DeparturesFilesRefresh, config.ConnectionTimeout)
		go sytralContext.RefreshDeparturesLoop(departuresContext)
	} else if config.DeparturesType == string(connectors.Connector_RENNES) { // Enable the Rennes connector
		var rennesContext rennes.RennesContext = rennes.RennesContext{}
		rennesContext.InitContext(
			config.DeparturesFilesURI,
			config.DeparturesFilesRefresh,
			config.DeparturesServiceURI,
			config.DeparturesServiceRefresh,
			config.DeparturesToken,
			config.ConnectionTimeout,
			location,
			serviceSwitchTime,
		)
		go rennesContext.RefereshDeparturesLoop(departuresContext)
	} else if config.DeparturesType == string(connectors.Connector_SIRI_SM) { // Enable the SIRI-SM connector
		var siriSmContext sirism.SiriSmContext = sirism.SiriSmContext{}
		siriSmContext.InitContext(config.ConnectionTimeout)
		go siriSmContext.RefereshDeparturesLoop(departuresContext)
	}
}

func Parkings(manager *manager.DataManager, config *Config, router *gin.Engine) {
	if len(config.ParkingsURI.String()) == 0 || config.ParkingsRefresh.Seconds() <= 0 {
		logrus.Debug("Parkings is disabled")
		return
	}
	parkingsContext := &parkings.ParkingsContext{}
	manager.SetParkingsContext(parkingsContext)
	go parkings.RefreshParkingsLoop(parkingsContext, config.ParkingsURI,
		config.ParkingsRefresh, config.ConnectionTimeout)
	parkings.AddParkingsEntryPoint(router, parkingsContext)
}

func VehicleOccupancies(manager *manager.DataManager, config *Config, router *gin.Engine, location *time.Location) {
	if len(config.OccupancyNavitiaURI.String()) == 0 || len(config.OccupancyServiceURI.String()) == 0 {
		logrus.Debug("Vehicle occupancies is disabled")
		return
	}

	var err error

	if config.Connector == string(connectors.Connector_ODITI) {
		var vehicleOccupanciesOditiContext vehicleoccupancies.IVehicleOccupancy
		vehicleOccupanciesOditiContext, err = vehicleoccupancies.VehicleOccupancyFactory(string(connectors.Connector_ODITI))
		if err != nil {
			logrus.Error(err)
			return
		}
		manager.SetVehicleOccupanciesContext(vehicleOccupanciesOditiContext)

		vehicleOccupanciesOditiContext.InitContext(config.OccupancyFilesURI, config.OccupancyServiceURI,
			config.OccupancyServiceToken, config.OccupancyNavitiaURI, config.OccupancyNavitiaToken,
			config.OccupancyRefresh, config.OccupancyCleanVJ, config.OccupancyCleanVO, config.ConnectionTimeout,
			location, config.OccupancyActive)

		go vehicleOccupanciesOditiContext.RefreshVehicleOccupanciesLoop(config.OccupancyServiceURI,
			config.OccupancyServiceToken, config.OccupancyNavitiaURI, config.OccupancyNavitiaToken,
			config.OccupancyRefresh, config.OccupancyCleanVJ, config.OccupancyCleanVO, config.ConnectionTimeout,
			location)
		vehicleoccupancies.AddVehicleOccupanciesEntryPoint(router, vehicleOccupanciesOditiContext)

	} else if config.Connector == string(connectors.Connector_GRFS_RT) {
		var vehicleOccupanciesContext vehicleoccupancies.IVehicleOccupancy
		vehicleOccupanciesContext, err = vehicleoccupancies.VehicleOccupancyFactory(string(connectors.Connector_GRFS_RT))
		if err != nil {
			logrus.Error(err)
			return
		}

		manager.SetVehicleOccupanciesContext(vehicleOccupanciesContext)

		vehicleOccupanciesContext.InitContext(config.OccupancyFilesURI, config.OccupancyServiceURI,
			config.OccupancyServiceToken, config.OccupancyNavitiaURI, config.OccupancyNavitiaToken,
			config.OccupancyRefresh, config.OccupancyCleanVJ, config.OccupancyCleanVO, config.ConnectionTimeout,
			location, config.OccupancyActive)

		go vehicleOccupanciesContext.RefreshVehicleOccupanciesLoop(config.OccupancyServiceURI,
			config.OccupancyServiceToken, config.OccupancyNavitiaURI, config.OccupancyNavitiaToken,
			config.OccupancyRefresh, config.OccupancyCleanVJ, config.OccupancyCleanVO, config.ConnectionTimeout,
			location)
		vehicleoccupancies.AddVehicleOccupanciesEntryPoint(router, vehicleOccupanciesContext)
	} else {
		logrus.Error("Wrong vehicleoccupancy type passed")
		return
	}
}

func VehiclePositions(
	manager *manager.DataManager,
	config *Config,
	router *gin.Engine,
) {
	if len(config.PositionsServiceURI.String()) == 0 {
		logrus.Debug("Vehicle positions is disabled")
		return
	}

	var vehiclePositionsContext vehiclepositions.IConnectors
	var err error

	if config.Connector == string(connectors.Connector_GRFS_RT) {
		vehiclePositionsContext, err = vehiclepositions.ConnectorFactory(string(connectors.Connector_GRFS_RT))
		if err != nil {
			logrus.Error(err)
			return
		}
	} else if config.Connector == string(connectors.Connector_RENNES) {
		vehiclePositionsContext, err = vehiclepositions.ConnectorFactory(string(connectors.Connector_RENNES))
		if err != nil {
			logrus.Error(err)
			return
		}
	} else {
		logrus.Error("Wrong connector type passed")
		return
	}

	manager.SetVehiclePositionsContext(vehiclePositionsContext)

	vehiclePositionsContext.InitContext(
		config.PositionsFilesURI,
		config.PositionsFilesRefresh,
		config.PositionsServiceURI,
		config.PositionsServiceToken,
		config.PositionsServiceRefresh,
		config.PositionsNavitiaURI,
		config.PositionsNavitiaToken,
		config.PositionsNavitiaCoverage,
		config.ConnectionTimeout,
		config.PositionsCleanVP,
		config.TimeZoneLocation,
		config.PositionsActive,
	)
	go vehiclePositionsContext.RefreshVehiclePositionsLoop()

	vehiclepositions.AddVehiclePositionsEntryPoint(router, vehiclePositionsContext)
}

func initLog(jsonLog bool, logLevel string) {
	if jsonLog {
		// Log as JSON instead of the default ASCII formatter.
		logrus.SetFormatter(&logrus.JSONFormatter{})
	}
	logrus.SetOutput(os.Stdout)
	level, err := logrus.ParseLevel(logLevel)
	if err != nil {
		logrus.Fatal(err)
	}
	logrus.SetLevel(level)
}

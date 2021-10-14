package vehicleoccupanciesv2

import (
	"testing"

	"github.com/CanalTP/forseti/internal/connectors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_VehicleOccupancyFactory(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	// Create vehicle occupancies type of GTFS-RT
	vehicleoccupancies, err := VehicleOccupancyFactory(string(connectors.Connector_GRFS_RT))
	require.Nil(err)
	_, ok := vehicleoccupancies.(*VehicleOccupanciesGtfsRtContext)
	require.True(ok)

	// Create vehicle occupancies type of ODITI
	vehicleoccupancies, err = VehicleOccupancyFactory(string(connectors.Connector_ODITI))
	require.Nil(err)
	_, ok = vehicleoccupancies.(*VehicleOccupanciesGtfsRtContext)
	require.False(ok)

	// Create vehicle occupancies type of unknown
	vehicleoccupancies, err = VehicleOccupancyFactory("unknown")
	require.NotNil(err)
	assert.EqualError(err, "Wrong vehicleoccupancy type passed")
	_, ok = vehicleoccupancies.(*VehicleOccupanciesGtfsRtContext)
	require.False(ok)

	// Create vehicle occupancies type of GTFS-RT
	vehicleoccupancies, err = VehicleOccupancyFactory(string(connectors.Connector_ODITI))
	require.Nil(err)
	_, ok = vehicleoccupancies.(*VehicleOccupanciesOditiContext)
	require.True(ok)

	// Create vehicle occupancies type of ODITI
	vehicleoccupancies, err = VehicleOccupancyFactory(string(connectors.Connector_GRFS_RT))
	require.Nil(err)
	_, ok = vehicleoccupancies.(*VehicleOccupanciesOditiContext)
	require.False(ok)
}

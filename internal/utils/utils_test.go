package utils

import (
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/ory/dockertest"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const oneline = "1;87A;Mions Bourdelle;11 min;E;2018-09-17 20:28:00;35998;87A-022AM:5:2:12\r\n"

var defaultTimeout time.Duration = time.Second * 10

var sftpPort string
var fixtureDir string

func TestMain(m *testing.M) {

	fixtureDir = os.Getenv("FIXTUREDIR")
	if fixtureDir == "" {
		panic("$FIXTUREDIR isn't set")
	}

	flag.Parse() //required to get Short() from testing
	if testing.Short() {
		logrus.Warn("skipping test Docker in short mode.")
		os.Exit(m.Run())
	}

	pool, err := dockertest.NewPool("")
	if err != nil {
		logrus.Fatalf("Could not connect to docker: %s", err)
	}
	opts := dockertest.RunOptions{
		Repository: "atmoz/sftp",
		Tag:        "alpine",
		Cmd:        []string{"forseti:pass"},
		Mounts:     []string{fmt.Sprintf("%s:/home/forseti", fixtureDir)},
	}
	resource, err := pool.RunWithOptions(&opts)
	if err != nil {
		logrus.Fatalf("Could not start resource: %s", err)
	}
	//lets wait a bit for the docker to start :(
	time.Sleep(3 * time.Second)
	sftpPort = resource.GetPort("22/tcp")
	//Run tests
	code := m.Run()

	// You can't defer this because os.Exit doesn't care for defer
	if err := pool.Purge(resource); err != nil {
		logrus.Fatalf("Could not purge resource: %s", err)
	}

	os.Exit(code)
}

func TestStringToInt(t *testing.T) {

	assert := assert.New(t)

	result := StringToInt("10", 0)
	assert.Equal(result, 10)

	result = StringToInt("aas", 0)
	assert.Equal(result, 0)
}

func TestAddDateAndTime(t *testing.T) {

	assert := assert.New(t)
	require := require.New(t)

	str := "2014-11-12T00:00:00.000Z"
	date, err := time.Parse(time.RFC3339, str)
	require.Nil(err)

	offsetStr := "0001-01-01T00:00:00.000Z"
	offsetDate, err := time.Parse(time.RFC3339, offsetStr)
	require.Nil(err)

	result := AddDateAndTime(date, offsetDate)
	assert.Equal(result.String(), "2015-11-12 00:00:00 +0000 UTC")
}

func TestCalculateOccupancy(t *testing.T) {

	assert := assert.New(t)

	result := CalculateOccupancy(0)
	assert.Equal(result, 0)
	result = CalculateOccupancy(50)
	assert.Equal(result, 50)
}

func TestGetSFTPFileTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test Docker in short mode.")
	}
	uri, err := url.Parse(fmt.Sprintf("sftp://forseti:pass@localhost:%s/oneline.txt", sftpPort))
	require.Nil(t, err)
	_, err = GetFileWithSftp(*uri, time.Microsecond)
	require.NotNil(t, err)
}
func TestGetSFTPFile(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test Docker in short mode.")
	}
	uri, err := url.Parse(fmt.Sprintf("sftp://forseti:pass@localhost:%s/oneline.txt", sftpPort))
	require.Nil(t, err)
	reader, err := GetFileWithSftp(*uri, defaultTimeout)
	require.Nil(t, err)
	b := make([]byte, 100)
	len, err := reader.Read(b)
	assert.Nil(t, err)
	assert.Equal(t, oneline, string(b[0:len]))

	reader, err = GetFile(*uri, defaultTimeout)
	assert.Nil(t, err)
	len, err = reader.Read(b)
	assert.Nil(t, err)
	assert.Equal(t, oneline, string(b[0:len]))
}

func TestGetSFTPFS(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test Docker in short mode.")
	}
	uri, err := url.Parse(fmt.Sprintf("file://%s/oneline.txt", fixtureDir))
	require.Nil(t, err)
	reader, err := GetFileWithFS(*uri)
	require.Nil(t, err)
	b := make([]byte, 100)
	len, err := reader.Read(b)
	assert.Nil(t, err)
	assert.Equal(t, oneline, string(b[0:len]))

	reader, err = GetFile(*uri, defaultTimeout)
	assert.Nil(t, err)
	len, err = reader.Read(b)
	assert.Nil(t, err)
	assert.Equal(t, oneline, string(b[0:len]))

}

func TestGetSFTPFileError(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test Docker in short mode.")
	}
	uri, err := url.Parse(fmt.Sprintf("sftp://forseti:wrongpass@localhost:%s/oneline.txt", sftpPort))
	require.Nil(t, err)
	_, err = GetFileWithSftp(*uri, defaultTimeout)
	require.Error(t, err)

	_, err = GetFile(*uri, defaultTimeout)
	require.Error(t, err)

	uri, err = url.Parse(fmt.Sprintf("sftp://monuser:pass@localhost:%s/oneline.txt", sftpPort))
	require.Nil(t, err)
	_, err = GetFileWithSftp(*uri, defaultTimeout)
	require.Error(t, err)
	_, err = GetFile(*uri, defaultTimeout)
	require.Error(t, err)

	uri, err = url.Parse(fmt.Sprintf("sftp://forseti:pass@localhost:%s/not.txt", sftpPort))
	require.Nil(t, err)
	_, err = GetFileWithSftp(*uri, defaultTimeout)
	require.Error(t, err)
	_, err = GetFile(*uri, defaultTimeout)
	require.Error(t, err)
}

func TestChekrespStatus200(t *testing.T) {
	require := require.New(t)

	handler := func(w http.ResponseWriter) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintln(w, `{""}`)
	}

	w := httptest.NewRecorder()
	handler(w)
	resp := w.Result()
	err := CheckResponseStatus(resp)
	require.Nil(err)
}

func TestChekrespStatus404WithOutMessage(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	handler := func(w http.ResponseWriter) {
		w.WriteHeader(http.StatusNotFound)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintln(w, `{}`)
	}

	w := httptest.NewRecorder()
	handler(w)
	resp := w.Result()
	err := CheckResponseStatus(resp)
	require.Error(err)

	assert.Equal(err.Error(), "ERROR 404: no details for this error")
}

func TestChekrespStatus404WithMessage(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	handler := func(w http.ResponseWriter) {
		w.WriteHeader(http.StatusNotFound)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintln(w, `{"message": "page or data not found"}`)
	}

	w := httptest.NewRecorder()
	handler(w)
	resp := w.Result()
	err := CheckResponseStatus(resp)
	require.Error(err)

	assert.Equal(err.Error(), "ERROR 404: page or data not found")
}

func TestChekrespStatus401(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	handler := func(w http.ResponseWriter) {
		w.WriteHeader(http.StatusUnauthorized)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintln(w, `{"message": "unauthorized access is denied"}`)
	}

	w := httptest.NewRecorder()
	handler(w)
	resp := w.Result()
	err := CheckResponseStatus(resp)
	require.Error(err)

	assert.Equal(err.Error(), "ERROR 401: unauthorized access is denied")
}

func TestChekrespStatusNotManage(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	handler := func(w http.ResponseWriter) {
		w.WriteHeader(http.StatusContinue)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintln(w, `{"message": "message from not manage status"}`)
	}

	w := httptest.NewRecorder()
	handler(w)
	resp := w.Result()
	err := CheckResponseStatus(resp)
	require.Error(err)

	assert.Equal(err.Error(), "ERROR 100: no details for this error")
}

func TestPaginate(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	paginate, indexS, indexE := PaginateEndPoint(100, 10, 0)
	require.NotNil(paginate)
	assert.Equal(paginate.Start_page, 0)
	assert.Equal(paginate.Items_on_page, 10)
	assert.Equal(paginate.Items_per_page, 10)
	assert.Equal(paginate.Total_result, 100)
	assert.Equal(indexS, 0)
	assert.Equal(indexE, 10)
}

func TestPaginateWithOutCount(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	paginate, indexS, indexE := PaginateEndPoint(20, 21, 0)
	require.NotNil(paginate)
	assert.Equal(paginate.Start_page, 0)
	assert.Equal(paginate.Items_on_page, 20)
	assert.Equal(paginate.Items_per_page, 21)
	assert.Equal(paginate.Total_result, 20)
	assert.Equal(indexS, 0)
	assert.Equal(indexE, 20)
}

func TestPaginateWithOutStartPage(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	paginate, indexS, indexE := PaginateEndPoint(20, 20, 1)
	require.NotNil(paginate)
	assert.Equal(paginate.Start_page, 1)
	assert.Equal(paginate.Items_on_page, 0)
	assert.Equal(paginate.Items_per_page, 20)
	assert.Equal(paginate.Total_result, 20)
	assert.Equal(indexS, -1)
	assert.Equal(indexE, 20)
}

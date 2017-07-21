package pandora

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/influxdata/telegraf/testutil"

	"reflect"

	"github.com/stretchr/testify/require"
)

func TestHTTPConnectError_InvalidURL(t *testing.T) {
	i := PandoraTSDB{
		URL: "htt://foobar:8089",
	}

	err := i.Connect()
	require.Error(t, err)

}

func TestHTTPError_DatabaseNotFound(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/write":
			w.WriteHeader(http.StatusNotFound)
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintln(w, `{"results":[{}],"error":"database not found"}`)
		case "/query":
			w.WriteHeader(http.StatusNotFound)
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintln(w, `{"results":[{}],"error":"database not found"}`)
		}
	}))
	defer ts.Close()

	i := PandoraTSDB{
		URL:  ts.URL,
		Repo: "test",
	}

	err := i.Connect()
	require.NoError(t, err)
	err = i.Write(testutil.MockMetrics())
	require.Error(t, err)
	require.NoError(t, i.Close())
}

// field type conflict does not return an error, instead we
func TestHTTPError_FieldTypeConflict(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/write":
			w.WriteHeader(http.StatusNotFound)
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintln(w, `{"results":[{}],"error":"field type conflict: input field \"value\" on measurement \"test\" is type integer, already exists as type float dropped=1"}`)
		}
	}))
	defer ts.Close()

	i := PandoraTSDB{
		URL:  ts.URL,
		Repo: "test",
	}

	err := i.Connect()
	require.NoError(t, err)
	err = i.Write(testutil.MockMetrics())
	require.NoError(t, err)
	require.NoError(t, i.Close())
}

func Test_createSeries(t *testing.T) {
	points := []byte("cpu,host=h1 value=123\ngpu,region=g1 value=123\ntest,host=h1 value=123\nmem,host=h1 value=123")
	series := getSeries(points)
	exp := []string{"cpu", "gpu", "test", "mem"}
	if !reflect.DeepEqual(series, exp) {
		t.Error(series, exp)
	}
	t.Log(series)
}

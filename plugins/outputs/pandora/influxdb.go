package pandora

import (
	"bytes"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/plugins/outputs"

	"github.com/qiniu/pandora-go-sdk/pipeline"
	"github.com/qiniu/pandora-go-sdk/tsdb"

	sdkbase "github.com/qiniu/pandora-go-sdk/base"
)

type PandoraTSDB struct {
	// URL is only for backwards compatability
	URL              string            `toml:"url"`
	AK               string            `toml:"ak"`
	SK               string            `toml:"sk"`
	Repo             string            `toml:"repo"`
	RetentionPolicy  string            `toml:"retention_policy`
	AutoCreateSeries bool              `toml:"auto_create_series`
	Timeout          internal.Duration `toml:"timeout"`

	client tsdb.TsdbAPI
}

var sampleConfig = `
 # Configuration for PandoraTSDB server to send metrics to
  [[outputs.pandora]]
  url = "http://localhost:8086" # required
  ## The target repo for metrics (telegraf will create it if not exists).
  repo = "telegraf" # required
  
  ## 是否自动创建series
  auto_create_series = false
  ## 自创创建的series的retention，支持的retention为[1-30]d
  retention_policy = ""
  ## Write timeout (for the PandoraTSDB client), formatted as a string.
  ## If not provided, will default to 5s. 0s means no timeout (not recommended).
  timeout = "5s"
  ak = "ACCESS_KEY"
  sk = "SECRET_KEY"
`

func (i *PandoraTSDB) Connect() error {
	log.Println(i.URL)
	u, err := url.Parse(i.URL)
	if err != nil {
		return fmt.Errorf("error parsing config.URL: %s", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("config.URL scheme must be http(s), got %s", u.Scheme)
	}
	log.Println(u.String())
	cfg := pipeline.NewConfig().
		WithAccessKeySecretKey(i.AK, i.SK).
		WithEndpoint(i.URL).
		WithLogger(sdkbase.NewDefaultLogger()).
		WithLoggerLevel(sdkbase.LogDebug).
		WithResponseTimeout(i.Timeout.Duration)

	// 生成client实例
	client, err := tsdb.New(cfg)
	if err != nil {
		log.Println(err)
		return err
	}
	i.client = client

	return nil
}

func (i *PandoraTSDB) Close() error {
	return nil
}

func (i *PandoraTSDB) SampleConfig() string {
	return sampleConfig
}

func (i *PandoraTSDB) Description() string {
	return "Configuration for PandoraTSDB server to send metrics to"
}

// Choose a random server in the cluster to write to until a successful write
// occurs, logging each unsuccessful. If all servers fail, return error.
func (i *PandoraTSDB) Write(metrics []telegraf.Metric) error {
	bufsize := 0
	for _, m := range metrics {
		bufsize += m.Len()
	}
	r := metric.NewReader(metrics)
	p := make([]byte, bufsize)
	n, err := r.Read(p)
	if err != nil {
		return err
	}
	// This will get set to nil if a successful write occurs
	err = fmt.Errorf("Could not write to any PandoraTSDB server in cluster")

	if e := i.client.PostPointsFromBytes(&tsdb.PostPointsFromBytesInput{
		RepoName: i.Repo,
		Buffer:   p[:n],
	}); e != nil {
		log.Printf("E! PandoraTSDB Output Error: %s", e)
		if strings.Contains(e.Error(), "field type conflict") {
			log.Printf("E! Field type conflict, dropping conflicted points: %s", e)
			// setting err to nil, otherwise we will keep retrying and points
			// w/ conflicting types will get stuck in the buffer forever.
			err = nil
		} else if strings.Contains(e.Error(), "E7101") && i.AutoCreateSeries {
			log.Println("I! Seires does not exists, start to create series")
			createSeries(i.Repo, i.RetentionPolicy, p[:n], i.client)
		}
		// Log write failure
	} else {
		err = nil
	}

	return err
}

func newPandoraTSDB() *PandoraTSDB {
	return &PandoraTSDB{
		Timeout: internal.Duration{Duration: time.Second * 5},
	}
}

func init() {
	outputs.Add("pandora", func() telegraf.Output { return newPandoraTSDB() })
}

func createSeries(repo, retention string, points []byte, client tsdb.TsdbAPI) (err error) {
	series := getSeries(points)
	for _, s := range series {
		log.Printf("I! create series:%v, retention:%v for repo:%v", s, retention, repo)
		err = client.CreateSeries(&tsdb.CreateSeriesInput{
			RepoName:   repo,
			SeriesName: s,
			Retention:  retention,
		})
		if err != nil {
			log.Printf("E! create series fail, %v", err)
		}
	}

	return
}

func getSeries(points []byte) (series []string) {

	series = make([]string, 0)
	lines := bytes.Split(points, []byte("\n"))
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		ss := bytes.Split(line, []byte(","))
		if len(ss) > 1 {
			series = append(series, string(ss[0]))
		}
	}

	return
}

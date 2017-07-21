package pipeline

import (
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	tsdb "github.com/influxdata/influxdb/models"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/plugins/outputs"

	"github.com/qiniu/pandora-go-sdk/pipeline"

	sdkbase "github.com/qiniu/pandora-go-sdk/base"
	tsdbSdk "github.com/qiniu/pandora-go-sdk/tsdb"
)

type Pipeline struct {
	// URL is only for backwards compatability
	URL            string            `toml:"url"`
	AK             string            `toml:"ak"`
	SK             string            `toml:"sk"`
	Repo           string            `toml:"repo"`
	AutoCreateRepo bool              `toml:"auto_create_repo`
	Timeout        internal.Duration `toml:"timeout"`

	client pipeline.PipelineAPI

	tsdbClient tsdbSdk.TsdbAPI
}

var sampleConfig = `
 # Configuration for Pandora Pipeline server to send metrics to
  [[outputs.pipeline]]
  url = "https://pipeline.qiniu.com" # required
  ## The target repo for metrics (telegraf will create it if not exists).
  repo = "monitor" # required
  ## 是否自动创建repo以及自动根据数据源中新增字段更新repo schema
  auto_create_repo = false
  ## Write timeout (for the Pandora client), formatted as a string.
  ## If not provided, will default to 5s. 0s means no timeout (not recommended).
  timeout = "5s"
  ak = "ACCESS_KEY"
  sk = "SECRET_KEY"
`

func (i *Pipeline) Connect() error {
	u, err := url.Parse(i.URL)
	if err != nil {
		return fmt.Errorf("error parsing config.URL: %s", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("config.URL scheme must be http(s), got %s", u.Scheme)
	}
	cfg := pipeline.NewConfig().
		WithAccessKeySecretKey(i.AK, i.SK).
		WithEndpoint(i.URL).
		WithLogger(sdkbase.NewDefaultLogger()).
		WithLoggerLevel(sdkbase.LogDebug).
		WithResponseTimeout(i.Timeout.Duration)

	// 生成client实例
	client, err := pipeline.New(cfg)
	if err != nil {
		log.Println(err)
		return err
	}
	i.client = client

	//生成tsdb client实例
	tsdbCfg := pipeline.NewConfig().
		WithAccessKeySecretKey(i.AK, i.SK).
		WithEndpoint("https://tsdb.qiniu.com").
		WithLogger(sdkbase.NewDefaultLogger()).
		WithLoggerLevel(sdkbase.LogDebug).
		WithResponseTimeout(i.Timeout.Duration)

	tsdbClient, err := tsdbSdk.New(tsdbCfg)
	if err != nil {
		log.Println(err)
		return err
	}
	i.tsdbClient = tsdbClient

	return nil
}

func (i *Pipeline) Close() error {
	return nil
}

func (i *Pipeline) SampleConfig() string {
	return sampleConfig
}

func (i *Pipeline) Description() string {
	return "Configuration for Pipeline server to send metrics to"
}

func convertTag(repoName string, tags tsdb.Tags) string {
	result := ""

	for _, val := range tags {
		result += fmt.Sprintf("%s_%s=%s\t", repoName, string(val.Key), string(val.Value))
	}

	return result
}

func convertField(repoName string, fields tsdb.Fields) string {
	result := ""

	for key, val := range fields {
		result += fmt.Sprintf("%s_%s=%v\t", repoName, key, val)
	}

	return result
}

// Choose a random server in the cluster to write to until a successful write
// occurs, logging each unsuccessful. If all servers fail, return error.
func (i *Pipeline) Write(metrics []telegraf.Metric) error {
	bufsize := 0
	for _, m := range metrics {
		bufsize += m.Len()
	}
	// fmt.Println(metrics)
	r := metric.NewReader(metrics)
	p := make([]byte, bufsize)
	n, err := r.Read(p)
	if err != nil && n != bufsize {
		log.Print("E! ", err)
		return err
	}
	pts, err := tsdb.ParsePoints(p)
	if err != nil {
		log.Printf("E! invalid points format", err)
		return err
	}
	// fmt.Println("I! ", string(p))
	// fmt.Println("I! >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
	points := make(map[int64]tsdb.Points)
	for _, pt := range pts {
		// fmt.Println(pt.String())
		timestamp := pt.UnixNano()
		if _, ok := points[timestamp]; !ok {
			points[timestamp] = make(tsdb.Points, 0)
		}
		points[timestamp] = append(points[timestamp], pt)
		if strings.Contains(string(pt.Name()), "nginx") {
			log.Println("D! ", time.Now().String(), pt.String())
		}
	}

	var data string
	for timestamp, pts := range points {
		for _, pt := range pts {
			repoName := string(pt.Name())
			data += convertTag(repoName, pt.Tags())
			fields, _ := pt.Fields()
			data += convertField(repoName, fields)
		}
		data += fmt.Sprintf("timestamp=%d\n", timestamp)
	}

	// This will get set to nil if a successful write occurs
	// fmt.Println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
	fmt.Println("D! ", time.Now().String(), data)
	if e := i.client.PostDataFromBytes(&pipeline.PostDataFromBytesInput{
		RepoName: i.Repo,
		Buffer:   []byte(data),
	}); e != nil {
		log.Printf("E! Pandora Pipeline Output Error: %s", e)
		if strings.Contains(e.Error(), "E18102") {
			log.Printf("E! repo %s does not exists", i.Repo)
			// setting err to nil, otherwise we will keep retrying and points
			// w/ conflicting types will get stuck in the buffer forever.
			if i.AutoCreateRepo {
				log.Println("I! start to create pipeline repo")
				err = i.updateSchema(pts)
				if err != nil {
					fmt.Println(err)
				}
			} else {
				err = nil
			}
		} else if strings.Contains(e.Error(), "E18111") {
			log.Println("E! schema  does not match")
			if i.AutoCreateRepo {
				log.Printf("I! schema not match, updating...")
				err = i.updateSchema(pts)
			}
		}
		// Log write failure
	} else {
		if time.Now().Unix()%60 < 11 {
			err = i.updateExport(pts)
			if err != nil {
				fmt.Println(err)
			}
		}
		err = nil
	}

	return err
}

func getFieldType(val interface{}) string {

	switch val.(type) {
	case int, int16, int32, int64:
		return "long"
	case float32, float64:
		return "float"
	case string:
		return "string"
	case bool:
		return "boolean"
	default:
		return "string"
	}

	return ""
}

func extractSchemaFromPoints(points tsdb.Points) (tags []string, fields map[string]string) {

	tags = []string{}
	fields = make(map[string]string)

	for _, pt := range points {
		for _, val := range pt.Tags() {
			tags = append(tags, string(pt.Name())+"_"+string(val.Key))
		}
		fs, _ := pt.Fields()
		for key, val := range fs {
			fields[string(pt.Name())+"_"+string(key)] = getFieldType(val)
		}
	}
	return
}

//查看指定的export是否存在，如果不存在则创建；
//如果存在则更新
func (i *Pipeline) createOrUpdateExport(seriesName string, tags map[string]struct{}, fields map[string]struct{}) (err error) {

	err = i.tsdbClient.CreateSeries(&tsdbSdk.CreateSeriesInput{
		RepoName:   i.Repo,
		SeriesName: seriesName,
		Retention:  "7d",
	})
	if err != nil {
		if !strings.Contains(err.Error(), "E6302") {
			fmt.Printf("create series %s for repo %s fail %v", seriesName, i.Repo, err)
			err = nil
		}
	}

	exportTagSpec := make(map[string]string)
	for tag := range tags {
		exportTagSpec[tag] = fmt.Sprintf("#%s_%s", seriesName, tag)
	}

	exportFieldSpec := make(map[string]string)
	for filed := range fields {
		exportFieldSpec[filed] = fmt.Sprintf("#%s_%s", seriesName, filed)
	}

	err = i.client.CreateExport(&pipeline.CreateExportInput{
		RepoName:   i.Repo,
		ExportName: fmt.Sprintf("export_%s_toTSDB", seriesName),
		Type:       "tsdb",
		Whence:     "oldest",
		Spec: &pipeline.ExportTsdbSpec{
			DestRepoName: i.Repo,
			SeriesName:   seriesName,
			Timestamp:    "#timestamp",
			Tags:         exportTagSpec,
			Fields:       exportFieldSpec,
		},
	})
	if err != nil { //出错误了
		if strings.Contains(err.Error(), "E18301") { //已经存在
			//start to update
			err = i.client.UpdateExport(&pipeline.UpdateExportInput{ //开始update
				RepoName:   i.Repo,
				ExportName: fmt.Sprintf("export_%s_toTSDB", seriesName),
				Spec: &pipeline.ExportTsdbSpec{
					DestRepoName: i.Repo,
					SeriesName:   seriesName,
					Timestamp:    "#timestamp",
					Tags:         exportTagSpec,
					Fields:       exportFieldSpec,
				},
			})
			if err != nil {
				fmt.Println(err)
			}
		} else { //不是已经存在的错误，报错
			return err
		}
	}

	return
}

func (i *Pipeline) updateExport(points tsdb.Points) (err error) {

	measurements := make(map[string]struct {
		tags   map[string]struct{}
		fields map[string]struct{}
	})

	for _, pt := range points {
		ptName := string(pt.Name())
		if _, ok := measurements[ptName]; !ok {
			measurements[ptName] = struct {
				tags   map[string]struct{}
				fields map[string]struct{}
			}{
				tags:   make(map[string]struct{}),
				fields: make(map[string]struct{}),
			}
			// measurements[ptName].tags = make(map[string]struct{})
			// measurements[ptName].fields = make(map[string]struct{})
		}
		for _, tag := range pt.Tags() {
			measurements[ptName].tags[string(tag.Key)] = struct{}{}
		}
		fields, _ := pt.Fields()
		for field := range fields {
			measurements[ptName].fields[field] = struct{}{}
		}

	}
	for seriesName, value := range measurements {
		err = i.createOrUpdateExport(seriesName, value.tags, value.fields)
		if err != nil {
			fmt.Println(err)
		}
	}

	return

}

func (i *Pipeline) updateSchema(points tsdb.Points) error {
	tags, fields := extractSchemaFromPoints(points)

	schema, err := i.client.GetRepo(&pipeline.GetRepoInput{
		RepoName: i.Repo,
	})
	createRepo := false
	if err != nil {
		if strings.Contains(err.Error(), "E18102") {
			createRepo = true
		}
	}

	schemas := make(map[string]string)
	for _, schema := range schema.Schema {
		schemas[schema.Key] = schema.ValueType
	}

	//根据tags，fields更新schema
	for _, tag := range tags {
		if _, ok := schemas[tag]; !ok {
			schemas[tag] = "string"
		}
	}

	for field, valType := range fields {
		if _, ok := schemas[field]; !ok {
			schemas[field] = valType
		}
	}
	if _, ok := schemas["timestamp"]; !ok {
		schemas["timestamp"] = "long"
	}
	//剔除原来的字段
	for _, schema := range schema.Schema {
		delete(schemas, schema.Key)
	}

	target := make([]pipeline.RepoSchemaEntry, 0)
	for field, valType := range schemas {
		target = append(target, pipeline.RepoSchemaEntry{
			Required:  false,
			Key:       field,
			ValueType: valType,
		})
	}
	//log.Println("E! %v", target[])
	if createRepo {
		err = i.client.CreateRepo(&pipeline.CreateRepoInput{
			RepoName: i.Repo,
			Region:   "nb",
			Schema:   append(schema.Schema, target...),
		})
		if err != nil {
			fmt.Printf("create pipeline repo %s fail %v", i.Repo, err)
			return err
		}
		fmt.Printf("create pipeline repo %s success", i.Repo)

		err = i.tsdbClient.CreateRepo(&tsdbSdk.CreateRepoInput{
			RepoName: i.Repo,
			Region:   "nb",
		})
		if err != nil {
			err = fmt.Errorf("create tsdb repo %s fail, %v", i.Repo, err.Error())
		}
		fmt.Printf("create tsdb repo %s success", i.Repo)

		err = i.updateExport(points)
		if err != nil {
			fmt.Println(err)
		}

	} else {
		err = i.client.UpdateRepo(&pipeline.UpdateRepoInput{
			RepoName: i.Repo,
			Schema:   append(schema.Schema, target...),
		})

		err = i.updateExport(points)
		if err != nil {
			fmt.Println(err)
			return err
		}
	}

	return err
}
func newPipeline() *Pipeline {
	return &Pipeline{
		Timeout: internal.Duration{Duration: time.Second * 5},
	}
}

func init() {
	outputs.Add("pipeline", func() telegraf.Output { return newPipeline() })
}

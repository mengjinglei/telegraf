# PandoraTSDB Output Plugin

This plugin writes to [PandoraTSDB](https://github.com/qbox/pandora) via HTTP.

### Configuration:

```toml
# Configuration for PandoraTSDB server to send metrics to
[[outputs.pandora]]
  url = "http://localhost:8086" # required
  ## The target repo for metrics (telegraf will create it if not exists).
  repo = "telegraf" # required
  
  ## 是否自动创建series
  auto_create_series = false
  ## 自创创建的series的retention，支持的retention为[1-30]d
  retention_policy = "3d"
  ## Write timeout (for the PandoraTSDB client), formatted as a string.
  ## If not provided, will default to 5s. 0s means no timeout (not recommended).
  timeout = "5s"
  ak = "ACCESS_KEY"
  sk = "SECRET_KEY"

```

### Required parameters:

* `url`: List of strings, this is for PandoraTSDB clustering
* `repo`: The name of the repo to write to.
* `ak`: ACCESS_KEY
* `sk`: SECRET_KEY


### Optional parameters:

* `retention_policy`:  自创创建的series的retention，支持的retention为[1-30]d
* `timeout`: Write timeout (for the PandoraTSDB client), formatted as a string. If not provided, will default to 5s. 0s means no timeout (not recommended).
* `auto_create_series`: 是否自动创建series
# go-pq-cdc-kafka [![Go Reference](https://pkg.go.dev/badge/github.com/Trendyol/go-dcp.svg)](https://pkg.go.dev/github.com/Trendyol/go-pq-cdc-kafka) [![Go Report Card](https://goreportcard.com/badge/github.com/Trendyol/go-pq-cdc-kafka)](https://goreportcard.com/report/github.com/Trendyol/go-pq-cdc-kafka)

Kafka connector for [go-pq-cdc](https://github.com/Trendyol/go-pq-cdc).

go-pq-cdc-kafka streams documents from PostgreSql and writes to Kafka topic in near real-time.

### Contents

* [Usage](#usage)
* [Examples](#examples)
* [Availability](#availability)
* [Configuration](#configuration)
* [API](#api)
* [Exposed Metrics](#exposed-metrics)
* [Compatibility](#compatibility)
* [Breaking Changes](#breaking-changes)

### Usage

> ### ⚠️ For production usage check the [production tutorial](./docs/production_tutorial.md) doc

> ### ⚠️ For other usages check the dockerfile and code at [examples](./example).

```sh
go get github.com/Trendyol/go-pq-cdc-kafka
```

```go
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strconv"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc-kafka"
	"github.com/Trendyol/go-pq-cdc-kafka/config"
	cdcconfig "github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	gokafka "github.com/segmentio/kafka-go"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx := context.TODO()
	cfg := config.Connector{
		CDC: cdcconfig.Config{
			Host:      "127.0.0.1",
			Username:  "cdc_user",
			Password:  "cdc_pass",
			Database:  "cdc_db",
			DebugMode: false,
			Publication: publication.Config{
				CreateIfNotExists: true,
				Name:              "cdc_publication",
				Operations: publication.Operations{
					publication.OperationInsert,
					publication.OperationDelete,
					publication.OperationTruncate,
					publication.OperationUpdate,
				},
				Tables: publication.Tables{publication.Table{
					Name:            "users",
					ReplicaIdentity: publication.ReplicaIdentityFull,
				}},
			},
			Slot: slot.Config{
				CreateIfNotExists:           true,
				Name:                        "cdc_slot",
				SlotActivityCheckerInterval: 3000,
			},
			Metric: cdcconfig.MetricConfig{
				Port: 8081,
			},
			Logger: cdcconfig.LoggerConfig{
				LogLevel: slog.LevelInfo,
			},
		},
		Kafka: config.Kafka{
			TableTopicMapping:           map[string]string{"public.users": "users.0"},
			Brokers:                     []string{"localhost:19092"},
			AllowAutoTopicCreation:      true,
			ProducerBatchTickerDuration: time.Millisecond * 200,
		},
	}

	connector, err := cdc.NewConnector(ctx, cfg, Handler)
	if err != nil {
		slog.Error("new connector", "error", err)
		os.Exit(1)
	}

	defer connector.Close()
	connector.Start(ctx)
}

func Handler(msg *cdc.Message) []gokafka.Message {
	slog.Info("change captured", "message", msg)
	if msg.Type.IsUpdate() || msg.Type.IsInsert() {
		msg.NewData["operation"] = msg.Type
		newData, _ := json.Marshal(msg.NewData)

		return []gokafka.Message{
			{
				Headers: nil,
				Key:     []byte(strconv.Itoa(int(msg.NewData["id"].(int32)))),
				Value:   newData,
			},
		}
	}

	if msg.Type.IsDelete() {
		msg.OldData["operation"] = msg.Type
		oldData, _ := json.Marshal(msg.OldData)

		return []gokafka.Message{
			{
				Headers: nil,
				Key:     []byte(strconv.Itoa(int(msg.OldData["id"].(int32)))),
				Value:   oldData,
			},
		}
	}

	return []gokafka.Message{}
}
```

### Examples

* [Simple](./example/simple)

### Availability

The go-pq-cdc operates in passive/active modes for PostgreSQL change data capture (CDC). Here's how it ensures
availability:

* **Active Mode:** When the PostgreSQL replication slot (slot.name) is active, go-pq-cdc continuously monitors changes
  and streams them to downstream systems as configured.
* **Passive Mode:** If the PostgreSQL replication slot becomes inactive (detected via slot.slotActivityCheckerInterval),
  go-pq-cdc automatically captures the slot again and resumes data capturing. Other deployments also monitor slot
  activity,
  and when detected as inactive, they initiate data capturing.

This setup ensures continuous data synchronization and minimal downtime in capturing database changes.

### Configuration

| Variable                                    |       Type        | Required | Default | Description                                                                                                     | Options                                                                                                                                                                  |
| ------------------------------------------- | :---------------: | :------: | :-----: | --------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `cdc.host`                                  |      string       |   yes    |    -    | PostgreSQL host                                                                                                 | Should be a valid hostname or IP address. Example: `localhost`.                                                                                                          |
| `cdc.username`                              |      string       |   yes    |    -    | PostgreSQL username                                                                                             | Should have sufficient privileges to perform required database operations.                                                                                               |
| `cdc.password`                              |      string       |   yes    |    -    | PostgreSQL password                                                                                             | Keep secure and avoid hardcoding in the source code.                                                                                                                     |
| `cdc.database`                              |      string       |   yes    |    -    | PostgreSQL database                                                                                             | The database must exist and be accessible by the specified user.                                                                                                         |
| `cdc.debugMode`                             |       bool        |    no    |  false  | For debugging purposes                                                                                          | Enables pprof for trace.                                                                                                                                                 |
| `cdc.metric.port`                           |        int        |    no    |  8080   | Set API port                                                                                                    | Choose a port that is not in use by other applications.                                                                                                                  |
| `cdc.logger.logLevel`                       |      string       |    no    |  info   | Set logging level                                                                                               | [`DEBUG`, `WARN`, `INFO`, `ERROR`]                                                                                                                                       |
| `cdc.logger.logger`                         |      Logger       |    no    |  slog   | Set logger                                                                                                      | Can be customized with other logging frameworks if `slog` is not used.                                                                                                   |
| `cdc.publication.createIfNotExists`         |       bool        |    no    |    -    | Create publication if not exists. Otherwise, return `publication is not exists` error.                          |                                                                                                                                                                          |
| `cdc.publication.name`                      |      string       |   yes    |    -    | Set PostgreSQL publication name                                                                                 | Should be unique within the database.                                                                                                                                    |
| `cdc.publication.operations`                |     []string      |   yes    |    -    | Set PostgreSQL publication operations. List of operations to track; all or a subset can be specified.           | **INSERT:** Track insert operations. <br> **UPDATE:** Track update operations. <br> **DELETE:** Track delete operations.                                                 |
| `cdc.publication.tables`                    |      []Table      |   yes    |    -    | Set tables which are tracked by data change capture                                                             | Define multiple tables as needed.                                                                                                                                        |
| `cdc.publication.tables[i].name`            |      string       |   yes    |    -    | Set the data change captured table name                                                                         | Must be a valid table name in the specified database.                                                                                                                    |
| `cdc.publication.tables[i].replicaIdentity` |      string       |   yes    |    -    | Set the data change captured table replica identity [`FULL`, `DEFAULT`]                                         | **FULL:** Captures all columns of old row when a row is updated or deleted. <br> **DEFAULT:** Captures only the primary key of old row when a row is updated or deleted. |
| `cdc.slot.createIfNotExists`                |       bool        |    no    |    -    | Create replication slot if not exists. Otherwise, return `replication slot is not exists` error.                |                                                                                                                                                                          |
| `cdc.slot.name`                             |      string       |   yes    |    -    | Set the logical replication slot name                                                                           | Should be unique and descriptive.                                                                                                                                        |
| `cdc.slot.slotActivityCheckerInterval`      |        int        |   yes    |  1000   | Set the slot activity check interval time in milliseconds                                                       | Specify as an integer value in milliseconds (e.g., `1000` for 1 second).                                                                                                 |
| `kafka.tableTopicMapping`                   | map[string]string |   yes    |    -    | Mapping of PostgreSQL table events to Kafka topics                                                              | Maps table names to Kafka topics.                                                                                                                                        |
| `kafka.brokers`                             |     []string      |   yes    |    -    | Broker IP and port information                                                                                  |                                                                                                                                                                          |
| `kafka.producerBatchSize`                   |      integer      |    no    |  2000   | Maximum message count for batch, if exceeded, flush will be triggered.                                          |                                                                                                                                                                          |
| `kafka.producerBatchBytes`                  |      string       |    no    |  10mb   | Maximum size (bytes) for batch. If exceeded, flush will be triggered.                                           |                                                                                                                                                                          |
| `kafka.producerMaxAttempts`                 |        int        |    no    | maxInt  | Limit on how many attempts will be made to deliver a message.                                                   |                                                                                                                                                                          |
| `kafka.producerBatchTickerDuration`         |   time.Duration   |    no    |   10s   | Batch is flushed automatically at specific time intervals for long-waiting messages in the batch.               |                                                                                                                                                                          |
| `kafka.readTimeout`                         |   time.Duration   |    no    |   30s   | Timeout for read operations in `segmentio/kafka-go`.                                                            |                                                                                                                                                                          |
| `kafka.writeTimeout`                        |   time.Duration   |    no    |   30s   | Timeout for write operations in `segmentio/kafka-go`.                                                           |                                                                                                                                                                          |
| `kafka.compression`                         |      integer      |    no    |    0    | Compression can be used if message size is large, CPU usage may be affected.                                    | **0:** None <br> **1:** Gzip <br> **2:** Snappy <br> **3:** Lz4 <br> **4:** Zstd                                                                                         |
| `kafka.balancer`                            |      string       |    no    |  Hash   | Define balancer strategy.                                                                                       | [`Hash`, `LeastBytes`, `RoundRobin`, `ReferenceHash`, `CRC32Balancer`, `Murmur2Balancer`]                                                                                |
| `kafka.requiredAcks`                        |      integer      |    no    |    1    | Number of acknowledgments from partition replicas required before receiving a response.                         | **0:** fire-and-forget <br> **1:** wait for leader to acknowledge writes <br> **-1:** wait for full ISR                                                 |
| `kafka.secureConnection`                    |       bool        |    no    |  false  | Enable secure Kafka connection.                                                                                 |                                                                                                                                                                          |
| `kafka.rootCA`                              |      []byte       |    no    |    -    | Define root CA certificate.                                                                                     |                                                                                                                                                                          |
| `kafka.interCA`                             |      []byte       |    no    |    -    | Define intermediate CA certificate.                                                                             |                                                                                                                                                                          |
| `kafka.scramUsername`                       |      string       |    no    |    -    | Define SCRAM username.                                                                                          |                                                                                                                                                                          |
| `kafka.scramPassword`                       |      string       |    no    |    -    | Define SCRAM password.                                                                                          |                                                                                                                                                                          |
| `kafka.metadataTTL`                         |   time.Duration   |    no    |   60s   | TTL for the metadata cached by `segmentio/kafka-go`. Increase it to reduce network requests.                    | For more detail, check [docs](https://pkg.go.dev/github.com/segmentio/kafka-go#Transport.MetadataTTL).                                                                   |
| `kafka.metadataTopics`                      |     []string      |    no    |    -    | Topic names for the metadata cached by `segmentio/kafka-go`. Define topics here that the connector may produce. | For more detail, check [docs](https://pkg.go.dev/github.com/segmentio/kafka-go#Transport.MetadataTopics).                                                                |
| `kafka.clientID`                            |      string       |    no    |    -    | Unique identifier that the transport communicates to the brokers.                                               | For more detail, check [docs](https://pkg.go.dev/github.com/segmentio/kafka-go#Transport.ClientID).                                                                      |
| `kafka.allowAutoTopicCreation`              |       bool        |    no    |  false  | Create topic if missing.                                                                                        | For more detail, check [docs](https://pkg.go.dev/github.com/segmentio/kafka-go#Writer.AllowAutoTopicCreation).                                                           |

| Endpoint             | Description                                                                               |
|----------------------|-------------------------------------------------------------------------------------------|
| `GET /status`        | Returns a 200 OK status if the client is able to ping the PostgreSQL server successfully. |
| `GET /metrics`       | Prometheus metric endpoint.                                                               |
| `GET /debug/pprof/*` | (Only for `debugMode=true`) [pprof](https://pkg.go.dev/net/http/pprof)                    |

### Exposed Metrics

The client collects relevant metrics related to PostgreSQL change data capture (CDC) and makes them available at
the `/metrics` endpoint.

| Metric Name                                                  | Description                                                                                           | Labels          | Value Type |
|--------------------------------------------------------------|-------------------------------------------------------------------------------------------------------|-----------------|------------|
| go_pq_cdc_kafka_process_latency_current              | The latest kafka connector process latency in nanoseconds.                                    | slot_name, host | Gauge      |
| go_pq_cdc_kafka_bulk_request_process_latency_current | The latest kafka connector bulk request process latency in nanoseconds.                       | slot_name, host | Gauge      |

You can also use all cdc related metrics explained [here](https://github.com/Trendyol/go-pq-cdc#exposed-metrics). 
All cdc related metrics are automatically injected. It means you don't need to do anything.

### Compatibility

| go-pq-cdc-kafka Version | Minimum PostgreSQL Server Version |
|-------------------|-----------------------------------|
| 0.0.0 or higher   | 14                                |

### Breaking Changes

| Date taking effect | Version | Change | How to check |
|--------------------|---------|--------|--------------| 
| -                  | -       | -      | -            |


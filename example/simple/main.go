package simple

import (
	"context"
	"encoding/json"
	cdc "github.com/Trendyol/go-dcp-cdc-kafka"
	"github.com/Trendyol/go-dcp-cdc-kafka/config"
	"github.com/Trendyol/go-dcp-cdc-kafka/kafka"
	cdcconfig "github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"log/slog"
	"os"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx := context.TODO()
	cfg := config.Connector{
		CDC: cdcconfig.Config{
			Host:      "127.0.0.1",
			Username:  "es_cdc_user",
			Password:  "es_cdc_pass",
			Database:  "es_cdc_db",
			DebugMode: false,
			Publication: publication.Config{
				CreateIfNotExists: true,
				Name:              "es_cdc_publication",
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
				Name:                        "es_cdc_slot",
				SlotActivityCheckerInterval: 3000,
			},
			Metric: cdcconfig.MetricConfig{
				Port: 8081,
			},
		},
		Kafka: config.Kafka{
			CollectionTopicMapping: map[string]string{"_default": "topic"},
			Brokers:                []string{"localhost:9092"},
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

func Handler(msg *cdc.Message) []kafka.Event {
	if msg.Type.IsUpdate() || msg.Type.IsInsert() {
		msg.NewData["operation"] = msg.Type
		newData, _ := json.Marshal(msg.NewData)

		return []kafka.Event{
			{
				Headers: nil,
				Key:     []byte(msg.NewData["id"].(string)),
				Value:   newData,
			},
		}
	}

	if msg.Type.IsDelete() {
		msg.OldData["operation"] = msg.Type
		oldData, _ := json.Marshal(msg.OldData)

		return []kafka.Event{
			{
				Headers: nil,
				Key:     []byte(msg.OldData["id"].(string)),
				Value:   oldData,
			},
		}
	}

	return []kafka.Event{}
}

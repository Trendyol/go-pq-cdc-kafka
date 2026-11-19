package cdc

import (
	"errors"
	"fmt"
	"strings"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc-kafka/config"
	"github.com/Trendyol/go-pq-cdc-kafka/internal/bytes"
	cdcconfig "github.com/Trendyol/go-pq-cdc/config"
)

// ErrOnlineAdmission reports a configuration that cannot certify a synchronous
// broker acknowledgement. Admission runs before any connection is opened, so a
// refusal is always a local, explicit fault rather than a startup surprise.
var ErrOnlineAdmission = errors.New("online kafka admission")

// requireAllAcks is the only ack mode the online path accepts: the leader plus
// its in-sync replicas. kafka.RequireAll has the value -1.
const requireAllAcks = -1

// maxOnlineAttempts bounds internal write retries. The legacy batch path retries
// for as long as the process lives, which cannot bound a synchronous handoff.
const maxOnlineAttempts = 10

// onlineAdmission is the admitted, owned settings of one online connector.
type onlineAdmission struct {
	topics         map[string]string
	closeTimeout   time.Duration
	maxBytes       int64
	maxAttempts    int
	maxRecords     int
	maxOutputBytes int64
}

// keyPreservingBalancer reports whether one Kafka key always maps to one
// partition. LeastBytes and RoundRobin spread the versions of the same row
// across partitions, which breaks per-row ordering.
func keyPreservingBalancer(name string) bool {
	switch name {
	case "", "Hash", "ReferenceHash", "CRC32Balancer", "Murmur2Balancer":
		return true
	}
	return false
}

// newOnlineAdmission validates the effective configuration of the online path.
// cfg must already carry defaults.
func newOnlineAdmission(cfg *config.Connector) (onlineAdmission, error) {
	if cfg.CDC.Snapshot.Strategy != cdcconfig.SnapshotStrategyOnline {
		return onlineAdmission{}, fmt.Errorf("%w: the online connector requires snapshot.strategy: online", ErrOnlineAdmission)
	}
	if cfg.CDC.IsSnapshotOnlyMode() {
		return onlineAdmission{}, fmt.Errorf("%w: snapshot.mode: snapshot_only has no change stream to deliver", ErrOnlineAdmission)
	}

	var err error
	reject := func(condition bool, format string, args ...any) {
		if condition {
			err = errors.Join(err, fmt.Errorf("%w: %s", ErrOnlineAdmission, fmt.Sprintf(format, args...)))
		}
	}

	reject(cfg.Kafka.RequiredAcks != requireAllAcks, "requiredAcks must be -1 (acks=all), got %d", cfg.Kafka.RequiredAcks)
	reject(cfg.Kafka.SkipOversizedMessages, "skipOversizedMessages is unsupported online: an oversized envelope must stop delivery instead of being dropped")
	reject(cfg.Kafka.AllowAutoTopicCreation, "allowAutoTopicCreation is unsupported online: provision destination topics with the intended partition count")
	reject(cfg.Kafka.Online.MaxAttempts < 1 || cfg.Kafka.Online.MaxAttempts > maxOnlineAttempts, "online.maxAttempts must be between 1 and %d, got %d", maxOnlineAttempts, cfg.Kafka.Online.MaxAttempts)
	reject(cfg.Kafka.Online.CloseTimeout <= 0, "online.closeTimeout must be greater than 0")
	reject(cfg.Kafka.WriteTimeout <= 0, "writeTimeout must be greater than 0")
	reject(cfg.Kafka.ReadTimeout <= 0, "readTimeout must be greater than 0")
	reject(cfg.Kafka.Compression < 0 || cfg.Kafka.Compression > 4, "compression must be between 0 and 4, got %d", cfg.Kafka.Compression)
	reject(len(cfg.Kafka.Brokers) == 0, "brokers must list at least one broker")
	for i, broker := range cfg.Kafka.Brokers {
		reject(strings.TrimSpace(broker) == "", "brokers[%d] is empty", i)
	}
	if balancerErr := cfg.Kafka.ValidateBalancer(); balancerErr != nil {
		err = errors.Join(err, fmt.Errorf("%w: %w", ErrOnlineAdmission, balancerErr))
	} else {
		reject(!keyPreservingBalancer(cfg.Kafka.Balancer), "balancer %q does not keep one key on one partition; use Hash, ReferenceHash, CRC32Balancer or Murmur2Balancer", cfg.Kafka.Balancer)
	}

	topics, topicErr := onlineTableTopics(cfg)
	err = errors.Join(err, topicErr)

	maxBytes, sizeErr := bytes.ParseSize(cfg.Kafka.MaxMessageBytes)
	reject(maxBytes == 0, "maxMessageBytes must be greater than 0")
	reject(cfg.Kafka.ProducerBatchSize <= 0, "producerBatchSize must be greater than 0 to bound Handler fanout")
	outputBytes, outputErr := bytes.ParseSize(cfg.Kafka.ProducerBatchBytes)
	reject(outputBytes == 0, "producerBatchBytes must be greater than 0 to bound Handler fanout")
	err = errors.Join(err, outputErr)
	err = errors.Join(err, sizeErr)
	if err != nil {
		return onlineAdmission{}, err
	}

	return onlineAdmission{
		topics:         topics,
		maxBytes:       int64(maxBytes),
		maxAttempts:    cfg.Kafka.Online.MaxAttempts,
		closeTimeout:   cfg.Kafka.Online.CloseTimeout,
		maxRecords:     cfg.Kafka.ProducerBatchSize,
		maxOutputBytes: int64(outputBytes),
	}, nil
}

// onlineTableTopics owns a copy of the configured destinations and refuses a
// publication/snapshot table that has no topic. An existing publication whose
// tables are not listed in configuration cannot be checked here; such a table
// then fails closed on its first event instead of being dropped.
func onlineTableTopics(cfg *config.Connector) (map[string]string, error) {
	if len(cfg.Kafka.TableTopicMapping) == 0 {
		return nil, fmt.Errorf("%w: kafka.tableTopicMapping must map every captured table to a topic", ErrOnlineAdmission)
	}
	topics := make(map[string]string, len(cfg.Kafka.TableTopicMapping))
	var err error
	for table, topic := range cfg.Kafka.TableTopicMapping {
		if strings.TrimSpace(table) == "" || strings.TrimSpace(topic) == "" {
			err = errors.Join(err, fmt.Errorf("%w: kafka.tableTopicMapping entry %q -> %q must name both a table and a topic", ErrOnlineAdmission, table, topic))
			continue
		}
		topics[table] = topic
	}
	for _, table := range onlineCapturedTables(cfg) {
		name := table.Schema + "." + table.Name
		if _, ok := topics[name]; !ok {
			err = errors.Join(err, fmt.Errorf("%w: kafka.tableTopicMapping has no topic for %s", ErrOnlineAdmission, name))
		}
	}
	if err != nil {
		return nil, err
	}
	return topics, nil
}

// onlineCapturedTables is the configured capture set. Core defaults schema
// names, so this runs after cfg.SetDefault().
func onlineCapturedTables(cfg *config.Connector) []cdc.OnlineTable {
	tables := make([]cdc.OnlineTable, 0, len(cfg.CDC.Publication.Tables)+len(cfg.CDC.Snapshot.Tables))
	for _, table := range cfg.CDC.Publication.Tables {
		tables = append(tables, cdc.OnlineTable{Schema: table.Schema, Name: table.Name})
	}
	for _, table := range cfg.CDC.Snapshot.Tables {
		tables = append(tables, cdc.OnlineTable{Schema: table.Schema, Name: table.Name})
	}
	return tables
}

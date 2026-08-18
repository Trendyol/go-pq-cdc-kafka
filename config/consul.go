package config

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	cdccfg "github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
)

// ConsulConfig holds deployment-specific overrides supplied via a JSON file
// (typically rendered from Consul / TBP).
//
// YAML at resources/config.yml carries process-level defaults; this JSON
// overlays per-deployment connection details and secrets.
type ConsulConfig struct {
	TableKeyMapping              map[string]string `json:"tableKeyMapping,omitempty"`
	TableTopicMapping            map[string]string `json:"tableTopicMapping"`
	KafkaSecureConnection        *bool             `json:"kafkaSecureConnection,omitempty"`
	KafkaAllowAutoTopicCreate    *bool             `json:"kafkaAllowAutoTopicCreation,omitempty"`
	SnapshotEnabled              *bool             `json:"snapshotEnabled,omitempty"`
	PublicationCreateIfNotExists *bool             `json:"publicationCreateIfNotExists,omitempty"`
	SlotCreateIfNotExists        *bool             `json:"slotCreateIfNotExists,omitempty"`
	PostgresDatabase             string            `json:"postgresDatabase"`
	PostgresUsername             string            `json:"postgresUsername"`
	KeyField                     string            `json:"keyField,omitempty"`
	PublicationName              string            `json:"publicationName"`
	SnapshotMode                 string            `json:"snapshotMode,omitempty"`
	PostgresTbpSecretPath        string            `json:"postgresTbpSecretPath"`
	SlotName                     string            `json:"slotName"`
	KafkaScramUsername           string            `json:"kafkaScramUsername"`
	KafkaScramPassword           string            `json:"kafkaScramPassword"`
	KafkaTbpSecretPath           string            `json:"kafkaTbpSecretPath"`
	KafkaRootCAPath              string            `json:"kafkaRootCAPath"`
	KafkaInterCAPath             string            `json:"kafkaInterCAPath"`
	KafkaClientID                string            `json:"kafkaClientID"`
	PostgresHost                 string            `json:"postgresHost"`
	PostgresPassword             string            `json:"postgresPassword"`
	PublicationOperations        []string          `json:"publicationOperations,omitempty"`
	KafkaBrokers                 []string          `json:"kafkaBrokers"`
	Tables                       []ConsulTable     `json:"tables,omitempty"`
	KafkaProducerBatchSize       int               `json:"kafkaProducerBatchSize,omitempty"`
	KafkaRequiredAcks            int               `json:"kafkaRequiredAcks,omitempty"`
	SnapshotChunkSize            int64             `json:"snapshotChunkSize,omitempty"`
	SlotActivityCheckerInterval  int               `json:"slotActivityCheckerInterval,omitempty"`
	PostgresPort                 int               `json:"postgresPort,omitempty"`
	KafkaCompression             int8              `json:"kafkaCompression,omitempty"`
}

type ConsulTable struct {
	Name            string `json:"name"`
	Schema          string `json:"schema,omitempty"`
	ReplicaIdentity string `json:"replicaIdentity"`
	Partitioned     bool   `json:"partitioned,omitempty"`
}

type TbpSecret struct {
	Username *string `json:"username"`
	Password *string `json:"password"`
}

func LoadConsulConfig(path string) (*ConsulConfig, error) {
	file, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read consul config: %w", err)
	}
	var c ConsulConfig
	if err := json.Unmarshal(file, &c); err != nil {
		return nil, fmt.Errorf("unmarshal consul config: %w", err)
	}
	return &c, nil
}

func LoadTbpSecret(path string) (*TbpSecret, error) {
	if path == "" {
		return nil, nil
	}
	file, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read tbp secret %s: %w", path, err)
	}
	var s TbpSecret
	if err := json.Unmarshal(file, &s); err != nil {
		return nil, fmt.Errorf("unmarshal tbp secret %s: %w", path, err)
	}
	return &s, nil
}

func (c *Connector) applyConsulOverrides(cc *ConsulConfig) error {
	if cc == nil {
		return nil
	}
	if err := c.applyPostgresOverrides(cc); err != nil {
		return err
	}
	c.applyPublicationOverrides(cc)
	c.applySlotSnapshotOverrides(cc)
	if err := c.applyKafkaOverrides(cc); err != nil {
		return err
	}
	c.applyMapperOverrides(cc)
	return nil
}

func (c *Connector) applyPostgresOverrides(cc *ConsulConfig) error {
	secret, err := LoadTbpSecret(cc.PostgresTbpSecretPath)
	if err != nil {
		return err
	}
	if secret != nil && secret.Username != nil && secret.Password != nil {
		c.CDC.Username = *secret.Username
		c.CDC.Password = *secret.Password
	} else {
		if cc.PostgresUsername != "" {
			c.CDC.Username = cc.PostgresUsername
		}
		if cc.PostgresPassword != "" {
			c.CDC.Password = cc.PostgresPassword
		}
	}
	if cc.PostgresHost != "" {
		c.CDC.Host = cc.PostgresHost
	}
	if cc.PostgresPort > 0 {
		c.CDC.Port = cc.PostgresPort
	}
	if cc.PostgresDatabase != "" {
		c.CDC.Database = cc.PostgresDatabase
	}
	return nil
}

func (c *Connector) applyPublicationOverrides(cc *ConsulConfig) {
	if cc.PublicationName != "" {
		c.CDC.Publication.Name = cc.PublicationName
	}
	if cc.PublicationCreateIfNotExists != nil {
		c.CDC.Publication.CreateIfNotExists = *cc.PublicationCreateIfNotExists
	}
	if len(cc.PublicationOperations) > 0 {
		ops := make(publication.Operations, 0, len(cc.PublicationOperations))
		for _, op := range cc.PublicationOperations {
			ops = append(ops, publication.Operation(op))
		}
		c.CDC.Publication.Operations = ops
	}
	if len(cc.Tables) > 0 {
		tables := make(publication.Tables, 0, len(cc.Tables))
		for _, t := range cc.Tables {
			tables = append(tables, publication.Table{
				Name:            t.Name,
				Schema:          t.Schema,
				ReplicaIdentity: t.ReplicaIdentity,
				Partitioned:     t.Partitioned,
			})
		}
		c.CDC.Publication.Tables = tables
	}
}

func (c *Connector) applySlotSnapshotOverrides(cc *ConsulConfig) {
	if cc.SlotName != "" {
		c.CDC.Slot.Name = cc.SlotName
	}
	if cc.SlotCreateIfNotExists != nil {
		c.CDC.Slot.CreateIfNotExists = *cc.SlotCreateIfNotExists
	}
	if cc.SlotActivityCheckerInterval > 0 {
		c.CDC.Slot.SlotActivityCheckerInterval = time.Duration(cc.SlotActivityCheckerInterval)
	}
	if cc.SnapshotEnabled != nil {
		c.CDC.Snapshot.Enabled = *cc.SnapshotEnabled
	}
	if cc.SnapshotMode != "" {
		c.CDC.Snapshot.Mode = cdccfg.SnapshotMode(cc.SnapshotMode)
	}
	if cc.SnapshotChunkSize > 0 {
		c.CDC.Snapshot.ChunkSize = cc.SnapshotChunkSize
	}
}

func (c *Connector) applyKafkaOverrides(cc *ConsulConfig) error {
	if len(cc.KafkaBrokers) > 0 {
		c.Kafka.Brokers = cc.KafkaBrokers
	}
	if cc.KafkaClientID != "" {
		c.Kafka.ClientID = cc.KafkaClientID
	}
	if len(cc.TableTopicMapping) > 0 {
		c.Kafka.TableTopicMapping = cc.TableTopicMapping
	}
	if cc.KafkaProducerBatchSize > 0 {
		c.Kafka.ProducerBatchSize = cc.KafkaProducerBatchSize
	}
	if cc.KafkaRequiredAcks != 0 {
		c.Kafka.RequiredAcks = cc.KafkaRequiredAcks
	}
	if cc.KafkaCompression != 0 {
		c.Kafka.Compression = cc.KafkaCompression
	}
	if cc.KafkaSecureConnection != nil {
		c.Kafka.SecureConnection = *cc.KafkaSecureConnection
	}
	if cc.KafkaAllowAutoTopicCreate != nil {
		c.Kafka.AllowAutoTopicCreation = *cc.KafkaAllowAutoTopicCreate
	}
	if cc.KafkaRootCAPath != "" {
		c.Kafka.RootCAPath = cc.KafkaRootCAPath
	}
	if cc.KafkaInterCAPath != "" {
		c.Kafka.InterCAPath = cc.KafkaInterCAPath
	}

	secret, err := LoadTbpSecret(cc.KafkaTbpSecretPath)
	if err != nil {
		return err
	}
	if secret != nil && secret.Username != nil && secret.Password != nil {
		c.Kafka.ScramUsername = *secret.Username
		c.Kafka.ScramPassword = *secret.Password
	} else {
		if cc.KafkaScramUsername != "" {
			c.Kafka.ScramUsername = cc.KafkaScramUsername
		}
		if cc.KafkaScramPassword != "" {
			c.Kafka.ScramPassword = cc.KafkaScramPassword
		}
	}
	return nil
}

func (c *Connector) applyMapperOverrides(cc *ConsulConfig) {
	if cc.KeyField != "" {
		c.Mapper.KeyField = cc.KeyField
	}
	if len(cc.TableKeyMapping) > 0 {
		c.Mapper.TableKeyMapping = cc.TableKeyMapping
	}
}

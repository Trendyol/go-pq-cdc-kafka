package config

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	cdccfg "github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/publication"
)

type ConsulConfig struct {
	TableKeyMapping              map[string]string `json:"tableKeyMapping,omitempty"`
	TableTopicMapping            map[string]string `json:"tableTopicMapping"`
	KafkaSecureConnection        *bool             `json:"kafkaSecureConnection,omitempty"`
	KafkaAllowAutoTopicCreate    *bool             `json:"kafkaAllowAutoTopicCreation,omitempty"`
	SnapshotEnabled              *bool             `json:"snapshotEnabled,omitempty"`
	PublicationCreateIfNotExists *bool             `json:"publicationCreateIfNotExists,omitempty"`
	SlotCreateIfNotExists        *bool             `json:"slotCreateIfNotExists,omitempty"`
	KafkaRequiredAcks            *int              `json:"kafkaRequiredAcks,omitempty"`
	KafkaCompression             *int8             `json:"kafkaCompression,omitempty"`
	PostgresDatabase             string            `json:"postgresDatabase"`
	PostgresUsername             string            `json:"postgresUsername"`
	KeyField                     string            `json:"keyField,omitempty"`
	PublicationName              string            `json:"publicationName"`
	SnapshotMode                 string            `json:"snapshotMode,omitempty"`
	PostgresSecretPath           string            `json:"postgresSecretPath"`
	SlotName                     string            `json:"slotName"`
	KafkaScramUsername           string            `json:"kafkaScramUsername"`
	KafkaScramPassword           string            `json:"kafkaScramPassword"`
	KafkaSecretPath              string            `json:"kafkaSecretPath"`
	KafkaRootCAPath              string            `json:"kafkaRootCAPath"`
	KafkaInterCAPath             string            `json:"kafkaInterCAPath"`
	KafkaClientID                string            `json:"kafkaClientID"`
	PostgresHost                 string            `json:"postgresHost"`
	PostgresPassword             string            `json:"postgresPassword"`
	PublicationOperations        []string          `json:"publicationOperations,omitempty"`
	KafkaBrokers                 []string          `json:"kafkaBrokers"`
	Tables                       []ConsulTable     `json:"tables,omitempty"`
	KafkaProducerBatchSize       int               `json:"kafkaProducerBatchSize,omitempty"`
	SnapshotChunkSize            int64             `json:"snapshotChunkSize,omitempty"`
	SlotActivityCheckerInterval  int               `json:"slotActivityCheckerInterval,omitempty"`
	PostgresPort                 int               `json:"postgresPort,omitempty"`
}

type ConsulTable struct {
	Name            string `json:"name"`
	Schema          string `json:"schema,omitempty"`
	ReplicaIdentity string `json:"replicaIdentity"`
	Partitioned     bool   `json:"partitioned,omitempty"`
}

type FileSecret struct {
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

func LoadFileSecret(path string) (*FileSecret, error) {
	if path == "" {
		return nil, nil
	}
	file, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read secret %s: %w", path, err)
	}
	var s FileSecret
	if err := json.Unmarshal(file, &s); err != nil {
		return nil, fmt.Errorf("unmarshal secret %s: %w", path, err)
	}
	if s.Username == nil || *s.Username == "" || s.Password == nil || *s.Password == "" {
		return nil, fmt.Errorf("secret %s: username and password are required", path)
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
	if err := c.applyPublicationOverrides(cc); err != nil {
		return err
	}
	c.applySlotSnapshotOverrides(cc)
	if err := c.applyKafkaOverrides(cc); err != nil {
		return err
	}
	c.applyMapperOverrides(cc)
	return nil
}

func (c *Connector) applyPostgresOverrides(cc *ConsulConfig) error {
	secret, err := LoadFileSecret(cc.PostgresSecretPath)
	if err != nil {
		return err
	}
	if secret != nil {
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

	host, port, err := splitPostgresHostPort(cc.PostgresHost, cc.PostgresPort)
	if err != nil {
		return err
	}
	if host != "" {
		c.CDC.Host = host
	}
	if port > 0 {
		c.CDC.Port = port
	}
	if cc.PostgresDatabase != "" {
		c.CDC.Database = cc.PostgresDatabase
	}
	return nil
}

func splitPostgresHostPort(host string, port int) (string, int, error) {
	if host == "" {
		return host, port, nil
	}
	h, pStr, err := net.SplitHostPort(host)
	if err != nil {
		return host, port, nil
	}
	p, err := strconv.Atoi(pStr)
	if err != nil {
		return "", 0, fmt.Errorf("postgresHost port: %w", err)
	}
	if port > 0 && port != p {
		return "", 0, fmt.Errorf("postgresHost includes port %d but postgresPort is %d", p, port)
	}
	return h, p, nil
}

func (c *Connector) applyPublicationOverrides(cc *ConsulConfig) error {
	if cc.PublicationName != "" {
		c.CDC.Publication.Name = cc.PublicationName
	}
	if cc.PublicationCreateIfNotExists != nil {
		c.CDC.Publication.CreateIfNotExists = *cc.PublicationCreateIfNotExists
	}
	if len(cc.PublicationOperations) > 0 {
		ops := make(publication.Operations, 0, len(cc.PublicationOperations))
		for _, raw := range cc.PublicationOperations {
			op := publication.Operation(raw)
			if err := op.Validate(); err != nil {
				return fmt.Errorf("invalid publication operation %q: %w", raw, err)
			}
			ops = append(ops, op)
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
	return nil
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
	if cc.KafkaRequiredAcks != nil {
		c.Kafka.RequiredAcks = *cc.KafkaRequiredAcks
	}
	if cc.KafkaCompression != nil {
		c.Kafka.Compression = *cc.KafkaCompression
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

	secret, err := LoadFileSecret(cc.KafkaSecretPath)
	if err != nil {
		return err
	}
	if secret != nil {
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

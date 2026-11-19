package config

import (
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/segmentio/kafka-go"
)

type Kafka struct {
	ProducerBatchBytes          string            `yaml:"producerBatchBytes"`
	TableTopicMapping           map[string]string `yaml:"tableTopicMapping"`
	InterCA                     []byte            `yaml:"interCA"`
	ScramUsername               string            `yaml:"scramUsername"`
	ScramPassword               string            `yaml:"scramPassword"`
	RootCA                      []byte            `yaml:"rootCA"`
	ClientID                    string            `yaml:"clientID"`
	Balancer                    string            `yaml:"balancer"`
	Brokers                     []string          `yaml:"brokers"`
	MetadataTopics              []string          `yaml:"metadataTopics"`
	Online                      OnlineKafka       `yaml:"online" mapstructure:"online"`
	ProducerMaxAttempts         int               `yaml:"producerMaxAttempts"`
	ReadTimeout                 time.Duration     `yaml:"readTimeout"`
	WriteTimeout                time.Duration     `yaml:"writeTimeout"`
	RequiredAcks                int               `yaml:"requiredAcks"`
	ProducerBatchSize           int               `yaml:"producerBatchSize"`
	MetadataTTL                 time.Duration     `yaml:"metadataTTL"`
	ProducerBatchTickerDuration time.Duration     `yaml:"producerBatchTickerDuration"`
	Compression                 int8              `yaml:"compression"`
	SecureConnection            bool              `yaml:"secureConnection"`
	AllowAutoTopicCreation      bool              `yaml:"allowAutoTopicCreation"`
	SkipOversizedMessages       bool              `yaml:"skipOversizedMessages"`
	MaxMessageBytes             string            `yaml:"maxMessageBytes"`
}

// OnlineKafka bounds the additive synchronous online write path. It never
// applies to legacy batching, whose defaults and fields stay unchanged.
type OnlineKafka struct {
	MaxAttempts  int           `yaml:"maxAttempts" mapstructure:"maxAttempts"`
	CloseTimeout time.Duration `yaml:"closeTimeout" mapstructure:"closeTimeout"`
}

// onlineKafkaDefaultMaxAttempts keeps retries finite: the legacy batch path
// retries forever by design, which cannot bound a synchronous acknowledgement.
const onlineKafkaDefaultMaxAttempts = 3

const onlineKafkaDefaultCloseTimeout = 5 * time.Second

func (o *OnlineKafka) setDefault() {
	if o.MaxAttempts == 0 {
		o.MaxAttempts = onlineKafkaDefaultMaxAttempts
	}
	if o.CloseTimeout == 0 {
		o.CloseTimeout = onlineKafkaDefaultCloseTimeout
	}
}

type Connector struct {
	CDC   config.Config
	Kafka Kafka `yaml:"kafka" mapstructure:"kafka"`
}

func (k *Kafka) GetBalancer() kafka.Balancer {
	switch k.Balancer {
	case "", "Hash":
		return &kafka.Hash{}
	case "LeastBytes":
		return &kafka.LeastBytes{}
	case "RoundRobin":
		return &kafka.RoundRobin{}
	case "ReferenceHash":
		return &kafka.ReferenceHash{}
	case "CRC32Balancer":
		return kafka.CRC32Balancer{}
	case "Murmur2Balancer":
		return kafka.Murmur2Balancer{}
	default:
		panic("invalid kafka balancer method, given: " + k.Balancer)
	}
}

// ValidateBalancer rejects an unknown name before GetBalancer panics.
func (k *Kafka) ValidateBalancer() error {
	switch k.Balancer {
	case "", "Hash", "LeastBytes", "RoundRobin", "ReferenceHash", "CRC32Balancer", "Murmur2Balancer":
		return nil
	}
	return fmt.Errorf("invalid kafka balancer method, given: %q", k.Balancer)
}

func (k *Kafka) GetCompression() int8 {
	if k.Compression < 0 || k.Compression > 4 {
		panic("invalid kafka compression method, given: " + strconv.Itoa(int(k.Compression)))
	}
	return k.Compression
}

func (c *Connector) SetDefault() {
	if c.Kafka.ReadTimeout == 0 {
		c.Kafka.ReadTimeout = 30 * time.Second
	}

	if c.Kafka.WriteTimeout == 0 {
		c.Kafka.WriteTimeout = 30 * time.Second
	}

	if c.Kafka.ProducerBatchTickerDuration == 0 {
		c.Kafka.ProducerBatchTickerDuration = 10 * time.Second
	}

	if c.Kafka.ProducerBatchSize == 0 {
		c.Kafka.ProducerBatchSize = 2000
	}

	if c.Kafka.ProducerBatchBytes == "" {
		c.Kafka.ProducerBatchBytes = "1mb"
	}

	if c.Kafka.RequiredAcks == 0 {
		c.Kafka.RequiredAcks = 1
	}

	if c.Kafka.MetadataTTL == 0 {
		c.Kafka.MetadataTTL = 60 * time.Second
	}

	if c.Kafka.ProducerMaxAttempts == 0 {
		c.Kafka.ProducerMaxAttempts = math.MaxInt
	}

	if c.Kafka.MaxMessageBytes == "" {
		c.Kafka.MaxMessageBytes = "1mb"
	}

	c.Kafka.Online.setDefault()
}

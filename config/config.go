package config

import (
	"fmt"
	"math"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Trendyol/go-pq-cdc/config"
	"github.com/segmentio/kafka-go"
	"gopkg.in/yaml.v3"
)

// ConsulConfigPathEnv is the env var for the deployment JSON path.
// If unset, defaults to "config/config.json". A missing file means no overlay.
const ConsulConfigPathEnv = "CONFIG_PATH"

var envPattern = regexp.MustCompile(`\$\{([^}]+)\}`)

type Kafka struct {
	ProducerBatchBytes          string            `yaml:"producerBatchBytes"`
	TableTopicMapping           map[string]string `yaml:"tableTopicMapping"`
	InterCA                     []byte            `yaml:"interCA"`
	ScramUsername               string            `yaml:"scramUsername"`
	ScramPassword               string            `yaml:"scramPassword"`
	RootCA                      []byte            `yaml:"rootCA"`
	RootCAPath                  string            `yaml:"rootCAPath"`
	InterCAPath                 string            `yaml:"interCAPath"`
	ClientID                    string            `yaml:"clientID"`
	Balancer                    string            `yaml:"balancer"`
	Brokers                     []string          `yaml:"brokers"`
	MetadataTopics              []string          `yaml:"metadataTopics"`
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

type MapperConfig struct {
	TableKeyMapping map[string]string `yaml:"tableKeyMapping"`
	KeyField        string            `yaml:"keyField"`
}

type Connector struct {
	Mapper MapperConfig  `yaml:"mapper"`
	CDC    config.Config `yaml:"cdc" mapstructure:"cdc"`
	Kafka  Kafka         `yaml:"kafka" mapstructure:"kafka"`
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

func (k *Kafka) GetCompression() int8 {
	if k.Compression < 0 || k.Compression > 4 {
		panic("invalid kafka compression method, given: " + strconv.Itoa(int(k.Compression)))
	}
	return k.Compression
}

func (c *Connector) SetDefault() {
	c.CDC.SetDefault()

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
}

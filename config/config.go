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

const (
	ConsulConfigPathEnv       = "CDC_CONSUL_CONFIG_PATH"
	legacyConsulConfigPathEnv = "CONFIG_PATH"
	defaultConsulConfigPath   = "config/config.json"
)

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

	if c.Mapper.KeyField == "" {
		c.Mapper.KeyField = "id"
	}
}

func Load(path string) (*Connector, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	raw = expandEnv(raw)
	raw, err = coerceMillisecondDurations(raw)
	if err != nil {
		return nil, fmt.Errorf("normalize config: %w", err)
	}

	var cfg Connector
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	consul, err := LoadConsulConfig(consulPath())
	if err != nil {
		return nil, err
	}

	cfg.SetDefault()
	if err := cfg.applyConsulOverrides(consul); err != nil {
		return nil, err
	}
	cfg.CDC.Slot.SlotActivityCheckerInterval = millisecondCountDuration(cfg.CDC.Slot.SlotActivityCheckerInterval)
	if err := cfg.loadCerts(); err != nil {
		return nil, err
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func consulPath() string {
	if p := os.Getenv(ConsulConfigPathEnv); p != "" {
		return p
	}
	if p := os.Getenv(legacyConsulConfigPathEnv); p != "" {
		return p
	}
	return defaultConsulConfigPath
}

func expandEnv(raw []byte) []byte {
	return []byte(envPattern.ReplaceAllStringFunc(string(raw), func(match string) string {
		name := match[2 : len(match)-1]
		if val, ok := os.LookupEnv(name); ok {
			return val
		}
		return ""
	}))
}

func coerceMillisecondDurations(raw []byte) ([]byte, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	coerceIntDuration(&doc, "slotActivityCheckerInterval")
	out, err := yaml.Marshal(&doc)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func coerceIntDuration(n *yaml.Node, key string) {
	if n == nil {
		return
	}
	switch n.Kind {
	case yaml.DocumentNode, yaml.SequenceNode:
		for _, c := range n.Content {
			coerceIntDuration(c, key)
		}
	case yaml.MappingNode:
		for i := 0; i+1 < len(n.Content); i += 2 {
			k, v := n.Content[i], n.Content[i+1]
			if k.Value == key && v.Tag == "!!int" {
				v.Tag = "!!str"
				v.Value += "ns"
			}
			coerceIntDuration(v, key)
		}
	}
}

func millisecondCountDuration(d time.Duration) time.Duration {
	if d >= time.Millisecond {
		return d / time.Millisecond
	}
	return d
}

func (c *Connector) validate() error {
	if strings.TrimSpace(c.CDC.Username) == "" || c.CDC.Password == "" {
		return fmt.Errorf("cdc username and password are required")
	}
	if len(c.CDC.Publication.Tables) == 0 {
		return fmt.Errorf("cdc.publication.tables is empty")
	}
	if len(c.Kafka.Brokers) == 0 {
		return fmt.Errorf("kafka.brokers is empty")
	}
	return nil
}

func (c *Connector) loadCerts() error {
	if c.Kafka.RootCAPath != "" && len(c.Kafka.RootCA) == 0 {
		b, err := os.ReadFile(c.Kafka.RootCAPath)
		if err != nil {
			return fmt.Errorf("read kafka rootCAPath: %w", err)
		}
		c.Kafka.RootCA = b
	}
	if c.Kafka.InterCAPath != "" && len(c.Kafka.InterCA) == 0 {
		b, err := os.ReadFile(c.Kafka.InterCAPath)
		if err != nil {
			return fmt.Errorf("read kafka interCAPath: %w", err)
		}
		c.Kafka.InterCA = b
	}
	return nil
}

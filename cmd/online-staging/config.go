package main

import (
	"bytes"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Trendyol/go-pq-cdc-kafka/config"
	cdcconfig "github.com/Trendyol/go-pq-cdc/config"
	"gopkg.in/yaml.v3"
)

var errConfiguration = errors.New("configuration")

// Credentials and trust files are runtime-only. PostgreSQL TLS is controlled
// by PGSSLMODE/PGSSLROOTCERT, which the actual source drivers already consume.
type runtimeConfig struct {
	Connector        config.Connector `yaml:"connector"`
	KeyColumn        string           `yaml:"keyColumn"`
	HealthAddress    string           `yaml:"healthAddress"`
	StartupTimeout   time.Duration    `yaml:"startupTimeout"`
	KafkaRootCAFile  string           `yaml:"kafkaRootCAFile"`
	KafkaInterCAFile string           `yaml:"kafkaInterCAFile"`
}

func loadRuntimeConfig(file string) (runtimeConfig, error) {
	var cfg runtimeConfig
	data, err := readRuntimeFile(file, 1<<20)
	if err != nil {
		return cfg, errConfiguration
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err = decoder.Decode(&cfg); err != nil {
		return cfg, errConfiguration
	}
	if err = decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return cfg, errConfiguration
	}
	if err = applyRuntimeSecrets(&cfg); err != nil {
		return cfg, errConfiguration
	}
	if cfg.HealthAddress == "" {
		cfg.HealthAddress = ":8081"
	}
	if cfg.StartupTimeout == 0 {
		cfg.StartupTimeout = 30 * time.Second
	}
	if err = validateRuntimeConfig(cfg); err != nil {
		return cfg, errConfiguration
	}
	return cfg, nil
}

func readRuntimeFile(file string, limit int64) ([]byte, error) {
	file = filepath.Clean(file)
	info, err := os.Stat(file)
	if err != nil || !info.Mode().IsRegular() || info.Size() > limit {
		return nil, errConfiguration
	}
	handle, err := os.Open(file)
	if err != nil {
		return nil, errConfiguration
	}
	defer handle.Close()
	data, err := io.ReadAll(io.LimitReader(handle, limit+1))
	if err != nil || int64(len(data)) > limit {
		return nil, errConfiguration
	}
	return data, nil
}

// Explicit environment value (even empty) wins over a Secret file, which wins
// over mounted configuration. Secret values never become error/log strings.
func runtimeSecret(name string, current string) (string, error) {
	if value, ok := os.LookupEnv(name); ok {
		return value, nil
	}
	if file := os.Getenv(name + "_FILE"); file != "" {
		data, err := readRuntimeFile(file, 64<<10)
		if err != nil {
			return "", err
		}
		return strings.TrimSuffix(strings.TrimSuffix(string(data), "\n"), "\r"), nil
	}
	return current, nil
}

func applyRuntimeSecrets(cfg *runtimeConfig) error {
	var err error
	cfg.Connector.CDC.Password, err = runtimeSecret("CDC_PG_PASSWORD", cfg.Connector.CDC.Password)
	if err != nil {
		return err
	}
	cfg.Connector.Kafka.ScramPassword, err = runtimeSecret("CDC_KAFKA_SCRAM_PASSWORD", cfg.Connector.Kafka.ScramPassword)
	if err != nil {
		return err
	}
	if value, ok := os.LookupEnv("CDC_PG_USERNAME"); ok {
		cfg.Connector.CDC.Username = value
	}
	if value, ok := os.LookupEnv("CDC_KAFKA_SCRAM_USERNAME"); ok {
		cfg.Connector.Kafka.ScramUsername = value
	}
	for _, item := range []struct {
		env    string
		file   *string
		target *[]byte
	}{
		{"CDC_KAFKA_ROOT_CA_FILE", &cfg.KafkaRootCAFile, &cfg.Connector.Kafka.RootCA},
		{"CDC_KAFKA_INTER_CA_FILE", &cfg.KafkaInterCAFile, &cfg.Connector.Kafka.InterCA},
	} {
		if value, ok := os.LookupEnv(item.env); ok {
			*item.file = value
		}
		if *item.file != "" {
			*item.target, err = readRuntimeFile(*item.file, 1<<20)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func validateRuntimeConfig(cfg runtimeConfig) error {
	source := cfg.Connector.CDC
	if cfg.KeyColumn == "" || cfg.StartupTimeout <= 0 || cfg.StartupTimeout > 5*time.Minute {
		return errConfiguration
	}
	_, port, err := net.SplitHostPort(cfg.HealthAddress)
	healthPort, portErr := strconv.Atoi(port)
	corePort := source.Metric.Port
	if corePort == 0 {
		corePort = 8080
	}
	if err != nil || portErr != nil || healthPort < 1 || healthPort > 65535 || healthPort == corePort {
		return errConfiguration
	}
	if err := validateSourceConfig(source); err != nil {
		return err
	}
	if err := validateKafkaConfig(cfg.Connector.Kafka); err != nil {
		return err
	}
	return validatePostgresTLS()
}

func validateSourceConfig(source cdcconfig.Config) error {
	if source.Host == "" || source.Username == "" || source.Password == "" || source.Database == "" || source.DebugMode {
		return errConfiguration
	}
	if source.Snapshot.Strategy != cdcconfig.SnapshotStrategyOnline || !source.Snapshot.Enabled || source.Snapshot.Mode != cdcconfig.SnapshotModeInitial || source.Slot.ProtoVersion != 1 {
		return errConfiguration
	}
	return nil
}

func validateKafkaConfig(kafka config.Kafka) error {
	if kafka.RequiredAcks != -1 || kafka.AllowAutoTopicCreation || kafka.SkipOversizedMessages || kafka.Online.CloseTimeout < 0 || kafka.Online.CloseTimeout > 30*time.Second {
		return errConfiguration
	}
	if kafka.SecureConnection {
		if kafka.ScramUsername == "" || kafka.ScramPassword == "" || !validRuntimeCA(kafka.RootCA) {
			return errConfiguration
		}
		if len(kafka.InterCA) > 0 && !validRuntimeCA(kafka.InterCA) {
			return errConfiguration
		}
	} else if kafka.ScramUsername != "" || kafka.ScramPassword != "" || len(kafka.RootCA) > 0 || len(kafka.InterCA) > 0 {
		return errConfiguration
	}
	return nil
}

func validatePostgresTLS() error {
	switch os.Getenv("PGSSLMODE") {
	case "disable":
		if os.Getenv("PGSSLROOTCERT") != "" {
			return errConfiguration
		}
	case "verify-full":
		file := os.Getenv("PGSSLROOTCERT")
		if file == "" {
			return errConfiguration
		}
		data, err := readRuntimeFile(file, 1<<20)
		if err != nil || !validRuntimeCA(data) {
			return errConfiguration
		}
	default:
		return errConfiguration
	}
	return nil
}

func validRuntimeCA(data []byte) bool { return x509.NewCertPool().AppendCertsFromPEM(data) }

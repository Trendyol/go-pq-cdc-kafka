package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc-kafka/config"
	cdcconfig "github.com/Trendyol/go-pq-cdc/config"
	"github.com/Trendyol/go-pq-cdc/pq/slot"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func testRuntimeConfig() runtimeConfig {
	return runtimeConfig{KeyColumn: "id", HealthAddress: ":18081", StartupTimeout: time.Second, Connector: config.Connector{
		CDC:   cdcconfig.Config{Host: "127.0.0.1", Username: "runtime", Password: "mounted-password", Database: "source", Metric: cdcconfig.MetricConfig{Port: 18080}, Slot: slot.Config{Name: "runtime_slot", ProtoVersion: 1}, Snapshot: cdcconfig.SnapshotConfig{Enabled: true, Mode: cdcconfig.SnapshotModeInitial, Strategy: cdcconfig.SnapshotStrategyOnline}},
		Kafka: config.Kafka{RequiredAcks: -1, Brokers: []string{"127.0.0.1:9092"}, TableTopicMapping: map[string]string{"public.items": "items"}},
	}}
}

func runtimeConfigFile(t *testing.T, cfg runtimeConfig) string {
	t.Helper()
	data, err := yaml.Marshal(cfg)
	require.NoError(t, err)
	file := filepath.Join(t.TempDir(), "runtime.yaml")
	require.NoError(t, os.WriteFile(file, data, 0600))
	return file
}

func clearRuntimeEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range []string{"CDC_PG_PASSWORD", "CDC_PG_PASSWORD_FILE", "CDC_KAFKA_SCRAM_PASSWORD", "CDC_KAFKA_SCRAM_PASSWORD_FILE", "CDC_PG_USERNAME", "CDC_KAFKA_SCRAM_USERNAME", "CDC_KAFKA_ROOT_CA_FILE", "CDC_KAFKA_INTER_CA_FILE", "PGSSLROOTCERT"} {
		t.Setenv(name, "")
		require.NoError(t, os.Unsetenv(name))
	}
	t.Setenv("PGSSLMODE", "disable")
}

func TestOnlineStagingConfigRejectsUnknownFieldsWithoutConnecting(t *testing.T) {
	clearRuntimeEnvironment(t)
	file := runtimeConfigFile(t, testRuntimeConfig())
	original, err := os.ReadFile(file)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(file, append(original, []byte("unknownRuntimeField: ignored\n")...), 0600))
	_, err = loadRuntimeConfig(file)
	require.ErrorIs(t, err, errConfiguration, "unsupported configuration cannot be silently ignored")
}

func TestOnlineStagingSecretPrecedenceAndBoundedFailures(t *testing.T) {
	clearRuntimeEnvironment(t)
	file := runtimeConfigFile(t, testRuntimeConfig())
	secret := filepath.Join(t.TempDir(), "password")
	require.NoError(t, os.WriteFile(secret, []byte("file-secret\n"), 0600))
	t.Setenv("CDC_PG_PASSWORD_FILE", secret)
	cfg, err := loadRuntimeConfig(file)
	require.NoError(t, err)
	require.Equal(t, "file-secret", cfg.Connector.CDC.Password)
	t.Setenv("CDC_PG_PASSWORD", "environment-secret")
	cfg, err = loadRuntimeConfig(file)
	require.NoError(t, err)
	require.Equal(t, "environment-secret", cfg.Connector.CDC.Password)
	t.Setenv("CDC_PG_PASSWORD", "")
	_, err = loadRuntimeConfig(file)
	require.ErrorIs(t, err, errConfiguration, "explicit empty override must not fall back to a secret")
	require.NoError(t, os.Unsetenv("CDC_PG_PASSWORD"))
	t.Setenv("CDC_PG_PASSWORD_FILE", filepath.Join(t.TempDir(), "missing-secret"))
	_, err = loadRuntimeConfig(file)
	require.ErrorIs(t, err, errConfiguration)
	require.Equal(t, "configuration", err.Error(), "path/secret must not enter error strings")
}

func TestOnlineStagingConfigurationAdmission(t *testing.T) {
	clearRuntimeEnvironment(t)
	for name, mutate := range map[string]func(*runtimeConfig){
		"debug payloads":           func(c *runtimeConfig) { c.Connector.CDC.DebugMode = true },
		"health metrics collision": func(c *runtimeConfig) { c.HealthAddress = ":18080" },
		"acks leader":              func(c *runtimeConfig) { c.Connector.Kafka.RequiredAcks = 1 },
		"auto topic":               func(c *runtimeConfig) { c.Connector.Kafka.AllowAutoTopicCreation = true },
		"skip oversize":            func(c *runtimeConfig) { c.Connector.Kafka.SkipOversizedMessages = true },
		"proto2":                   func(c *runtimeConfig) { c.Connector.CDC.Slot.ProtoVersion = 2 },
		"missing key":              func(c *runtimeConfig) { c.KeyColumn = "" },
		"plaintext with SASL":      func(c *runtimeConfig) { c.Connector.Kafka.ScramUsername = "secret-user" },
		"incomplete TLS SCRAM":     func(c *runtimeConfig) { c.Connector.Kafka.SecureConnection = true },
		"unbounded startup":        func(c *runtimeConfig) { c.StartupTimeout = 10 * time.Minute },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := testRuntimeConfig()
			mutate(&cfg)
			_, err := loadRuntimeConfig(runtimeConfigFile(t, cfg))
			require.ErrorIs(t, err, errConfiguration)
		})
	}
	t.Setenv("PGSSLMODE", "require")
	_, err := loadRuntimeConfig(runtimeConfigFile(t, testRuntimeConfig()))
	require.ErrorIs(t, err, errConfiguration)
	t.Setenv("PGSSLMODE", "verify-full")
	_, err = loadRuntimeConfig(runtimeConfigFile(t, testRuntimeConfig()))
	require.ErrorIs(t, err, errConfiguration, "scratch runner requires mounted source trust roots")
}

package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeFile(t *testing.T, dir, name, body string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(p, []byte(body), 0o644))
	return p
}

func minimalYAML() string {
	return `cdc:
  host: yaml-host
  username: yaml-user
  password: yaml-pass
  database: yaml-db
  publication:
    name: yaml-pub
    createIfNotExists: true
    operations: [INSERT, UPDATE]
    tables:
      - name: users
        replicaIdentity: FULL
  slot:
    name: yaml-slot
    slotActivityCheckerInterval: 3000
    createIfNotExists: true
kafka:
  brokers: [yaml-broker:9092]
  tableTopicMapping:
    public.users: yaml-topic
`
}

func TestLoad_ConsulOverridesPlainFields(t *testing.T) {
	tmp := t.TempDir()
	yamlPath := writeFile(t, tmp, "c.yml", minimalYAML())
	consulPath := writeFile(t, tmp, "config.json", `{
        "postgresUsername": "consul-user",
        "postgresPassword": "consul-pass",
        "postgresHost": "consul-host",
        "postgresPort": 5433,
        "postgresDatabase": "consul-db",
        "publicationName": "consul-pub",
        "slotName": "consul-slot",
        "kafkaBrokers": ["broker1:9092","broker2:9092"],
        "tableTopicMapping": {"public.orders":"topic.orders"},
        "kafkaScramUsername": "kuser",
        "kafkaScramPassword": "kpass",
        "tables": [{"name":"orders","schema":"public","replicaIdentity":"FULL"}]
    }`)
	t.Setenv(ConsulConfigPathEnv, consulPath)

	cfg, err := Load(yamlPath)
	require.NoError(t, err)

	assert.Equal(t, "consul-user", cfg.CDC.Username)
	assert.Equal(t, "consul-pass", cfg.CDC.Password)
	assert.Equal(t, "consul-host", cfg.CDC.Host)
	assert.Equal(t, 5433, cfg.CDC.Port)
	assert.Equal(t, "consul-db", cfg.CDC.Database)
	assert.Equal(t, "consul-pub", cfg.CDC.Publication.Name)
	assert.Equal(t, "consul-slot", cfg.CDC.Slot.Name)
	assert.Equal(t, []string{"broker1:9092", "broker2:9092"}, cfg.Kafka.Brokers)
	assert.Equal(t, "topic.orders", cfg.Kafka.TableTopicMapping["public.orders"])
	assert.Equal(t, "kuser", cfg.Kafka.ScramUsername)
	assert.Equal(t, "kpass", cfg.Kafka.ScramPassword)
	require.Len(t, cfg.CDC.Publication.Tables, 1)
	assert.Equal(t, "orders", cfg.CDC.Publication.Tables[0].Name)
}

func TestLoad_TbpSecretOverridesPlainCredentials(t *testing.T) {
	tmp := t.TempDir()
	yamlPath := writeFile(t, tmp, "c.yml", minimalYAML())
	pgSecret := writeFile(t, tmp, "pg-secret.json", `{"username":"tbp-pg-user","password":"tbp-pg-pass"}`)
	kSecret := writeFile(t, tmp, "k-secret.json", `{"username":"tbp-k-user","password":"tbp-k-pass"}`)
	consulPath := writeFile(t, tmp, "config.json", `{
        "postgresUsername": "ignored-plain-user",
        "postgresPassword": "ignored-plain-pass",
        "postgresHost": "secret-host",
        "postgresTbpSecretPath": "`+pgSecret+`",
        "kafkaScramUsername": "ignored-k-user",
        "kafkaScramPassword": "ignored-k-pass",
        "kafkaTbpSecretPath": "`+kSecret+`"
    }`)
	t.Setenv(ConsulConfigPathEnv, consulPath)

	cfg, err := Load(yamlPath)
	require.NoError(t, err)

	assert.Equal(t, "tbp-pg-user", cfg.CDC.Username)
	assert.Equal(t, "tbp-pg-pass", cfg.CDC.Password)
	assert.Equal(t, "secret-host", cfg.CDC.Host)
	assert.Equal(t, "tbp-k-user", cfg.Kafka.ScramUsername)
	assert.Equal(t, "tbp-k-pass", cfg.Kafka.ScramPassword)
}

func TestLoad_MissingConsulFile_LeavesYAMLIntact(t *testing.T) {
	tmp := t.TempDir()
	yamlPath := writeFile(t, tmp, "c.yml", minimalYAML())
	t.Setenv(ConsulConfigPathEnv, filepath.Join(tmp, "does-not-exist.json"))

	cfg, err := Load(yamlPath)
	require.NoError(t, err)
	assert.Equal(t, "yaml-user", cfg.CDC.Username)
	assert.Equal(t, "yaml-pass", cfg.CDC.Password)
	assert.Equal(t, "yaml-host", cfg.CDC.Host)
}

func TestLoad_EmptyConsulConfig_DoesNotClobberYAML(t *testing.T) {
	tmp := t.TempDir()
	yamlPath := writeFile(t, tmp, "c.yml", minimalYAML())
	consulPath := writeFile(t, tmp, "config.json", `{}`)
	t.Setenv(ConsulConfigPathEnv, consulPath)

	cfg, err := Load(yamlPath)
	require.NoError(t, err)
	assert.Equal(t, "yaml-user", cfg.CDC.Username)
	assert.Equal(t, "yaml-db", cfg.CDC.Database)
	assert.Equal(t, []string{"yaml-broker:9092"}, cfg.Kafka.Brokers)
}

func TestLoad_ConsulOverridesKeyField(t *testing.T) {
	tmp := t.TempDir()
	yamlPath := writeFile(t, tmp, "c.yml", minimalYAML())
	consulPath := writeFile(t, tmp, "config.json", `{
        "keyField": "uuid",
        "tableKeyMapping": {"public.users": "email"}
    }`)
	t.Setenv(ConsulConfigPathEnv, consulPath)

	cfg, err := Load(yamlPath)
	require.NoError(t, err)
	assert.Equal(t, "uuid", cfg.Mapper.KeyField)
	assert.Equal(t, "email", cfg.Mapper.TableKeyMapping["public.users"])
}

func TestLoad_ConsulCanDisableCreateIfNotExists(t *testing.T) {
	tmp := t.TempDir()
	yamlPath := writeFile(t, tmp, "c.yml", minimalYAML())
	consulPath := writeFile(t, tmp, "config.json", `{
        "publicationCreateIfNotExists": false,
        "slotCreateIfNotExists": false
    }`)
	t.Setenv(ConsulConfigPathEnv, consulPath)

	cfg, err := Load(yamlPath)
	require.NoError(t, err)
	assert.False(t, cfg.CDC.Publication.CreateIfNotExists)
	assert.False(t, cfg.CDC.Slot.CreateIfNotExists)
}

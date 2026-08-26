package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Trendyol/go-pq-cdc/pq/publication"
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

func TestLoad_ConsulHostWithPort(t *testing.T) {
	tmp := t.TempDir()
	yamlPath := writeFile(t, tmp, "c.yml", minimalYAML())
	consulPath := writeFile(t, tmp, "config.json", `{
        "postgresHost": "postgres.example:5432"
    }`)
	t.Setenv(ConsulConfigPathEnv, consulPath)

	cfg, err := Load(yamlPath)
	require.NoError(t, err)
	assert.Equal(t, "postgres.example", cfg.CDC.Host)
	assert.Equal(t, 5432, cfg.CDC.Port)
}

func TestLoad_ConsulHostPortConflict(t *testing.T) {
	tmp := t.TempDir()
	yamlPath := writeFile(t, tmp, "c.yml", minimalYAML())
	consulPath := writeFile(t, tmp, "config.json", `{
        "postgresHost": "postgres.example:5432",
        "postgresPort": 6543
    }`)
	t.Setenv(ConsulConfigPathEnv, consulPath)

	_, err := Load(yamlPath)
	require.Error(t, err)
}

func TestLoad_FileSecretOverridesPlainCredentials(t *testing.T) {
	tmp := t.TempDir()
	yamlPath := writeFile(t, tmp, "c.yml", minimalYAML())
	pgSecret := writeFile(t, tmp, "pg-secret.json", `{"username":"file-pg-user","password":"file-pg-pass"}`)
	kSecret := writeFile(t, tmp, "k-secret.json", `{"username":"file-k-user","password":"file-k-pass"}`)
	consulPath := writeFile(t, tmp, "config.json", `{
        "postgresUsername": "ignored-plain-user",
        "postgresPassword": "ignored-plain-pass",
        "postgresHost": "secret-host",
        "postgresSecretPath": "`+pgSecret+`",
        "kafkaScramUsername": "ignored-k-user",
        "kafkaScramPassword": "ignored-k-pass",
        "kafkaSecretPath": "`+kSecret+`"
    }`)
	t.Setenv(ConsulConfigPathEnv, consulPath)

	cfg, err := Load(yamlPath)
	require.NoError(t, err)

	assert.Equal(t, "file-pg-user", cfg.CDC.Username)
	assert.Equal(t, "file-pg-pass", cfg.CDC.Password)
	assert.Equal(t, "secret-host", cfg.CDC.Host)
	assert.Equal(t, "file-k-user", cfg.Kafka.ScramUsername)
	assert.Equal(t, "file-k-pass", cfg.Kafka.ScramPassword)
}

func TestLoad_MissingSecretFile(t *testing.T) {
	tmp := t.TempDir()
	yamlPath := writeFile(t, tmp, "c.yml", minimalYAML())
	consulPath := writeFile(t, tmp, "config.json", `{
        "postgresSecretPath": "`+filepath.Join(tmp, "missing-secret.json")+`"
    }`)
	t.Setenv(ConsulConfigPathEnv, consulPath)

	_, err := Load(yamlPath)
	require.Error(t, err)
}

func TestLoad_IncompleteFileSecret(t *testing.T) {
	tmp := t.TempDir()
	yamlPath := writeFile(t, tmp, "c.yml", minimalYAML())
	pgSecret := writeFile(t, tmp, "pg-secret.json", `{"username":"only-user"}`)
	consulPath := writeFile(t, tmp, "config.json", `{
        "postgresSecretPath": "`+pgSecret+`"
    }`)
	t.Setenv(ConsulConfigPathEnv, consulPath)

	_, err := Load(yamlPath)
	require.Error(t, err)
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

func TestLoad_ConsulCanSetRequiredAcksZero(t *testing.T) {
	tmp := t.TempDir()
	yamlPath := writeFile(t, tmp, "c.yml", minimalYAML())
	consulPath := writeFile(t, tmp, "config.json", `{"kafkaRequiredAcks": 0}`)
	t.Setenv(ConsulConfigPathEnv, consulPath)

	cfg, err := Load(yamlPath)
	require.NoError(t, err)
	assert.Equal(t, 0, cfg.Kafka.RequiredAcks)
}

func TestLoad_ConsulCanSetCompressionNone(t *testing.T) {
	tmp := t.TempDir()
	yamlPath := writeFile(t, tmp, "c.yml", minimalYAML()+`
  compression: 1
`)
	zero := int8(0)
	consulPath := writeFile(t, tmp, "config.json", `{"kafkaCompression": 0}`)
	t.Setenv(ConsulConfigPathEnv, consulPath)

	cfg, err := Load(yamlPath)
	require.NoError(t, err)
	assert.Equal(t, zero, cfg.Kafka.Compression)
}

func TestLoad_InvalidPublicationOperation(t *testing.T) {
	tmp := t.TempDir()
	yamlPath := writeFile(t, tmp, "c.yml", minimalYAML())
	consulPath := writeFile(t, tmp, "config.json", `{"publicationOperations": ["INSERT", "NOPE"]}`)
	t.Setenv(ConsulConfigPathEnv, consulPath)

	_, err := Load(yamlPath)
	require.Error(t, err)
}

func TestLoad_ValidPublicationOperations(t *testing.T) {
	tmp := t.TempDir()
	yamlPath := writeFile(t, tmp, "c.yml", minimalYAML())
	consulPath := writeFile(t, tmp, "config.json", `{"publicationOperations": ["INSERT", "UPDATE", "DELETE", "TRUNCATE"]}`)
	t.Setenv(ConsulConfigPathEnv, consulPath)

	cfg, err := Load(yamlPath)
	require.NoError(t, err)
	assert.Equal(t, publication.Operations{
		publication.OperationInsert,
		publication.OperationUpdate,
		publication.OperationDelete,
		publication.OperationTruncate,
	}, cfg.CDC.Publication.Operations)
}

func TestLoad_UnsetPasswordFilledByConsul(t *testing.T) {
	tmp := t.TempDir()
	yamlPath := writeFile(t, tmp, "c.yml", `cdc:
  host: yaml-host
  username: yaml-user
  password: ${MISSING_PWD}
  database: yaml-db
  publication:
    name: yaml-pub
    createIfNotExists: true
    operations: [INSERT]
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
`)
	consulPath := writeFile(t, tmp, "config.json", `{"postgresPassword": "from-consul"}`)
	t.Setenv(ConsulConfigPathEnv, consulPath)

	cfg, err := Load(yamlPath)
	require.NoError(t, err)
	assert.Equal(t, "from-consul", cfg.CDC.Password)
}

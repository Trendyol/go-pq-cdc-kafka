package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testFixture(name string) string {
	wd, err := os.Getwd()
	if err != nil {
		panic(err)
	}
	return filepath.Join(wd, "..", "resources", "testdata", name)
}

func TestLoad_Minimal(t *testing.T) {
	t.Setenv(ConsulConfigPathEnv, filepath.Join(t.TempDir(), "missing.json"))

	cfg, err := Load(testFixture("minimal.yml"))
	require.NoError(t, err)

	assert.Equal(t, "host", cfg.CDC.Host)
	assert.Equal(t, "db", cfg.CDC.Database)
	assert.Equal(t, "topic.x", cfg.Kafka.TableTopicMapping["public.users"])
	assert.Equal(t, "id", cfg.Mapper.KeyField)
	assert.Equal(t, time.Duration(3000), cfg.CDC.Slot.SlotActivityCheckerInterval)
}

func TestLoad_EnvSubstitution(t *testing.T) {
	t.Setenv("MY_PWD", "secret123")
	t.Setenv(ConsulConfigPathEnv, filepath.Join(t.TempDir(), "missing.json"))
	path := filepath.Join(t.TempDir(), "c.yml")
	body := []byte(`cdc:
  host: h
  username: u
  password: ${MY_PWD}
  database: db
  publication:
    name: pub
    createIfNotExists: true
    operations: [INSERT]
    tables: [{name: users, replicaIdentity: FULL}]
  slot:
    name: slot
    slotActivityCheckerInterval: 3000
    createIfNotExists: true
kafka:
  brokers: [b]
  tableTopicMapping: {public.users: t}
`)
	require.NoError(t, os.WriteFile(path, body, 0o644))

	cfg, err := Load(path)
	require.NoError(t, err)
	assert.Equal(t, "secret123", cfg.CDC.Password)
}

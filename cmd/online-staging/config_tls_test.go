package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func runtimeTestCA(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	certificate, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate})
}

func TestOnlineStagingTLSRequiresRuntimeRootsAndSCRAM512Credentials(t *testing.T) {
	clearRuntimeEnvironment(t)
	ca := filepath.Join(t.TempDir(), "public-root.pem")
	require.NoError(t, os.WriteFile(ca, runtimeTestCA(t), 0600))
	cfg := testRuntimeConfig()
	cfg.KafkaRootCAFile = ca
	cfg.Connector.Kafka.SecureConnection = true
	cfg.Connector.Kafka.ScramUsername = "mounted-user"
	cfg.Connector.Kafka.ScramPassword = "mounted-password"
	t.Setenv("PGSSLMODE", "verify-full")
	t.Setenv("PGSSLROOTCERT", ca)
	loaded, err := loadRuntimeConfig(runtimeConfigFile(t, cfg))
	require.NoError(t, err)
	require.True(t, validRuntimeCA(loaded.Connector.Kafka.RootCA))
	t.Setenv("CDC_KAFKA_SCRAM_PASSWORD", "environment-password")
	loaded, err = loadRuntimeConfig(runtimeConfigFile(t, cfg))
	require.NoError(t, err)
	require.Equal(t, "environment-password", loaded.Connector.Kafka.ScramPassword)
	t.Setenv("CDC_KAFKA_ROOT_CA_FILE", filepath.Join(t.TempDir(), "missing-root"))
	_, err = loadRuntimeConfig(runtimeConfigFile(t, cfg))
	require.ErrorIs(t, err, errConfiguration)
}

func TestOnlineStagingRuntimeFilesAreRegularAndBounded(t *testing.T) {
	_, err := readRuntimeFile(t.TempDir(), 1024)
	require.ErrorIs(t, err, errConfiguration)
	file := filepath.Join(t.TempDir(), "oversized-secret")
	require.NoError(t, os.WriteFile(file, make([]byte, 1025), 0600))
	_, err = readRuntimeFile(file, 1024)
	require.ErrorIs(t, err, errConfiguration)
}

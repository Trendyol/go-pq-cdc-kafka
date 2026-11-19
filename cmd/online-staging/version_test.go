package main

import (
	"encoding/json"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOnlineStagingVersionBeforeConfigAndNetwork(t *testing.T) {
	kafkaSHA := strings.Repeat("a", 40)
	coreSHA := strings.Repeat("b", 40)
	command := exec.CommandContext(t.Context(), "go", "run", "-ldflags", "-X main.kafkaRevision="+kafkaSHA+" -X main.coreRevision="+coreSHA, ".", "--version", "--config", "/definitely/missing/private-config.yaml")
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	var version map[string]string
	require.NoError(t, json.Unmarshal(output, &version))
	require.Equal(t, kafkaSHA, version["kafkaRevision"])
	require.Equal(t, coreSHA, version["coreRevision"])
	require.Equal(t, "go1.26.10", version["goVersion"])
	require.NotEmpty(t, version["platform"])
}

package integration

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	gokafka "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func buildOnlineStagingProcess(t *testing.T) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "online-staging")
	command := exec.CommandContext(t.Context(), "go", "build", "-trimpath", "-o", binary, "./cmd/online-staging")
	command.Dir = ".."
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	version := exec.CommandContext(t.Context(), binary, "--version", "--config", "/definitely/missing")
	output, err = version.Output()
	require.NoError(t, err)
	var info map[string]string
	require.NoError(t, json.Unmarshal(output, &info))
	require.Equal(t, "go1.26.10", info["goVersion"])
	require.Equal(t, "uncommitted", info["sourceState"], "plain test build must not claim exact committed source binding")
	return binary
}

func processHealthAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	require.NoError(t, listener.Close())
	return address
}

type stagingProcess struct {
	command  *exec.Cmd
	result   chan error
	finished chan struct{}
	log      string
	health   string
}

func startStagingProcess(t *testing.T, binary string, source *onlineKafkaSource, keyColumn string) *stagingProcess {
	t.Helper()
	var version int
	require.NoError(t, source.conn.QueryRow(t.Context(), "SELECT current_setting('server_version_num')::int").Scan(&version))
	require.Equal(t, 16, version/10000, "this genuine process fixture must be PostgreSQL16")
	t.Logf("actual runner process source server_version_num=%d", version)
	health := processHealthAddress(t)
	cfg := source.config(t, nil)
	cfg.Kafka.Online.MaxAttempts = 1
	cfg.Kafka.WriteTimeout = 3 * time.Second
	cfg.Kafka.Online.CloseTimeout = 5 * time.Second
	settings := map[string]any{"connector": cfg, "keyColumn": keyColumn, "healthAddress": health, "startupTimeout": "30s"}
	data, err := yaml.Marshal(settings)
	require.NoError(t, err)
	directory := t.TempDir()
	configFile := filepath.Join(directory, "runtime.yaml")
	secretFile := filepath.Join(directory, "password")
	require.NoError(t, os.WriteFile(configFile, data, 0600))
	require.NoError(t, os.WriteFile(secretFile, []byte("cdc_pass\n"), 0600))
	logFile := filepath.Join(directory, "runner.log")
	output, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY, 0600)
	require.NoError(t, err)
	command := exec.CommandContext(t.Context(), binary, "--config", configFile)
	command.Env = []string{"PGSSLMODE=disable", "CDC_PG_PASSWORD_FILE=" + secretFile}
	command.Stdout = output
	command.Stderr = output
	require.NoError(t, command.Start())
	process := &stagingProcess{command: command, result: make(chan error, 1), finished: make(chan struct{}), log: logFile, health: "http://" + health}
	go func() { err := command.Wait(); _ = output.Close(); process.result <- err; close(process.finished) }()
	t.Cleanup(func() {
		select {
		case <-process.finished:
			return
		default:
		}
		_ = command.Process.Signal(syscall.SIGTERM)
		select {
		case <-process.finished:
		case <-time.After(10 * time.Second):
			_ = command.Process.Kill()
			<-process.finished
		}
	})
	return process
}

func (p *stagingProcess) waitReady(t *testing.T) {
	t.Helper()
	client := &http.Client{Timeout: time.Second}
	require.Eventually(t, func() bool {
		request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, p.health+"/readyz", nil)
		if err != nil {
			return false
		}
		response, err := client.Do(request)
		if err != nil {
			return false
		}
		defer response.Body.Close()
		return response.StatusCode == http.StatusOK
	}, 30*time.Second, 20*time.Millisecond, "actual runner readiness must follow source capture")
}

func (p *stagingProcess) waitExit(t *testing.T) error {
	t.Helper()
	select {
	case err := <-p.result:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("runner did not finish bounded owned cleanup")
		return nil
	}
}

func (p *stagingProcess) assertSafeLog(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile(p.log)
	require.NoError(t, err)
	for _, secret := range []string{"cdc_pass", "postgres://", "must not leak row value"} {
		require.NotContains(t, string(data), secret)
	}
	return string(data)
}

func waitProcessSnapshot(t *testing.T, source *onlineKafkaSource) {
	t.Helper()
	require.Eventually(t, func() bool {
		var complete bool
		err := source.conn.QueryRow(t.Context(), "SELECT status='snapshot_complete' FROM cdc_online.jobs WHERE slot_name=$1", source.slot).Scan(&complete)
		return err == nil && complete
	}, 30*time.Second, 20*time.Millisecond, "real snapshot completion, not a fixed sleep")
}

func TestOnlineStagingProcessReadInsertUpdateDeleteAndGracefulSignals(t *testing.T) {
	binary := buildOnlineStagingProcess(t)
	for _, signal := range []os.Signal{syscall.SIGTERM, syscall.SIGINT} {
		t.Run(signal.String(), func(t *testing.T) {
			source := newOnlineKafkaSource(t, "staging_signals", 1, `id integer PRIMARY KEY, name text NOT NULL, payload jsonb`)
			source.exec(t, `INSERT INTO `+source.qualified()+` VALUES(1,'seed','{"n":9007199254740993}')`)
			process := startStagingProcess(t, binary, source, "id")
			process.waitReady(t)
			waitProcessSnapshot(t, source)
			source.exec(t, `INSERT INTO `+source.qualified()+` VALUES(2,'inserted','null')`)
			source.exec(t, `UPDATE `+source.qualified()+` SET name='updated' WHERE id=2`)
			source.exec(t, `DELETE FROM `+source.qualified()+` WHERE id=2`)
			source.waitSlotAtLeast(t, source.walLSN(t))
			records := readExistingHandlerRecords(t, source, 4)
			operations := []string{"READ", "INSERT", "UPDATE", "DELETE"}
			for i, record := range records {
				decoder := json.NewDecoder(strings.NewReader(string(record.Value)))
				decoder.UseNumber()
				var payload map[string]any
				require.NoError(t, decoder.Decode(&payload))
				require.Equal(t, operations[i], payload["operation"])
				require.Equal(t, json.Number("1"), payload["version"])
				require.NotContains(t, payload, "eventTime")
				headers := map[string]string{}
				for _, h := range record.Headers {
					headers[h.Key] = string(h.Value)
				}
				require.NotEmpty(t, headers["cdc-event-id"])
				require.Equal(t, "0", headers["cdc-output-index"])
				if i == 0 {
					require.Equal(t, json.Number("9007199254740993"), payload["after"].(map[string]any)["payload"].(map[string]any)["n"])
				}
				if i == 2 {
					require.Equal(t, "inserted", payload["before"].(map[string]any)["name"])
					require.Equal(t, "updated", payload["after"].(map[string]any)["name"])
				}
				if i == 3 {
					require.Nil(t, payload["after"])
					require.Equal(t, "updated", payload["before"].(map[string]any)["name"])
				}
			}
			require.NoError(t, process.command.Process.Signal(signal))
			require.NoError(t, process.waitExit(t))
			require.NoError(t, source.waitSlotInactive(t.Context()))
			process.assertSafeLog(t)
		})
	}
}

func TestOnlineStagingProcessTransformationFailureNoWriteOrSourceACK(t *testing.T) {
	binary := buildOnlineStagingProcess(t)
	source := newOnlineKafkaSource(t, "staging_transform", 1, `id integer PRIMARY KEY, name text NOT NULL`)
	source.exec(t, `INSERT INTO `+source.qualified()+` VALUES(1,'must not leak row value')`)
	process := startStagingProcess(t, binary, source, "missing_key")
	require.Error(t, process.waitExit(t))
	require.Contains(t, process.assertSafeLog(t), "online-staging: transform")
	var belowHigh bool
	require.NoError(t, source.conn.QueryRow(t.Context(), `SELECT j.source_lsn < w.high_end AND s.confirmed_flush_lsn < w.high_end FROM cdc_online.jobs j JOIN cdc_online.windows w USING(job_id) JOIN pg_replication_slots s ON s.slot_name=j.slot_name WHERE j.slot_name=$1`, source.slot).Scan(&belowHigh))
	require.True(t, belowHigh, "actual protected NewConnector invalid-record bridge must not certify READ HIGH")
	reader := gokafka.NewReader(gokafka.ReaderConfig{Brokers: onlineKafkaBrokers(), Topic: source.topic, Partition: 0, MinBytes: 1, MaxBytes: 1e6})
	defer reader.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	_, err := reader.ReadMessage(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded, "invalid transformation must never reach durable writer")
	require.NoError(t, source.waitSlotInactive(t.Context()))
}

func TestOnlineStagingProcessTerminalBrokerFailureIsNonzeroAndCleansUp(t *testing.T) {
	binary := buildOnlineStagingProcess(t)
	source := newOnlineKafkaSource(t, "staging_failure", 1, `id integer PRIMARY KEY, name text NOT NULL`)
	source.exec(t, `INSERT INTO `+source.qualified()+` VALUES(1,'durable seed')`)
	process := startStagingProcess(t, binary, source, "id")
	process.waitReady(t)
	waitProcessSnapshot(t, source)
	require.Len(t, readExistingHandlerRecords(t, source, 1), 1, "a real output was durable before terminal failure")
	client := &gokafka.Client{Addr: gokafka.TCP(onlineKafkaBrokers()...), Timeout: 10 * time.Second}
	response, err := client.DeleteTopics(t.Context(), &gokafka.DeleteTopicsRequest{Topics: []string{source.topic}})
	require.NoError(t, err)
	require.NoError(t, response.Errors[source.topic])
	source.exec(t, `INSERT INTO `+source.qualified()+` VALUES(2,'must not leak row value')`)
	fence := source.walLSN(t)
	require.Error(t, process.waitExit(t))
	require.Contains(t, process.assertSafeLog(t), "online-staging: capture")
	var bounded bool
	require.NoError(t, source.conn.QueryRow(t.Context(), `SELECT j.source_lsn < $2::pg_lsn AND s.confirmed_flush_lsn < $2::pg_lsn FROM cdc_online.jobs j JOIN pg_replication_slots s ON s.slot_name=j.slot_name WHERE j.slot_name=$1`, source.slot, fence).Scan(&bounded))
	require.True(t, bounded, "real terminal delivery failure must not acknowledge the failed source transaction")
	require.NoError(t, source.waitSlotInactive(t.Context()))
	t.Logf("genuine PostgreSQL16 process terminal failure cleaned up; source ceiling=%s", fence)
}

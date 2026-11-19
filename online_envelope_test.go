package cdc

import (
	"encoding/json"
	"math"
	"testing"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc/pq"
	"github.com/Trendyol/go-pq-cdc/pq/snapshot/online"
	gokafka "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

func onlineEnvelopeEvent(operation cdc.OnlineOperation, key int64, image map[string]any) cdc.OnlineEvent {
	phase := uint8(0)
	commit := pq.LSN(1<<40 + 5)
	if operation == cdc.OnlineRead {
		// A synthetic READ is taken at the dedicated boundary, not at a commit.
		phase, commit = 1, 0
	}
	return cdc.OnlineEvent{
		ID:        online.CDCEventID("binding", pq.LSN(1<<40+7), 3),
		Table:     cdc.OnlineTable{Schema: "public", Name: "accounts"},
		Key:       key,
		Operation: operation,
		Image:     image,
		CommitLSN: commit,
		Order:     cdc.OnlineOrderToken{EndLSN: pq.LSN(1<<40 + 7), Phase: phase, Ordinal: 3},
	}
}

func TestOnlineEnvelopeRoundTripsExactIntegers(t *testing.T) {
	const aboveFloatExact = int64(9007199254740993) // 2^53+1
	image := map[string]any{
		"id":      aboveFloatExact,
		"minimum": int64(math.MinInt64),
		"maximum": int64(math.MaxInt64),
		"payload": map[string]any{"n": json.Number("9007199254740993")},
		"tags":    []any{},
		"holes":   []any{nil, json.Number("-9007199254740993")},
		"comment": nil,
		"big":     "9223372036854775808",
	}
	event := onlineEnvelopeEvent(cdc.OnlineInsert, aboveFloatExact, image)

	message, err := EncodeOnlineMessage(event)
	require.NoError(t, err)
	require.NotEmpty(t, message.Value)

	// The raw value must carry the exact digits, not a rounded float rendering.
	require.Contains(t, string(message.Value), `"id":9007199254740993`)
	require.Contains(t, string(message.Value), `"key":9007199254740993`)
	require.Contains(t, string(message.Value), `"maximum":9223372036854775807`)

	decoded, err := DecodeOnlineEnvelope(message.Value)
	require.NoError(t, err)
	require.Equal(t, aboveFloatExact, decoded.Key)
	// Image numbers decode as exact decimal text, never as float64.
	for column, want := range map[string]string{
		"id":      "9007199254740993",
		"minimum": "-9223372036854775808",
		"maximum": "9223372036854775807",
	} {
		number, ok := decoded.Image[column].(json.Number)
		require.True(t, ok, "%s must decode as json.Number, got %T", column, decoded.Image[column])
		require.Equal(t, want, number.String())
	}
	tightest, err := decoded.Image["maximum"].(json.Number).Int64()
	require.NoError(t, err)
	require.Equal(t, int64(math.MaxInt64), tightest)
	require.Equal(t, json.Number("9007199254740993"), decoded.Image["payload"].(map[string]any)["n"])
	require.Equal(t, []any{}, decoded.Image["tags"])
	require.Equal(t, []any{nil, json.Number("-9007199254740993")}, decoded.Image["holes"])
	require.Nil(t, decoded.Image["comment"])
	require.Equal(t, "9223372036854775808", decoded.Image["big"])

	// The identifier and the versioned order survive without precision loss.
	require.Equal(t, event.ID, decoded.ID)
	require.Equal(t, event.CommitLSN.String(), decoded.CommitLSN)
	require.Equal(t, event.Order.EndLSN.String(), decoded.Order.EndLSN)
	require.Equal(t, event.Order.Ordinal, decoded.Order.Ordinal)
	require.EqualValues(t, 0, decoded.Order.Phase)
	require.Equal(t, OnlineEnvelopeVersion, decoded.Version)
}

func TestOnlineEnvelopeKeepsDeleteIdentityAndOrder(t *testing.T) {
	event := onlineEnvelopeEvent(cdc.OnlineDelete, 42, nil)
	message, err := EncodeOnlineMessage(event)
	require.NoError(t, err)

	decoded, err := DecodeOnlineEnvelope(message.Value)
	require.NoError(t, err)
	require.Equal(t, cdc.OnlineDelete, cdc.OnlineOperation(decoded.Operation))
	require.Equal(t, int64(42), decoded.Key)
	require.Nil(t, decoded.Image, "a DELETE carries identity and order, not a re-invented row image")
	require.Equal(t, event.ID, decoded.ID)
	require.Equal(t, event.Order.EndLSN.String(), decoded.Order.EndLSN)
	require.Contains(t, string(message.Value), `"image":null`)
}

func TestOnlineEnvelopeReadUsesBoundaryPhase(t *testing.T) {
	event := onlineEnvelopeEvent(cdc.OnlineRead, 7, map[string]any{"id": 7, "name": "read"})
	message, err := EncodeOnlineMessage(event)
	require.NoError(t, err)
	decoded, err := DecodeOnlineEnvelope(message.Value)
	require.NoError(t, err)
	require.EqualValues(t, 1, decoded.Order.Phase)
	require.Equal(t, "0/0", decoded.CommitLSN, "a READ has no source commit")
}

func TestOnlineEnvelopeRefusesUnrepresentableOrInconsistentValues(t *testing.T) {
	valid := onlineEnvelopeEvent(cdc.OnlineInsert, 1, map[string]any{"id": 1})

	tests := []struct {
		name   string
		mutate func(event cdc.OnlineEvent) cdc.OnlineEvent
	}{
		{"missing image", func(event cdc.OnlineEvent) cdc.OnlineEvent { event.Image = nil; return event }},
		{"zero order", func(event cdc.OnlineEvent) cdc.OnlineEvent {
			event.Order.EndLSN = 0
			return event
		}},
		{"wrong phase", func(event cdc.OnlineEvent) cdc.OnlineEvent { event.Order.Phase = 1; return event }},
		{"missing id", func(event cdc.OnlineEvent) cdc.OnlineEvent { event.ID = ""; return event }},
		{"missing table", func(event cdc.OnlineEvent) cdc.OnlineEvent { event.Table.Name = ""; return event }},
		{"unsupported operation", func(event cdc.OnlineEvent) cdc.OnlineEvent {
			event.Operation = cdc.OnlineOperation("TRUNCATE")
			return event
		}},
		{"non-finite float image", func(event cdc.OnlineEvent) cdc.OnlineEvent {
			event.Image = map[string]any{"ratio": math.NaN()}
			return event
		}},
		{"unencodable image value", func(event cdc.OnlineEvent) cdc.OnlineEvent {
			event.Image = map[string]any{"callback": func() {}}
			return event
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := EncodeOnlineMessage(test.mutate(valid))
			require.Error(t, err)
		})
	}
}

func TestDecodeOnlineEnvelopeRefusesForeignWireValues(t *testing.T) {
	valid, err := EncodeOnlineMessage(onlineEnvelopeEvent(cdc.OnlineInsert, 1, map[string]any{"id": 1}))
	require.NoError(t, err)

	tests := []struct {
		name  string
		value string
	}{
		{"unknown version", `{"version":2,"id":"cdc:x","schema":"public","table":"t","key":1,"operation":"INSERT","commitLsn":"0/1","order":{"endLsn":"0/1","phase":0,"ordinal":0},"image":{"id":1}}`},
		{"trailing input", string(valid.Value) + `{}`},
		{"not json", `{`},
		{"missing order", `{"version":1,"id":"cdc:x","schema":"public","table":"t","key":1,"operation":"INSERT","commitLsn":"0/1","image":{"id":1}}`},
		{"float key", `{"version":1,"id":"cdc:x","schema":"public","table":"t","key":1.5,"operation":"INSERT","commitLsn":"0/1","order":{"endLsn":"0/1","phase":0,"ordinal":0},"image":{"id":1}}`},
		{"non LSN commit", `{"version":1,"id":"cdc:x","schema":"public","table":"t","key":1,"operation":"INSERT","commitLsn":"not-a-lsn","order":{"endLsn":"0/1","phase":0,"ordinal":0},"image":{"id":1}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := DecodeOnlineEnvelope([]byte(test.value))
			require.Error(t, err)
		})
	}
}

func TestOnlineMessageKeyIsLengthDelimitedAndStable(t *testing.T) {
	// The documented wire format is <len(schema)>:<schema><len(table)>:<table><key>.
	require.Equal(t, "6:public8:accounts7", string(OnlineMessageKey("public", "accounts", 7)))
	require.NotEqual(t, OnlineMessageKey("public", "accounts", 7), OnlineMessageKey("public", "accounts", 8))
	require.NotEqual(t, OnlineMessageKey("public", "accounts", 7), OnlineMessageKey("public", "accounts_7", 0))

	// Quoted names with dots or underscores must not be able to alias another
	// table/key pair: each component is prefixed with its own length.
	pairs := [][2]string{{"a.b", "c"}, {"a", "b.c"}, {"a", "b_c"}, {"a.b.c", ""}}
	seen := map[string]bool{}
	for _, pair := range pairs {
		key := string(OnlineMessageKey(pair[0], pair[1], 1))
		require.False(t, seen[key], "ambiguous message key %q", key)
		seen[key] = true
	}

	// Component lengths count bytes, so Unicode identifiers stay unambiguous.
	require.Equal(t, "5:şema8:tablo_ü-1", string(OnlineMessageKey("şema", "tablo_ü", -1)))
}

func TestOnlineMessageSizeCountsFramingAndHeaders(t *testing.T) {
	bare := onlineEnvelopeEvent(cdc.OnlineInsert, 1, map[string]any{"id": 1})
	message, err := EncodeOnlineMessage(bare)
	require.NoError(t, err)

	size := onlineMessageSize(message)
	require.Greater(t, size, int64(len(message.Key)+len(message.Value)), "framing and headers must be accounted")

	grown := message
	grown.Headers = append(append([]gokafka.Header(nil), message.Headers...), gokafka.Header{Key: "x", Value: []byte("y")})
	require.Greater(t, onlineMessageSize(grown), size)
}

package cdc

import (
	"context"
	"math"
	"reflect"
	"testing"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"
	"github.com/Trendyol/go-pq-cdc-kafka/config"
	"github.com/Trendyol/go-pq-cdc-kafka/kafka"
	"github.com/Trendyol/go-pq-cdc-kafka/kafka/producer"
	gokafka "github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
)

// The legacy surface must keep its exact shape: existing callers pass a void
// batch handler, a type-asserting Option and a core connector to the producer.
// Any drift here is a breaking change, so it fails at compile time.
var (
	_ Handler = func(*Message) []gokafka.Message { return nil }
	_ Option  = WithResponseHandler(kafka.NewDefaultResponseHandler(false))

	_ func(context.Context, config.Connector, Handler, ...Option) (Connector, error)                         = NewConnector
	_ func(context.Context, config.Connector, ...OnlineOption) (OnlineConnector, error)                      = NewOnlineConnector
	_ func(kafka.Client) *gokafka.Writer                                                                     = kafka.Client.Producer
	_ func(kafka.Client, *config.Connector, kafka.ResponseHandler, cdc.Connector) (producer.Producer, error) = producer.NewProducer
	_ OnlineOption                                                                                           = WithOnlineLogger(nil)
)

func TestLegacyHandlerKeepsBatchContract(t *testing.T) {
	handler := Handler(func(event *Message) []gokafka.Message {
		return []gokafka.Message{{Topic: "legacy", Value: []byte(event.Type)}}
	})
	messages := handler(&Message{Type: InsertMessage})
	require.Len(t, messages, 1)
	require.Equal(t, "legacy", messages[0].Topic)
	require.Equal(t, "INSERT", string(messages[0].Value))
}

func TestLegacyDefaultsAreUnchanged(t *testing.T) {
	cfg := config.Connector{}
	cfg.SetDefault()

	require.Equal(t, 1, cfg.Kafka.RequiredAcks, "legacy batch acks default must stay 1")
	require.Equal(t, math.MaxInt, cfg.Kafka.ProducerMaxAttempts, "legacy batch keeps unbounded retries")
	require.False(t, cfg.Kafka.SkipOversizedMessages)
	require.False(t, cfg.Kafka.AllowAutoTopicCreation)
	require.IsType(t, &gokafka.Hash{}, cfg.Kafka.GetBalancer(), "implicit/legacy balancer stays Hash")
	require.Equal(t, "1mb", cfg.Kafka.MaxMessageBytes)
	require.Equal(t, 30*time.Second, cfg.Kafka.WriteTimeout)
	require.Equal(t, 2000, cfg.Kafka.ProducerBatchSize)

	// Online bounds are additive and never leak into a legacy caller.
	require.Equal(t, 3, cfg.Kafka.Online.MaxAttempts)
	require.Equal(t, 5*time.Second, cfg.Kafka.Online.CloseTimeout)
}

func TestOnlineOptionIsNotTheLegacyOption(t *testing.T) {
	legacyOption := WithResponseHandler(kafka.NewDefaultResponseHandler(false))
	onlineOption := WithOnlineLogger(nil)

	require.NotEqual(t,
		reflect.TypeOf(legacyOption).String(),
		reflect.TypeOf(onlineOption).String(),
		"a legacy option must never be accepted as an online option",
	)
	require.Equal(t, reflect.Func, reflect.TypeOf(onlineOption).Kind())
}

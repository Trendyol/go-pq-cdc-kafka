package kafka

import (
	"log/slog"
	"os"
	"testing"

	cdcLogger "github.com/Trendyol/go-pq-cdc/logger"
	gokafka "github.com/segmentio/kafka-go"
)

func TestMain(m *testing.M) {
	cdcLogger.InitLogger(slog.Default())
	os.Exit(m.Run())
}

func TestDefaultResponseHandler_OnError_MessageSizeTooLarge_SkipEnabled_DoesNotPanic(t *testing.T) {
	handler := NewDefaultResponseHandler(true)

	defer func() {
		if recovered := recover(); recovered != nil {
			t.Fatalf("expected no panic, got %v", recovered)
		}
	}()

	handler.OnError(&ResponseHandlerContext{
		Message: &gokafka.Message{
			Topic: "test.topic",
			Key:   []byte("key"),
			Value: make([]byte, 2000000),
		},
		Err: gokafka.MessageSizeTooLarge,
	})
}

func TestDefaultResponseHandler_OnError_MessageSizeTooLarge_SkipDisabled_Panics(t *testing.T) {
	handler := NewDefaultResponseHandler(false)

	defer func() {
		if recovered := recover(); recovered == nil {
			t.Fatal("expected panic for oversized message when skip is disabled")
		}
	}()

	handler.OnError(&ResponseHandlerContext{
		Message: &gokafka.Message{Topic: "test.topic"},
		Err:     gokafka.MessageSizeTooLarge,
	})
}

func TestIsMessageTooLarge(t *testing.T) {
	if !IsMessageTooLarge(gokafka.MessageSizeTooLarge) {
		t.Fatal("expected MessageSizeTooLarge to be detected")
	}

	err := gokafka.MessageTooLargeError{
		Message: gokafka.Message{Topic: "test.topic"},
	}
	if !IsMessageTooLarge(err) {
		t.Fatal("expected MessageTooLargeError to be detected")
	}
}

package cdc

import (
	"log/slog"
	"os"
	"testing"

	cdcLogger "github.com/Trendyol/go-pq-cdc/logger"
)

func TestMain(m *testing.M) {
	cdcLogger.InitLogger(slog.Default())
	os.Exit(m.Run())
}

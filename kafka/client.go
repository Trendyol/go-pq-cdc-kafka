package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"math"
	"net"
	"os"
	"time"

	"github.com/Trendyol/go-dcp/logger"
	"github.com/Trendyol/go-pq-cdc-kafka/config"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/scram"

	"github.com/segmentio/kafka-go"
)

type Client interface {
	Producer() *kafka.Writer
}

type client struct {
	addr        net.Addr
	kafkaClient *kafka.Client
	config      *config.Connector
	transport   *kafka.Transport
	dialer      *kafka.Dialer
}

type tlsContent struct {
	config *tls.Config
	sasl   sasl.Mechanism
}

func (c *client) Producer() *kafka.Writer {
	return &kafka.Writer{
		Addr:                   kafka.TCP(c.config.Kafka.Brokers...),
		Balancer:               c.config.Kafka.GetBalancer(),
		BatchSize:              c.config.Kafka.ProducerBatchSize,
		BatchBytes:             math.MaxInt,
		BatchTimeout:           time.Nanosecond,
		MaxAttempts:            c.config.Kafka.ProducerMaxAttempts,
		ReadTimeout:            c.config.Kafka.ReadTimeout,
		WriteTimeout:           c.config.Kafka.WriteTimeout,
		RequiredAcks:           kafka.RequiredAcks(c.config.Kafka.RequiredAcks),
		Compression:            kafka.Compression(c.config.Kafka.GetCompression()),
		Transport:              c.transport,
		AllowAutoTopicCreation: c.config.Kafka.AllowAutoTopicCreation,
	}
}

func newTLSContent(
	scramUsername,
	scramPassword,
	rootCAPath,
	interCAPath string,
) (*tlsContent, error) {
	mechanism, err := scram.Mechanism(scram.SHA512, scramUsername, scramPassword)
	if err != nil {
		return nil, err
	}

	caCert, err := os.ReadFile(os.ExpandEnv(rootCAPath))
	if err != nil {
		logger.Log.Error("an error occurred while reading ca.pem file! Error: %s", err.Error())
		return nil, err
	}

	intCert, err := os.ReadFile(os.ExpandEnv(interCAPath))
	if err != nil {
		logger.Log.Error("an error occurred while reading int.pem file! Error: %s", err.Error())
		return nil, err
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	caCertPool.AppendCertsFromPEM(intCert)

	return &tlsContent{
		config: &tls.Config{
			RootCAs:    caCertPool,
			MinVersion: tls.VersionTLS12,
		},
		sasl: mechanism,
	}, nil
}

func NewClient(config *config.Connector) (Client, error) {
	addr := kafka.TCP(config.Kafka.Brokers...)

	newClient := &client{
		addr: addr,
		kafkaClient: &kafka.Client{
			Addr: addr,
		},
		config: config,
	}

	newClient.transport = &kafka.Transport{
		MetadataTTL:    config.Kafka.MetadataTTL,
		MetadataTopics: config.Kafka.MetadataTopics,
		ClientID:       config.Kafka.ClientID,
	}

	if config.Kafka.SecureConnection {
		tlsContent, err := newTLSContent(
			config.Kafka.ScramUsername,
			config.Kafka.ScramPassword,
			config.Kafka.RootCAPath,
			config.Kafka.InterCAPath,
		)
		if err != nil {
			return nil, err
		}

		newClient.transport.TLS = tlsContent.config
		newClient.transport.SASL = tlsContent.sasl

		newClient.dialer = &kafka.Dialer{
			Timeout:       10 * time.Second,
			DualStack:     true,
			TLS:           tlsContent.config,
			SASLMechanism: tlsContent.sasl,
		}
	}
	newClient.kafkaClient.Transport = newClient.transport
	return newClient, nil
}

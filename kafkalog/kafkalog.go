package kafkalog

import (
  "github.com/stealthly/siesta"
  "time"
  "log"
)

func New(brokerlist []string, topic string) *Logger {
  connectorconfig := siesta.NewConnectorConfig()
  connectorconfig.BrokerList = brokerlist
  connector, err := siesta.NewDefaultConnector(connectorconfig)
  if err != nil {
    panic(err)
  }
  config := siesta.NewProducerConfig()
  config.BrokerList = brokerlist
  config.BatchSize = 10
  config.RequiredAcks = 1
  producer := siesta.NewKafkaProducer(config, siesta.ByteSerializer, siesta.ByteSerializer, connector)

  return &Logger{
    topic:    topic,
    producer: producer,
  }
}

type Logger struct {
  producer *siesta.KafkaProducer
  topic    string
}

func (l *Logger) Write(p []byte) (n int, err error) {
  local := make([]byte, len(p), len(p))
  copy(local, p)
  l.producer.Send(&siesta.ProducerRecord{Topic: l.topic, Value: local})
  go func() {
    select {
    case metadata := <-l.producer.RecordsMetadata:
      if metadata.Error != siesta.ErrNoError {
        log.Fatal(metadata.Error)
      }
    case <-time.After(5 * time.Second):
      log.Fatal("Could not get produce response within 5 seconds")
    }
  }()

  return len(p), nil
}

func (l *Logger) Close(timeout time.Duration) {
  l.producer.Close(timeout)
}

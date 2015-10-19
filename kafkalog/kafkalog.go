package kafkalog

import (
  "github.com/stealthly/siesta"
  "time"
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
  return len(p), nil
}

func (l *Logger) Close(timeout time.Duration) {
  l.producer.Close(timeout)
}

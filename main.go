package main

import (
  "github.com/elodina/go-log-kafka/kafkalog"
  "log"
  "io"
  "os"
)

func main() {
  klog := kafkalog.New([]string{"docker:9092"}, "log")
  defer klog.Close(5000)

  logger := log.New(io.MultiWriter(os.Stdout, klog), "KLOG: ", log.Ldate|log.Ltime|log.Lshortfile)
  for i := 0; i < 1000; i++ {
    logger.Printf("Test message %d", i)
  }
}

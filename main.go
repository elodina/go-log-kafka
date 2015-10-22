package main

import (
  "github.com/elodina/go-log-kafka/kafkalog"
  _ "net/http/pprof"
  "net/http"
  "log"
  "io"
  "os"
)

func main() {
  go profileServer()

  klog := kafkalog.New([]string{"localhost:9092"}, "log")
  defer klog.Close(5000)

  logger := log.New(io.MultiWriter(os.Stdout, klog), "KLOG: ", log.Ldate|log.Ltime|log.Lshortfile)
  for i := 0; i < 1000; i++ {
    logger.Printf("Test message %d", i)
  }
}

func profileServer() {
  log.Println(http.ListenAndServe("localhost:6060", nil))
}

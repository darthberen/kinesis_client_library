package main

import (
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/suicidejack/goprimitives"
	"github.com/suicidejack/kinesis_client_library"
)

var (
	totalRecords = 0
	stats        = make(map[string]map[string]int) // [customer id][data type]count
)

func main() {
	log.SetLevel(log.DebugLevel)
	//log.SetLevel(log.InfoLevel)

	queryFreq, _ := goprimitives.NewDuration("1s")
	cfg := &kcl.Config{
		ApplicationName: "example_table",
		StreamName:      "tapdev_metadata",
		AWSDebugMode:    false,
		NumRecords:      10,
		BufferSize:      10000,
		QueryFrequency:  queryFreq,
		ReadCapacity:    10,
		WriteCapacity:   10,
		WorkerID:        "testworkerid",
	}
	consumer, err := kcl.NewStreamConsumer(cfg)
	if err != nil {
		log.WithField("error", err).Error("unable to create consumer")
		return
	}
	err = consumer.ValidateStream()
	if err != nil {
		log.WithField("error", err).Error("unable to validate stream")
		return
	}
	//return
	consumer.Start()
	go printStats()
	for data := range consumer.Consume() {
		log.WithFields(log.Fields{
			"data": string(data),
		}).Debug("got consumption data")
		totalRecords++
	}
}

func printStats() {
	log.WithFields(log.Fields{
		"totalCount": totalRecords,
	}).Info("metadata info")
	time.AfterFunc(1*time.Second, printStats)
}

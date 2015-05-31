package main

import (
	"flag"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/suicidejack/goprimitives"
	"github.com/suicidejack/kinesis_client_library"
)

var (
	totalRecords              = 0
	stats                     = make(map[string]map[string]int) // [customer id][data type]count
	streamName                string
	workerID                  string
	consumerGroup             string
	verbose                   bool
	consumerExpirationSeconds int64
)

func init() {
	flag.StringVar(&streamName, "stream-name", "tapdev_metadata", "the kinesis stream to read from")
	flag.StringVar(&workerID, "id", "testid", "the unique id for this consumer")
	flag.StringVar(&consumerGroup, "consumer-group", "example", "the name for this consumer group")
	flag.BoolVar(&verbose, "v", false, "verbose mode")
	flag.Int64Var(&consumerExpirationSeconds, "consumer-expiration-seconds", 30, "amount of time until another consumer starts processing this shard")
}

func main() {
	flag.Parse()

	if verbose {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	queryFreq, _ := goprimitives.NewDuration("1s")
	cfg := &kcl.Config{
		ConsumerGroup:             consumerGroup,
		StreamName:                streamName,
		AWSDebugMode:              false,
		NumRecords:                10,
		BufferSize:                10000,
		QueryFrequency:            queryFreq,
		ReadCapacity:              10,
		WriteCapacity:             10,
		ConsumerExpirationSeconds: consumerExpirationSeconds,
		WorkerID:                  workerID,
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
	//for range consumer.Consume() {
	for record := range consumer.Consume() {
		//log.WithFields(log.Fields{
		//	"data": string(data),
		//}).Debug("got consumption data")
		totalRecords++
		record.Checkpoint()
	}
}

func printStats() {
	log.WithFields(log.Fields{
		"totalCount": totalRecords,
	}).Info("data count info")
	time.AfterFunc(1*time.Second, printStats)
}

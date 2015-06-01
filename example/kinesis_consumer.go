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
	streamName                string
	workerID                  string
	consumerGroup             string
	iteratorType              string
	verbose                   bool
	consumerExpirationSeconds int64
	totalToConsume            int
	numRecords                int64
	bufferSize                int
	queryFreq                 *goprimitives.Duration
)

func init() {
	flag.StringVar(&streamName, "stream-name", "your_stream", "the kinesis stream to read from")
	flag.StringVar(&workerID, "id", "", "the unique id for this consumer")
	flag.StringVar(&consumerGroup, "consumer-group", "example", "the name for this consumer group")
	flag.StringVar(&iteratorType, "iterator-type", "LATEST", "valid options are LATEST or TRIM_HORIZON which is how this consumer group will initial start handling the kinesis stream")
	flag.BoolVar(&verbose, "v", false, "verbose mode")
	flag.Int64Var(&consumerExpirationSeconds, "consumer-expiration-seconds", 15, "amount of time until another consumer starts processing this shard")
	flag.IntVar(&totalToConsume, "consume-total", -1, "total number of records to count as consumed before closing consumer")
	flag.Int64Var(&numRecords, "num-records", 5000, "total number of records to consumer per iteration")
	flag.IntVar(&bufferSize, "buffer-size", 100000, "size of the internal buffer that holds records to process")
	flag.Var(queryFreq, "query-freq", "how frequently to query kinesis for records [default: 1s]")
}

func main() {
	flag.Parse()

	if verbose {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}
	if queryFreq == nil || !queryFreq.IsPositive() {
		queryFreq, _ = goprimitives.NewDuration("1s")
	}

	cfg := &kcl.Config{
		ConsumerGroup:             consumerGroup,
		StreamName:                streamName,
		AWSDebugMode:              false,
		NumRecords:                numRecords,
		BufferSize:                bufferSize,
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
	if err := consumer.ValidateStream(); err != nil {
		log.WithField("error", err).Error("unable to validate stream")
		return
	}

	consumer.Start()
	go printStats()
	for record := range consumer.Consume() {
		//log.WithFields(log.Fields{
		//	"data": string(data),
		//}).Debug("got consumption data")
		totalRecords++
		if totalRecords == totalToConsume {
			log.Error("************** SHUT DOWN SHUTDOWN SHUTDOWN (currently doesn't work) *****************************")
			consumer.Shutdown()
		}
		record.Checkpoint()
	}

	log.WithFields(log.Fields{
		"totalRecordsConsumed":        totalRecords,
		"totalToConsumeBeforeClosing": totalToConsume,
	}).Error("info consuming records")
}

func printStats() {
	log.WithFields(log.Fields{
		"totalCount": totalRecords,
	}).Info("data count info")
	time.AfterFunc(1*time.Second, printStats)
}

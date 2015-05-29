package kcl

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/aws/awsutil"
	"github.com/awslabs/aws-sdk-go/service/kinesis"
	"github.com/nu7hatch/gouuid"
	"github.com/suicidejack/goprimitives"
)

// For each shard we need to track its latest checkpointed seq num
// and periodically update its value in dynamo

// we need to periodically scan dynamo and make sure that we are still
// the shard owner or if anything as expired pick up a single shard

// Track state across multiple consumers:
// http://docs.aws.amazon.com/kinesis/latest/dev/kinesis-record-processor-ddb.html
// basically create dynamodb table (unique per application/consumer group)
// tuple includes: shard if (hash key), checkpoint, workerid, heartbeat

// describe stream
// get the shard iterator: GetShardIterator
// get the records: GetRecords
// renew shard iterator when necessary

// StreamConsumer TODO
type StreamConsumer struct {
	client                 *kinesis.Kinesis
	config                 *Config
	data                   chan []byte
	numRecordsPerIteration *int64
	dynamo                 *dynamo
}

// Config for a kinesis producer (high level, stream, etc.)
type Config struct {
	// [REQUIRED] the application's name - it will be the name of the dynamodb
	// table that tracks this applications name
	ApplicationName string `json:"applicationName"`

	// [REQUIRED] the name of stream to consume from
	StreamName string `json:"streamName"`

	// run the AWS client in debug mode
	AWSDebugMode   bool                   `json:"awsDebugMode"`
	NumRecords     int64                  `json:"numRecords"`
	BufferSize     int                    `json:"bufferSize"`
	QueryFrequency *goprimitives.Duration `json:"queryFrequency"`
	ReadCapacity   int64                  `json:"readCapacity"`
	WriteCapacity  int64                  `json:"writeCapacity"`
	// WorkerID a unique ID for this consumer
	WorkerID string `json:"workerID"`
}

func getAWSConfig(runInDebugMode bool) *aws.Config {
	logLevel := uint(0)
	if runInDebugMode {
		logLevel = 5
	}
	return aws.DefaultConfig.Merge(&aws.Config{
		LogLevel: logLevel,
	})
}

func validateConfig(opts *Config) (*Config, error) {
	if opts.NumRecords <= 0 {
		opts.NumRecords = 1
	} else if opts.NumRecords > 10000 {
		opts.NumRecords = 10000
	}
	if opts.BufferSize <= 0 {
		opts.BufferSize = 1000
	}
	if opts.ApplicationName == "" {
		return nil, fmt.Errorf("you must have an application name")
	}
	if opts.ReadCapacity <= 0 {
		opts.ReadCapacity = 10
	}
	if opts.WriteCapacity <= 0 {
		opts.WriteCapacity = 10
	}
	if opts.WorkerID == "" {
		id, err := uuid.NewV4()
		if err != nil {
			return nil, err
		}
		opts.WorkerID = id.String()
	}
	if !opts.QueryFrequency.IsPositive() {
		opts.QueryFrequency, _ = goprimitives.NewDuration("1s")
	}

	return opts, nil
}

// NewStreamConsumer TODO
func NewStreamConsumer(opts *Config) (consumer *StreamConsumer, err error) {
	if opts, err = validateConfig(opts); err != nil {
		return
	}

	dynamo := newDynamo(opts.ApplicationName, opts.ReadCapacity, opts.WriteCapacity)
	if err = dynamo.ValidateTable(); err != nil {
		return
	}

	consumer = &StreamConsumer{
		client: kinesis.New(getAWSConfig(opts.AWSDebugMode)),
		config: opts,
		data:   make(chan []byte, opts.BufferSize),
		numRecordsPerIteration: aws.Long(opts.NumRecords),
		dynamo:                 dynamo,
	}
	return
}

// ValidateStream checks if the stream exists (or even if you have a valid AWS connection)
// and returns and error if the stream does not exist
func (c *StreamConsumer) ValidateStream() error {
	// due to the way the AWS api works the stream name we give the list streams API is
	// exclusive so by trimming the last rune on the name we can sufficiently limit
	// our stream list request to the stream of interest (to avoid iteration on this request)
	a := []rune(c.config.ApplicationName)
	endingStreamName := string(a[:len(a)-1])
	input := &kinesis.ListStreamsInput{
		ExclusiveStartStreamName: aws.String(endingStreamName),
		Limit: aws.Long(1000),
	}

	if out, err := c.client.ListStreams(input); err != nil {
		return err
	} else if len(out.StreamNames) >= 1 {
		for _, stream := range out.StreamNames {
			log.WithField("streamName", stringPtrToString(stream)).Debug("found a stream")
			if stringPtrToString(stream) == c.config.StreamName {
				return nil
			}
		}
	}

	return fmt.Errorf("stream [%s] not found", c.config.StreamName)
}

// Start TODO
func (c *StreamConsumer) Start() {
	shards := c.getShards()
	log.WithField("shards", strings.Join(shards, ", ")).Info("retrieved shard IDs")
	for _, shard := range shards {
		go c.consumeFromShard(shard)
	}
}

// Consume TODO
func (c *StreamConsumer) Consume() chan []byte {
	return c.data
}

func (c *StreamConsumer) consumeFromShard(shardID string) {
	shardIterator := c.getShardIterator(shardID)
	for {
		input := &kinesis.GetRecordsInput{
			Limit:         c.numRecordsPerIteration, // can go up to 10000
			ShardIterator: shardIterator,
		}
		out, err := c.client.GetRecords(input)
		if err != nil {
			log.WithFields(log.Fields{
				"error":   err,
				"shardID": shardID,
			}).Error("StreamConsumer: unable to get records")
		} else {
			log.WithFields(log.Fields{
				"shardID":           shardID,
				"NextShardIterator": stringPtrToString(out.NextShardIterator),
				"numRecords":        len(out.Records),
			}).Debug("got records")
			shardIterator = out.NextShardIterator
			for i, record := range out.Records {
				log.WithFields(log.Fields{
					"shardID":        shardID,
					"sequenceNumber": stringPtrToString(record.SequenceNumber),
					"partitionKey":   stringPtrToString(record.PartitionKey),
					"data":           string(record.Data),
					"index":          i,
				}).Debug("got records")
				c.data <- record.Data

				curTime := time.Now().UnixNano() + (30 * 1000 * 1000 * 1000) // current time + 30s

				c.dynamo.Checkpoint(shardID, stringPtrToString(record.SequenceNumber), strconv.FormatInt(curTime, 10), c.config.WorkerID)
			}
		}
		time.Sleep(c.config.QueryFrequency.TimeDuration())
	}
}

func (c *StreamConsumer) getShardIterator(shardID string) *string {
	input := &kinesis.GetShardIteratorInput{
		ShardID:           aws.String(shardID),
		ShardIteratorType: aws.String("LATEST"),
		StreamName:        aws.String(c.config.StreamName),
	}
	out, err := c.client.GetShardIterator(input)
	if err != nil {
		log.WithField("error", err).Error("StreamConsumer: unable to get shard iterator")
		return nil
	}
	return out.ShardIterator
}

// TODO: iterate if more shards
func (c *StreamConsumer) getShards() (shards []string) {
	var shardID *string
	hasMoreShards := true

	for hasMoreShards {
		input := &kinesis.DescribeStreamInput{
			Limit:                 aws.Long(100),
			StreamName:            aws.String(c.config.StreamName),
			ExclusiveStartShardID: shardID,
		}
		out, err := c.client.DescribeStream(input)
		if err != nil {
			log.WithField("error", err).Error("StreamConsumer: unable to describe stream")
			return
		}
		if out.StreamDescription.HasMoreShards != nil && *out.StreamDescription.HasMoreShards == false {
			hasMoreShards = false
		}
		log.WithFields(log.Fields{
			"HasMoreShards": awsutil.StringValue(out.StreamDescription.HasMoreShards),
			"StreamStatus":  stringPtrToString(out.StreamDescription.StreamStatus),
		}).Info("got output")
		for i, shard := range out.StreamDescription.Shards {
			log.WithFields(log.Fields{
				"index":                       i,
				"AdjacentParentShardID":       stringPtrToString(shard.AdjacentParentShardID),
				"ParentShardID":               stringPtrToString(shard.ParentShardID),
				"ShardID":                     stringPtrToString(shard.ShardID),
				"StartingHashKeyRange":        stringPtrToString(shard.HashKeyRange.StartingHashKey),
				"EndingHashKeyRange":          stringPtrToString(shard.HashKeyRange.EndingHashKey),
				"StartingSequenceNumberRange": stringPtrToString(shard.SequenceNumberRange.StartingSequenceNumber),
				"EndingSequenceNumberRange":   stringPtrToString(shard.SequenceNumberRange.EndingSequenceNumber),
			}).Info("got output")
			shards = append(shards, stringPtrToString(shard.ShardID))
			shardID = shard.ShardID
		}
	}
	return
}

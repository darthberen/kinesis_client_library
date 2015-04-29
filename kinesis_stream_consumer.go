package kcl

import (
	"fmt"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/service/kinesis"
)

// Duration has JSON Marshaller and Unmarshaller interfaces.
type Duration time.Duration

// MarshalJSON implements the JSON Marshaller interface
func (d *Duration) MarshalJSON() ([]byte, error) {
	if d == nil {
		return []byte("null"), nil
	}
	dur := time.Duration(*d)
	return []byte(`"` + dur.String() + `"`), nil
}

// UnmarshalJSON implements the JSON Unmarshaller interface
func (d *Duration) UnmarshalJSON(b []byte) error {
	dur, err := ReadDuration(string(b))
	*d = Duration(dur)
	return err
}

func (d *Duration) String() string {
	if d == nil {
		return "<nil>"
	}
	return time.Duration(*d).String()
}

// ToTimeDuration converts a custom Duration to a normal time.Duration
func (d Duration) ToTimeDuration() time.Duration {
	return time.Duration(d)
}

// StringPtrToString converts a string pointer to a string value
func StringPtrToString(str *string) string {
	if str == nil {
		return "<nil>"
	}
	return string(*str)
}

// ReadDuration helper function for parsing time duration representations
func ReadDuration(duration string) (d time.Duration, err error) {
	// some duration strings are quoted so this removes any quoting that may have occurred
	duration = strings.Replace(duration, `"`, "", -1)
	d, err = time.ParseDuration(duration)
	return
}

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
	AWSDebugMode   bool     `json:"awsDebugMode"`
	NumRecords     int64    `json:"numRecords"`
	BufferSize     int      `json:"bufferSize"`
	QueryFrequency Duration `json:"queryFrequency"`
	ReadCapacity   int64
	WriteCapacity  int64
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

// NewStreamConsumer TODO
func NewStreamConsumer(opts *Config) (consumer *StreamConsumer, err error) {
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

	dynamo := newDynamo(opts.ApplicationName, opts.ReadCapacity, opts.WriteCapacity)
	if err = dynamo.ValidateTable(); err != nil {
		//log.WithFields(log.Fields{
		//	"error": err,
		//}).Error("validating table")
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
// TODO: this should limit it to the stream name that was given
func (c *StreamConsumer) ValidateStream() error {
	var endingStreamName *string
	input := &kinesis.ListStreamsInput{
		ExclusiveStartStreamName: endingStreamName,
		Limit: aws.Long(1000),
	}
	if out, err := c.client.ListStreams(input); err != nil {
		return err
	} else if len(out.StreamNames) >= 1 {
		for _, stream := range out.StreamNames {
			log.WithField("streamName", StringPtrToString(stream)).Debug("found a stream")
			if StringPtrToString(stream) == c.config.StreamName {
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
				"NextShardIterator": StringPtrToString(out.NextShardIterator),
				"numRecords":        len(out.Records),
			}).Debug("got records")
			shardIterator = out.NextShardIterator
			for i, record := range out.Records {
				log.WithFields(log.Fields{
					"shardID":        shardID,
					"sequenceNumber": StringPtrToString(record.SequenceNumber),
					"partitionKey":   StringPtrToString(record.PartitionKey),
					"data":           string(record.Data),
					"index":          i,
				}).Debug("got records")
				c.data <- record.Data
			}
		}
		time.Sleep(c.config.QueryFrequency.ToTimeDuration())
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

func (c *StreamConsumer) getShards() (shards []string) {
	input := &kinesis.DescribeStreamInput{
		Limit:      aws.Long(100),
		StreamName: aws.String(c.config.StreamName),
	}
	out, err := c.client.DescribeStream(input)
	if err != nil {
		log.WithField("error", err).Error("StreamConsumer: unable to describe stream")
		return
	}
	log.WithFields(log.Fields{
		"HasMoreShards": *out.StreamDescription.HasMoreShards,
		"StreamStatus":  StringPtrToString(out.StreamDescription.StreamStatus),
	}).Info("got output")
	for i, shard := range out.StreamDescription.Shards {
		log.WithFields(log.Fields{
			"index":                       i,
			"AdjacentParentShardID":       StringPtrToString(shard.AdjacentParentShardID),
			"ParentShardID":               StringPtrToString(shard.ParentShardID),
			"ShardID":                     StringPtrToString(shard.ShardID),
			"StartingHashKeyRange":        StringPtrToString(shard.HashKeyRange.StartingHashKey),
			"EndingHashKeyRange":          StringPtrToString(shard.HashKeyRange.EndingHashKey),
			"StartingSequenceNumberRange": StringPtrToString(shard.SequenceNumberRange.StartingSequenceNumber),
			"EndingSequenceNumberRange":   StringPtrToString(shard.SequenceNumberRange.EndingSequenceNumber),
		}).Info("got output")
		shards = append(shards, StringPtrToString(shard.ShardID))
	}
	return
}

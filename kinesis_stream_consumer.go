package kcl

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/awslabs/aws-sdk-go/aws"
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

// KinesisRecord todo
type KinesisRecord struct {
	Data       []byte
	Checkpoint func()
}

// TODO might have to track time so older numbers don't mess up the check pointing
func checkpoint(shardID, seqNum string, callback func(shardID, seqNum string)) func() {
	return func() {
		callback(shardID, seqNum)
	}
}

func (c *StreamConsumer) checkpoint(shardID, seqNum string) {
	c.checkpoints[shardID] = seqNum
}

// StreamConsumer TODO
type StreamConsumer struct {
	client                 *kinesis.Kinesis
	config                 *Config
	data                   chan *KinesisRecord
	numRecordsPerIteration *int64
	dynamo                 *dynamo
	ownedShards            []string
	checkpoints            map[string]string
}

// Config for a kinesis producer (high level, stream, etc.)
type Config struct {
	// [REQUIRED] the application's name - it will be the name of the dynamodb
	// table that tracks this applications name
	ConsumerGroup string `json:"ConsumerGroup"`

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
	// ConsumerExpirationSeconds the amount of time that a consumer has to checkpoint before
	// another consumer takes over consuming from that shard
	ConsumerExpirationSeconds int64 `json:"consumerExpirationSeconds"`
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
	if opts.ConsumerGroup == "" {
		return nil, fmt.Errorf("you must specify a consumer group")
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

	if opts.ConsumerExpirationSeconds <= 0 {
		opts.ConsumerExpirationSeconds = 30
	}

	return opts, nil
}

// NewStreamConsumer TODO
func NewStreamConsumer(opts *Config) (consumer *StreamConsumer, err error) {
	if opts, err = validateConfig(opts); err != nil {
		return
	}

	dynamo := newDynamo(opts.ConsumerGroup, opts.ReadCapacity, opts.WriteCapacity)
	if err = dynamo.ValidateTable(); err != nil {
		return
	}

	consumer = &StreamConsumer{
		client: kinesis.New(getAWSConfig(opts.AWSDebugMode)),
		config: opts,
		data:   make(chan *KinesisRecord, opts.BufferSize),
		numRecordsPerIteration: aws.Long(opts.NumRecords),
		dynamo:                 dynamo,
		ownedShards:            make([]string, 0),
		checkpoints:            make(map[string]string),
	}
	return
}

// ValidateStream checks if the stream exists (or even if you have a valid AWS connection)
// and returns and error if the stream does not exist
func (c *StreamConsumer) ValidateStream() error {
	// due to the way the AWS api works the stream name we give the list streams API is
	// exclusive so by trimming the last rune on the name we can sufficiently limit
	// our stream list request to the stream of interest (to avoid iteration on this request)
	a := []rune(c.config.ConsumerGroup)
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
func (c *StreamConsumer) Start() (err error) {
	var shardRecords map[string]*shardRecord
	if shardRecords, err = c.balanceShardsOnStart(); err != nil {
		return err
	}
	log.WithField("shards", strings.Join(c.ownedShards, ", ")).Info("retrieved shard IDs")
	for _, shardID := range c.ownedShards {
		expTime := time.Now().UnixNano() + (c.config.ConsumerExpirationSeconds * 1000 * 1000 * 1000) // current time + 30s

		checkpoint := ""
		if record, ok := shardRecords[shardID]; ok {
			checkpoint = record.Checkpoint
		}
		c.dynamo.Checkpoint(shardID, checkpoint, strconv.FormatInt(expTime, 10), c.config.WorkerID)
		go c.consumeFromShard(shardID)
	}
	go c.periodicCheck()
	return nil
}

// get the data from dynamo
// balance out shards evenly
func (c *StreamConsumer) balanceShardsOnStart() (shardRecords map[string]*shardRecord, err error) {
	funcName := "balanceShardsOnStart"
	var shards []string
	if shards, err = c.getShards(); err != nil {
		return
	}
	log.WithFields(log.Fields{
		"shardsFound": shards,
		"function":    funcName,
	}).Debug("results of fetching shards")

	// get data from dynamo
	shardRecords, err = c.dynamo.GetShardData(shards)
	if err != nil {
		log.WithFields(log.Fields{
			"Function": funcName,
			"error":    err,
		}).Error("unable to retrieve shard records from dynamo")
		return
	}

	c.ownedShards = c.calculateBalancedShards(shardRecords, shards)

	return
}

func (c *StreamConsumer) calculateBalancedShards(records map[string]*shardRecord, kinesisShards []string) (ownedShards []string) {
	funcName := "calculateBalancedShards"
	missingShards := identifyMissingShards(keys(records), kinesisShards)
	log.WithFields(log.Fields{
		"missingShards": missingShards,
		"Function":      funcName,
	}).Debug("identified missing shards")

	expTime := time.Now().UnixNano()
	workerCount, shardOwners, expiredShards, ownedShards := otherWorkerCount(records, c.config.WorkerID, expTime)

	if workerCount == 0 {
		ownedShards = kinesisShards
		return
	}

	totalShards := len(kinesisShards)
	numShardsToOwn := totalShards / (workerCount + 1)

	ownedShards = append(ownedShards, expiredShards...)

	log.WithFields(log.Fields{
		"workerCount":    workerCount,
		"shardOwners":    shardOwners,
		"expiredShards":  expiredShards,
		"ownedShards":    ownedShards,
		"totalShards":    totalShards,
		"numShardsToOwn": numShardsToOwn,
	}).Error("testing")

	if len(ownedShards) >= numShardsToOwn {
		return
	}

	// for each shard owner take over processing some of those shards
	for shardOwner, ownerShards := range shardOwners {
		numShardsToTake := len(ownerShards) - numShardsToOwn
		if numShardsToTake > 0 {
			ownedShards = append(ownedShards, ownerShards[:numShardsToTake]...)
			log.WithFields(log.Fields{
				"shardOwner":      shardOwner,
				"numShardsToTake": numShardsToTake,
				"numOwnerShards":  len(ownerShards),
				"ownedShards":     ownedShards,
			}).Debug("detected shard owner owns too many shards")
		}
	}
	return
}

// get shards
// check that all shards are being consumed from
//    shards should be evenly distributed across consumers
//    if any have expired - grab only 1 per iteration (except on start up?)
func (c *StreamConsumer) periodicCheck() {
	var err error
	funcName := "periodicCheck"
	for {
		time.Sleep(5 * time.Second)
		log.WithFields(log.Fields{
			"ownedShards": c.ownedShards,
			"function":    funcName,
		}).Debug("starting check")

		var kinesisShards []string
		if kinesisShards, err = c.getShards(); err != nil {
			log.WithFields(log.Fields{
				"function": funcName,
				"error":    err,
			}).Error("unable to get kinesis shards")
			continue
		}

		shardRecords, err := c.dynamo.GetShardData(kinesisShards)
		if err != nil {
			log.WithFields(log.Fields{
				"Function": funcName,
				"error":    err,
			}).Error("unable to retrieve shard records from dynamo")
			continue
		}

		ownedShards := c.calculateBalancedShards(shardRecords, kinesisShards)
		newShards := identifyMissingShards(c.ownedShards, ownedShards)
		for _, shardID := range newShards {

			expTime := time.Now().UnixNano() + (c.config.ConsumerExpirationSeconds * 1000 * 1000 * 1000) // current time + 30s
			checkpoint := ""
			if record, ok := shardRecords[shardID]; ok {
				checkpoint = record.Checkpoint
			}
			c.dynamo.Checkpoint(shardID, checkpoint, strconv.FormatInt(expTime, 10), c.config.WorkerID)
			go c.consumeFromShard(shardID)
		}
		log.WithFields(log.Fields{
			"currentlyOwnedShards": c.ownedShards,
			"newOwnedShards":       ownedShards,
			"function":             funcName,
			"lenOwnedShards":       len(c.ownedShards),
			"newShards":            newShards,
			"lenNewShards":         len(newShards),
		}).Debug("finished check rebalancing")
		c.ownedShards = ownedShards
		for _, shardID := range c.ownedShards {
			if lastSeqNum, ok := c.checkpoints[shardID]; ok {
				expTime := time.Now().UnixNano() + (c.config.ConsumerExpirationSeconds * 1000 * 1000 * 1000) // current time + 30s
				c.dynamo.Checkpoint(shardID, lastSeqNum, strconv.FormatInt(expTime, 10), c.config.WorkerID)
			}
		}

		// remove any shards we don't own
		//expTime := time.Now().UnixNano()
		//workerCount, shardOwners, expiredShards, ownedShards := otherWorkerCount(records, c.config.WorkerID, expTime)
	}
}

// Consume TODO
func (c *StreamConsumer) Consume() chan *KinesisRecord {
	return c.data
}

// TODO: check number of remaining records, if 0 then close?
func (c *StreamConsumer) consumeFromShard(shardID string) {
	log.WithFields(log.Fields{
		"shardID": shardID,
	}).Warn("STARTED consuming from shard")
	shardIterator := c.getShardIterator(shardID)
	for existsInArray(shardID, c.ownedShards) {
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
			//log.WithFields(log.Fields{
			//	"shardID": shardID,
			//	//"NextShardIterator": stringPtrToString(out.NextShardIterator),
			//	"numRecords": len(out.Records),
			//}).Debug("got records")
			shardIterator = out.NextShardIterator
			for _, record := range out.Records {
				//log.WithFields(log.Fields{
				//	"shardID":        shardID,
				//	"sequenceNumber": stringPtrToString(record.SequenceNumber),
				//	"partitionKey":   stringPtrToString(record.PartitionKey),
				//	"data":           string(record.Data),
				//	"index":          i,
				//}).Debug("got records")
				seqNum := stringPtrToString(record.SequenceNumber)
				c.data <- &KinesisRecord{
					Data:       record.Data,
					Checkpoint: checkpoint(shardID, seqNum, c.checkpoint),
				}
			}
		}

		time.Sleep(c.config.QueryFrequency.TimeDuration())
	}
	log.WithFields(log.Fields{
		"shardID": shardID,
	}).Warn("STOPPED consuming from shard")
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

func (c *StreamConsumer) getShards() (shards []string, err error) {
	var shardID *string
	hasMoreShards := true

	for hasMoreShards {
		input := &kinesis.DescribeStreamInput{
			Limit:                 aws.Long(100),
			StreamName:            aws.String(c.config.StreamName),
			ExclusiveStartShardID: shardID,
		}
		var out *kinesis.DescribeStreamOutput
		if out, err = c.client.DescribeStream(input); err != nil {
			log.WithField("error", err).Error("StreamConsumer: unable to describe stream")
			return
		}
		if out.StreamDescription.HasMoreShards != nil && *out.StreamDescription.HasMoreShards == false {
			hasMoreShards = false
		}
		//log.WithFields(log.Fields{
		//	"HasMoreShards": awsutil.StringValue(out.StreamDescription.HasMoreShards),
		//	"StreamStatus":  stringPtrToString(out.StreamDescription.StreamStatus),
		//}).Debug("got output")
		for _, shard := range out.StreamDescription.Shards {
			//log.WithFields(log.Fields{
			//	"index":                       i,
			//	"AdjacentParentShardID":       stringPtrToString(shard.AdjacentParentShardID),
			//	"ParentShardID":               stringPtrToString(shard.ParentShardID),
			//	"ShardID":                     stringPtrToString(shard.ShardID),
			//	"StartingHashKeyRange":        stringPtrToString(shard.HashKeyRange.StartingHashKey),
			//	"EndingHashKeyRange":          stringPtrToString(shard.HashKeyRange.EndingHashKey),
			//	"StartingSequenceNumberRange": stringPtrToString(shard.SequenceNumberRange.StartingSequenceNumber),
			//	"EndingSequenceNumberRange":   stringPtrToString(shard.SequenceNumberRange.EndingSequenceNumber),
			//}).Debug("got output")
			shards = append(shards, stringPtrToString(shard.ShardID))
			shardID = shard.ShardID
		}
	}
	return
}

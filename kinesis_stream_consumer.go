package kcl

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/aws/awserr"
	"github.com/awslabs/aws-sdk-go/service/kinesis"
)

// KinesisRecord TODO
type KinesisRecord struct {
	Data       []byte
	Checkpoint func()
}

// TODO might have to track time so older numbers don't mess up the check pointing
func recordCheckpoint(shardID, seqNum string, callback func(shardID, seqNum string)) func() {
	return func() {
		callback(shardID, seqNum)
	}
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

// Consume TODO
func (c *StreamConsumer) Consume() chan *KinesisRecord {
	return c.data
}

// Shutdown TODO
func (c *StreamConsumer) Shutdown() (err error) {
	log.Warn("shutting stream consumer down")
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
		c.startConsumption(shardID, shardRecords)
	}
	go c.periodicCheck()
	return nil
}

func (c *StreamConsumer) startConsumption(shardID string, shardRecords map[string]*shardRecord) {
	checkpoint := ""
	if record, ok := shardRecords[shardID]; ok {
		checkpoint = record.Checkpoint
	}
	c.checkpoints[shardID] = checkpoint
	expTime := time.Now().UnixNano() + (c.config.ConsumerExpirationSeconds * 1000 * 1000 * 1000)
	c.dynamo.Checkpoint(shardID, checkpoint, strconv.FormatInt(expTime, 10), c.config.WorkerID)
	go c.consumeFromShard(shardID, checkpoint)
}

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

	if len(expiredShards) > 0 {
		ownedShards = append(ownedShards, expiredShards[0])
	}

	log.WithFields(log.Fields{
		"workerCount":    workerCount,
		"shardOwners":    shardOwners,
		"expiredShards":  expiredShards,
		"ownedShards":    ownedShards,
		"totalShards":    totalShards,
		"numShardsToOwn": numShardsToOwn,
	}).Debug("testing")

	if len(ownedShards) >= numShardsToOwn {
		return
	}

	// for each shard owner take over processing some of those shards
assignmentLoop:
	for shardOwner, ownerShards := range shardOwners {
		numShardsToTake := len(ownerShards) - numShardsToOwn
		if numShardsToTake > 0 {
			if len(ownedShards)+numShardsToTake > numShardsToOwn {
				numShardsToTake = numShardsToOwn - len(ownedShards)
			}
			ownedShards = append(ownedShards, ownerShards[:numShardsToTake]...)
			log.WithFields(log.Fields{
				"shardOwner":      shardOwner,
				"numShardsToTake": numShardsToTake,
				"numOwnerShards":  len(ownerShards),
				"ownedShards":     ownedShards,
			}).Debug("detected shard owner owns too many shards")
		}

		if len(ownedShards) >= numShardsToOwn {
			break assignmentLoop
		}
	}

	return
}

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
			c.startConsumption(shardID, shardRecords)
		}
		log.WithFields(log.Fields{
			"currentlyOwnedShards": c.ownedShards,
			"newOwnedShards":       ownedShards,
			"function":             funcName,
			"lenOwnedShards":       len(c.ownedShards),
			"newShards":            newShards,
			"lenNewShards":         len(newShards),
		}).Debug("finished rebalancing")
		c.ownedShards = ownedShards

		c.dynamoCheckpoint()
	}
}

func (c *StreamConsumer) dynamoCheckpoint() {
	for _, shardID := range c.ownedShards {
		if lastSeqNum, ok := c.checkpoints[shardID]; ok {
			expTime := time.Now().UnixNano() + (c.config.ConsumerExpirationSeconds * 1000 * 1000 * 1000)
			c.dynamo.Checkpoint(shardID, lastSeqNum, strconv.FormatInt(expTime, 10), c.config.WorkerID)
		}
	}
}

// TODO: check number of remaining records, if 0 then close?
func (c *StreamConsumer) consumeFromShard(shardID, checkpoint string) {
	log.WithFields(log.Fields{
		"shardID": shardID,
	}).Warn("STARTED consuming from shard")

	shardIterator := c.getShardIterator(shardID, checkpoint)
	var err error
	numErrors := 0
	numRetries := 3

processLoop:
	for existsInArray(shardID, c.ownedShards) {
		if shardIterator, err = c.getShardData(shardID, shardIterator); err != nil {
			if awsErr, ok := err.(awserr.Error); !ok {
				log.WithFields(log.Fields{
					"errorCode":    awsErr.Code(),
					"errorMessage": awsErr.Message(),
					"origErr":      awsErr.OrigErr(),
					"shardID":      shardID,
				}).Error("StreamConsumer: got aws error")
				numErrors++
			} else {
				log.WithFields(log.Fields{
					"error":   err,
					"shardID": shardID,
				}).Error("StreamConsumer: unable to get records")
				numErrors++
				if numErrors == numRetries {
					break processLoop
				}
			}
		} else {
			numErrors = 0
		}
		time.Sleep(c.config.QueryFrequency.TimeDuration())
	}

	log.WithFields(log.Fields{
		"shardID": shardID,
	}).Warn("STOPPED consuming from shard")
}

func (c *StreamConsumer) getShardData(shardID string, shardIterator *string) (*string, error) {
	input := &kinesis.GetRecordsInput{
		Limit:         c.numRecordsPerIteration,
		ShardIterator: shardIterator,
	}
	out, err := c.client.GetRecords(input)
	if err != nil {
		return nil, err
	}

	shardIterator = out.NextShardIterator
	for _, record := range out.Records {
		seqNum := stringPtrToString(record.SequenceNumber)
		c.data <- &KinesisRecord{
			Data:       record.Data,
			Checkpoint: recordCheckpoint(shardID, seqNum, c.checkpoint),
		}
	}

	return shardIterator, nil
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
		for _, shard := range out.StreamDescription.Shards {
			shards = append(shards, stringPtrToString(shard.ShardID))
			shardID = shard.ShardID
		}
	}
	return
}

// TODO: test that trim horizon works
func (c *StreamConsumer) getShardIterator(shardID, checkpoint string) *string {
	input := &kinesis.GetShardIteratorInput{
		ShardID:    aws.String(shardID),
		StreamName: aws.String(c.config.StreamName),
	}
	if checkpoint != "" {
		log.Error("using AFTER_SEQUENCE_NUMBER")
		input.ShardIteratorType = aws.String("AFTER_SEQUENCE_NUMBER")
		input.StartingSequenceNumber = aws.String(checkpoint)
	} else {
		log.Errorf("using %s", c.config.IteratorType)
		input.ShardIteratorType = aws.String(c.config.IteratorType)
	}
	out, err := c.client.GetShardIterator(input)
	if err != nil {
		log.WithField("error", err).Error("StreamConsumer: unable to get shard iterator")
		return nil
	}
	return out.ShardIterator
}

func (c *StreamConsumer) checkpoint(shardID, seqNum string) {
	c.checkpoints[shardID] = seqNum
}

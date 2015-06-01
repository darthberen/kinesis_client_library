package kcl

import (
	"fmt"

	"github.com/nu7hatch/gouuid"
	"github.com/suicidejack/goprimitives"
)

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
	// IteratorType valid options are "LATEST" or "TRIM_HORIZON" defaults to "LATEST"
	IteratorType string `json:"iteratorType"`
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
	if opts.IteratorType != "LATEST" || opts.IteratorType != "TRIM_HORIZON" {
		opts.IteratorType = "LATEST"
	}

	return opts, nil
}

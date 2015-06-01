package kcl

import (
	log "github.com/Sirupsen/logrus"
	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/aws/awsutil"
	"github.com/awslabs/aws-sdk-go/service/dynamodb"
)

func getAWSConfig(runInDebugMode bool) *aws.Config {
	logLevel := uint(0)
	if runInDebugMode {
		logLevel = 5
	}
	return aws.DefaultConfig.Merge(&aws.Config{
		LogLevel: logLevel,
	})
}

func stringPtrToString(str *string) string {
	if str == nil {
		return "<nil>"
	}
	return string(*str)
}

func identifyMissingShards(dynamoShards, existingShards []string) (missingShards []string) {
	for _, shard := range existingShards {
		if !existsInArray(shard, dynamoShards) {
			missingShards = append(missingShards, shard)
		}
	}
	return
}

func existsInArray(value string, array []string) bool {
	for _, arrayValue := range array {
		if arrayValue == value {
			return true
		}
	}
	return false
}

func keys(records map[string]*shardRecord) (keys []string) {
	for key := range records {
		keys = append(keys, key)
	}
	return
}

func otherWorkerCount(records map[string]*shardRecord, workerID string, expTime int64) (count int, shardOwners map[string][]string, expiredShards, ownedShards []string) {
	seenWorkers := make(map[string]bool)
	shardOwners = make(map[string][]string)
	for shardID, record := range records {
		if record.WorkerID != workerID && record.LeaseExpiration > expTime {
			if _, ok := seenWorkers[record.WorkerID]; !ok {
				seenWorkers[record.WorkerID] = true
				shardOwners[record.WorkerID] = append(shardOwners[record.WorkerID], shardID)
				count++
			} else {
				shardOwners[record.WorkerID] = append(shardOwners[record.WorkerID], shardID)
			}
		} else if record.WorkerID == workerID {
			ownedShards = append(ownedShards, shardID)
		} else if record.LeaseExpiration < expTime {
			expiredShards = append(expiredShards, shardID)
		}
	}

	log.WithFields(log.Fields{
		"count":         count,
		"shardOwners":   shardOwners,
		"seenWorkers":   seenWorkers,
		"ownedShards":   ownedShards,
		"expiredShards": expiredShards,
		"function":      "otherWorkerCount",
	}).Debug("grouped shards")
	return
}

func isValidTableSchema(output *dynamodb.DescribeTableOutput) (validSchema bool) {
	validSchema = true

	switch {
	case output == nil:
		log.Debug("findTable: nil output")
		validSchema = false
	case output.Table == nil:
		log.Debug("findTable: nil output.Table")
		validSchema = false
	case len(output.Table.AttributeDefinitions) != 1:
		log.Debug("findTable: should only be 1 attribute definition")
		validSchema = false
	case stringPtrToString(output.Table.AttributeDefinitions[0].AttributeName) != "shard_id":
		log.Debugf("findTable: attribute name is not 'shard_id' found %s", stringPtrToString(output.Table.AttributeDefinitions[0].AttributeName))
		validSchema = false
	case awsutil.StringValue(output.Table.AttributeDefinitions[0].AttributeType) != `"S"`:
		log.Debugf("findTable: 'shard_id' attribute is not a string it is a %s type", awsutil.StringValue(output.Table.AttributeDefinitions[0].AttributeType))
		validSchema = false
	case len(output.Table.KeySchema) != 1:
		log.Debug("findTable: should only be 1 key schema definition")
		validSchema = false
	case awsutil.StringValue(output.Table.KeySchema[0].AttributeName) != `"shard_id"`:
		log.Debugf("findTable: key schema attribute name is not 'shard_id' found %s", awsutil.StringValue(output.Table.KeySchema[0].AttributeName))
		validSchema = false
	case awsutil.StringValue(output.Table.KeySchema[0].KeyType) != `"HASH"`:
		log.Debugf("findTable: key schema key type is not 'HASH' found %s", awsutil.StringValue(output.Table.KeySchema[0].KeyType))
		validSchema = false
	}

	return
}

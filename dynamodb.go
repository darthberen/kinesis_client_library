package kcl

import (
	"fmt"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/aws/awserr"
	"github.com/awslabs/aws-sdk-go/aws/awsutil"
	"github.com/awslabs/aws-sdk-go/service/dynamodb"
)

type shardRecord struct {
	ShardID         string
	Checkpoint      string
	LeaseExpiration int64
	WorkerID        string
}

func (s shardRecord) String() string {
	return fmt.Sprintf("ShardID: %s, Checkpoint: %s, LeaseExpiration: %d, WorkerID: %s",
		s.ShardID,
		s.Checkpoint,
		s.LeaseExpiration,
		s.WorkerID,
	)
}

// DynamoDBManager TODO
type dynamo struct {
	db            *dynamodb.DynamoDB
	tableName     string
	readCapacity  int64
	writeCapacity int64
}

// NewDynamoDBManager TODO (delete)
var NewDynamoDBManager = newDynamo

func newDynamo(name string, readCapacity, writeCapacity int64) *dynamo {
	cfg := aws.DefaultConfig
	return &dynamo{
		db:            dynamodb.New(cfg),
		tableName:     name,
		readCapacity:  readCapacity,
		writeCapacity: writeCapacity,
	}
}

func (d *dynamo) ValidateTable() (err error) {
	err = d.findTable()
	if awserr, ok := err.(awserr.Error); ok {
		log.WithField("error", awserr).Error("awserror: unable to describe table")
		if awserr.Code() == "ResourceNotFoundException" {
			log.Error("we should create the table here")
			err = d.createTable()
		}
	} else {
	}
	return
}

func (d *dynamo) findTable() error {
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(d.tableName),
	}
	output, err := d.db.DescribeTable(input)
	if err != nil {
		return err
	}
	if !isValidTableSchema(output) {
		return fmt.Errorf("dynamo: invalid table schema")
	}
	return nil
}

func isValidTableSchema(output *dynamodb.DescribeTableOutput) (validSchema bool) {
	validSchema = true
	fmt.Println(awsutil.StringValue(output))
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

func (d *dynamo) createTable() (err error) {
	tableDefinition := &dynamodb.CreateTableInput{
		TableName:            aws.String(d.tableName),
		AttributeDefinitions: make([]*dynamodb.AttributeDefinition, 1, 1),
		KeySchema:            make([]*dynamodb.KeySchemaElement, 1, 1),
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Long(d.readCapacity),
			WriteCapacityUnits: aws.Long(d.writeCapacity),
		},
	}
	tableDefinition.KeySchema[0] = &dynamodb.KeySchemaElement{
		AttributeName: aws.String("shard_id"),
		KeyType:       aws.String("HASH"),
	}
	tableDefinition.AttributeDefinitions[0] = &dynamodb.AttributeDefinition{
		AttributeName: aws.String("shard_id"),
		AttributeType: aws.String("S"),
	}
	var out *dynamodb.CreateTableOutput
	out, err = d.db.CreateTable(tableDefinition)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
			"out":   out,
		}).Error("unable to create table")
		return
	}
	if out != nil && out.TableDescription != nil {
		log.WithFields(log.Fields{
			"TableStatus": stringPtrToString(out.TableDescription.TableStatus),
			"TableName":   d.tableName,
		}).Debug("created dynamodb table")
	}

	d.validateTableCreated()

	return
}

// blocks until the table status comes back as "ACTIVE"
func (d *dynamo) validateTableCreated() {
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(d.tableName),
	}
	isActive := false

	for !isActive {
		time.Sleep(1 * time.Second)
		if out, err := d.db.DescribeTable(input); err == nil {
			log.WithField("status", awsutil.StringValue(out.Table.TableStatus)).Debug("got describe table output")
			if stringPtrToString(out.Table.TableStatus) == "ACTIVE" {
				isActive = true
			}
		}
	}
}
func (d *dynamo) Checkpoint(shardID, seqNum, leaseExpiration, workerID string) (err error) {
	attributes := map[string]*dynamodb.AttributeValue{
		"shard_id": &dynamodb.AttributeValue{
			S: aws.String(shardID),
		},
		"checkpoint": &dynamodb.AttributeValue{
			S: aws.String(seqNum),
		},
		"lease_expiration": &dynamodb.AttributeValue{
			N: aws.String(leaseExpiration),
		},
		"worker_id": &dynamodb.AttributeValue{
			S: aws.String(workerID),
		},
	}
	input := &dynamodb.PutItemInput{
		TableName: aws.String(d.tableName),
		Item:      &attributes,
	}
	//var out *dynamodb.PutItemOutput
	if _, err = d.db.PutItem(input); err == nil {
		//log.WithFields(log.Fields{
		//	"out": out,
		//}).Debug("put an item")
	}
	return
}

func (d *dynamo) GetShardData(shards []string) (shardRecords map[string]*shardRecord, err error) {
	funcName := "GetShardData"

	// form the request for the records
	keys := make([]*map[string]*dynamodb.AttributeValue, len(shards), len(shards))
	for i, shard := range shards {
		keys[i] = &map[string]*dynamodb.AttributeValue{
			"shard_id": &dynamodb.AttributeValue{
				S: aws.String(shard),
			},
		}
	}

	input := &dynamodb.BatchGetItemInput{
		RequestItems: &map[string]*dynamodb.KeysAndAttributes{
			d.tableName: &dynamodb.KeysAndAttributes{
				Keys: keys,
				/*
					[]*map[string]*dynamodb.AttributeValue{
						&map[string]*dynamodb.AttributeValue{
							"shard_id": &dynamodb.AttributeValue{
								S: aws.String("shardId-000000000000"),
							},
						},
					},
				*/
				ProjectionExpression: aws.String("shard_id,checkpoint,lease_expiration,worker_id"),
				ConsistentRead:       aws.Boolean(true),
			},
		},
	}
	out, err := d.db.BatchGetItem(input)
	if out, err = d.db.BatchGetItem(input); err != nil {
		log.WithFields(log.Fields{
			"error":    err,
			"function": funcName,
		}).Error("unable to batch get items")
	}

	return d.ParseShardData(out)
}

// TODO: need something for unprocessed shards
func (d *dynamo) ParseShardData(resp *dynamodb.BatchGetItemOutput) (shardRecords map[string]*shardRecord, err error) {
	funcName := "ParseShardData"
	if resp == nil {
		log.WithField("function", funcName).Error("resp is nil")
		return
	}
	if resp.Responses == nil {
		log.WithField("function", funcName).Error("resp.Responses is nil")
		return
	}
	var records []*map[string]*dynamodb.AttributeValue
	var ok bool
	if records, ok = (*resp.Responses)[d.tableName]; !ok {
		log.WithField("function", funcName).Error("could not find table")
		return
	}

	if len(records) == 0 {
		log.WithFields(log.Fields{
			"function": funcName,
		}).Debug("there are no records in dynamodb")
	}

	shardRecords = make(map[string]*shardRecord)
	for _, record := range records {
		shardID := stringPtrToString((*record)["shard_id"].S)
		leaseExpiration, _ := strconv.ParseInt(stringPtrToString((*record)["lease_expiration"].N), 10, 64)
		shardRecords[shardID] = &shardRecord{
			ShardID:         shardID,
			Checkpoint:      stringPtrToString((*record)["checkpoint"].S),
			LeaseExpiration: leaseExpiration,
			WorkerID:        stringPtrToString((*record)["worker_id"].S),
		}
	}

	//log.WithFields(log.Fields{
	//	"resp":         awsutil.StringValue(resp),
	//	"shardRecords": shardRecords,
	//}).Debug("batch retrieved items")
	return
}

package kcl

import (
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/aws/awserr"
	"github.com/awslabs/aws-sdk-go/aws/awsutil"
	"github.com/awslabs/aws-sdk-go/service/dynamodb"
)

// DynamoDBManager TODO
type dynamo struct {
	db            *dynamodb.DynamoDB
	tableName     string
	readCapacity  int64
	writeCapacity int64
}

// NewDynamoDBManager TODO
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
		}).Info("created table")
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
	var out *dynamodb.PutItemOutput
	if out, err = d.db.PutItem(input); err == nil {
		log.WithFields(log.Fields{
			"out": out,
		}).Info("put an item")
	}
	return
}

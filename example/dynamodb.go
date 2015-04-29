package main

import (
	log "github.com/Sirupsen/logrus"
	"github.com/awslabs/aws-sdk-go/aws"
	"github.com/awslabs/aws-sdk-go/service/dynamodb"
	kcl "github.com/suicidejack/kinesis_client_library"
)

/*
shardID: primary key, the unique kinesis shard ID
workerID: the unique id of the worker processing this shard
heartbeat: epoch time (in ms) of the last time this shard was processed by a worker
checkpoint: the last confirmed sequence number that was processed
*/
func main() {
	putItem()
}

func putItem() {
	manager := kcl.NewDynamoDBManager()
	attributes := map[string]*dynamodb.AttributeValue{
		"shard_id": &dynamodb.AttributeValue{
			S: aws.String("shard_00001"),
		},
		"checkpoint": &dynamodb.AttributeValue{
			S: aws.String("1092384718273612349876123671234"),
		},
		"lease_expiration": &dynamodb.AttributeValue{
			N: aws.String("12091987123573"),
		},
		"worker_id": &dynamodb.AttributeValue{
			S: aws.String("askshjdf-1sdflkj-234sd-skdfjh234"),
		},
	}
	input := &dynamodb.PutItemInput{
		TableName: aws.String("example_table"),
		Item:      &attributes,
	}
	out, err := manager.DB.PutItem(input)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
			"out":   out,
		}).Error("unable to put item")
	}
	if out != nil {
		log.WithFields(log.Fields{
			"out": out,
		}).Info("put an item")
	}
}

func createTable() {
	manager := kcl.NewDynamoDBManager()
	tableDefinition := &dynamodb.CreateTableInput{
		AttributeDefinitions: make([]*dynamodb.AttributeDefinition, 1, 1),
		TableName:            aws.String("example_table"),
		KeySchema:            make([]*dynamodb.KeySchemaElement, 1, 1),
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Long(10),
			WriteCapacityUnits: aws.Long(10),
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
	out, err := manager.DB.CreateTable(tableDefinition)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
			"out":   out,
		}).Error("unable to create table")
	}
	if out != nil && out.TableDescription != nil {
		log.WithFields(log.Fields{
			"TableStatus": kcl.StringPtrToString(out.TableDescription.TableStatus),
		}).Info("created table")
	}
}

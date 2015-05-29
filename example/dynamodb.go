package main

import (
	log "github.com/Sirupsen/logrus"
	kcl "github.com/suicidejack/kinesis_client_library"
)

/*
shardID: primary key, the unique kinesis shard ID
workerID: the unique id of the worker processing this shard
heartbeat: epoch time (in ms) of the last time this shard was processed by a worker
checkpoint: the last confirmed sequence number that was processed
*/
func dynamoMain() {
	manager := kcl.NewDynamoDBManager("example_table", 10, 10)
	if err := manager.ValidateTable(); err != nil {
		log.WithField("error", err).Error("got an error when validating table")
		return
	}

	if err := manager.Checkpoint("shard-0001", "1234567890", "20398423048", "1"); err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("unable to put item")
	}
	log.Info("completed all operations successfully")
}

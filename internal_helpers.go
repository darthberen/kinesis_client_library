package kcl

import log "github.com/Sirupsen/logrus"

func stringPtrToString(str *string) string {
	if str == nil {
		return "<nil>"
	}
	return string(*str)
}

// TODO: can make this more efficient if necessary
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
		//log.WithFields(log.Fields{
		//	"workerID":        record.WorkerID,
		//	"leaseExpiration": record.LeaseExpiration,
		//	"expTime":         expTime,
		//}).Info("finding other workers")
		if record.WorkerID != workerID && record.LeaseExpiration > expTime {
			if _, ok := seenWorkers[record.WorkerID]; !ok {
				seenWorkers[record.WorkerID] = true
				shardOwners[record.WorkerID] = append(shardOwners[record.WorkerID], shardID)
				count++
			} else {
				shardOwners[record.WorkerID] = append(shardOwners[record.WorkerID], shardID)
			}
		} else if record.LeaseExpiration < expTime {
			// expired shards that this worker does not own
			expiredShards = append(expiredShards, shardID)
		} else if record.WorkerID == workerID {
			// shards that this worker owns
			ownedShards = append(ownedShards, shardID)
		}
	}
	log.WithFields(log.Fields{
		"count":         count,
		"shardOwners":   shardOwners,
		"seenWorkers":   seenWorkers,
		"ownedShards":   ownedShards,
		"expiredShards": expiredShards,
	}).Error("testing shard things")
	return
}

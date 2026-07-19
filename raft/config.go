package main

import (
	"time"
)

type peer struct {
	Id      uint8
	Address string
}
type config struct {
	// Time until a new heartbeat has to be send
	HeartbeatTimeout time.Duration
	// Time to resent entry to followers that did not answer appendEntry request
	CommitTimeout time.Duration
	// Time between Join retries for a follower that hasn't heard back from the leader yet
	JoinRetryTimeout time.Duration
}

//	close       same datacenter, nodes as close as possible (depends on cloud provider)
//	datacenter  same datacenter
//	zone        different zones, same region
//	region      different regions
//
// TODO: Might not be perfectly adjusted yet
var configProfiles = map[string]config{
	"close": {
		HeartbeatTimeout: 100 * time.Millisecond,
		CommitTimeout:    500 * time.Millisecond,
		JoinRetryTimeout: 200 * time.Millisecond,
	},
	"datacenter": {
		HeartbeatTimeout: 200 * time.Millisecond,
		CommitTimeout:    1000 * time.Millisecond,
		JoinRetryTimeout: 500 * time.Millisecond,
	},
	"zone": {
		HeartbeatTimeout: 300 * time.Millisecond,
		CommitTimeout:    1000 * time.Millisecond,
		JoinRetryTimeout: 500 * time.Millisecond,
	},
	"region": {
		HeartbeatTimeout: 1000 * time.Millisecond,
		CommitTimeout:    2000 * time.Millisecond,
		JoinRetryTimeout: 2000 * time.Millisecond,
	},
}

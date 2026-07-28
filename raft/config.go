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
	// Time between Join retries for a follower that hasn't heard back from the leader yet
	JoinRetryTimeout time.Duration
}

//	close       same datacenter, nodes as close as possible (depends on cloud provider)
//	datacenter  same datacenter
//	zone        different zones, same region
//	region      different regions
//
// TODO: The Heartbeat timeouts have to be adjusted before aws/gcp testing!
var configProfiles = map[string]config{
	"close": {
		HeartbeatTimeout: 10 * time.Millisecond,
		JoinRetryTimeout: 200 * time.Millisecond,
	},
	"datacenter": {
		HeartbeatTimeout: 25 * time.Millisecond,
		JoinRetryTimeout: 500 * time.Millisecond,
	},
	"zone": {
		HeartbeatTimeout: 100 * time.Millisecond,
		JoinRetryTimeout: 500 * time.Millisecond,
	},
	"region": {
		HeartbeatTimeout: 1000 * time.Millisecond,
		JoinRetryTimeout: 2000 * time.Millisecond,
	},
}

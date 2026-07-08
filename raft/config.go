package main

import (
	"time"
)

type peer struct {
	Id     int
	Adress string
}
type config struct {
	// Time until a new heartbeat has to be send
	HeartbeatTimeout time.Duration
	// Time until a new election should be started, i.e. become a candidate
	ElectionTimeout time.Duration
	// Time to resent entry to followers that did not answer appendEntry request
	CommitTimeout time.Duration

	Peers []peer
}

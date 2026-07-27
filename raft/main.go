package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "Leader":
		runLeader(os.Args[2:])
	case "Follower":
		runFollower(os.Args[2:])
	default:
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("usage:")
	fmt.Println("raft Leader <id> <raft-address:port> <client-address:port> <udp|tcp> [config flags]")
	fmt.Println("raft Follower <id> <raft-address:port> <leader-raft-address:port> <udp|tcp> [config flags]")
	fmt.Println()
	fmt.Println("config flags (a -profile preset first, explicit flags override single values):")
	fmt.Println("  -profile <name>       deployment scenario: " + strings.Join(profileNames(), ", ") + " (default datacenter)")
	fmt.Println("  -heartbeat <dur>      override heartbeat interval, e.g. 200ms")
	fmt.Println("  -join-retry <dur>     override join retry interval")
}

func profileNames() []string {
	names := make([]string, 0, len(configProfiles))
	for name := range configProfiles {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func parseConfig(flagArgs []string) config {
	fs := flag.NewFlagSet("config", flag.ExitOnError)
	profile := fs.String("profile", "datacenter", "deployment scenario: "+strings.Join(profileNames(), ", "))
	heartbeat := fs.Duration("heartbeat", 0, "override heartbeat interval")
	joinRetry := fs.Duration("join-retry", 0, "override join retry interval")
	fs.Parse(flagArgs)

	cfg, ok := configProfiles[*profile]
	if !ok {
		fmt.Printf("unknown profile %q, available: %s\n", *profile, strings.Join(profileNames(), ", "))
		os.Exit(1)
	}

	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "heartbeat":
			cfg.HeartbeatTimeout = *heartbeat
		case "join-retry":
			cfg.JoinRetryTimeout = *joinRetry
		}
	})

	fmt.Printf("config: profile=%s heartbeat=%s join-retry=%s\n",
		*profile, cfg.HeartbeatTimeout, cfg.JoinRetryTimeout)

	return cfg
}

func startNode(id uint8, raftAddress string, t string, cfg config) *RaftNode {
	var transport Transport
	var err error
	switch t {
	case "udp":
		transport, err = setupUDPTransport(raftAddress, 1024)
		if err != nil {
			panic(err)
		}
	case "tcp":
		transport, err = setupTCPTransport(raftAddress, 1024)
		if err != nil {
			panic(err)
		}
	default:
		printUsage()
		os.Exit(1)
	}

	stateMachine := &kvFsm{db: &sync.Map{}}
	return createNode(id, transport, cfg, stateMachine)
}

func runLeader(args []string) {
	if len(args) < 4 {
		printUsage()
		os.Exit(1)
	}
	id, err := strconv.Atoi(args[0])
	if err != nil {
		fmt.Println("Provide an int as ID!")
		return
	}
	raftAddress := args[1]
	clientAddress := args[2]
	transport := args[3]
	cfg := parseConfig(args[4:])

	node := startNode(uint8(id), raftAddress, transport, cfg)
	node.makeLeader()

	if _, err := setupClientServer(clientAddress, node); err != nil {
		panic(err)
	}

	fmt.Printf("leader %d: raft=%s client=%s\n", id, raftAddress, clientAddress)
	select {}
}

func runFollower(args []string) {
	if len(args) < 4 {
		printUsage()
		os.Exit(1)
	}
	id, err := strconv.Atoi(args[0])
	if err != nil {
		fmt.Println("Provide an int as ID!")
		return
	}
	raftAddress := args[1]
	leaderAddress := args[2]
	transport := args[3]
	cfg := parseConfig(args[4:])

	node := startNode(uint8(id), raftAddress, transport, cfg)
	node.makeFollower(leaderAddress)

	fmt.Printf("follower %d: raft=%s leader=%s\n", id, raftAddress, leaderAddress)
	select {}
}

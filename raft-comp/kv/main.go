package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

// Host application that runs the unmodified hashicorp/raft library with the
// same map-based state machine and the same client protocol as the own
// implementation, as a comparison baseline for benchmarks.
//
// Cluster formation mirrors the own implementation's "leader knows the
// cluster" model: the leader bootstraps itself and then adds the followers
// passed on the command line as voters. Followers just start and wait to be
// contacted.

// compConfig holds the scenario-dependent knobs. Constraints enforced by
// hashicorp's ValidateConfig: ElectionTimeout >= HeartbeatTimeout,
// LeaderLeaseTimeout <= HeartbeatTimeout, all >= 5ms, CommitTimeout >= 1ms.
type compConfig struct {
	HeartbeatTimeout   time.Duration
	ElectionTimeout    time.Duration
	LeaderLeaseTimeout time.Duration
	CommitTimeout      time.Duration
	// RPCTimeout bounds a single RPC: the UDP transport's request/response
	// timeout, and the NetworkTransport (TCP) I/O timeout. Must comfortably
	// exceed the scenario's worst-case RTT, or slow-but-alive peers count
	// as failures.
	RPCTimeout time.Duration
}

// configProfiles maps a deployment scenario to a preset. Same profile names
// and RTT assumptions as the own implementation (raft/config.go), so a
// scenario keyword means the same environment in both systems.
var configProfiles = map[string]compConfig{
	"rack": {
		HeartbeatTimeout:   100 * time.Millisecond,
		ElectionTimeout:    200 * time.Millisecond,
		LeaderLeaseTimeout: 100 * time.Millisecond,
		CommitTimeout:      10 * time.Millisecond,
		RPCTimeout:         250 * time.Millisecond,
	},
	"datacenter": {
		HeartbeatTimeout:   200 * time.Millisecond,
		ElectionTimeout:    400 * time.Millisecond,
		LeaderLeaseTimeout: 200 * time.Millisecond,
		CommitTimeout:      20 * time.Millisecond,
		RPCTimeout:         500 * time.Millisecond,
	},
	"zone": {
		HeartbeatTimeout:   300 * time.Millisecond,
		ElectionTimeout:    600 * time.Millisecond,
		LeaderLeaseTimeout: 300 * time.Millisecond,
		CommitTimeout:      50 * time.Millisecond,
		RPCTimeout:         1000 * time.Millisecond,
	},
	"region": {
		HeartbeatTimeout:   1000 * time.Millisecond,
		ElectionTimeout:    2000 * time.Millisecond,
		LeaderLeaseTimeout: 500 * time.Millisecond,
		CommitTimeout:      100 * time.Millisecond,
		RPCTimeout:         3000 * time.Millisecond,
	},
}

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
	fmt.Println("  kvcomp Leader   <id> <raft-addr:port> <client-addr:port> <udp|tcp> <follower-id>=<follower-raft-addr:port> ... [config flags]")
	fmt.Println("  kvcomp Follower <id> <raft-addr:port> <udp|tcp> [config flags]")
	fmt.Println()
	fmt.Println("config flags (a -profile preset first, explicit flags override single values):")
	fmt.Println("  -profile <name>      deployment scenario: " + strings.Join(profileNames(), ", ") + " (default datacenter)")
	fmt.Println("  -heartbeat <dur>     override heartbeat timeout")
	fmt.Println("  -election <dur>      override election timeout (must be >= heartbeat)")
	fmt.Println("  -lease <dur>         override leader lease timeout (must be <= heartbeat)")
	fmt.Println("  -commit-timeout <dur> override commit timeout")
	fmt.Println("  -rpc-timeout <dur>   override per-RPC timeout (UDP request/response, TCP I/O)")
}

func profileNames() []string {
	names := make([]string, 0, len(configProfiles))
	for name := range configProfiles {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// splitFlags separates positional arguments from config flags: everything
// from the first "-"-prefixed argument onward is treated as flags. Needed
// because the Leader role takes a variable number of follower arguments.
func splitFlags(args []string) (positionals []string, flagArgs []string) {
	for i, arg := range args {
		if strings.HasPrefix(arg, "-") {
			return args[:i], args[i:]
		}
	}
	return args, nil
}

func parseConfig(flagArgs []string) compConfig {
	fs := flag.NewFlagSet("config", flag.ExitOnError)
	profile := fs.String("profile", "datacenter", "deployment scenario: "+strings.Join(profileNames(), ", "))
	heartbeat := fs.Duration("heartbeat", 0, "override heartbeat timeout")
	election := fs.Duration("election", 0, "override election timeout")
	lease := fs.Duration("lease", 0, "override leader lease timeout")
	commitTimeout := fs.Duration("commit-timeout", 0, "override commit timeout")
	rpcTimeout := fs.Duration("rpc-timeout", 0, "override per-RPC timeout")
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
		case "election":
			cfg.ElectionTimeout = *election
		case "lease":
			cfg.LeaderLeaseTimeout = *lease
		case "commit-timeout":
			cfg.CommitTimeout = *commitTimeout
		case "rpc-timeout":
			cfg.RPCTimeout = *rpcTimeout
		}
	})

	fmt.Printf("config: profile=%s heartbeat=%s election=%s lease=%s commit-timeout=%s rpc-timeout=%s\n",
		*profile, cfg.HeartbeatTimeout, cfg.ElectionTimeout, cfg.LeaderLeaseTimeout, cfg.CommitTimeout, cfg.RPCTimeout)

	return cfg
}

func newRaftNode(id string, raftAddr string, proto string, cfg compConfig) (*raft.Raft, *kvFsm, raft.Transport) {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(id)
	config.HeartbeatTimeout = cfg.HeartbeatTimeout
	config.ElectionTimeout = cfg.ElectionTimeout
	config.LeaderLeaseTimeout = cfg.LeaderLeaseTimeout
	config.CommitTimeout = cfg.CommitTimeout
	// Snapshotting is disabled for benchmarks (and InstallSnapshot is not
	// implementable over plain UDP): with this threshold it never triggers.
	config.SnapshotThreshold = 1 << 60
	config.SnapshotInterval = 24 * time.Hour
	config.Logger = hclog.New(&hclog.LoggerOptions{
		Name:   "raft-" + id,
		Level:  hclog.Warn,
		Output: os.Stderr,
	})

	var transport raft.Transport
	switch proto {
	case "udp":
		udpTransport, err := NewUDPTransport(raftAddr, cfg.RPCTimeout)
		if err != nil {
			panic(err)
		}
		transport = udpTransport
	case "tcp":
		tcpAddr, err := net.ResolveTCPAddr("tcp", raftAddr)
		if err != nil {
			panic(err)
		}
		tcpTransport, err := raft.NewTCPTransport(raftAddr, tcpAddr, 3, cfg.RPCTimeout, os.Stderr)
		if err != nil {
			panic(err)
		}
		transport = tcpTransport
	default:
		fmt.Println("proto must be \"udp\" or \"tcp\"")
		os.Exit(1)
	}

	stateMachine := &kvFsm{db: &sync.Map{}}

	// In-memory log/stable stores and a discarding snapshot store: keeps the
	// comparison against the own implementation (which persists nothing) fair.
	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()
	snapshotStore := raft.NewDiscardSnapshotStore()

	node, err := raft.NewRaft(config, stateMachine, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		panic(err)
	}

	return node, stateMachine, transport
}

func runLeader(args []string) {
	positionals, flagArgs := splitFlags(args)
	if len(positionals) < 4 {
		printUsage()
		os.Exit(1)
	}
	id := positionals[0]
	raftAddr := positionals[1]
	clientAddr := positionals[2]
	proto := positionals[3]
	followerArgs := positionals[4:]
	cfg := parseConfig(flagArgs)

	node, stateMachine, transport := newRaftNode(id, raftAddr, proto, cfg)

	bootstrap := raft.Configuration{
		Servers: []raft.Server{
			{ID: raft.ServerID(id), Address: transport.LocalAddr()},
		},
	}
	if err := node.BootstrapCluster(bootstrap).Error(); err != nil {
		panic(err)
	}

	for range time.Tick(50 * time.Millisecond) {
		if node.State() == raft.Leader {
			break
		}
	}

	for _, followerArg := range followerArgs {
		parts := strings.SplitN(followerArg, "=", 2)
		if len(parts) != 2 {
			fmt.Printf("invalid follower argument %q, expected <id>=<raft-addr:port>\n", followerArg)
			os.Exit(1)
		}
		if err := node.AddVoter(raft.ServerID(parts[0]), raft.ServerAddress(parts[1]), 0, 10*time.Second).Error(); err != nil {
			panic(fmt.Errorf("failed to add follower %s: %w", followerArg, err))
		}
		fmt.Printf("added follower %s (%s)\n", parts[0], parts[1])
	}

	if _, err := setupClientServer(clientAddr, node, stateMachine); err != nil {
		panic(err)
	}

	fmt.Printf("hashicorp leader %s: raft=%s client=%s proto=%s\n", id, raftAddr, clientAddr, proto)
	select {}
}

func runFollower(args []string) {
	positionals, flagArgs := splitFlags(args)
	if len(positionals) != 3 {
		printUsage()
		os.Exit(1)
	}
	id := positionals[0]
	raftAddr := positionals[1]
	proto := positionals[2]
	cfg := parseConfig(flagArgs)

	newRaftNode(id, raftAddr, proto, cfg)

	fmt.Printf("hashicorp follower %s: raft=%s proto=%s\n", id, raftAddr, proto)
	select {}
}

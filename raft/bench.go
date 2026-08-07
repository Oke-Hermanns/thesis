package main

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type benchServer struct {
	node     *RaftNode
	listener net.Listener
}

func startBenchServer(node *RaftNode, socketPath string) (*benchServer, error) {
	os.Remove(socketPath)
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, err
	}
	server := &benchServer{node: node, listener: listener}
	go server.acceptLoop()
	return server, nil
}

func (server *benchServer) acceptLoop() {
	for {
		conn, err := server.listener.Accept()
		if err != nil {
			return
		}
		go server.handleConn(conn)
	}
}

func (server *benchServer) handleConn(conn net.Conn) {
	defer conn.Close()

	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return
	}
	fields := strings.Fields(line)
	if len(fields) == 0 {
		fmt.Fprintln(conn, "Empty command")
		return
	}

	switch strings.ToLower(fields[0]) {
	case "latency":
		server.runLatency(conn, fields[1:])
	case "throughput":
		server.runThroughput(conn, fields[1:])
	case "get":
		server.runGet(conn, fields[1:])
	default:
		fmt.Fprintln(conn, "Unknown command")
	}
}

func startCPUProfile(conn net.Conn, path string) (ok bool, stop func()) {
	noop := func() {}
	if path == "" {
		return false, noop
	}
	f, err := os.Create(path)
	if err != nil {
		fmt.Fprintf(conn, "Failed to create pprof output %s: %v\n", path, err)
		return false, noop
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		fmt.Fprintf(conn, "Failed to start CPU profile: %v\n", err)
		f.Close()
		return false, noop
	}
	return true, func() {
		pprof.StopCPUProfile()
		f.Close()
	}
}

func (server *benchServer) runGet(conn net.Conn, args []string) {
	if len(args) < 1 {
		fmt.Fprintln(conn, "Usage: GET <key>")
		return
	}

	key := args[0]
	value, found := server.node.StateMachine.Get(key)
	if !found {
		fmt.Fprintf(conn, "Not found: %s\n", key)
		return
	}
	fmt.Fprintf(conn, "%s = %q\n", key, value.([]byte))
}

func (server *benchServer) runLatency(conn net.Conn, args []string) {
	if len(args) < 2 {
		fmt.Fprintln(conn, "Usage: Latency <duration-seconds> <value-size>")
		return
	}
	durationSec, err := strconv.ParseFloat(args[0], 64)
	if err != nil {
		fmt.Fprintln(conn, "Duration needs to be a float")
		return
	}
	valueSize, err := strconv.Atoi(args[1])
	if err != nil {
		fmt.Fprintln(conn, "Size must be an int")
		return
	}

	value := []byte(strings.Repeat("x", valueSize))
	duration := time.Duration(durationSec * float64(time.Second))
	var latencies []time.Duration

	lossStart := server.captureLossWindow()

	start := time.Now()
	for i := 0; time.Since(start) < duration; i++ {
		key := "benchkey" + strconv.Itoa(i)
		reqStart := time.Now()
		index, err := server.node.applyCommandToLeader(Put, encodeCommand(Put, key, value))
		if err != nil {
			fmt.Fprintf(conn, "ApplyCommandToLeader failed at iteration %d: %v\n", i, err)
			return
		}
		server.node.awaitAppliedToStateMachine(index)
		latencies = append(latencies, time.Since(reqStart))
	}
	elapsed := time.Since(start)

	lossEnd := server.captureLossWindow()

	latencyReport(conn, latencies, elapsed)
	lossReport(conn, lossStart, lossEnd)
}

func (server *benchServer) runThroughput(conn net.Conn, args []string) {
	if len(args) < 4 {
		fmt.Fprintln(conn, "Usage: THROUGHPUT <duration-seconds> <workers> <value-size> <rate-per-worker> [pprof-output-path]")
		return
	}
	durationSec, err := strconv.ParseFloat(args[0], 64)
	if err != nil {
		fmt.Fprintf(conn, "Invalid duration-seconds %q\n", args[0])
		return
	}
	workers, err := strconv.Atoi(args[1])
	if err != nil {
		fmt.Fprintf(conn, "Invalid workers %q\n", args[1])
		return
	}
	valueSize, err := strconv.Atoi(args[2])
	if err != nil {
		fmt.Fprintf(conn, "Invalid value-size %q\n", args[2])
		return
	}
	ratePerWorker, err := strconv.ParseFloat(args[3], 64)
	if err != nil {
		fmt.Fprintf(conn, "Invalid rate-per-worker %q\n", args[3])
		return
	}
	pprofPath := ""
	if len(args) >= 5 {
		pprofPath = args[4]
	}
	_, stopProfile := startCPUProfile(conn, pprofPath)
	defer stopProfile()

	var interval time.Duration
	if ratePerWorker > 0 {
		interval = time.Duration(float64(time.Second) / ratePerWorker)
	}

	value := []byte(strings.Repeat("x", valueSize))
	duration := time.Duration(durationSec * float64(time.Second))

	var mu sync.Mutex
	var allLatencies []time.Duration
	perWorker := make([]int, workers)
	var firstErr error

	stop := make(chan struct{})
	var wg sync.WaitGroup

	lossStart := server.captureLossWindow()

	start := time.Now()
	for w := range workers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			var local []time.Duration
			i := 0
			nextSend := time.Now()
			for {
				select {
				case <-stop:
					mu.Lock()
					allLatencies = append(allLatencies, local...)
					perWorker[workerID] = len(local)
					mu.Unlock()
					return
				default:
				}
				if interval > 0 {
					if now := time.Now(); now.Before(nextSend) {
						time.Sleep(nextSend.Sub(now))
					}
					nextSend = nextSend.Add(interval)
				}
				i++
				key := fmt.Sprintf("benchkey-w%d-%d", workerID, i)
				reqStart := time.Now()
				index, err := server.node.applyCommandToLeader(Put, encodeCommand(Put, key, value))
				if err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					allLatencies = append(allLatencies, local...)
					perWorker[workerID] = len(local)
					mu.Unlock()
					return
				}
				server.node.awaitAppliedToStateMachine(index)
				local = append(local, time.Since(reqStart))
			}
		}(w)
	}

	time.Sleep(duration)
	close(stop)
	wg.Wait()
	elapsed := time.Since(start)

	if firstErr != nil {
		fmt.Fprintf(conn, "ApplyCommandToLeader failed: %v\n", firstErr)
		return
	}

	lossEnd := server.captureLossWindow()

	fmt.Fprintf(conn, "\n%d workers over %s", workers, elapsed)
	if interval > 0 {
		fmt.Fprintf(conn, " (target %.1f req/sec/worker)", ratePerWorker)
	}
	fmt.Fprintln(conn)
	fmt.Fprintln(conn, "per-worker:")
	for w, n := range perWorker {
		fmt.Fprintf(conn, "  worker %d: %d ok, %.1f/sec\n", w, n, float64(n)/elapsed.Seconds())
	}

	latencyReport(conn, allLatencies, elapsed)
	lossReport(conn, lossStart, lossEnd)
}

type lossWindow struct {
	peers    []peer
	downlink map[uint8]LinkStats
	uplink   map[string]LinkStats
}

func (server *benchServer) captureLossWindow() lossWindow {
	peers, downlink := server.node.lossSnapshot()
	return lossWindow{peers: peers, downlink: downlink, uplink: server.node.Transport.LossStats()}
}

func lossReport(w io.Writer, start, end lossWindow) {
	for _, p := range end.peers {
		upStart := start.uplink[p.Address]
		upEnd := end.uplink[p.Address]
		upRecv := diffU64(upEnd.Received, upStart.Received)
		upLost := diffU64(upEnd.Lost, upStart.Lost)
		upOutOfOrder := diffU64(upEnd.OutOfOrder, upStart.OutOfOrder)

		downStart := start.downlink[p.Id]
		downEnd := end.downlink[p.Id]
		downRecv := diffU64(downEnd.Received, downStart.Received)
		downLost := diffU64(downEnd.Lost, downStart.Lost)
		downOutOfOrder := diffU64(downEnd.OutOfOrder, downStart.OutOfOrder)

		fmt.Fprintf(w, "\npeer %d (%s):\n", p.Id, p.Address)
		fmt.Fprintf(w, "  downlink (leader->follower, self-reported by follower): received=%d lost=%d loss=%.4f%% out-of-order=%d\n",
			downRecv, downLost, lossPercent(downRecv, downLost), downOutOfOrder)
		fmt.Fprintf(w, "  uplink   (follower->leader, observed by leader):        received=%d lost=%d loss=%.4f%% out-of-order=%d\n",
			upRecv, upLost, lossPercent(upRecv, upLost), upOutOfOrder)
	}
}

func diffU64(end, start uint64) uint64 {
	if end < start {
		return end
	}
	return end - start
}

func lossPercent(received, lost uint64) float64 {
	total := received + lost
	if total == 0 {
		return 0
	}
	return float64(lost) / float64(total) * 100
}

func latencyReport(writer io.Writer, latencies []time.Duration, elapsed time.Duration) {
	n := len(latencies)
	fmt.Fprintf(writer, "\n%d committed puts over %s\n", n, elapsed)
	fmt.Fprintf(writer, "throughput: %.1f committed puts/sec\n", float64(n)/elapsed.Seconds())
	if n == 0 {
		return
	}

	us := make([]float64, n)
	for i, d := range latencies {
		us[i] = float64(d.Microseconds())
	}
	sort.Float64s(us)

	var sum float64
	for _, v := range us {
		sum += v
	}
	avg := sum / float64(n)

	var variance float64
	for _, v := range us {
		variance += (v - avg) * (v - avg)
	}
	stdev := 0.0
	if n > 1 {
		stdev = math.Sqrt(variance / float64(n-1))
	}

	percentile := func(p float64) float64 {
		idx := int(float64(n) * p / 100)
		if idx >= n {
			idx = n - 1
		}
		return us[idx]
	}

	fmt.Fprintf(writer, "\n====> avg-latency=%.3f usec (std-dev=%.3f usec)\n", avg, stdev)
	fmt.Fprintf(writer, "Total %d observations; each percentile contains %.2f observations\n", n, float64(n)/100)
	fmt.Fprintf(writer, "---> <MAX> observation = %8.3f usec\n", us[n-1])
	for _, p := range []float64{99.999, 99.990, 99.900, 99.000, 90.000, 75.000, 50.000, 25.000} {
		fmt.Fprintf(writer, "---> percentile %6.3f = %8.3f usec\n", p, percentile(p))
	}
	fmt.Fprintf(writer, "---> <MIN> observation = %8.3f usec\n", us[0])
}

func runBenchClient(args []string) {
	if len(args) < 2 {
		fmt.Println("usage: raft Bench <socket-path> latency [duration-seconds] [value-size]")
		fmt.Println("       raft Bench <socket-path> throughput [duration-seconds] [workers] [value-size] [rate-per-worker] [pprof-output-path]")
		fmt.Println("       raft Bench <socket-path> get <key>")
		os.Exit(1)
	}
	socketPath := args[0]
	benchmark := args[1]
	rest := args[2:]

	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		fmt.Printf("Failed to connect to bench socket %s: %v\n", socketPath, err)
		os.Exit(1)
	}
	defer conn.Close()

	switch strings.ToUpper(benchmark) {
	case "LATENCY":
		durationSec := "30"
		valueSize := "14"
		if len(rest) >= 1 {
			durationSec = rest[0]
		}
		if len(rest) >= 2 {
			valueSize = rest[1]
		}
		fmt.Fprintf(conn, "Latency %s %s\n", durationSec, valueSize)
	case "THROUGHPUT":
		durationSec := "30"
		workers := "8"
		valueSize := "14"
		rate := "0"
		pprofPath := ""
		if len(rest) >= 1 {
			durationSec = rest[0]
		}
		if len(rest) >= 2 {
			workers = rest[1]
		}
		if len(rest) >= 3 {
			valueSize = rest[2]
		}
		if len(rest) >= 4 {
			rate = rest[3]
		}
		if len(rest) >= 5 {
			pprofPath = rest[4]
		}
		if pprofPath != "" {
			fmt.Fprintf(conn, "Throughput %s %s %s %s %s\n", durationSec, workers, valueSize, rate, pprofPath)
		} else {
			fmt.Fprintf(conn, "Throughput %s %s %s %s\n", durationSec, workers, valueSize, rate)
		}
	case "GET":
		if len(rest) < 1 {
			fmt.Println("Usage: raft Bench <socket-path> get <key>")
			os.Exit(1)
		}
		fmt.Fprintf(conn, "Get %s\n", rest[0])
	default:
		fmt.Fprintf(conn, "%s %s\n", benchmark, strings.Join(rest, " "))
	}

	io.Copy(os.Stdout, conn)
}

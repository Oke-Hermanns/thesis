package main

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"runtime"
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
	case "status":
		server.runStatus(conn)
	case "kernelstats":
		server.runKernelStats(conn)
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

func (server *benchServer) runStatus(conn net.Conn) {
	peers, _ := server.node.lossSnapshot()

	role := "follower"
	if server.node.Role == Leader {
		role = "leader"
	}

	fmt.Fprintf(conn, "role=%s peers=%d\n", role, len(peers))
	for _, p := range peers {
		fmt.Fprintf(conn, "peer %d %s\n", p.Id, p.Address)
	}
}

func (server *benchServer) runKernelStats(conn net.Conn) {
	stats, ok := readKernelUDPStats()
	if !ok {
		fmt.Fprintln(conn, "kernel UDP stats unavailable (requires Linux /proc/net/snmp)")
		return
	}
	fmt.Fprintf(conn, "InErrors=%d RcvbufErrors=%d\n", stats.InErrors, stats.RcvbufErrors)
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
	kernelStart, kernelStartOK := readKernelUDPStats()

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
	kernelEnd, kernelEndOK := readKernelUDPStats()

	latencyReport(conn, latencies, elapsed)
	lossReport(conn, lossStart, lossEnd)
	kernelUDPReport(conn, kernelStartOK, kernelEndOK, kernelStart, kernelEnd)
}

const outlierLatencyThreshold = 150 * time.Millisecond

type outlierEntry struct {
	At      time.Time
	Latency time.Duration
	Worker  int
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
	var allSchedDelays []time.Duration
	var allOutliers []outlierEntry
	perWorker := make([]int, workers)
	var firstErr error

	stop := make(chan struct{})
	var wg sync.WaitGroup

	lossStart := server.captureLossWindow()
	kernelStart, kernelStartOK := readKernelUDPStats()

	useSpinPacer := interval > 0 && os.Getenv("RAFT_PACER") != "sleep"
	var slots chan time.Time

	start := time.Now()
	if useSpinPacer {
		slots = make(chan time.Time, 8192)
		go runPacer(start, interval/time.Duration(workers), slots, stop)
	}
	for w := range workers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			var local []time.Duration
			var localSched []time.Duration
			var localOutliers []outlierEntry
			i := 0
			nextSend := time.Now()
			for {
				select {
				case <-stop:
					mu.Lock()
					allLatencies = append(allLatencies, local...)
					allSchedDelays = append(allSchedDelays, localSched...)
					allOutliers = append(allOutliers, localOutliers...)
					perWorker[workerID] = len(local)
					mu.Unlock()
					return
				default:
				}
				scheduled := nextSend
				if useSpinPacer {
					slot, ok := <-slots
					if !ok {
						mu.Lock()
						allLatencies = append(allLatencies, local...)
						allSchedDelays = append(allSchedDelays, localSched...)
						allOutliers = append(allOutliers, localOutliers...)
						perWorker[workerID] = len(local)
						mu.Unlock()
						return
					}
					scheduled = slot
				} else if interval > 0 {
					if now := time.Now(); now.Before(nextSend) {
						time.Sleep(nextSend.Sub(now))
					}
					nextSend = nextSend.Add(interval)
				}
				i++
				key := fmt.Sprintf("benchkey-w%d-%d", workerID, i)
				reqStart := time.Now()
				if interval == 0 {
					scheduled = reqStart
				}
				index, err := server.node.applyCommandToLeader(Put, encodeCommand(Put, key, value))
				if err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					allLatencies = append(allLatencies, local...)
					allSchedDelays = append(allSchedDelays, localSched...)
					allOutliers = append(allOutliers, localOutliers...)
					perWorker[workerID] = len(local)
					mu.Unlock()
					return
				}
				server.node.awaitAppliedToStateMachine(index)
				lat := time.Since(scheduled)
				localSched = append(localSched, reqStart.Sub(scheduled))
				local = append(local, lat)
				if interval > 0 && lat > outlierLatencyThreshold {
					localOutliers = append(localOutliers, outlierEntry{At: time.Now(), Latency: lat, Worker: workerID})
				}
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
	kernelEnd, kernelEndOK := readKernelUDPStats()

	fmt.Fprintf(conn, "\n%d workers over %s", workers, elapsed)
	if interval > 0 {
		fmt.Fprintf(conn, " (open loop, target %.1f req/sec/worker, latency measured from intended send time)", ratePerWorker)
	} else {
		fmt.Fprintf(conn, " (closed loop)")
	}
	fmt.Fprintln(conn)

	if interval > 0 {
		offered := ratePerWorker * float64(workers)
		achieved := float64(len(allLatencies)) / elapsed.Seconds()
		verdict := "SUSTAINED"
		if achieved < offered*0.95 {
			verdict = "NOT SUSTAINED - offered load exceeds capacity"
		}
		fmt.Fprintf(conn, "offered=%.1f/sec achieved=%.1f/sec (%.1f%% of offered) %s\n",
			offered, achieved, 100*achieved/offered, verdict)
	}
	fmt.Fprintln(conn, "per-worker:")
	for w, n := range perWorker {
		fmt.Fprintf(conn, "  worker %d: %d ok, %.1f/sec\n", w, n, float64(n)/elapsed.Seconds())
	}

	latencyReport(conn, allLatencies, elapsed)
	if interval > 0 {
		schedDelayReport(conn, allSchedDelays)
		outlierReport(conn, allOutliers)
	}
	lossReport(conn, lossStart, lossEnd)
	kernelUDPReport(conn, kernelStartOK, kernelEndOK, kernelStart, kernelEnd)
}

func outlierReport(w io.Writer, outliers []outlierEntry) {
	if len(outliers) == 0 {
		fmt.Fprintf(w, "\nno requests exceeded the %s outlier threshold\n", outlierLatencyThreshold)
		return
	}
	sort.Slice(outliers, func(i, j int) bool { return outliers[i].At.Before(outliers[j].At) })
	fmt.Fprintf(w, "\n%d requests exceeded the %s outlier threshold (completion time UTC, latency, worker):\n",
		len(outliers), outlierLatencyThreshold)
	for _, o := range outliers {
		fmt.Fprintf(w, "  %s  %10.3f ms  worker %d\n",
			o.At.UTC().Format("2006-01-02T15:04:05.000Z"), float64(o.Latency.Microseconds())/1000, o.Worker)
	}
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

type kernelUDPStats struct {
	InErrors     uint64
	RcvbufErrors uint64
}

func readKernelUDPStats() (kernelUDPStats, bool) {
	data, err := os.ReadFile("/proc/net/snmp")
	if err != nil {
		return kernelUDPStats{}, false
	}

	lines := strings.Split(string(data), "\n")
	for i, line := range lines {
		if !strings.HasPrefix(line, "Udp:") || i+1 >= len(lines) {
			continue
		}
		header := strings.Fields(line)
		values := strings.Fields(lines[i+1])
		if len(header) != len(values) {
			return kernelUDPStats{}, false
		}
		var stats kernelUDPStats
		for j, name := range header {
			switch name {
			case "InErrors":
				stats.InErrors, _ = strconv.ParseUint(values[j], 10, 64)
			case "RcvbufErrors":
				stats.RcvbufErrors, _ = strconv.ParseUint(values[j], 10, 64)
			}
		}
		return stats, true
	}
	return kernelUDPStats{}, false
}

func kernelUDPReport(w io.Writer, startOK, endOK bool, start, end kernelUDPStats) {
	if !startOK || !endOK {
		return
	}
	fmt.Fprintf(w, "\nkernel UDP stats (system-wide, /proc/net/snmp, Linux only):\n")
	fmt.Fprintf(w, "  InErrors delta:     %d\n", diffU64(end.InErrors, start.InErrors))
	fmt.Fprintf(w, "  RcvbufErrors delta: %d  (socket receive buffer overflow - kernel-side loss, not network loss)\n",
		diffU64(end.RcvbufErrors, start.RcvbufErrors))
}

func waitUntil(target time.Time) {
	for time.Now().Before(target) {
	}
}

func runPacer(start time.Time, interval time.Duration, slots chan<- time.Time, stop <-chan struct{}) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	defer close(slots)

	for seq := int64(0); ; seq++ {
		target := start.Add(time.Duration(seq) * interval)
		waitUntil(target)
		select {
		case <-stop:
			return
		default:
		}
		select {
		case slots <- target:
		case <-stop:
			return
		}
	}
}

func schedDelayReport(w io.Writer, delays []time.Duration) {
	if len(delays) == 0 {
		return
	}
	sorted := make([]float64, len(delays))
	var sum float64
	for i, d := range delays {
		sorted[i] = float64(d.Microseconds())
		sum += sorted[i]
	}
	sort.Float64s(sorted)
	pct := func(p float64) float64 {
		idx := int(float64(len(sorted)) * p / 100)
		if idx >= len(sorted) {
			idx = len(sorted) - 1
		}
		return sorted[idx]
	}
	fmt.Fprintf(w, "\nscheduling delay (actual send minus scheduled send; load-generator timer error):\n")
	fmt.Fprintf(w, "  avg=%.3f usec  p50=%.3f  p99=%.3f  max=%.3f\n",
		sum/float64(len(sorted)), pct(50), pct(99), sorted[len(sorted)-1])
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
		fmt.Println("       raft Bench <socket-path> status")
		fmt.Println("       raft Bench <socket-path> kernelstats")
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

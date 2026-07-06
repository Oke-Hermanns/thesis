/*
Go UDP programming guide that I used to implement this:
https://dev.to/jones_charles_ad50858dbc0/go-udp-programming-a-beginner-friendly-guide-to-building-fast-real-time-apps-4ik
*/
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"net"
	"os"
	"sort"
	"time"
)

func main() {
	ip_address := flag.String("ip", "", "Specify the ipv4 address")
	port := flag.Int("port", 5555, "Specify the UDP port")
	identifier := flag.String("id", "", "Either \"client\" or \"server\"")
	benchmark_time := flag.Int("t", 30, "Specify how long the benchmark should run in seconds")
	flag.Parse()

	switch *identifier {
	case "client":
		sender(*ip_address, *port, *benchmark_time)
		os.Exit(0)
	case "server":
		receiver(*port)
		os.Exit(0)
	}
	fmt.Println("Check usage, e.g. \"./benchmarks -ip 1.1.1.1 -port 1234 -id client -t 60\"")
	fmt.Println("Time is optional (default 30s), specify port for server; specify port and ip-address for receiver")
	fmt.Println("Start the server before the client as there is no check if client is up as of right now")
	os.Exit(1)
}

func sender(ip string, port int, benchmark_time int) {
	sent := 0
	received := 0
	address := fmt.Sprintf("%s:%d", ip, port)
	// empty field is where errors are returned
	raddr, _ := net.ResolveUDPAddr("udp", address)

	conn, _ := net.DialUDP("udp", nil, raddr)
	// golang best practice -> Will be executed at the end of the function
	defer conn.Close()

	var times []time.Duration

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(benchmark_time)*time.Second)
	defer cancel()

	// Sockperf standard message size is 14 Byte without UDP and ipv4 header, so 14 characters here
	message := []byte("ABABABABABABAB")
	buffer := make([]byte, 1024)

	start_time := time.Now()
	// Loop without any condition runs until break/return
	for {
		select {
		case <-ctx.Done():
			total_time := time.Since(start_time)
			printData(times, total_time, sent, received)
			return
		default:
			send_time := time.Now()
			_, err := conn.Write(message)
			sent++
			if err != nil {
				log.Printf("Send failed: %v", err)
				return
			}

			// Wait 2 seconds at maximum for reply -> Probably hurts performance when computed in every iteration
			//conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			_, err = conn.Read(buffer)
			recv_time := time.Now()
			if err != nil {
				// Log it, but ignore it
				log.Printf("Receive error: %v", err)
				continue
			}
			rtt := recv_time.Sub(send_time)
			times = append(times, rtt)
			received++
		}

	}
}

func receiver(port int) {
	// Currently does not shut down, as loop runs forever
	addr := net.UDPAddr{Port: port, IP: net.IPv4zero}
	conn, err := net.ListenUDP("udp", &addr)
	if err != nil {
		log.Fatal("Listen failed:", err)
	}
	defer conn.Close()

	buffer := make([]byte, 1024)
	for {
		n, clientAddr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			log.Printf("Read error: %v", err)
			continue
		}

		// Send the same message back
		_, err = conn.WriteToUDP(buffer[:n], clientAddr)
		if err != nil {
			log.Printf("Write error: %v", err)
		}
	}

}

// Mostly AI generated (claude sonnet 4.6) to mimic sockperf output
func printData(measurements []time.Duration, total_time time.Duration, sent, received int) {
	dropped := sent - received

	fmt.Printf("go latency: [Total Run] RunTime=%.3f sec; SentMessages=%d; ReceivedMessages=%d\n",
		total_time.Seconds(), sent, received)

	if len(measurements) == 0 {
		fmt.Println("go latency: no successful round trips, cannot compute RTT stats")
		return
	}

	sorted := make([]time.Duration, len(measurements))
	copy(sorted, measurements)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var sum time.Duration
	for _, r := range measurements {
		sum += r
	}
	mean := sum / time.Duration(len(measurements))
	meanUsec := float64(mean.Nanoseconds()) / 1000.0

	var variance float64
	for _, r := range measurements {
		d := float64(r-mean) / 1000.0 // usec
		variance += d * d
	}
	variance /= float64(len(measurements))
	stddevUsec := math.Sqrt(variance)

	fmt.Printf("go latency: ====> avg-rtt=%.3f (std-dev=%.3f)\n", meanUsec, stddevUsec)
	fmt.Printf("go latency: # dropped messages = %d; # duplicated messages = 0; # out-of-order messages = 0\n", dropped)
	fmt.Printf("go latency: Summary: Round trip is %.3f usec\n", meanUsec)
	fmt.Printf("go latency: Total %d observations; each percentile contains %.2f observations\n",
		len(sorted), float64(len(sorted))/100.0)

	usec := func(d time.Duration) float64 {
		return float64(d.Nanoseconds()) / 1000.0
	}
	percentile := func(p float64) float64 {
		idx := int(p / 100 * float64(len(sorted)))
		if idx >= len(sorted) {
			idx = len(sorted) - 1
		}
		return usec(sorted[idx])
	}

	fmt.Printf("go latency: ---> <MAX> observation = %8.3f\n", usec(sorted[len(sorted)-1]))
	fmt.Printf("go latency: ---> percentile 99.999 = %8.3f\n", percentile(99.999))
	fmt.Printf("go latency: ---> percentile 99.990 = %8.3f\n", percentile(99.990))
	fmt.Printf("go latency: ---> percentile 99.900 = %8.3f\n", percentile(99.900))
	fmt.Printf("go latency: ---> percentile 99.000 = %8.3f\n", percentile(99.000))
	fmt.Printf("go latency: ---> percentile 90.000 = %8.3f\n", percentile(90.000))
	fmt.Printf("go latency: ---> percentile 75.000 = %8.3f\n", percentile(75.000))
	fmt.Printf("go latency: ---> percentile 50.000 = %8.3f\n", percentile(50.000))
	fmt.Printf("go latency: ---> percentile 25.000 = %8.3f\n", percentile(25.000))
	fmt.Printf("go latency: ---> <MIN> observation = %8.3f\n", usec(sorted[0]))
}

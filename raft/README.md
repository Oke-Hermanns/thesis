This is an implementation of the Raft hot-path with a transport interface that enables the use of UDP and TCP transport without changes to Raft.
Data is stored in a simple in-memory key-value map.

## Example Usage

For a three node cluster located in one datacenter:

1. **Start the leader:**
   `./raft Leader 1 ip-address:port1 ip-address:port2 udp -profile datacenter`

2. **Start the first follower:**
   `./raft Follower 2 ip-address:port leader-address:port1 udp -profile datacenter`

3. **Start the second follower:**
   `./raft Follower 3 ip-address:port leader-address:port1 udp -profile datacenter`

4. **Use a bash script to put/get/delete values, e.g.:**
   `./send_puts.sh leader-address:port2 100 512 -d 30`
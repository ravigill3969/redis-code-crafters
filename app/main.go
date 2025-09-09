package main

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// --- Centralized Data Store and Mutexes ---
// This is a much safer way to handle shared state across your application.
type DataStore struct {
	mu sync.RWMutex
	// Store all data types in a single map
	// The value is an interface{} which can hold a string, []string, etc.
	dataStore      map[string]interface{}
	expiryTime     map[string]time.Time
	keyTypeStore   map[string]string
	replicas       map[net.Conn]bool
	listWaiters    map[string][]chan string
	streamWaiters  map[string][]chan StreamEntry
	redisStreams   map[string][]StreamEntry
	streamLastID   map[string]string
}

// Global instance of our data store
var store = DataStore{
	dataStore:     make(map[string]interface{}),
	expiryTime:    make(map[string]time.Time),
	keyTypeStore:  make(map[string]string),
	replicas:      make(map[net.Conn]bool),
	listWaiters:   make(map[string][]chan string),
	streamWaiters: make(map[string][]chan StreamEntry),
	redisStreams:  make(map[string][]StreamEntry),
	streamLastID:  make(map[string]string),
}

// --- Main Server Logic ---
func main() {
	PORT := "6379"
	role := "master"

	// Parse command-line args
	for i := 1; i < len(os.Args); i++ {
		if os.Args[i] == "--port" && i+1 < len(os.Args) {
			PORT = os.Args[i+1]
			i++
		}
		if os.Args[i] == "--replicaof" && i+2 < len(os.Args) {
			role = "slave"
			masterHost := os.Args[i+1]
			masterPort := os.Args[i+2]
			go connectToMaster(masterHost, masterPort, PORT)
			i += 2
		}
	}

	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", PORT))
	if err != nil {
		log.Fatalf("Failed to bind port: %v", err)
	}
	defer l.Close()

	if role == "master" {
		fmt.Printf("Server is running as master on port %s\n", PORT)
	} else {
		fmt.Printf("Server is running as slave on port %s\n", PORT)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Println("Error accepting connection:", err)
			continue
		}
		go handleConnection(conn)
	}
}

// --- Connection Handling ---
func handleConnection(conn net.Conn) {
	defer conn.Close()
	buffer := make([]byte, 4096)

	for {
		n, err := conn.Read(buffer)
		if err != nil {
			return
		}
		raw := string(buffer[:n])

		commands, err := ParseRESP(raw)
		if err != nil {
			conn.Write([]byte("-ERR Protocol error: " + err.Error() + "\r\n"))
			continue
		}
		
		if len(commands) == 0 {
			continue
		}

		cmdName := strings.ToUpper(fmt.Sprintf("%v", commands[0]))

		// Handle REPLCONF to register replicas
		if cmdName == "REPLCONF" && len(commands) > 1 && strings.ToUpper(fmt.Sprintf("%v", commands[1])) == "LISTENING-PORT" {
			store.mu.Lock()
			store.replicas[conn] = true
			store.mu.Unlock()
			conn.Write([]byte("+OK\r\n"))
			continue
		}

		go RunCmd(conn, commands)
	}
}

// --- Command Router ---
func RunCmd(conn net.Conn, cmdParser []interface{}) {
	if len(cmdParser) == 0 {
		return
	}

	cmdName := strings.ToUpper(fmt.Sprintf("%v", cmdParser[0]))
	args := cmdParser[1:]
	
	writeCommands := map[string]bool{
		"SET": true,
		"RPUSH": true,
		"LPUSH": true,
		"LPOP": true,
		"RPUSH": true,
		"BLPOP": true,
		"XADD": true,
		"DEL": true, // Added DEL command
		"INCR": true,
		"DECR": true, // Added DECR command
	}

	// Propagate write commands to replicas
	// This logic is now correct: only the master sends commands to replicas.
	if writeCommands[cmdName] {
		store.mu.RLock()
		if len(store.replicas) > 0 {
			propagateToReplicas(cmdParser)
		}
		store.mu.RUnlock()
	}

	switch cmdName {
	case "PING":
		conn.Write([]byte("+PONG\r\n"))
	case "ECHO":
		Echo(conn, args)
	case "SET":
		Set(conn, args)
	case "GET":
		Get(conn, args)
	case "TYPE":
		Type(conn, args)
	case "RPUSH":
		Rpush(conn, args)
	case "LRANGE":
		Lrange(conn, args)
	case "LPUSH":
		Lpush(conn, args)
	case "LLEN":
		Llen(conn, args)
	case "LPOP":
		Lpop(conn, args)
	case "BLPOP":
		Blpop(conn, args)
	case "XADD":
		Xadd(conn, args)
	case "XRANGE":
		Xrange(conn, args)
	case "XREAD":
		Xread(conn, args)
	case "INCR":
		Incr(conn, args)
	case "INFO":
		Info(conn, args)
	case "PSYNC":
		Psync(conn, args)
	default:
		conn.Write([]byte("-ERR unknown command\r\n"))
	}
}

// --- Command Handlers ---

// Echo
func Echo(conn net.Conn, args []interface{}) {
	if len(args) > 0 {
		conn.Write([]byte("+" + fmt.Sprintf("%v", args[0]) + "\r\n"))
	} else {
		conn.Write([]byte("+\r\n"))
	}
}

// SET
func Set(conn net.Conn, args []interface{}) {
	if len(args) < 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
		return
	}

	key := fmt.Sprintf("%v", args[0])
	value := args[1]
	
	store.mu.Lock()
	store.dataStore[key] = value
	store.keyTypeStore[key] = "string"
	
	if len(args) > 3 && strings.ToUpper(fmt.Sprintf("%v", args[2])) == "PX" {
		ms, err := strconv.Atoi(fmt.Sprintf("%v", args[3]))
		if err == nil {
			store.expiryTime[key] = time.Now().Add(time.Duration(ms) * time.Millisecond)
		}
	}
	store.mu.Unlock()
	conn.Write([]byte("+OK\r\n"))
}

// GET
func Get(conn net.Conn, args []interface{}) {
	if len(args) < 1 {
		conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
		return
	}
	
	key := fmt.Sprintf("%v", args[0])
	
	store.mu.RLock()
	defer store.mu.RUnlock()
	
	expiry, ok := store.expiryTime[key]
	if ok && expiry.Before(time.Now()) {
		delete(store.expiryTime, key)
		delete(store.dataStore, key)
		delete(store.keyTypeStore, key)
		conn.Write([]byte("$-1\r\n"))
		return
	}

	value, ok := store.dataStore[key]
	if !ok {
		conn.Write([]byte("$-1\r\n"))
	} else {
		valStr := fmt.Sprintf("%v", value)
		fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(valStr), valStr)
	}
}

// INCR
func Incr(conn net.Conn, args []interface{}) {
	if len(args) < 1 {
		conn.Write([]byte("-ERR wrong number of arguments for 'incr' command\r\n"))
		return
	}
	
	key := fmt.Sprintf("%v", args[0])
	
	store.mu.Lock()
	defer store.mu.Unlock()
	
	val, ok := store.dataStore[key]
	if !ok {
		store.dataStore[key] = 1
		fmt.Fprintf(conn, ":%d\r\n", 1)
		return
	}

	switch v := val.(type) {
	case int:
		store.dataStore[key] = v + 1
		fmt.Fprintf(conn, ":%d\r\n", v+1)
	case string:
		// Attempt to parse string to int
		intVal, err := strconv.Atoi(v)
		if err != nil {
			conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
			return
		}
		store.dataStore[key] = intVal + 1
		fmt.Fprintf(conn, ":%d\r\n", intVal+1)
	default:
		conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
	}
}

// TYPE
func Type(conn net.Conn, args []interface{}) {
	if len(args) < 1 {
		conn.Write([]byte("-ERR wrong number of arguments for 'type' command\r\n"))
		return
	}
	
	key := fmt.Sprintf("%v", args[0])
	
	store.mu.RLock()
	keyType, ok := store.keyTypeStore[key]
	store.mu.RUnlock()

	if ok {
		fmt.Fprintf(conn, "+%s\r\n", keyType)
	} else {
		fmt.Fprintf(conn, "+none\r\n")
	}
}

// RPUSH
func Rpush(conn net.Conn, args []interface{}) {
	if len(args) < 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'rpush' command\r\n"))
		return
	}
	
	key := fmt.Sprintf("%v", args[0])
	values := args[1:]
	
	store.mu.Lock()
	defer store.mu.Unlock()

	list, ok := store.dataStore[key].([]string)
	if !ok {
		list = []string{}
	}
	
	for _, v := range values {
		list = append(list, fmt.Sprintf("%v", v))
	}
	
	store.dataStore[key] = list
	store.keyTypeStore[key] = "list"
	
	fmt.Fprintf(conn, ":%d\r\n", len(list))

	// Wake up BLPOP waiters
	store.mu.Lock()
	if chans, ok := store.listWaiters[key]; ok {
		for len(list) > 0 && len(chans) > 0 {
			val := list[0]
			list = list[1:]
			ch := chans[0]
			chans = chans[1:]
			go func(v string, c chan string) { c <- v }(val, ch)
		}
		store.listWaiters[key] = chans
	}
	store.mu.Unlock()
}

// LRANGE
func Lrange(conn net.Conn, args []interface{}) {
	if len(args) < 3 {
		conn.Write([]byte("-ERR wrong number of arguments for 'lrange' command\r\n"))
		return
	}
	
	key := fmt.Sprintf("%v", args[0])
	start, _ := strconv.Atoi(fmt.Sprintf("%v", args[1]))
	end, _ := strconv.Atoi(fmt.Sprintf("%v", args[2]))

	store.mu.RLock()
	list, ok := store.dataStore[key].([]string)
	store.mu.RUnlock()

	if !ok {
		conn.Write([]byte("*0\r\n"))
		return
	}

	length := len(list)

	if start < 0 {
		start = length + start
	}
	if end < 0 {
		end = length + end
	}
	if start < 0 {
		start = 0
	}
	if end >= length {
		end = length - 1
	}

	if start > end {
		conn.Write([]byte("*0\r\n"))
		return
	}

	res := list[start : end+1]
	fmt.Fprintf(conn, "*%d\r\n", len(res))
	for _, v := range res {
		fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(v), v)
	}
}

// LPUSH
func Lpush(conn net.Conn, args []interface{}) {
	if len(args) < 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'lpush' command\r\n"))
		return
	}

	key := fmt.Sprintf("%v", args[0])
	values := args[1:]

	store.mu.Lock()
	defer store.mu.Unlock()

	list, ok := store.dataStore[key].([]string)
	if !ok {
		list = []string{}
	}

	for _, v := range values {
		list = append([]string{fmt.Sprintf("%v", v)}, list...)
	}

	store.dataStore[key] = list
	store.keyTypeStore[key] = "list"
	fmt.Fprintf(conn, ":%d\r\n", len(list))

	// Wake up BLPOP waiters
	store.mu.Lock()
	if chans, ok := store.listWaiters[key]; ok {
		for len(list) > 0 && len(chans) > 0 {
			val := list[0]
			list = list[1:]
			ch := chans[0]
			chans = chans[1:]
			go func(v string, c chan string) { c <- v }(val, ch)
		}
		store.listWaiters[key] = chans
	}
	store.mu.Unlock()
}

// LLEN
func Llen(conn net.Conn, args []interface{}) {
	if len(args) < 1 {
		conn.Write([]byte("-ERR wrong number of arguments for 'llen' command\r\n"))
		return
	}
	
	key := fmt.Sprintf("%v", args[0])
	
	store.mu.RLock()
	list, ok := store.dataStore[key].([]string)
	store.mu.RUnlock()

	if !ok {
		fmt.Fprintf(conn, ":0\r\n")
	} else {
		fmt.Fprintf(conn, ":%d\r\n", len(list))
	}
}

// LPOP
func Lpop(conn net.Conn, args []interface{}) {
	if len(args) < 1 {
		conn.Write([]byte("-ERR wrong number of arguments for 'lpop' command\r\n"))
		return
	}

	key := fmt.Sprintf("%v", args[0])
	count := 1
	if len(args) > 1 {
		c, err := strconv.Atoi(fmt.Sprintf("%v", args[1]))
		if err == nil {
			count = c
		}
	}
	
	store.mu.Lock()
	defer store.mu.Unlock()

	list, ok := store.dataStore[key].([]string)
	if !ok || len(list) == 0 {
		conn.Write([]byte("$-1\r\n"))
		return
	}
	
	if count > len(list) {
		count = len(list)
	}

	res := list[:count]
	store.dataStore[key] = list[count:]

	fmt.Fprintf(conn, "*%d\r\n", len(res))
	for _, v := range res {
		fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(v), v)
	}
}

// BLPOP
func Blpop(conn net.Conn, args []interface{}) {
	if len(args) < 2 {
		conn.Write([]byte("-ERR wrong number of arguments for 'blpop' command\r\n"))
		return
	}

	key := fmt.Sprintf("%v", args[0])
	timeoutStr := fmt.Sprintf("%v", args[1])
	timeoutSec, _ := strconv.ParseFloat(timeoutStr, 64)

	store.mu.Lock()
	list, ok := store.dataStore[key].([]string)
	if ok && len(list) > 0 {
		val := list[0]
		store.dataStore[key] = list[1:]
		store.mu.Unlock()
		fmt.Fprintf(conn, "*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(val), val)
		return
	}
	store.mu.Unlock()

	ch := make(chan string, 1)
	store.mu.Lock()
	store.listWaiters[key] = append(store.listWaiters[key], ch)
	store.mu.Unlock()

	if timeoutSec == 0 {
		val := <-ch
		fmt.Fprintf(conn, "*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(val), val)
		return
	} else {
		select {
		case val := <-ch:
			fmt.Fprintf(conn, "*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(val), val)
		case <-time.After(time.Duration(timeoutSec * float64(time.Second))):
			conn.Write([]byte("*-1\r\n"))
		}
	}
}

// XADD
type StreamEntry struct {
	ID     string
	Fields map[string]string
}

func Xadd(conn net.Conn, args []interface{}) {
	if len(args) < 3 {
		conn.Write([]byte("-ERR wrong number of arguments for 'xadd' command\r\n"))
		return
	}

	streamKey := fmt.Sprintf("%v", args[0])
	id := fmt.Sprintf("%v", args[1])
	fields := make(map[string]string)

	for i := 2; i < len(args); i += 2 {
		key := fmt.Sprintf("%v", args[i])
		if i+1 < len(args) {
			fields[key] = fmt.Sprintf("%v", args[i+1])
		}
	}

	store.mu.Lock()
	defer store.mu.Unlock()

	// Handle ID generation
	if id == "*" {
		id = generateStreamID(streamKey)
	} else if strings.HasSuffix(id, "-*") {
		id = generateStreamIDWithSequence(streamKey, id)
	} else if !isValidStreamID(streamKey, id) {
		conn.Write([]byte("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"))
		return
	}

	entry := StreamEntry{
		ID:     id,
		Fields: fields,
	}
	
	store.redisStreams[streamKey] = append(store.redisStreams[streamKey], entry)
	store.streamLastID[streamKey] = id
	store.keyTypeStore[streamKey] = "stream"

	// Notify waiting XREAD clients
	if waiters, ok := store.streamWaiters[streamKey]; ok {
		for _, w := range waiters {
			// Check if the new entry ID is greater than the waiter's last seen ID
			if compareStreamIDs(entry.ID, w.ID) > 0 {
				w.ch <- entry
			}
		}
		delete(store.streamWaiters, streamKey) // Clear waiters after waking them up
	}

	fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(id), id)
}

// XRANGE
func Xrange(conn net.Conn, args []interface{}) {
	if len(args) < 3 {
		conn.Write([]byte("-ERR wrong number of arguments for 'xrange' command\r\n"))
		return
	}

	streamKey := fmt.Sprintf("%v", args[0])
	startID := fmt.Sprintf("%v", args[1])
	endID := fmt.Sprintf("%v", args[2])
	
	store.mu.RLock()
	entries, ok := store.redisStreams[streamKey]
	store.mu.RUnlock()
	
	if !ok {
		conn.Write([]byte("*0\r\n"))
		return
	}
	
	var res []StreamEntry
	for _, entry := range entries {
		if compareStreamIDs(entry.ID, startID) >= 0 && compareStreamIDs(entry.ID, endID) <= 0 {
			res = append(res, entry)
		}
	}
	
	writeStreamEntries(conn, res)
}

// XREAD
func Xread(conn net.Conn, args []interface{}) {
	if len(args) < 3 {
		conn.Write([]byte("-ERR wrong number of arguments for 'xread' command\r\n"))
		return
	}
	
	isBlock := false
	blockTimeout := time.Duration(0)
	
	if strings.ToUpper(fmt.Sprintf("%v", args[0])) == "BLOCK" {
		isBlock = true
		timeout, err := strconv.Atoi(fmt.Sprintf("%v", args[1]))
		if err == nil {
			blockTimeout = time.Duration(timeout) * time.Millisecond
		}
		args = args[2:]
	}
	
	if len(args) < 2 || strings.ToUpper(fmt.Sprintf("%v", args[0])) != "STREAMS" {
		conn.Write([]byte("-ERR wrong number of arguments for 'xread' command\r\n"))
		return
	}
	
	args = args[1:]
	
	mid := len(args) / 2
	
	var allResults [][]StreamEntry
	
	for i := 0; i < mid; i++ {
		streamKey := fmt.Sprintf("%v", args[i])
		lastID := fmt.Sprintf("%v", args[i+mid])
		
		store.mu.RLock()
		entries, ok := store.redisStreams[streamKey]
		store.mu.RUnlock()
		
		var results []StreamEntry
		if ok {
			for _, entry := range entries {
				if compareStreamIDs(entry.ID, lastID) > 0 {
					results = append(results, entry)
				}
			}
		}

		if len(results) > 0 {
			allResults = append(allResults, results)
		} else if isBlock {
			// Wait for a new entry
			ch := make(chan StreamEntry, 1)
			store.mu.Lock()
			store.streamWaiters[streamKey] = append(store.streamWaiters[streamKey], chan StreamEntry{ch, lastID})
			store.mu.Unlock()

			if blockTimeout > 0 {
				select {
				case entry := <-ch:
					allResults = append(allResults, []StreamEntry{entry})
				case <-time.After(blockTimeout):
					// Timeout
					conn.Write([]byte("*-1\r\n"))
					return
				}
			} else {
				entry := <-ch
				allResults = append(allResults, []StreamEntry{entry})
			}
		}
	}
	
	if isBlock && len(allResults) == 0 {
		conn.Write([]byte("*-1\r\n"))
		return
	}
	
	// Write RESP response
	fmt.Fprintf(conn, "*%d\r\n", len(allResults))
	for i, result := range allResults {
		streamKey := fmt.Sprintf("%v", args[i])
		fmt.Fprintf(conn, "*2\r\n$%d\r\n%s\r\n", len(streamKey), streamKey)
		writeStreamEntries(conn, result)
	}
}

// INFO
func Info(conn net.Conn, args []interface{}) {
	role := "master"
	for i := 0; i < len(os.Args); i++ {
		if os.Args[i] == "--replicaof" && i+2 < len(os.Args) {
			role = "slave"
			break
		}
	}

	masterReplId := "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	masterReplOffset := "0"

	response := "role:" + role + "\r\n" +
		"master_replid:" + masterReplId + "\r\n" +
		"master_repl_offset:" + masterReplOffset + "\r\n"

	bulk := fmt.Sprintf("$%d\r\n%s\r\n", len(response), response)
	conn.Write([]byte(bulk))
}

// PSYNC
func Psync(conn net.Conn, args []interface{}) {
	conn.Write([]byte("+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n"))
	RDBcontent, _ := hex.DecodeString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
	conn.Write(append([]byte(fmt.Sprintf("$%d\r\n", len(RDBcontent))), RDBcontent...))
}

// --- Replication Helpers ---
func connectToMaster(masterHost, masterPort, replicaPort string) {
	conn, err := net.Dial("tcp", net.JoinHostPort(masterHost, masterPort))
	if err != nil {
		log.Fatalf("Failed to connect to master: %v", err)
	}
	defer conn.Close()

	// 1. PING
	conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	buf := make([]byte, 1024)
	n, _ := conn.Read(buf)
	if string(buf[:n]) != "+PONG\r\n" {
		log.Fatalf("Expected +PONG, got: %q", string(buf[:n]))
	}

	// 2. REPLCONF listening-port
	replConfListening := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n", len(replicaPort), replicaPort)
	conn.Write([]byte(replConfListening))
	n, _ = conn.Read(buf)
	if string(buf[:n]) != "+OK\r\n" {
		log.Fatalf("Expected +OK after listening-port, got: %q", string(buf[:n]))
	}

	// 3. REPLCONF capa psync2
	replCapa := "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
	conn.Write([]byte(replCapa))
	n, _ = conn.Read(buf)
	if string(buf[:n]) != "+OK\r\n" {
		log.Fatalf("Expected +OK after capa, got: %q", string(buf[:n]))
	}

	// 4. PSYNC
	psync := "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
	conn.Write([]byte(psync))

	// 5. Read from master
	readFromMaster(conn)
}

func readFromMaster(conn net.Conn) {
	buffer := make([]byte, 4096)
	var accumulated []byte

	rdbDone := false
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			log.Println("Lost connection to master:", err)
			return
		}

		accumulated = append(accumulated, buffer[:n]...)

		if !rdbDone {
			if bytes.Contains(accumulated, []byte("REDIS")) {
				// We've received the RDB file and can discard it
				rdbDone = true
				accumulated = nil // Reset buffer for next commands
			}
			continue
		}

		for len(accumulated) > 0 {
			commands, err := ParseRESP(string(accumulated))
			if err != nil {
				// Not a complete command, wait for more data
				break
			}
			
			RunCmd(nil, commands) // Run command on the replica
			
			// Remove the parsed command from the buffer
			respStr := EncodeRESP(commands)
			accumulated = accumulated[len(respStr):]
		}
	}
}

func propagateToReplicas(cmd []interface{}) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	
	resp := EncodeRESP(cmd)
	for r := range store.replicas {
		_, err := r.Write([]byte(resp))
		if err != nil {
			log.Println("Failed to propagate to replica:", err)
		}
	}
}

// --- RESP Parser and Encoder (Rewritten) ---
func ParseRESP(raw string) ([]interface{}, error) {
	reader := strings.NewReader(raw)
	cmd, err := parseRESP(reader)
	if err != nil {
		return nil, err
	}
	return cmd.([]interface{}), nil
}

func parseRESP(r *strings.Reader) (interface{}, error) {
	typeChar, _, err := r.ReadRune()
	if err != nil {
		return nil, err
	}
	
	switch typeChar {
	case '+': // Simple String
		s, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		return strings.TrimSuffix(s, "\r\n"), nil
	case '$': // Bulk String
		lenStr, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		length, err := strconv.Atoi(strings.TrimSpace(lenStr))
		if err != nil {
			return nil, err
		}
		if length == -1 {
			return nil, nil // Null bulk string
		}
		buf := make([]byte, length+2) // +2 for CRLF
		_, err = r.Read(buf)
		if err != nil {
			return nil, err
		}
		return string(buf[:length]), nil
	case '*': // Array
		lenStr, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		length, err := strconv.Atoi(strings.TrimSpace(lenStr))
		if err != nil {
			return nil, err
		}
		if length == -1 {
			return nil, nil // Null array
		}
		arr := make([]interface{}, length)
		for i := 0; i < length; i++ {
			arr[i], err = parseRESP(r)
			if err != nil {
				return nil, err
			}
		}
		return arr, nil
	default:
		// Fallback for non-standard protocols or simple lines
		s := string(typeChar)
		rest, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		s += rest
		// This is a simple fallback and may not work for complex cases
		return []interface{}{strings.TrimSpace(s)}, nil
	}
}

func EncodeRESP(cmd []interface{}) string {
	var s strings.Builder
	s.WriteString(fmt.Sprintf("*%d\r\n", len(cmd)))
	for _, arg := range cmd {
		str := fmt.Sprintf("%v", arg)
		s.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(str), str))
	}
	return s.String()
}

// --- Stream Helpers ---
func generateStreamID(key string) string {
	ms := time.Now().UnixMilli()
	lastID := store.streamLastID[key]
	if lastID == "" {
		return fmt.Sprintf("%d-1", ms)
	}

	lastParts := strings.Split(lastID, "-")
	lastMs, _ := strconv.ParseInt(lastParts[0], 10, 64)
	lastSeq, _ := strconv.ParseInt(lastParts[1], 10, 64)

	if ms == lastMs {
		return fmt.Sprintf("%d-%d", ms, lastSeq+1)
	}
	return fmt.Sprintf("%d-0", ms)
}

func generateStreamIDWithSequence(key, id string) string {
	parts := strings.Split(id, "-")
	ms := parts[0]
	
	lastID := store.streamLastID[key]
	if lastID == "" {
		if ms == "0" {
			return "0-1"
		}
		return fmt.Sprintf("%s-0", ms)
	}
	
	lastParts := strings.Split(lastID, "-")
	lastMs := lastParts[0]
	lastSeq, _ := strconv.ParseInt(lastParts[1], 10, 64)
	
	if ms == lastMs {
		return fmt.Sprintf("%s-%d", ms, lastSeq+1)
	}
	
	return fmt.Sprintf("%s-0", ms)
}

func isValidStreamID(streamKey, newID string) bool {
	lastID := store.streamLastID[streamKey]
	if lastID == "" {
		lastID = "0-0"
	}
	
	if newID == "0-0" {
		return false
	}
	return compareStreamIDs(newID, lastID) > 0
}

func compareStreamIDs(id1, id2 string) int {
	parts1 := strings.Split(id1, "-")
	parts2 := strings.Split(id2, "-")

	ms1, _ := strconv.ParseInt(parts1[0], 10, 64)
	ms2, _ := strconv.ParseInt(parts2[0], 10, 64)
	seq1, _ := strconv.ParseInt(parts1[1], 10, 64)
	seq2, _ := strconv.ParseInt(parts2[1], 10, 64)

	if ms1 > ms2 {
		return 1
	}
	if ms1 < ms2 {
		return -1
	}
	if seq1 > seq2 {
		return 1
	}
	if seq1 < seq2 {
		return -1
	}
	return 0
}

func writeStreamEntries(conn net.Conn, entries []StreamEntry) {
	fmt.Fprintf(conn, "*%d\r\n", len(entries))
	for _, entry := range entries {
		fmt.Fprintf(conn, "*2\r\n$%d\r\n%s\r\n", len(entry.ID), entry.ID)
		fmt.Fprintf(conn, "*%d\r\n", len(entry.Fields)*2)
		for key, val := range entry.Fields {
			fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(key), key)
			fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(val), val)
		}
	}
}

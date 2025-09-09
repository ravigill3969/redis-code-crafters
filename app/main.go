package main

import (
	"bufio"
	"encoding/hex"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const emptyRdb = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
const nullRespStr = "$-1\r\n"

type CacheItem struct {
	value     string
	expiresAt int64
}

var cache = map[string]CacheItem{}
var configParams = map[string]string{}
var replicas = []net.Conn{}
var replicasLock = sync.Mutex{}
var bytesProcessed = 0

func main() {
	dirFlag := flag.String("dir", "", "")
	dbFilenameFlag := flag.String("dbfilename", "", "")
	portFlag := flag.String("port", "", "")
	replicaofFlag := flag.String("replicaof", "", "")
	flag.Parse()

	configParams["dir"] = *dirFlag
	configParams["dbfilename"] = *dbFilenameFlag
	configParams["port"] = *portFlag
	configParams["role"] = "master"
	configParams["master"] = *replicaofFlag

	if configParams["port"] == "" {
		configParams["port"] = "6379"
	}

	if *replicaofFlag != "" {
		configParams["role"] = "slave"
	}

	if configParams["role"] == "master" {
		configParams["replId"] = generateReplId()
		configParams["replOffset"] = "0"
	}

	l, err := net.Listen("tcp", "0.0.0.0:"+configParams["port"])
	if err != nil {
		fmt.Printf("Failed to bind to port %s\n", configParams["port"])
		os.Exit(1)
	}

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				continue
			}
			reader := bufio.NewReader(conn)
			go handleClient(conn, reader)
		}
	}()

	if configParams["role"] == "slave" {
		parts := strings.Split(configParams["master"], " ")
		conn, err := net.Dial("tcp", parts[0]+":"+parts[1])
		if err != nil {
			fmt.Printf("Failed to connect to master (%s:%s)\n", parts[0], parts[1])
			os.Exit(1)
		}
		fmt.Printf("Connected to master (%s:%s)\n", parts[0], parts[1])
		reader := bufio.NewReader(conn)
		go handleMaster(conn, reader)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	fmt.Println("Shutting down gracefully...")
	l.Close()
	os.Exit(0)
}

func handleClient(conn net.Conn, reader *bufio.Reader) {
	defer conn.Close()
	numArgsLeft := 0
	command := ""
	rawCommand := ""
	args := []string{}

	for {
		part, err := readResp(reader)
		if err != nil {
			fmt.Println("Error reading from connection (will close): ", err.Error())
			return
		}
		rawCommand += part + "\r\n"

		if numArgsLeft == 0 && (part[0] != '*' || len(part) == 1) {
			continue
		}

		if numArgsLeft == 0 {
			numArgsLeft, _ = strconv.Atoi(part[1:])
			continue
		}

		switch part[0] {
		case '$':
			continue
		default:
			if len(command) == 0 {
				command = strings.ToLower(part)
			} else {
				args = append(args, part)
			}
			numArgsLeft--
		}

		if numArgsLeft == 0 {
			runCommand(rawCommand, command, args, conn)
			command = ""
			rawCommand = ""
			args = []string{}
		}
	}
}

func handleMaster(conn net.Conn, reader *bufio.Reader) {
	// Perform handshake
	performHandshake(conn, reader)

	// Process commands from master
	numArgsLeft := 0
	command := ""
	rawCommand := ""
	args := []string{}

	for {
		part, err := readResp(reader)
		if err != nil {
			fmt.Println("Error reading from master (will close): ", err.Error())
			return
		}
		rawCommand += part + "\r\n"

		if numArgsLeft == 0 && (part[0] != '*' || len(part) == 1) {
			continue
		}

		if numArgsLeft == 0 {
			numArgsLeft, _ = strconv.Atoi(part[1:])
			continue
		}

		switch part[0] {
		case '$':
			continue
		default:
			if len(command) == 0 {
				command = strings.ToLower(part)
			} else {
				args = append(args, part)
			}
			numArgsLeft--
		}

		if numArgsLeft == 0 {
			// Handle commands from master
			if command == "replconf" && len(args) > 0 && strings.ToLower(args[0]) == "getack" {
				// Don't increment bytesProcessed for GETACK commands
				handleReplconfGetack(conn)
			} else {
				// Update bytes processed for other commands
				bytesProcessed += len(rawCommand)
				
				// Execute command locally
				if command == "set" {
					executeSetCommand(args)
				}
			}

			command = ""
			rawCommand = ""
			args = []string{}
		}
	}
}

func performHandshake(conn net.Conn, reader *bufio.Reader) {
	// Send PING
	conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	response, _ := readResp(reader)
	if response != "PONG" {
		fmt.Printf("Expected PONG, got: %s\n", response)
		return
	}

	// Send REPLCONF listening-port
	port := configParams["port"]
	replconfCmd := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n", len(port), port)
	conn.Write([]byte(replconfCmd))
	response, _ = readResp(reader)
	if response != "OK" {
		fmt.Printf("Expected OK after listening-port, got: %s\n", response)
		return
	}

	// Send REPLCONF capa psync2
	conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"))
	response, _ = readResp(reader)
	if response != "OK" {
		fmt.Printf("Expected OK after capa, got: %s\n", response)
		return
	}

	// Send PSYNC
	conn.Write([]byte("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"))
	
	// Read FULLRESYNC response
	response, _ = readResp(reader)
	fmt.Printf("PSYNC response: %s\n", response)

	// Read RDB file
	rdbLen, _ := readResp(reader)
	if len(rdbLen) > 0 && rdbLen[0] == '$' {
		length, err := strconv.Atoi(rdbLen[1:])
		if err == nil && length > 0 {
			// Read the RDB data
			rdbData := make([]byte, length)
			conn.Read(rdbData)
			fmt.Printf("Received RDB file of %d bytes\n", length)
		}
	}

	fmt.Println("Handshake completed successfully")
}

func handleReplconfGetack(conn net.Conn) {
	response := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%d\r\n", 
		len(strconv.Itoa(bytesProcessed)), bytesProcessed)
	conn.Write([]byte(response))
}

func executeSetCommand(args []string) {
	if len(args) < 2 {
		return
	}

	now := time.Now()
	expiresAt := int64(-1)

	// Parse expiration if provided
	for i, arg := range args {
		switch strings.ToLower(arg) {
		case "px":
			if i+1 < len(args) {
				ttl, err := strconv.Atoi(args[i+1])
				if err == nil {
					expiresAt = now.UnixMilli() + int64(ttl)
				}
			}
		}
	}

	cache[args[0]] = CacheItem{
		value:     args[1],
		expiresAt: expiresAt,
	}
	fmt.Printf("Set key: %s = %s\n", args[0], args[1])
}

func readResp(reader *bufio.Reader) (string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	
	line = strings.TrimRight(line, "\r\n")
	
	if len(line) == 0 {
		return "", fmt.Errorf("empty line")
	}

	switch line[0] {
	case '+':
		return line[1:], nil
	case '-':
		return line[1:], nil
	case ':':
		return line[1:], nil
	case '$':
		length, err := strconv.Atoi(line[1:])
		if err != nil {
			return "", err
		}
		if length == -1 {
			return "", nil
		}
		data := make([]byte, length+2) // +2 for \r\n
		_, err = reader.Read(data)
		if err != nil {
			return "", err
		}
		return string(data[:length]), nil
	case '*':
		return line, nil
	default:
		return line, nil
	}
}

// Command handlers
func echo(args []string, conn net.Conn) error {
	if len(args) == 0 {
		return fmt.Errorf("error performing echo: no args")
	}
	if _, err := write(conn, []byte(toRespStr(args[0]))); err != nil {
		return fmt.Errorf("error performing echo: %w", err)
	}
	return nil
}

func ping(args []string, conn net.Conn) error {
	if _, err := write(conn, []byte("+PONG\r\n")); err != nil {
		return fmt.Errorf("error performing ping: %w", err)
	}
	return nil
}

func set(args []string, conn net.Conn) error {
	now := time.Now()
	if len(args) < 2 {
		return fmt.Errorf("error performing set: not enough args")
	}

	expiresAt := int64(-1)
	for i, arg := range args {
		switch strings.ToLower(arg) {
		case "px":
			if i+1 < len(args) {
				ttl, err := strconv.Atoi(args[i+1])
				if err != nil {
					ttl = 0
				}
				expiresAt = now.UnixMilli() + int64(ttl)
			}
		}
	}

	cache[args[0]] = CacheItem{
		value:     args[1],
		expiresAt: expiresAt,
	}
	if _, err := write(conn, []byte("+OK\r\n")); err != nil {
		return fmt.Errorf("error performing set: %w", err)
	}
	return nil
}

func get(args []string, conn net.Conn) error {
	now := time.Now().UnixMilli()
	if len(args) == 0 {
		return fmt.Errorf("error performing get: no args")
	}

	entry, exists := cache[args[0]]
	if !exists {
		if _, err := conn.Write([]byte(nullRespStr)); err != nil {
			return fmt.Errorf("error performing get: %w", err)
		}
		return nil
	}

	writeVal := toRespStr(entry.value)
	if entry.expiresAt != -1 && now >= entry.expiresAt {
		writeVal = nullRespStr
	}

	if _, err := conn.Write([]byte(writeVal)); err != nil {
		return fmt.Errorf("error performing get: %w", err)
	}
	return nil
}

func info(args []string, conn net.Conn) error {
	if len(args) == 0 {
		return fmt.Errorf("error performing info: no args")
	}

	if args[0] == "replication" {
		response := "role:" + configParams["role"]
		if configParams["role"] == "master" {
			addToInfoResponse("master_replid", configParams["replId"], &response)
			addToInfoResponse("master_repl_offset", configParams["replOffset"], &response)
		}

		if _, err := conn.Write([]byte(toRespStr(response))); err != nil {
			return fmt.Errorf("error performing info: %w", err)
		}
	}
	return nil
}

func replconf(args []string, conn net.Conn) error {
	if len(args) == 0 {
		return fmt.Errorf("error performing replconf: no args")
	}

	switch strings.ToLower(args[0]) {
	case "listening-port":
		if _, err := conn.Write([]byte("+OK\r\n")); err != nil {
			return fmt.Errorf("error performing replconf: %w", err)
		}
	case "capa":
		if _, err := conn.Write([]byte("+OK\r\n")); err != nil {
			return fmt.Errorf("error performing replconf: %w", err)
		}
	case "getack":
		response := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%d\r\n", 
			len(strconv.Itoa(bytesProcessed)), bytesProcessed)
		if _, err := conn.Write([]byte(response)); err != nil {
			return fmt.Errorf("error performing replconf: %w", err)
		}
	}
	return nil
}

func psync(args []string, conn net.Conn) error {
	if len(args) < 2 {
		return fmt.Errorf("error performing psync: not enough args")
	}

	if args[0] == "?" {
		response := fmt.Sprintf("+FULLRESYNC %s 0\r\n", configParams["replId"])
		if _, err := write(conn, []byte(response)); err != nil {
			return fmt.Errorf("error performing psync: %w", err)
		}

		emptyRdbAsBytes, _ := hex.DecodeString(emptyRdb)
		fileResponse := append([]byte(fmt.Sprintf("$%d\r\n", len(emptyRdbAsBytes))), emptyRdbAsBytes...)
		if _, err := write(conn, fileResponse); err != nil {
			return fmt.Errorf("error performing psync: %w", err)
		}

		replicasLock.Lock()
		defer replicasLock.Unlock()
		replicas = append(replicas, conn)
	}
	return nil
}

type Command struct {
	handler         func([]string, net.Conn) error
	shouldReplicate bool
}

var commands = map[string]Command{
	"echo":     {handler: echo, shouldReplicate: false},
	"ping":     {handler: ping, shouldReplicate: false},
	"set":      {handler: set, shouldReplicate: true},
	"get":      {handler: get, shouldReplicate: false},
	"info":     {handler: info, shouldReplicate: false},
	"replconf": {handler: replconf, shouldReplicate: false},
	"psync":    {handler: psync, shouldReplicate: false},
}

func runCommand(rawCommand string, commandName string, args []string, conn net.Conn) {
	command, exists := commands[commandName]
	if !exists {
		fmt.Printf("Error running command '%s': command does not exist\n", commandName)
		return
	}

	fmt.Printf("%s running command: %s %v\n", configParams["role"], commandName, args)

	err := command.handler(args, conn)
	if err != nil {
		fmt.Println("Error running command: ", err.Error())
	}

	if command.shouldReplicate && configParams["role"] == "master" {
		go func() {
			replicasLock.Lock()
			defer replicasLock.Unlock()

			for _, replica := range replicas {
				if _, err := replica.Write([]byte(rawCommand)); err != nil {
					fmt.Println("Failed to relay command to replica")
					continue
				}
			}
		}()
	}
}

// Utility functions
func write(conn net.Conn, data []byte) (int, error) {
	return conn.Write(data)
}

func toRespStr(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

func toRespArr(elements ...string) string {
	result := fmt.Sprintf("*%d\r\n", len(elements))
	for _, element := range elements {
		result += toRespStr(element)
	}
	return result
}

func addToInfoResponse(key, value string, response *string) {
	if value != "" {
		*response += "\r\n" + key + ":" + value
	}
}

func generateReplId() string {
	return "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
}

// Placeholder functions for missing functionality
func getKeys() (map[string]interface{}, error) {
	return make(map[string]interface{}), nil
}

func getValue(valueInfo interface{}) (string, error) {
	return "", nil
}
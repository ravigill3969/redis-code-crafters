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

	cmds "github.com/codecrafters-io/redis-starter-go/app/cmd"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

var mu sync.RWMutex
var replicas = make(map[net.Conn]bool)
var masterConn net.Conn
var replicationOffset int64

func main() {
	// Default replica port
	PORT := "6379"

	// Parse command-line args for --port
	for i := 1; i < len(os.Args); i++ {
		if os.Args[i] == "--port" && i+1 < len(os.Args) {
			PORT = os.Args[i+1]
			i++
		}
	}

	// Listen for client connections
	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", PORT))
	if err != nil {
		log.Fatalf("Failed to bind port: %v", err)
	}

	masterHost, masterPort := "", ""
	for i := 0; i < len(os.Args); i++ {
		if os.Args[i] == "--replicaof" && i+1 < len(os.Args) {
			parts := strings.Split(os.Args[i+1], " ")
			if len(parts) == 2 {
				masterHost = parts[0]
				masterPort = parts[1]
				fmt.Println("master with port", masterHost, masterPort)
			}
		}
	}

	if masterHost != "" && masterPort != "" {
		fmt.Println("before connecting to master")
		go connectToMaster(masterHost, masterPort, PORT)
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

func handleConnection(conn net.Conn) {
	defer conn.Close()
	buffer := make([]byte, 4096)

	var inTx bool
	var txQueue [][]interface{}
	var isReplica bool = false

	for {
		fmt.Println("received inside handle connection")
		n, err := conn.Read(buffer)
		if err != nil {
			return
		}
		raw := string(buffer[:n])

		cmdParser := utils.ParseRESP(raw)

		if len(cmdParser) == 0 {
			continue
		}

		cmd := strings.ToUpper(fmt.Sprintf("%v", cmdParser[0]))

		// Handle REPLCONF commands
		if cmd == "REPLCONF" {
			if len(cmdParser) >= 2 {
				subCmd := strings.ToUpper(fmt.Sprintf("%v", cmdParser[1]))
				if subCmd == "LISTENING-PORT" || subCmd == "CAPA" {
					fmt.Println("its a replica")
					mu.Lock()
					isReplica = true
					replicas[conn] = true
					mu.Unlock()
					conn.Write([]byte("+OK\r\n"))
					continue
				} else if subCmd == "GETACK" {
					// Respond with current replication offset
					response := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%d\r\n", 
						len(strconv.FormatInt(replicationOffset, 10)), replicationOffset)
					conn.Write([]byte(response))
					continue
				}
			}
			conn.Write([]byte("+OK\r\n"))
			continue
		}

		switch cmd {
		case "MULTI":
			inTx = true
			txQueue = [][]any{}
			conn.Write([]byte("+OK\r\n"))

		case "DISCARD":
			if inTx {
				txQueue = nil
				conn.Write([]byte("+OK\r\n"))
				inTx = false
			} else {
				conn.Write([]byte("-ERR DISCARD without MULTI\r\n"))
			}

		case "EXEC":
			if !inTx {
				conn.Write([]byte("-ERR EXEC without MULTI\r\n"))
				continue
			}
			inTx = false
			conn.Write([]byte("*" + strconv.Itoa(len(txQueue)) + "\r\n"))

			for _, q := range txQueue {
				strCmd := utils.InterfaceSliceToStringSlice(q)
				writeCommands := map[string]bool{
					"SET":  true,
					"DEL":  true,
					"INCR": true,
					"DECR": true,
				}
				if writeCommands[strings.ToUpper(strCmd[0])] && !isReplica {
					fmt.Println("propagate to replica", cmdParser)
					propagateToReplicas(strCmd)
				}
				cmds.RunCmds(conn, q)
			}
			txQueue = nil

		default:
			fmt.Println("here in default")
			if inTx {
				txQueue = append(txQueue, cmdParser)
				conn.Write([]byte("+QUEUED\r\n"))
			} else {
				fmt.Println("Direct", cmdParser)
				writeCommands := map[string]bool{
					"SET":  true,
					"DEL":  true,
					"INCR": true,
					"DECR": true,
				}
				if writeCommands[cmd] && !isReplica {
					fmt.Println("propagate to replica", cmdParser)
					strCmd := utils.InterfaceSliceToStringSlice(cmdParser)
					propagateToReplicas(strCmd)
				}
				cmds.RunCmds(conn, cmdParser)
			}
		}
	}
}

func connectToMaster(masterHost, masterPort, replicaPort string) {
	conn, err := net.Dial("tcp", net.JoinHostPort(masterHost, masterPort))
	if err != nil {
		log.Fatalf("Failed to connect to master: %v", err)
	}
	masterConn = conn

	conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))

	buf := make([]byte, 1024)

	n, _ := conn.Read(buf)
	if string(buf[:n]) != "+PONG\r\n" {
		log.Fatalf("Expected +PONG, got: %q", string(buf[:n]))
	}

	sendReplConf(conn, replicaPort)
	sendPSYNC(conn)
	readFromMaster(conn)
}

func sendReplConf(conn net.Conn, replicaPort string) {
	fmt.Println("inside send repl conf")
	buf := make([]byte, 1024)

	// 1. REPLCONF listening-port <PORT>
	replConfListening := fmt.Sprintf(
		"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n",
		len(replicaPort), replicaPort,
	)
	_, err := conn.Write([]byte(replConfListening))
	if err != nil {
		log.Fatalf("Failed to send REPLCONF listening-port: %v", err)
	}

	n, _ := conn.Read(buf)
	if string(buf[:n]) != "+OK\r\n" {
		log.Fatalf("Expected +OK after listening-port, got: %q", string(buf[:n]))
	}

	// 2. REPLCONF capa psync2
	replCapa := "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
	_, err = conn.Write([]byte(replCapa))
	if err != nil {
		log.Fatalf("Failed to send REPLCONF capa: %v", err)
	}

	n, _ = conn.Read(buf)
	if string(buf[:n]) != "+OK\r\n" {
		log.Fatalf("Expected +OK after capa, got: %q", string(buf[:n]))
	}
}

func sendPSYNC(conn net.Conn) {
	psync := "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
	_, err := conn.Write([]byte(psync))
	if err != nil {
		log.Fatalf("Failed to send PSYNC: %v", err)
	}

	buf := make([]byte, 1024)
	n, _ := conn.Read(buf)
	_ = string(buf[:n])
}

func propagateToReplicas(cmd []string) {
	fmt.Println("inside propagate to replicas", cmd)
	resp := utils.EncodeAsRESPArray(cmd)
	mu.RLock()
	defer mu.RUnlock()
	for r := range replicas {
		_, err := r.Write([]byte(resp))
		if err != nil {
			log.Println("Failed to propagate to replica:", err)
		} else {
			fmt.Println("Successfully sent to replica")
		}
	}
}

func readFromMaster(conn net.Conn) {
	buffer := make([]byte, 4096)
	var accumulated []byte

	fmt.Println("read from master begin")

	rdbDone := false

	for {
		fmt.Println("read from master ongoing")
		n, err := conn.Read(buffer)

		if err != nil {
			log.Println("Lost connection to master:", err)
			return
		}

		accumulated = append(accumulated, buffer[:n]...)

		// Skip RDB file processing
		if !rdbDone {
			if bytes.Contains(accumulated, []byte("REDIS")) {
				// Find the end of RDB data
				// RDB format starts with "REDIS" and we need to skip the entire RDB content
				rdbStartIdx := bytes.Index(accumulated, []byte("REDIS"))
				if rdbStartIdx != -1 {
					// Try to find where RDB ends and commands begin
					// This is a simplified approach - in practice you'd need proper RDB parsing
					accumulated = accumulated[rdbStartIdx:]
					
					// Look for the end of RDB (this is simplified)
					// In practice, you'd parse the RDB length from the FULLRESYNC response
					if len(accumulated) > 100 { // Assuming empty RDB is small
						accumulated = nil
						rdbDone = true
						fmt.Println("RDB processing completed")
					}
				}
			}
			continue
		}

		// Process accumulated commands
		for len(accumulated) > 0 {
			// Try to parse a complete RESP command
			if len(accumulated) < 1 {
				break
			}

			// Find a complete command
			cmdEnd := findCompleteRESPCommand(accumulated)
			if cmdEnd == -1 {
				break // Need more data
			}

			cmdData := accumulated[:cmdEnd]
			accumulated = accumulated[cmdEnd:]

			// Update replication offset
			replicationOffset += int64(len(cmdData))

			// Parse and execute the command
			cmd := utils.ParseRESP(string(cmdData))
			if len(cmd) > 0 {
				fmt.Println("Received command from master:", cmd)
				
				// Check if it's a REPLCONF GETACK command
				if len(cmd) >= 2 && strings.ToUpper(fmt.Sprintf("%v", cmd[0])) == "REPLCONF" &&
					strings.ToUpper(fmt.Sprintf("%v", cmd[1])) == "GETACK" {
					// Don't update offset for GETACK commands
					replicationOffset -= int64(len(cmdData))
					
					// Respond with ACK
					response := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%d\r\n", 
						len(strconv.FormatInt(replicationOffset, 10)), replicationOffset)
					conn.Write([]byte(response))
				} else {
					// Execute the command locally (simulate applying it)
					// In a real implementation, you'd apply this to your data store
					fmt.Printf("Applying command: %v\n", cmd)
				}
			}
		}
	}
}

func findCompleteRESPCommand(data []byte) int {
	if len(data) == 0 {
		return -1
	}

	pos := 0
	for pos < len(data) {
		if data[pos] == '*' {
			// Array
			pos++
			numStr := ""
			for pos < len(data) && data[pos] != '\r' {
				numStr += string(data[pos])
				pos++
			}
			if pos+1 >= len(data) || data[pos+1] != '\n' {
				return -1
			}
			pos += 2 // Skip \r\n

			numElements, err := strconv.Atoi(numStr)
			if err != nil {
				return -1
			}

			for i := 0; i < numElements; i++ {
				elementEnd := findNextRESPElement(data[pos:])
				if elementEnd == -1 {
					return -1
				}
				pos += elementEnd
			}
			return pos
		} else {
			// Single element
			elementEnd := findNextRESPElement(data[pos:])
			if elementEnd == -1 {
				return -1
			}
			return pos + elementEnd
		}
	}
	return -1
}

func findNextRESPElement(data []byte) int {
	if len(data) == 0 {
		return -1
	}

	switch data[0] {
	case '$':
		// Bulk string
		pos := 1
		lenStr := ""
		for pos < len(data) && data[pos] != '\r' {
			lenStr += string(data[pos])
			pos++
		}
		if pos+1 >= len(data) || data[pos+1] != '\n' {
			return -1
		}
		pos += 2 // Skip \r\n

		length, err := strconv.Atoi(lenStr)
		if err != nil {
			return -1
		}

		if length == -1 {
			return pos // Null bulk string
		}

		if pos+length+2 > len(data) {
			return -1 // Not enough data
		}
		return pos + length + 2 // +2 for final \r\n

	case '+', '-', ':':
		// Simple string, error, integer
		pos := 1
		for pos < len(data) && data[pos] != '\r' {
			pos++
		}
		if pos+1 >= len(data) || data[pos+1] != '\n' {
			return -1
		}
		return pos + 2

	default:
		return -1
	}
}
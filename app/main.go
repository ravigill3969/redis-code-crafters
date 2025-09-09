package main

import (
	"bytes"
	"fmt"
	"io"
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
var isMaster bool = true

func main() {
	PORT := "6379"

	for i := 1; i < len(os.Args); i++ {
		if os.Args[i] == "--port" && i+1 < len(os.Args) {
			PORT = os.Args[i+1]
			i++
		}
	}

	masterHost, masterPort := "", ""
	for i := 0; i < len(os.Args); i++ {
		if os.Args[i] == "--replicaof" && i+1 < len(os.Args) {
			parts := strings.Split(os.Args[i+1], " ")
			if len(parts) == 2 {
				masterHost = parts[0]
				masterPort = parts[1]
				isMaster = false
				fmt.Println("This is a replica, connecting to master:", masterHost, masterPort)
			}
		}
	}

	if masterHost != "" && masterPort != "" {
		go connectToMaster(masterHost, masterPort, PORT)
	}

	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", PORT))
	if err != nil {
		log.Fatalf("Failed to bind port: %v", err)
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
	var inTx bool
	var txQueue [][]interface{}
	var isReplicaConn bool = false

	buffer := make([]byte, 4096)
	reader := bytes.NewBuffer(nil)

	for {
		n, err := conn.Read(buffer)
		if err != nil {
			if err != io.EOF {
				log.Println("Error reading from connection:", err)
			}
			return
		}
		reader.Write(buffer[:n])

		commands, err := utils.ParseRESP(reader)
		if err != nil {
			// Incomplete message, wait for more data
			if err == io.EOF {
				continue
			}
			log.Println("Error parsing RESP:", err)
			continue
		}

		for _, cmdParser := range commands {
			if len(cmdParser) == 0 {
				continue
			}

			cmd := strings.ToUpper(fmt.Sprintf("%v", cmdParser[0]))

			if cmd == "REPLCONF" {
				mu.Lock()
				isReplicaConn = true
				replicas[conn] = true
				mu.Unlock()
				conn.Write([]byte("+OK\r\n"))
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
					writeCommands := map[string]bool{"SET": true, "DEL": true, "INCR": true, "DECR": true}
					if writeCommands[strings.ToUpper(strCmd[0])] && isMaster && !isReplicaConn {
						go propagateToReplicas(strCmd)
					}
					cmds.RunCmds(conn, q)
				}
				txQueue = nil

			default:
				if inTx {
					txQueue = append(txQueue, cmdParser)
					conn.Write([]byte("+QUEUED\r\n"))
				} else {
					writeCommands := map[string]bool{"SET": true, "DEL": true, "INCR": true, "DECR": true}
					if writeCommands[cmd] && isMaster && !isReplicaConn {
						go propagateToReplicas(utils.InterfaceSliceToStringSlice(cmdParser))
					}
					cmds.RunCmds(conn, cmdParser)
				}
			}
		}
	}
}

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
	_, err = conn.Write([]byte(replConfListening))
	if err != nil {
		log.Fatalf("Failed to send REPLCONF listening-port: %v", err)
	}
	n, _ = conn.Read(buf)
	if string(buf[:n]) != "+OK\r\n" {
		log.Fatalf("Expected +OK after listening-port, got: %q", string(buf[:n]))
	}

	// 3. REPLCONF capa psync2
	replCapa := "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
	_, err = conn.Write([]byte(replCapa))
	if err != nil {
		log.Fatalf("Failed to send REPLCONF capa: %v", err)
	}
	n, _ = conn.Read(buf)
	if string(buf[:n]) != "+OK\r\n" {
		log.Fatalf("Expected +OK after capa, got: %q", string(buf[:n]))
	}

	// 4. PSYNC ? -1
	psync := "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
	_, err = conn.Write([]byte(psync))
	if err != nil {
		log.Fatalf("Failed to send PSYNC: %v", err)
	}

	readFromMaster(conn)
}

func sendReplConf(conn net.Conn, replicaPort string) {
	// Already part of connectToMaster
}

func sendPSYNC(conn net.Conn) {
	// Already part of connectToMaster
}

func propagateToReplicas(cmd []string) {
	resp := utils.EncodeAsRESPArray(cmd)
	mu.RLock()
	defer mu.RUnlock()
	for r := range replicas {
		_, err := r.Write([]byte(resp))
		if err != nil {
			log.Println("Failed to propagate to replica:", err)
		}
	}
}

func readFromMaster(conn net.Conn) {
	reader := bytes.NewBuffer(nil)
	buf := make([]byte, 4096)

	// Read the first chunk, which should contain the FULLRESYNC and the start of the RDB
	n, err := conn.Read(buf)
	if err != nil {
		log.Println("Lost connection to master:", err)
		return
	}
	reader.Write(buf[:n])

	// First, handle the FULLRESYNC response
	// It's a simple string, so we'll read until we find the \n
	line, err := reader.ReadString('\n')
	if err != nil || !strings.HasPrefix(line, "+FULLRESYNC") {
		log.Println("Error reading FULLRESYNC response or unexpected format:", err)
		return
	}

	// Now, read the RDB file which is sent as a bulk string
	lengthStr, err := reader.ReadString('\n')
	if err != nil || lengthStr[0] != '$' {
		log.Println("Error reading RDB bulk string length or unexpected format:", err)
		return
	}

	length, err := strconv.Atoi(strings.TrimSpace(lengthStr[1:]))
	if err != nil {
		log.Println("Error parsing RDB length:", err)
		return
	}

	// Read the RDB content
	rdbContent := make([]byte, length)
	bytesRead, err := io.ReadFull(reader, rdbContent)
	if err != nil {
		log.Println("Error reading RDB content:", err)
		return
	}

	log.Printf("Successfully read %d bytes of RDB file. Remaining in buffer: %d\n", bytesRead, reader.Len())

	// The remaining code is fine, it will process the commands in a loop.
	for {
		n, err := conn.Read(buf)
		if err != nil {
			log.Println("Lost connection to master:", err)
			return
		}
		reader.Write(buf[:n])

		commands, err := utils.ParseRESP(reader)
		if err != nil {
			if err == io.EOF {
				continue // Incomplete message, wait for more
			}
			log.Println("Error parsing RESP from master:", err)
			continue
		}

		for _, cmdParser := range commands {
			if len(cmdParser) > 0 {
				cmds.RunCmds(conn, cmdParser)
			}
		}
	}
}

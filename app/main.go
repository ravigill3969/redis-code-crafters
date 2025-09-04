package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/handlers"
)

var redisKeyValueStore = make(map[string]interface{})
var redisKeyExpiryTime = make(map[string]time.Time)
var redisKeyTypeStore = make(map[string]string)

var mu sync.RWMutex
var replicas []net.Conn

func main() {
	PORT := "6379"
	for i := 1; i < len(os.Args); i++ {
		if os.Args[i] == "--port" && i+1 < len(os.Args) {
			PORT = os.Args[i+1]
			i++
		}
	}

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
			}
		}
	}

	if masterHost != "" && masterPort != "" {
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

	for {
		n, err := conn.Read(buffer)
		if err != nil {
			return
		}
		raw := string(buffer[:n])
		cmdParser := ParseRESP(raw)
		if len(cmdParser) == 0 {
			continue
		}

		cmd := strings.ToUpper(fmt.Sprintf("%v", cmdParser[0]))

		// Register replica connection
		if cmd == "REPLCONF" {
			mu.Lock()
			replicas = append(replicas, conn)
			mu.Unlock()
			conn.Write([]byte("+OK\r\n"))
			log.Printf("New replica connected. Total replicas: %d", len(replicas))
			continue
		}

		writeCommands := map[string]bool{"SET": true, "DEL": true, "INCR": true, "DECR": true}

		switch cmd {
		case "MULTI":
			inTx = true
			txQueue = [][]interface{}{}
			conn.Write([]byte("+OK\r\n"))

		case "DISCARD":
			if inTx {
				txQueue = nil
				inTx = false
				conn.Write([]byte("+OK\r\n"))
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
				runCmds(conn, q)
				if writeCommands[strings.ToUpper(fmt.Sprintf("%v", q[0]))] {
					propagateToReplicas(interfaceSliceToStringSlice(q))
				}
			}
			txQueue = nil

		default:
			if inTx {
				txQueue = append(txQueue, deepCopyInterfaceSlice(cmdParser))
				conn.Write([]byte("+QUEUED\r\n"))
			} else {
				runCmds(conn, cmdParser)
				if writeCommands[cmd] {
					propagateToReplicas(interfaceSliceToStringSlice(cmdParser))
				}
			}
		}
	}
}

// Deep copy to avoid slice reuse issues
func deepCopyInterfaceSlice(cmd []interface{}) []interface{} {
	copy := make([]interface{}, len(cmd))
	for i, v := range cmd {
		copy[i] = v
	}
	return copy
}

func runCmds(conn net.Conn, cmdParser []interface{}) {
	switch strings.ToUpper(fmt.Sprintf("%v", cmdParser[0])) {
	case "PING":
		conn.Write([]byte("+PONG\r\n"))
	case "ECHO":
		if len(cmdParser) > 1 {
			conn.Write([]byte("+" + fmt.Sprintf("%v", cmdParser[1]) + "\r\n"))
		} else {
			conn.Write([]byte("+\r\n"))
		}
	case "SET":
		key := fmt.Sprintf("%v", cmdParser[1])
		redisKeyTypeStore[key] = "string"
		handlers.SET(cmdParser[1:], conn)
	case "GET":
		handlers.GET(cmdParser[1:], conn)
	case "TYPE":
		key, ok := cmdParser[1].(string)
		if !ok {
			fmt.Fprintf(conn, "+none\r\n")
			break
		}
		if val, exists := redisKeyTypeStore[key]; exists {
			fmt.Fprintf(conn, "+%s\r\n", val)
		} else {
			fmt.Fprintf(conn, "+none\r\n")
		}
	case "REPLCONF":
		conn.Write([]byte("+OK\r\n"))
	case "PSYNC":
		handlers.PSYNC(conn)
	default:
		conn.Write([]byte("-ERR unknown command\r\n"))
	}
}

func tokenizeRESP(raw string) []string {
	clean := strings.ReplaceAll(raw, "\r\n", "\n")
	lines := strings.Split(clean, "\n")
	tokens := []string{}
	for _, line := range lines {
		if line != "" {
			tokens = append(tokens, line)
		}
	}
	return tokens
}

func ParseRESP(raw string) []interface{} {
	lines := tokenizeRESP(raw)
	cmd := []interface{}{}
	for _, t := range lines {
		if t == "" {
			continue
		}
		if i, err := strconv.Atoi(t); err == nil {
			cmd = append(cmd, i)
		} else {
			cmd = append(cmd, t)
		}
	}
	return cmd
}

func interfaceSliceToStringSlice(cmd []interface{}) []string {
	strCmd := make([]string, len(cmd))
	for i, arg := range cmd {
		switch v := arg.(type) {
		case string:
			strCmd[i] = v
		case int:
			strCmd[i] = strconv.Itoa(v)
		case int64:
			strCmd[i] = strconv.FormatInt(v, 10)
		case []byte:
			strCmd[i] = string(v)
		default:
			strCmd[i] = fmt.Sprintf("%v", arg)
		}
	}
	return strCmd
}

func propagateToReplicas(cmd []string) {
	mu.RLock()
	defer mu.RUnlock()
	log.Printf("Propagating to replicas: %v Total replicas: %d", cmd, len(replicas))
	for _, r := range replicas {
		resp := encodeAsRESPArray(cmd)
		if _, err := r.Write([]byte(resp)); err != nil {
			log.Println("Failed to propagate to replica:", err)
		}
	}
}

func encodeAsRESPArray(cmd []string) string {
	s := fmt.Sprintf("*%d\r\n", len(cmd))
	for _, arg := range cmd {
		s += fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
	}
	return s
}

func connectToMaster(masterHost, masterPort, replicaPort string) {
	conn, err := net.Dial("tcp", net.JoinHostPort(masterHost, masterPort))
	if err != nil {
		log.Fatalf("Failed to connect to master: %v", err)
	}
	defer conn.Close()

	conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	buf := make([]byte, 1024)
	n, _ := conn.Read(buf)
	if string(buf[:n]) != "+PONG\r\n" {
		log.Fatalf("Expected +PONG, got: %q", string(buf[:n]))
	}

	sendReplConf(conn, replicaPort)
	sendPSYNC(conn)
}

func sendReplConf(conn net.Conn, replicaPort string) {
	buf := make([]byte, 1024)
	replConfListening := fmt.Sprintf(
		"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n",
		len(replicaPort), replicaPort,
	)
	conn.Write([]byte(replConfListening))
	conn.Read(buf)

	replCapa := "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
	conn.Write([]byte(replCapa))
	conn.Read(buf)

	log.Println("Replica sent both REPLCONF commands")
}

func sendPSYNC(conn net.Conn) {
	psync := "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
	conn.Write([]byte(psync))
	buf := make([]byte, 1024)
	n, _ := conn.Read(buf)
	log.Println("Received PSYNC response from master:", string(buf[:n]))
}

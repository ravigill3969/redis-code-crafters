package cmds

import (
	"time"

	"bytes"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/app/handlers"

	cmds "github.com/codecrafters-io/redis-starter-go/app/cmd"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

var redisKeyValueStore = make(map[string]interface{})
var redisKeyExpiryTime = make(map[string]time.Time)
var redisKeyTypeStore = make(map[string]string)

var mu sync.RWMutex
var replicas = make(map[net.Conn]bool)

func main() {
	// Default port
	PORT := "6379"

	// Parse --port
	for i := 1; i < len(os.Args); i++ {
		if os.Args[i] == "--port" && i+1 < len(os.Args) {
			PORT = os.Args[i+1]
			i++
		}
	}

	// Listen
	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", PORT))
	if err != nil {
		log.Fatalf("Failed to bind port: %v", err)
	}

	// Check if replica
	masterHost, masterPort := "", ""
	for i := 0; i < len(os.Args); i++ {
		if os.Args[i] == "--replicaof" && i+2 < len(os.Args) {
			masterHost = os.Args[i+1]
			masterPort = os.Args[i+2]
		}
	}

	if masterHost != "" && masterPort != "" {
		go connectToMaster(masterHost, masterPort, PORT)
	}

	// Accept loop
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
		cmdParser := utils.ParseRESP(raw)
		if len(cmdParser) == 0 {
			continue
		}

		cmd := strings.ToUpper(fmt.Sprintf("%v", cmdParser[0]))

		switch cmd {
		case "PSYNC":
			// New replica
			conn.Write([]byte("+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n"))
			conn.Write([]byte("$-1\r\n")) // empty RDB
			mu.Lock()
			replicas[conn] = true
			mu.Unlock()

		case "REPLCONF":
			// Accept ACKs or other replconf
			conn.Write([]byte("+OK\r\n"))

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
				handleCommand(conn, q)
			}
			txQueue = nil

		default:
			if inTx {
				txQueue = append(txQueue, cmdParser)
				conn.Write([]byte("+QUEUED\r\n"))
			} else {
				handleCommand(conn, cmdParser)
			}
		}
	}
}

func handleCommand(conn net.Conn, cmdParser []interface{}) {
	cmd := strings.ToUpper(fmt.Sprintf("%v", cmdParser[0]))
	writeCommands := map[string]bool{
		"SET":  true,
		"DEL":  true,
		"INCR": true,
		"DECR": true,
	}
	if writeCommands[cmd] {
		// Apply locally
		cmds.RunCmds(conn, cmdParser)
		// Propagate
		strCmd := utils.InterfaceSliceToStringSlice(cmdParser)
		propagateToReplicas(strCmd)
	} else {
		cmds.RunCmds(conn, cmdParser)
	}
}

func connectToMaster(masterHost, masterPort, replicaPort string) {
	conn, err := net.Dial("tcp", net.JoinHostPort(masterHost, masterPort))
	if err != nil {
		log.Fatalf("Failed to connect to master: %v", err)
	}

	// Handshake
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
	// REPLCONF listening-port
	replConfListening := fmt.Sprintf(
		"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n",
		len(replicaPort), replicaPort,
	)
	conn.Write([]byte(replConfListening))
	buf := make([]byte, 1024)
	conn.Read(buf)

	// REPLCONF capa psync2
	replCapa := "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
	conn.Write([]byte(replCapa))
	conn.Read(buf)
}

func sendPSYNC(conn net.Conn) {
	psync := "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
	conn.Write([]byte(psync))
	buf := make([]byte, 1024)
	conn.Read(buf) // +FULLRESYNC
	conn.Read(buf) // RDB
}

func propagateToReplicas(cmd []string) {
	resp := utils.EncodeAsRESPArray(cmd)
	mu.RLock()
	defer mu.RUnlock()
	for r := range replicas {
		_, err := r.Write([]byte(resp))
		if err != nil {
			log.Println("Failed to propagate:", err)
		}
	}
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
			if bytes.Contains(accumulated, []byte("REDIS")) || bytes.Contains(accumulated, []byte("$-1")) {
				accumulated = nil
				rdbDone = true
			}
			continue
		}

		cmd := utils.ParseRESP(string(accumulated))
		if len(cmd) == 0 {
			continue
		}
		// Apply locally
		cmds.RunCmds(nil, cmd)

		// Send ACK
		ack := "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n"
		conn.Write([]byte(ack))
		accumulated = nil
	}
}

func RunCmds(conn net.Conn, cmdParser []interface{}) {
	if len(cmdParser) == 0 {
		return
	}

	switch strings.ToUpper(fmt.Sprintf("%v", cmdParser[0])) {
	case "PING":
		if conn != nil {
			conn.Write([]byte("+PONG\r\n"))
		}
	case "ECHO":
		if conn != nil {
			if len(cmdParser) > 1 {
				conn.Write([]byte("+" + fmt.Sprintf("%v", cmdParser[1]) + "\r\n"))
			} else {
				conn.Write([]byte("+\r\n"))
			}
		}
	case "SET":
		key := fmt.Sprintf("%v", cmdParser[1])
		redisKeyTypeStore[key] = "string"
		handlers.SET(cmdParser[1:], conn)
	case "GET":
		handlers.GET(cmdParser[1:], conn)
	case "TYPE":
		key := fmt.Sprintf("%v", cmdParser[1])
		if val, ok := redisKeyTypeStore[key]; ok {
			fmt.Fprintf(conn, "+%s\r\n", val)
		} else {
			fmt.Fprintf(conn, "+none\r\n")
		}
	case "INFO":
		handlers.INFO(conn, cmdParser)
	default:
		if conn != nil {
			conn.Write([]byte("-ERR unknown command\r\n"))
		}
	}
}

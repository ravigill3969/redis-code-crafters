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

var isSlave = false

var mu sync.RWMutex
var replicas = make(map[net.Conn]bool)

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

	// Parse --replicaof argument
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

	// If replica, connect to master and perform handshake
	if masterHost != "" && masterPort != "" {
		connectToMaster(masterHost, masterPort, PORT)
	}

	// Accept client connections (for GET/SET/etc.)
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

		if cmd == "REPLCONF" {
			mu.Lock()
			isReplica = true
			replicas[conn] = true
			mu.Unlock()
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
				runCmds(conn, q)
				strCmd := interfaceSliceToStringSlice(q)
				writeCommands := map[string]bool{
					"SET":  true,
					"DEL":  true,
					"INCR": true,
					"DECR": true,
				}
				if writeCommands[strings.ToUpper(strCmd[0])] && !isReplica {
					propagateToReplicas(strCmd) // propagate only after execution
				}
			}
			txQueue = nil

		default:
			if inTx {
				txQueue = append(txQueue, cmdParser)
				conn.Write([]byte("+QUEUED\r\n"))
			} else {
				// Execute the command first
				runCmds(conn, cmdParser)

				// Then propagate if it's a write command
				writeCommands := map[string]bool{
					"SET":  true,
					"DEL":  true,
					"INCR": true,
					"DECR": true,
				}
				if writeCommands[cmd] {
					strCmd := interfaceSliceToStringSlice(cmdParser)
					fmt.Println("Executing and propagating:", strCmd)
					propagateToReplicas(strCmd)
				}
			}
		}
	}
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
		if len(cmdParser) < 2 {
			conn.Write([]byte("-ERR wrong number of arguments\r\n"))

		}
		key := fmt.Sprintf("%v", cmdParser[1])

		redisKeyTypeStore[key] = "string"
		handlers.SET(cmdParser[1:], conn)

	case "GET":
		if len(cmdParser) < 2 {
			conn.Write([]byte("-ERR wrong number of arguments\r\n"))
		}

		handlers.GET(cmdParser[1:], conn)

	case "TYPE":
		key, ok := cmdParser[1].(string)
		if !ok {
			fmt.Fprintf(conn, "+none\r\n")
			break
		}

		val, exists := redisKeyTypeStore[key]

		if exists {
			fmt.Fprintf(conn, "+%s\r\n", val)
		} else {
			fmt.Fprintf(conn, "+none\r\n")
		}

	case "LRANGE":
		res, err := handlers.LRANGE(cmdParser[1:])
		if err != nil {
			conn.Write([]byte("-ERR " + err.Error() + "\r\n"))
		} else {
			fmt.Fprintf(conn, "*%d\r\n", len(res))
			for _, v := range res {
				fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(v), v)
			}
		}

	case "LPUSH":
		redisKeyTypeStore[cmdParser[1].(string)] = "list"
		length, err := handlers.LPUSH(cmdParser[1:])
		if err != nil {
			conn.Write([]byte("-ERR " + err.Error() + "\r\n"))
		} else {
			fmt.Fprintf(conn, ":%d\r\n", length)
		}

	case "LLEN":
		length := handlers.LLEN(cmdParser[1:])

		fmt.Fprintf(conn, ":%d\r\n", length)

	case "LPOP":

		res, ok := handlers.LPOP(cmdParser[1:])

		if len(res) == 1 {
			fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(res[0]), res[0])
		} else if ok {
			fmt.Fprintf(conn, "*%d\r\n", len(res))
			for _, v := range res {
				fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(v), v)
			}
		} else {
			fmt.Fprintf(conn, "$-1\r\n")
		}

	case "RPUSH":
		redisKeyTypeStore[cmdParser[1].(string)] = "list"
		length, err := handlers.RPUSH(cmdParser[1:])
		if err != nil {
			conn.Write([]byte("-ERR " + err.Error() + "\r\n"))
		} else {
			fmt.Fprintf(conn, ":%d\r\n", length)
		}

	case "BLPOP":
		key := fmt.Sprintf("%s", cmdParser[1])
		val, ok := handlers.BLPOP(cmdParser[1:])

		fmt.Println(val)
		if ok {
			fmt.Fprintf(conn, "*2\r\n")
			fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(key), key)
			fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(val), val)
		} else {
			fmt.Fprintf(conn, "*-1\r\n")
		}

	case "XADD":
		key := fmt.Sprintf("%s", cmdParser[1])
		redisKeyTypeStore[key] = "stream"

		id, err := handlers.XADD(cmdParser[1:])
		if err != nil {
			fmt.Fprintf(conn, "-%s\r\n", err.Error())
		} else {
			// send RESP bulk string with the ID
			fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(id), id)
		}

	case "XRANGE":
		handlers.XRANGE(conn, cmdParser[1:])

	case "XREAD":

		handlers.XREAD(conn, cmdParser[1:])

	case "INCR":
		handlers.INCR(cmdParser[1:], conn)

	case "INFO":
		handlers.INFO(conn, cmdParser)

	case "REPLCONF":
		fmt.Fprintf(conn, "+OK\r\n")

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

		switch t[0] {
		case '*':
			if len(t) == 1 {
				cmd = append(cmd, t)
			}
		case '$':
			if len(t) == 1 {
				cmd = append(cmd, t)
			}
		default:
			if i, err := strconv.Atoi(t); err == nil {
				cmd = append(cmd, i)
			} else {
				cmd = append(cmd, t)
			}
		}
	}

	return cmd

}

func connectToMaster(masterHost, masterPort, replicaPort string) {
	conn, err := net.Dial("tcp", net.JoinHostPort(masterHost, masterPort))
	if err != nil {
		log.Fatalf("Failed to connect to master: %v", err)
	}

	// Send PING
	conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	buf := make([]byte, 1024)
	n, _ := conn.Read(buf)
	if string(buf[:n]) != "+PONG\r\n" {
		log.Fatalf("Expected +PONG, got: %q", string(buf[:n]))
	}

	sendReplConf(conn, replicaPort)
	sendPSYNC(conn)
	go readFromMaster(conn)
}

func sendReplConf(conn net.Conn, replicaPort string) {
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

	log.Println("Replica sent both REPLCONF commands")
}

func sendPSYNC(conn net.Conn) {
	psync := "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
	_, err := conn.Write([]byte(psync))
	if err != nil {
		log.Fatalf("Failed to send PSYNC: %v", err)
	}

	buf := make([]byte, 1024)
	n, _ := conn.Read(buf)
	resp := string(buf[:n])
	log.Println("Received PSYNC response from master:", resp)
}

func propagateToReplicas(cmd []string) {
	resp := encodeAsRESPArray(cmd)
	mu.RLock()
	defer mu.RUnlock()
	fmt.Println("Propagating to replicas:", cmd, "Total replicas:", len(replicas))
	for r := range replicas {
		_, err := r.Write([]byte(resp))
		if err != nil {
			log.Println("Failed to propagate to replica:", err)
		} else {
			fmt.Println("Successfully sent to replica")
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

func interfaceSliceToStringSlice(cmd []interface{}) []string {
	strCmd := make([]string, len(cmd))
	for i, arg := range cmd {
		switch v := arg.(type) {
		case string:
			strCmd[i] = v
		case []byte:
			strCmd[i] = string(v)
		default:
			strCmd[i] = fmt.Sprintf("%v", arg) // fallback
		}
	}

	return strCmd
}

func readFromMaster(conn net.Conn) {
	buffer := make([]byte, 4096)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			log.Println("Lost connection to master:", err)
			return
		}

		raw := string(buffer[:n])
		cmdParser := ParseRESP(raw)
		if len(cmdParser) == 0 {
			continue
		}

		runCmds(conn, cmdParser)
	}
}

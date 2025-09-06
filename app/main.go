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
			}
		}
	}

	// If replica, connect to master and perform handshake
	if masterHost != "" && masterPort != "" {
		go connectToMaster(masterHost, masterPort, PORT)
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
		cmdParser := utils.ParseRESP(raw)
		if len(cmdParser) == 0 {
			continue
		}

		cmd := strings.ToUpper(fmt.Sprintf("%v", cmdParser[0]))
		fmt.Println(cmd)

		if cmd == "REPLCONF" {
			mu.Lock()
			isReplica = true
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
				writeCommands := map[string]bool{
					"SET":  true,
					"DEL":  true,
					"INCR": true,
					"DECR": true,
				}
				if writeCommands[strings.ToUpper(strCmd[0])] && isReplica {
					fmt.Println("prpogate to replica", cmdParser)

					propagateToReplicas(strCmd)
				} else {
					fmt.Println("Direct", cmdParser)
					cmds.RunCmds(conn, q)
				}
			}
			txQueue = nil

		default:
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
				if writeCommands[cmd] && isReplica {
					fmt.Println("prpogate to replica", cmdParser)
					strCmd := utils.InterfaceSliceToStringSlice(cmdParser)
					propagateToReplicas(strCmd)
				} else {
					cmds.RunCmds(conn, cmdParser)
				}
			}
		}
	}
}

func connectToMaster(masterHost, masterPort, replicaPort string) {
	fmt.Println("hellllllllllllll yeahhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh")
	conn, err := net.Dial("tcp", net.JoinHostPort(masterHost, masterPort))
	if err != nil {
		log.Fatalf("Failed to connect to master: %v", err)
	}

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
	var replicaOffset int64 = 0
	var rdbMode bool = false

	for {
		n, err := conn.Read(buffer)
		if err != nil {
			log.Println("Lost connection to master:", err)
			return
		}

		accumulated = append(accumulated, buffer[:n]...)

		// Handle full resync (RDB)
		if !rdbMode && bytes.HasPrefix(accumulated, []byte("REDIS")) {
			rdbMode = true
		}

		if rdbMode {
			// Look for the end of the RDB (this is simplistic; in real Redis you'd detect EOF or start of RESP)
			if len(accumulated) < 9 || !bytes.HasPrefix(accumulated[len(accumulated)-9:], []byte("*1\r\n$4\r\nPING\r\n")) {
				// Still receiving RDB
				replicaOffset += int64(n)
				continue
			} else {
				// RDB finished, leave only leftover bytes for RESP parsing
				replicaOffset += int64(n)
				accumulated = accumulated[len(accumulated)-9:] // keep leftover
				rdbMode = false
			}
		}

		// Parse RESP commands
		for len(accumulated) > 0 {
			cmdParser := utils.ParseRESP(string(accumulated))
			if len(cmdParser) == 0 {
				break
			}

			cmd := strings.ToUpper(fmt.Sprintf("%v", cmdParser[0]))

			if cmd == "PING" {
				strCmd := utils.InterfaceSliceToStringSlice(cmdParser)
				resp := utils.EncodeAsRESPArray(strCmd)
				replicaOffset += int64(len(resp))
				accumulated = accumulated[len(resp):]

				continue
			}

			if cmd == "REPLCONF" && len(cmdParser) > 1 && strings.ToUpper(fmt.Sprintf("%v", cmdParser[1])) == "GETACK" {
				// Convert interface{} slice to string slice
				strCmd := utils.InterfaceSliceToStringSlice(cmdParser)

				// Encode the command to RESP
				resp := utils.EncodeAsRESPArray(strCmd)

				// Send REPLCONF ACK with the current replica offset
				response := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%d\r\n",
					len(strconv.FormatInt(replicaOffset, 10)), replicaOffset)
				conn.Write([]byte(response))

				// Update replicaOffset by the length of the RESP command
				replicaOffset += int64(len(resp))

				// Remove the processed bytes from accumulated
				accumulated = accumulated[len(resp):]

				continue
			}

			fmt.Println("received command from master:", cmdParser)
			cmds.RunCmds(conn, cmdParser)
			strCmd := utils.InterfaceSliceToStringSlice(cmdParser)

			// Encode as RESP
			resp := utils.EncodeAsRESPArray(strCmd)

			// Increment replica offset by the length of the RESP bytes
			replicaOffset += int64(len(resp))

			// Remove the processed bytes from accumulated
			accumulated = accumulated[len(resp):]

		}
	}
}

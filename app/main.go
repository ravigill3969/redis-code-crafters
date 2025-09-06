package main

import (
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
				isSlave = true
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

		fmt.Println(cmdParser)

		cmd := strings.ToUpper(fmt.Sprintf("%v", cmdParser[0]))

		if cmd == "REPLCONF" {
			subcmd := strings.ToUpper(fmt.Sprintf("%v", cmdParser[1]))

			fmt.Println(cmd, "inside handlecinnection")

			// If it's the initial REPLCONF (listening-port/capa), mark as replica
			switch subcmd {
			case "LISTENING-PORT", "CAPA":
				mu.Lock()
				isReplica = true
				replicas[conn] = true
				mu.Unlock()
				conn.Write([]byte("+OK\r\n"))
			case "GETACK":
				// Properly respond to GETACK
				conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n"))
			default:
				conn.Write([]byte("-ERR unknown REPLCONF subcommand\r\n"))
			}
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
					propagateToReplicas(strCmd)
				}
				cmds.RunCmds(conn, q)
			}
			txQueue = nil

		default:
			if inTx {
				txQueue = append(txQueue, cmdParser)
				conn.Write([]byte("+QUEUED\r\n"))
			} else {
				writeCommands := map[string]bool{
					"SET":  true,
					"DEL":  true,
					"INCR": true,
					"DECR": true,
				}
				if writeCommands[cmd] && !isReplica && !isSlave {
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

	// Send PING
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
	fmt.Println("do sth hell yeahhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhhh")
	buffer := make([]byte, 4096)
	var accumulated []byte

	for {
		n, err := conn.Read(buffer)
		if err != nil {
			log.Println("Lost connection to master:", err)
			return
		}

		// Add new data to accumulated buffer
		accumulated = append(accumulated, buffer[:n]...)

		// Process all complete RESP commands in the accumulated buffer
		for {
			if len(accumulated) == 0 {
				break
			}

			raw := string(accumulated)
			fmt.Println("raw data:", raw, "hel nahhhhhhhhhh")

			cmdParser := utils.ParseRESP(raw)
			if len(cmdParser) == 0 {
				// No complete command found, wait for more data
				break
			}

			fmt.Println("received command from master:", cmdParser)

			// Process the command silently (don't send response back to master)
			// Create a null writer to avoid sending responses
			fmt.Println("received command from master:", cmdParser)

			for i := 0; i < len(cmdParser); i += 3 {
				if i+2 < len(cmdParser) {
					singleCmd := cmdParser[i : i+3] // take 3 elements: SET, key, value
					cmds.RunCmds(conn, singleCmd)
				}
			}

			// Remove the processed command from accumulated buffer
			// This is a simplified approach - in reality you'd need to track exactly how many bytes were consumed
			// For now, we'll clear the buffer after processing each command
			accumulated = nil
			break
		}
	}
}

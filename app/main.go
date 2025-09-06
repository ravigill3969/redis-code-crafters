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
	resp := string(buf[:n])

	fmt.Println("send psync received res", resp)
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

	var replicaOffset int64 = 0     // tracks total processed bytes
	var readingRDB bool = false     // are we currently reading RDB?
	var rdbBytesRemaining int64 = 0 // how many bytes of RDB remain to read

	for {
		n, err := conn.Read(buffer)
		if err != nil {
			log.Println("Lost connection to master:", err)
			return
		}

		data := buffer[:n]

		// If we are reading RDB bulk data, process it first
		if readingRDB {
			if int64(len(data)) <= rdbBytesRemaining {
				replicaOffset += int64(len(data))
				rdbBytesRemaining -= int64(len(data))
				// Done with RDB?
				if rdbBytesRemaining == 0 {
					readingRDB = false
					fmt.Println("Finished reading RDB, now processing commands")
				}
				continue
			} else {
				// Last chunk of RDB + extra command bytes
				replicaOffset += rdbBytesRemaining
				data = data[rdbBytesRemaining:]
				rdbBytesRemaining = 0
				readingRDB = false
				accumulated = append(accumulated, data...)
			}
		} else {
			// Append new data for normal RESP parsing
			accumulated = append(accumulated, data...)
		}

		// Parse commands in accumulated buffer
		for len(accumulated) > 0 {
			cmdParser := utils.ParseRESP(string(accumulated))
			if len(cmdParser) == 0 {
				// incomplete command, wait for more data
				break
			}

			cmdName := strings.ToUpper(fmt.Sprintf("%v", cmdParser[0]))

			// Handle REPLCONF GETACK
			if cmdName == "REPLCONF" &&
				len(cmdParser) > 1 &&
				strings.ToUpper(fmt.Sprintf("%v", cmdParser[1])) == "GETACK" {

				resp := fmt.Sprintf(
					"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%d\r\n",
					len(strconv.FormatInt(replicaOffset, 10)), replicaOffset,
				)
				_, err := conn.Write([]byte(resp))
				if err != nil {
					log.Println("Failed to respond to GETACK:", err)
					return
				}

				// remove processed command from buffer
				accumulated = accumulated[len(utils.EncodeAsRESPArray(utils.InterfaceSliceToStringSlice(cmdParser))):]
				continue
			}

			// Ignore PING commands
			if cmdName == "PING" {
				replicaOffset += int64(len(utils.EncodeAsRESPArray(utils.InterfaceSliceToStringSlice(cmdParser))))
				accumulated = accumulated[len(utils.EncodeAsRESPArray(utils.InterfaceSliceToStringSlice(cmdParser))):]
				continue
			}

			// Run normal commands from master
			fmt.Println("received command from master:", cmdParser)
			cmds.RunCmds(conn, cmdParser)

			// Update replica offset and remove processed bytes
			replicaOffset += int64(len(utils.EncodeAsRESPArray(utils.InterfaceSliceToStringSlice(cmdParser))))
			accumulated = accumulated[len(utils.EncodeAsRESPArray(utils.InterfaceSliceToStringSlice(cmdParser))):]
		}
	}
}

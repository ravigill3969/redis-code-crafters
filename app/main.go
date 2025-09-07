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
	PORT := "6379"

	// parse --port
	for i := 1; i < len(os.Args); i++ {
		if os.Args[i] == "--port" && i+1 < len(os.Args) {
			PORT = os.Args[i+1]
			i++
		}
	}

	l, err := net.Listen("tcp", "0.0.0.0:"+PORT)
	if err != nil {
		log.Fatalf("Failed to bind port: %v", err)
	}

	// check if replica
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

	// accept client connections
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
					propagateToReplicas(strCmd)
				} else {
					cmds.RunCmds(conn, q)
				}
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
				if writeCommands[cmd] && isReplica {
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
	conn, err := net.Dial("tcp", net.JoinHostPort(masterHost, masterPort))
	if err != nil {
		log.Fatalf("Failed to connect to master: %v", err)
	}

	// PING
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

	// REPLCONF listening-port
	replConfListening := fmt.Sprintf(
		"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n",
		len(replicaPort), replicaPort,
	)
	conn.Write([]byte(replConfListening))
	n, _ := conn.Read(buf)
	if string(buf[:n]) != "+OK\r\n" {
		log.Fatalf("Expected +OK after listening-port, got: %q", string(buf[:n]))
	}

	// REPLCONF capa psync2
	replCapa := "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
	conn.Write([]byte(replCapa))
	n, _ = conn.Read(buf)
	if string(buf[:n]) != "+OK\r\n" {
		log.Fatalf("Expected +OK after capa, got: %q", string(buf[:n]))
	}
}

func sendPSYNC(conn net.Conn) {
	psync := "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
	conn.Write([]byte(psync))

	buf := make([]byte, 1024)
	n, _ := conn.Read(buf)
	resp := string(buf[:n])
	fmt.Println("PSYNC response:", resp)
}

// propagate commands to replicas
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

// read and process master stream
func readFromMaster(conn net.Conn) {
	buffer := make([]byte, 4096)
	var accumulated []byte
	var replicaOffset int64 = 0

	for {
		n, err := conn.Read(buffer)
		if err != nil {
			log.Println("Lost connection to master:", err)
			return
		}
		accumulated = append(accumulated, buffer[:n]...)

		for len(accumulated) > 0 {
			cmdParser := utils.ParseRESP(string(accumulated))
			if len(cmdParser) == 0 {
				break // incomplete command
			}

			cmdName := strings.ToUpper(fmt.Sprintf("%v", cmdParser[0]))

			// FULLRESYNC + RDB handling
			if cmdName == "FULLRESYNC" {
				parts := strings.SplitN(string(accumulated), "\r\n", 3)
				if len(parts) >= 3 && strings.HasPrefix(parts[2], "$") {
					var rdbLen int
					fmt.Sscanf(parts[2], "$%d", &rdbLen)
					// skip header + content + CRLF (and account offset including EOF byte)
					totalSkip := len(parts[0]) + len(parts[1]) + len(parts[2]) + rdbLen + 4
					replicaOffset += int64(totalSkip)
					accumulated = accumulated[totalSkip:]
					continue
				} else {
					accumulated = accumulated[len(parts[0])+len(parts[1])+2:]
					continue
				}
			}

			// REPLCONF GETACK
			if cmdName == "REPLCONF" &&
				len(cmdParser) > 1 &&
				strings.ToUpper(fmt.Sprintf("%v", cmdParser[1])) == "GETACK" {

				resp := fmt.Sprintf(
					"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%d\r\n",
					len(strconv.FormatInt(replicaOffset, 10)), replicaOffset,
				)
				conn.Write([]byte(resp))

				consumed := len(utils.EncodeAsRESPArray(utils.InterfaceSliceToStringSlice(cmdParser)))
				replicaOffset += int64(consumed)
				accumulated = accumulated[consumed:]
				continue
			}

			// Ignore PING
			if cmdName == "PING" {
				consumed := len(utils.EncodeAsRESPArray(utils.InterfaceSliceToStringSlice(cmdParser)))
				replicaOffset += int64(consumed)
				accumulated = accumulated[consumed:]
				continue
			}

			// Normal command from master — DO NOT reply
			fmt.Println("received command from master:", cmdParser)
			cmds.RunCmds(nil, cmdParser)

			consumed := len(utils.EncodeAsRESPArray(utils.InterfaceSliceToStringSlice(cmdParser)))
			replicaOffset += int64(consumed)
			accumulated = accumulated[consumed:]
		}
	}
}

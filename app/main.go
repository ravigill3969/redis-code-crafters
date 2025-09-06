package main

import (
	"bufio"
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

var isSlave = false

var mu sync.RWMutex
var replicas = make(map[net.Conn]bool)

// Offset tracking
var replicaOffset int64
var offsetMu sync.Mutex

func main() {
	PORT := "6379"

	// Parse --port argument
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

	// Parse --replicaof
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
	var isReplica bool

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
			subcmd := strings.ToUpper(fmt.Sprintf("%v", cmdParser[1]))
			switch subcmd {
			case "LISTENING-PORT", "CAPA":
				mu.Lock()
				isReplica = true
				replicas[conn] = true
				mu.Unlock()
				conn.Write([]byte("+OK\r\n"))

			case "GETACK":
				offsetMu.Lock()
				ack := replicaOffset
				offsetMu.Unlock()

				resp := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%d\r\n",
					len(strconv.FormatInt(ack, 10)), ack)
				conn.Write([]byte(resp))

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
	replConfListening := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n",
		len(replicaPort), replicaPort)
	conn.Write([]byte(replConfListening))
	n, _ := conn.Read(buf)
	if string(buf[:n]) != "+OK\r\n" {
		log.Fatalf("Expected +OK after listening-port, got: %q", string(buf[:n]))
	}

	// REPLCONF capa
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

	reader := bufio.NewReader(conn)
	reply, _ := reader.ReadString('\n')
	fmt.Println("PSYNC reply:", strings.TrimSpace(reply))

	if strings.HasPrefix(reply, "+FULLRESYNC") {
		consumeRDB(reader)
	}
}

func consumeRDB(reader *bufio.Reader) {
	line, _ := reader.ReadString('\n') // read $<length>\r\n
	line = strings.TrimSpace(line)
	if !strings.HasPrefix(line, "$") {
		log.Fatalf("Expected RDB bulk string, got: %q", line)
	}
	length, _ := strconv.Atoi(line[1:])
	discarded := 0
	buf := make([]byte, 4096)
	for discarded < length {
		n, _ := reader.Read(buf)
		discarded += n
	}

	fmt.Printf("✅ Discarded RDB payload (%d bytes)\n", discarded)

	offsetMu.Lock()
	replicaOffset += int64(len(line) + 2 + discarded) // $len\r\n + payload
	offsetMu.Unlock()
}

func propagateToReplicas(cmd []string) {
	resp := utils.EncodeAsRESPArray(cmd)
	mu.RLock()
	defer mu.RUnlock()
	for r := range replicas {
		r.Write([]byte(resp))
	}
}

func readFromMaster(conn net.Conn) {
	fmt.Println("Replica: reading from master")
	reader := bufio.NewReader(conn)
	accumulated := []byte{}

	for {
		buf := make([]byte, 4096)
		n, err := reader.Read(buf)
		if err != nil {
			if err == io.EOF {
				log.Println("Lost connection to master: EOF")
				return
			}
			log.Println("Lost connection to master:", err)
			return
		}

		offsetMu.Lock()
		replicaOffset += int64(n)
		offsetMu.Unlock()

		accumulated = append(accumulated, buf[:n]...)

		for {
			cmdParser, bytesRead := utils.ParseRESPWithOffset(accumulated)
			if len(cmdParser) == 0 || bytesRead == 0 {
				break // wait for more data
			}

			// Check if it's a REPLCONF GETACK from master
			if len(cmdParser) > 0 {
				strCmd := strings.ToUpper(fmt.Sprintf("%v", cmdParser[0]))
				if strCmd == "REPLCONF" && len(cmdParser) > 1 {
					subcmd := strings.ToUpper(fmt.Sprintf("%v", cmdParser[1]))
					if subcmd == "GETACK" {
						offsetMu.Lock()
						ack := replicaOffset
						offsetMu.Unlock()
						resp := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%d\r\n",
							len(strconv.FormatInt(ack, 10)), ack)
						conn.Write([]byte(resp))
					}
				} else {
					// normal commands
					cmds.RunCmds(conn, cmdParser)
				}
			}

			// Remove parsed bytes from buffer
			accumulated = accumulated[bytesRead:]
		}
	}
}

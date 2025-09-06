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
func sendPSYNC(conn net.Conn) {
	psync := "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
	_, err := conn.Write([]byte(psync))
	if err != nil {
		log.Fatalf("Failed to send PSYNC: %v", err)
	}

	// Read master reply: should be "+FULLRESYNC <runid> <offset>\r\n"
	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		log.Fatalf("Failed to read PSYNC response: %v", err)
	}
	line := string(buf[:n])
	if !strings.HasPrefix(line, "+FULLRESYNC") {
		log.Fatalf("Expected FULLRESYNC, got: %q", line)
	}

	fmt.Println("Master responded with:", line)

	// Now master will send the RDB file as a bulk string
	// Read the RDB bulk string length first: $<length>\r\n
	rdbStart := strings.Index(line, "$")
	if rdbStart == -1 {
		log.Fatalf("Cannot find RDB bulk start in line: %q", line)
	}
	rdbLenStr := line[rdbStart+1 : len(line)-2] // remove \r\n
	rdbLen, err := strconv.Atoi(rdbLenStr)
	if err != nil {
		log.Fatalf("Invalid RDB length: %v", err)
	}

	// Read RDB bytes
	rdbData := make([]byte, rdbLen)
	read := 0
	for read < rdbLen {
		n, err := conn.Read(rdbData[read:])
		if err != nil {
			log.Fatalf("Failed to read RDB data: %v", err)
		}
		read += n
	}

	fmt.Println("RDB file received, length:", rdbLen)

	// After RDB is fully read, we're ready to process normal commands
	readFromMaster(conn)
}
func readFromMaster(conn net.Conn) {
    // Step 1: Read and discard RDB
    rdbLength := <get from FULLRESYNC reply>
    rdbData := make([]byte, rdbLength)
    read := 0
    for read < rdbLength {
        n, _ := conn.Read(rdbData[read:])
        read += n
    }

    // Step 2: Ready to process RESP commands
    var accumulated []byte
    var offset int64 = 0
    buffer := make([]byte, 4096)

    for {
        n, err := conn.Read(buffer)
        if err != nil {
            log.Println("Lost connection to master:", err)
            return
        }
        accumulated = append(accumulated, buffer[:n]...)

        for len(accumulated) > 0 {
            cmd := utils.ParseRESP(string(accumulated))
            if len(cmd) == 0 {
                break
            }

            if strings.ToUpper(fmt.Sprintf("%v", cmd[0])) == "REPLCONF" &&
               len(cmd) > 1 &&
               strings.ToUpper(fmt.Sprintf("%v", cmd[1])) == "GETACK" {
                resp := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%d\r\n",
                    len(strconv.FormatInt(offset, 10)), offset)
                conn.Write([]byte(resp))
            } else {
                cmds.RunCmds(conn, cmd)
            }

            // Remove parsed bytes from accumulated
            encodedLen := len(utils.EncodeAsRESPArray(utils.InterfaceSliceToStringSlice(cmd)))
            accumulated = accumulated[encodedLen:]
            offset += int64(encodedLen)
        }
    }
}

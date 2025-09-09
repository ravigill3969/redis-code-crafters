package handlers

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/codecrafters-io/redis-starter-go/app/rdb"
	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

var (
	replicas     = make(map[net.Conn]bool)
	replicasLock sync.Mutex
)

// ForwardToReplicas sends a command to all connected replicas.
func ForwardToReplicas(rawCommand string) {
	replicasLock.Lock()
	defer replicasLock.Unlock()

	for replica := range replicas {
		_, err := replica.Write([]byte(rawCommand))
		if err != nil {
			fmt.Println("Failed to propagate command to replica:", err)
		}
	}
}

// ConnectToMaster handles the slave-side handshake with the master.
func ConnectToMaster(masterHost, masterPort, replicaPort string) {
	conn, err := net.Dial("tcp", net.JoinHostPort(masterHost, masterPort))
	if err != nil {
		fmt.Printf("Failed to connect to master: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	// Handshake Steps
	// 1. PING
	conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	resp, _ := reader.ReadString('\n')
	if strings.TrimSpace(resp) != "+PONG" {
		fmt.Printf("Expected +PONG, got: %q\n", resp)
		return
	}

	// 2. REPLCONF listening-port
	replConfListening := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n", len(replicaPort), replicaPort)
	conn.Write([]byte(replConfListening))
	resp, _ = reader.ReadString('\n')
	if strings.TrimSpace(resp) != "+OK" {
		fmt.Printf("Expected +OK after listening-port, got: %q\n", resp)
		return
	}

	// 3. REPLCONF capa psync2
	replCapa := "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
	conn.Write([]byte(replCapa))
	resp, _ = reader.ReadString('\n')
	if strings.TrimSpace(resp) != "+OK" {
		fmt.Printf("Expected +OK after capa, got: %q\n", resp)
		return
	}

	// 4. PSYNC
	psync := "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
	conn.Write([]byte(psync))

	// Receive FULLRESYNC
	fullResyncResp, _ := reader.ReadString('\n')
	if !strings.HasPrefix(fullResyncResp, "+FULLRESYNC") {
		fmt.Printf("Expected +FULLRESYNC, got: %q\n", fullResyncResp)
		return
	}

	// Read RDB file
	rdbLengthHeader, _ := reader.ReadString('\n')
	length, _ := strconv.Atoi(strings.TrimSpace(rdbLengthHeader[1:]))
	rdbFile := make([]byte, length)
	_, _ = reader.Read(rdbFile)
	rdb.SetRDBFile(rdbFile)
	fmt.Println("RDB file received and loaded.")

	// Read and process commands from master
	for {
		_, cmd, args, err := utils.ParseRespCommand(reader)
		if err != nil {
			fmt.Println("Lost connection to master:", err)
			return
		}
		// Slaves just run commands from the master without sending a response.
		handlers.RunCmds(nil, append([]string{cmd}, args...))
	}
}

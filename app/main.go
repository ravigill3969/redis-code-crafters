package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

// global map shared across all connections
var redisKeyValueStore = make(map[string]string)

var redisKeyExpiryTime = make(map[string]time.Time)

// optional mutex for concurrent access
var mu sync.RWMutex

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	// l, err := net.Listen("tcp", "127.0.0.1:6380")

	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	for {

		conn, err := l.Accept()

		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	buffer := make([]byte, 1024)
	for {

		n, err := conn.Read(buffer)
		if err != nil {
			conn.Write([]byte("+Unable to read from request\r\n"))
			return
		}
		cmd := string(buffer[:n])

		cmdParser := parseRESP(cmd)

		if len(cmdParser) == 0 {
			continue
		}

		switch strings.ToUpper(cmdParser[0]) {
		case "PING":
			conn.Write([]byte("+PONG\r\n"))
		case "ECHO":
			if len(cmdParser) > 1 {
				conn.Write([]byte("+" + cmdParser[1] + "\r\n"))
			} else {
				conn.Write([]byte("+\r\n"))
			}
		case "SET":
			if len(cmdParser) < 3 {
				conn.Write([]byte("-ERR wrong number of arguments\r\n"))
				break
			}
			redisKeyValueStore[cmdParser[1]] = cmdParser[2]

			if len(cmdParser) > 3 && cmdParser[3] == "px" {
				msStr := cmdParser[4]
				ms, err := strconv.ParseInt(msStr, 10, 64)
				if err != nil {
					panic(err)
				}

				t := time.Now().Add(time.Duration(ms) * time.Millisecond)

				redisKeyExpiryTime[cmdParser[1]] = t
			}

			fmt.Println(redisKeyExpiryTime)
			fmt.Println(redisKeyValueStore)

			conn.Write([]byte("+OK\r\n"))

		case "GET":
			if len(cmdParser) < 2 {
				conn.Write([]byte("-ERR wrong number of arguments\r\n"))
				break
			}

			val, ok := redisKeyExpiryTime[cmdParser[1]]

			checkExpiry := false

			if ok {
				checkExpiry = true
			}

			if checkExpiry && val.Before(time.Now()) {
				delete(redisKeyExpiryTime, cmdParser[1])
				delete(redisKeyValueStore, cmdParser[1])
			}

			value, ok := redisKeyValueStore[cmdParser[1]]

			fmt.Println(redisKeyExpiryTime)
			fmt.Println(redisKeyValueStore)

			if !ok {
				conn.Write([]byte("$-1\r\n"))
			} else {
				fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(value), value)
			}

		default:
			conn.Write([]byte("+Unknown command\r\n"))
		}

	}

}

func parseRESP(cmd string) []string {
	var parts []string
	lines := strings.SplitSeq(cmd, "\r\n")

	for line := range lines {
		if len(line) == 0 {
			continue
		}
		if line[0] == '*' || line[0] == '$' {
			continue
		}

		words := strings.Fields(line)
		parts = append(parts, words...)
	}

	return parts
}

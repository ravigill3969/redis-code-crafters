package main

import (
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

// global map shared across all connections
var redisGetSet = make(map[string]string)

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

		redisGetSet := map[string]string{}

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
			redisGetSet[cmdParser[1]] = cmdParser[2]
			conn.Write([]byte("+OK\r\n"))

		case "GET":
			if len(cmdParser) < 2 {
				conn.Write([]byte("-ERR wrong number of arguments\r\n"))
				break
			}
			value, ok := redisGetSet[cmdParser[1]]
			if !ok {
				conn.Write([]byte("$-1\r\n"))
			} else {
				conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)))
			}

		default:
			conn.Write([]byte("+Unknown command\r\n"))
		}

	}

}

func parseRESP(cmd string) []string {
	var parts []string
	lines := strings.Split(cmd, "\r\n") // split by CRLF

	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		if line[0] == '*' || line[0] == '$' {
			continue
		}
		parts = append(parts, line)
	}

	return parts
}

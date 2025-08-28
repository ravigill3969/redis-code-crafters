package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/handlers"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

// global map shared across all connections
var redisKeyValueStore = make(map[string]interface{})

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

		cmdParser := ParseRESP(cmd)

		if len(cmdParser) == 0 {
			continue
		}

		switch strings.ToUpper(cmdParser[0].(string)) {
		case "PING":
			conn.Write([]byte("+PONG\r\n"))
		case "ECHO":
			if len(cmdParser) > 1 {
				conn.Write([]byte("+" + cmdParser[1].(string) + "\r\n"))
			} else {
				conn.Write([]byte("+\r\n"))
			}
		case "SET":
			if len(cmdParser) < 3 {
				conn.Write([]byte("-ERR wrong number of arguments\r\n"))
				break
			}
			redisKeyValueStore[cmdParser[1].(string)] = cmdParser[2]

			if len(cmdParser) > 3 && cmdParser[3] == "px" {
				msStr := cmdParser[4]
				ms, err := strconv.ParseInt(msStr.(string), 10, 64)
				if err != nil {
					panic(err)
				}

				t := time.Now().Add(time.Duration(ms) * time.Millisecond)

				redisKeyExpiryTime[cmdParser[1].(string)] = t
			}

			conn.Write([]byte("+OK\r\n"))

		case "GET":
			if len(cmdParser) < 2 {
				conn.Write([]byte("-ERR wrong number of arguments for 'GET'\r\n"))
				break
			}

			key := cmdParser[1]

			if expiry, ok := redisKeyExpiryTime[key.(string)]; ok && expiry.Before(time.Now()) {
				delete(redisKeyExpiryTime, key.(string))
				delete(redisKeyValueStore, key.(string))
			}

			value, ok := redisKeyValueStore[key.(string)]
			if !ok {
				conn.Write([]byte("$-1\r\n"))
			} else {
				fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(value), value)
			}

		case "RPUSH":
			if len(cmdParser) < 3 {
				conn.Write([]byte("-ERR wrong number of arguments\r\n"))
				break
			}
			len, err := handlers.RPUSH(cmdParser[1:])

			if err != nil {
				conn.Write([]byte("$-1\r\n"))
			} else {
				fmt.Fprintf(conn, ":%d\r\n", len)
			}

		case "LRANGE":
			if len(cmdParser) < 3 {
				conn.Write([]byte("-ERR wrong number of arguments\r\n"))
				return
			}

			// res, err := handlers.LRANGE(cmdParser[1:])

			// if err != nil {
			// 	conn.Write([]byte("*0\r\n"))
			// }

			// fmt.Println(res)

		default:
			conn.Write([]byte("+Unknown command\r\n"))
		}

	}

}

// func parseRESP(cmd string) []string {
// 	var parts []string
// 	lines := strings.SplitSeq(cmd, "\r\n")

// 	for line := range lines {
// 		if len(line) == 0 {
// 			continue
// 		}
// 		if line[0] == '*' || line[0] == '$' {
// 			continue
// 		}

// 		words := strings.Fields(line)
// 		parts = append(parts, words...)
// 	}

// 	return parts
// }

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
	tokenizeRESPReturnVal := tokenizeRESP(raw)

	var cmd []interface{}

	for _, t := range tokenizeRESPReturnVal {
		if t[0] == '*' || t[0] == '$' {
			continue
		}

		if n, err := strconv.Atoi(t); err == nil {
			cmd = append(cmd, n)
		} else {
			cmd = append(cmd, t)
		}
	}
	return cmd
}

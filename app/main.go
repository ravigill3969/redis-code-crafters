package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/handlers"
)

// Global store
var redisKeyValueStore = make(map[string]interface{})
var redisKeyExpiryTime = make(map[string]time.Time)
var redisListStore = make(map[string][]string)
var mu sync.RWMutex

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind port:", err)
		return
	}
	fmt.Println("Server listening on 6379")

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	buffer := make([]byte, 4096)

	for {
		n, err := conn.Read(buffer)
		if err != nil {
			return
		}
		raw := string(buffer[:n])
		cmdParser := ParseRESP(raw)
		if len(cmdParser) == 0 {
			continue
		}

		fmt.Println(cmdParser...)

		switch strings.ToUpper(fmt.Sprintf("%v", cmdParser[0])) {
		case "PING":
			conn.Write([]byte("+PONG\r\n"))
		case "ECHO":
			if len(cmdParser) > 1 {
				conn.Write([]byte("+" + fmt.Sprintf("%v", cmdParser[1]) + "\r\n"))
			} else {
				conn.Write([]byte("+\r\n"))
			}

		case "SET":
			if len(cmdParser) < 3 {
				conn.Write([]byte("-ERR wrong number of arguments\r\n"))
				break
			}
			key := fmt.Sprintf("%v", cmdParser[1])
			value := cmdParser[2]

			mu.Lock()
			redisKeyValueStore[key] = value

			// PX expiration optional
			if len(cmdParser) > 3 && strings.ToUpper(fmt.Sprintf("%v", cmdParser[3])) == "PX" {
				ms, _ := strconv.Atoi(fmt.Sprintf("%v", cmdParser[4]))
				redisKeyExpiryTime[key] = time.Now().Add(time.Duration(ms) * time.Millisecond)
			}
			mu.Unlock()
			conn.Write([]byte("+OK\r\n"))

		case "GET":
			if len(cmdParser) < 2 {
				conn.Write([]byte("-ERR wrong number of arguments\r\n"))
				break
			}
			key := fmt.Sprintf("%v", cmdParser[1])

			mu.Lock()
			// Check expiration
			if expiry, ok := redisKeyExpiryTime[key]; ok && expiry.Before(time.Now()) {
				delete(redisKeyExpiryTime, key)
				delete(redisKeyValueStore, key)
			}

			value, ok := redisKeyValueStore[key]
			mu.Unlock()

			if !ok {
				conn.Write([]byte("$-1\r\n"))
			} else {
				valStr := fmt.Sprintf("%v", value)
				fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(valStr), valStr)
			}

		case "RPUSH":
			length, err := handlers.RPUSH(cmdParser[1:])
			if err != nil {
				conn.Write([]byte("-ERR " + err.Error() + "\r\n"))
			} else {
				fmt.Fprintf(conn, ":%d\r\n", length)
			}

		case "LRANGE":
			res, err := handlers.LRANGE(cmdParser[1:])
			if err != nil {
				conn.Write([]byte("-ERR " + err.Error() + "\r\n"))
			} else {
				fmt.Fprintf(conn, "*%d\r\n", len(res))
				for _, v := range res {
					fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(v), v)
				}
			}
			

		case "LPUSH":
			length, err := handlers.LPUSH(cmdParser[1:])
			if err != nil {
				conn.Write([]byte("-ERR " + err.Error() + "\r\n"))
			} else {
				fmt.Fprintf(conn, ":%d\r\n", length)
			}

		case "LLEN":
			length := handlers.LLEN(cmdParser[1:])

			fmt.Fprintf(conn, ":%d\r\n", length)

		case "LPOP":

			str, bool := handlers.LPOP(cmdParser[1:])

			if bool {
				fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(str), str)
				return
			}

			fmt.Fprintf(conn, "$%d\r\n", -1)

		default:
			conn.Write([]byte("-ERR unknown command\r\n"))
		}
	}
}

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
	lines := tokenizeRESP(raw)
	cmd := []interface{}{}
	for _, t := range lines {
		if t[0] == '*' || t[0] == '$' {
			continue
		}
		if n, err := strconv.Atoi(t); err == nil {
			cmd = append(cmd, n)
		} else {
			cmd = append(cmd, t)
		}
	}

		fmt.Println(cmd)

	return cmd
}

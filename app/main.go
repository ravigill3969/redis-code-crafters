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

var redisKeyTypeStore = make(map[string]string)

var mu sync.RWMutex

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	// l, err := net.Listen("tcp", "127.0.0.1:6700")
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
			redisKeyTypeStore[key] = "string"

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

		case "TYPE":
			key, ok := cmdParser[1].(string)
			if !ok {
				fmt.Fprintf(conn, "+none\r\n")
				break
			}

			val, exists := redisKeyTypeStore[key]

			if exists {
				fmt.Fprintf(conn, "+%s\r\n", val)
			} else {
				fmt.Fprintf(conn, "+none\r\n")
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
			redisKeyTypeStore[cmdParser[1].(string)] = "list"
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

			res, ok := handlers.LPOP(cmdParser[1:])

			if len(res) == 1 {
				fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(res[0]), res[0])
			} else if ok {
				fmt.Fprintf(conn, "*%d\r\n", len(res))
				for _, v := range res {
					fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(v), v)
				}
			} else {
				fmt.Fprintf(conn, "$-1\r\n")
			}

		case "RPUSH":
			redisKeyTypeStore[cmdParser[1].(string)] = "list"
			length, err := handlers.RPUSH(cmdParser[1:])
			if err != nil {
				conn.Write([]byte("-ERR " + err.Error() + "\r\n"))
			} else {
				fmt.Fprintf(conn, ":%d\r\n", length)
			}

		case "BLPOP":
			key := fmt.Sprintf("%s", cmdParser[1])
			val, ok := handlers.BLPOP(cmdParser[1:])

			if ok {
				fmt.Fprintf(conn, "*2\r\n")
				fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(key), key)
				fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(val), val)
			} else {
				fmt.Fprintf(conn, "*-1\r\n")
			}

		case "XADD":
			redisKeyTypeStore[cmdParser[1].(string)] = "stream"
			id := handlers.XADD(cmdParser[1:])

			fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(id), id)
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

	fmt.Println("parserdREsp", lines)

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

	fmt.Println("parserdREsp", cmd)

	return cmd
}

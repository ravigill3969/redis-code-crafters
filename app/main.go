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
			if len(cmdParser) < 2 {
				conn.Write([]byte("-ERR wrong number of arguments\r\n"))

			}
			key := fmt.Sprintf("%v", cmdParser[1])

			redisKeyTypeStore[key] = "string"
			handlers.SET(cmdParser[1:], conn)

		case "GET":
			if len(cmdParser) < 2 {
				conn.Write([]byte("-ERR wrong number of arguments\r\n"))
			}

			handlers.GET(cmdParser, conn)

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
			key := fmt.Sprintf("%s", cmdParser[1])
			redisKeyTypeStore[key] = "stream"

			id, err := handlers.XADD(cmdParser[1:])
			if err != nil {
				fmt.Fprintf(conn, "-%s\r\n", err.Error())
			} else {
				// send RESP bulk string with the ID
				fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(id), id)
			}

		case "XRANGE":
			handlers.XRANGE(conn, cmdParser[1:])

		case "XREAD":

			handlers.XREAD(conn, cmdParser[1:])

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
		if t == "" {
			continue
		}

		switch t[0] {
		case '*':
			if len(t) == 1 {
				cmd = append(cmd, t)
			}
		case '$':
			if len(t) == 1 {
				cmd = append(cmd, t)
			}
		default:
			if i, err := strconv.Atoi(t); err == nil {
				cmd = append(cmd, i)
			} else {
				cmd = append(cmd, t)
			}
		}
	}

	return cmd
}

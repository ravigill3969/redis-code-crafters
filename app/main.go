package cmds

import (
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/handlers"
)

var redisKeyValueStore = make(map[string]interface{})
var redisKeyExpiryTime = make(map[string]time.Time)
var redisKeyTypeStore = make(map[string]string)

func RunCmds(conn net.Conn, cmdParser []interface{}) {
	if len(cmdParser) == 0 {
		return
	}

	switch strings.ToUpper(fmt.Sprintf("%v", cmdParser[0])) {
	case "PING":
		if conn != nil {
			conn.Write([]byte("+PONG\r\n"))
		}
	case "ECHO":
		if conn != nil {
			if len(cmdParser) > 1 {
				conn.Write([]byte("+" + fmt.Sprintf("%v", cmdParser[1]) + "\r\n"))
			} else {
				conn.Write([]byte("+\r\n"))
			}
		}
	case "SET":
		key := fmt.Sprintf("%v", cmdParser[1])
		redisKeyTypeStore[key] = "string"
		handlers.SET(cmdParser[1:], conn)
	case "GET":
		handlers.GET(cmdParser[1:], conn)
	case "TYPE":
		key := fmt.Sprintf("%v", cmdParser[1])
		if val, ok := redisKeyTypeStore[key]; ok {
			fmt.Fprintf(conn, "+%s\r\n", val)
		} else {
			fmt.Fprintf(conn, "+none\r\n")
		}
	case "INFO":
		handlers.INFO(conn, cmdParser)
	default:
		if conn != nil {
			conn.Write([]byte("-ERR unknown command\r\n"))
		}
	}
}

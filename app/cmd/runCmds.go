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
	cmdName := strings.ToUpper(fmt.Sprintf("%v", cmdParser[0]))

	// Determine if this is a "silent" run (master connection)
	silent := conn == nil

	switch cmdName {
	case "PING":
		if !silent {
			conn.Write([]byte("+PONG\r\n"))
		}

	case "ECHO":
		if !silent {
			if len(cmdParser) > 1 {
				conn.Write([]byte("+" + fmt.Sprintf("%v", cmdParser[1]) + "\r\n"))
			} else {
				conn.Write([]byte("+\r\n"))
			}
		}

	case "SET":
		key := fmt.Sprintf("%v", cmdParser[1])
		redisKeyTypeStore[key] = "string"
		handlers.SET(cmdParser[1:], nil) // pass nil, because handlers don't need to respond for master

		if !silent && conn != nil {
			conn.Write([]byte("+OK\r\n"))
		}

	case "GET":
		if !silent {
			handlers.GET(cmdParser[1:], conn)
		}

	case "REPLCONF":
		subcmd := strings.ToUpper(fmt.Sprintf("%v", cmdParser[1]))
		switch subcmd {
		case "LISTENING-PORT", "CAPA":
			if !silent {
				conn.Write([]byte("+OK\r\n"))
			}
		case "GETACK":
			if conn != nil {
				conn.Write([]byte("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n"))
			}
		}

	default:
		if !silent && conn != nil {
			conn.Write([]byte("-ERR unknown command\r\n"))
		}
	}
}

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

		handlers.GET(cmdParser[1:], conn)

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

	case "INCR":
		handlers.INCR(cmdParser[1:], conn)

	case "INFO":
		handlers.INFO(conn, cmdParser)

	case "REPLCONF":
		subcmd := strings.ToUpper(fmt.Sprintf("%v", cmdParser[1]))
		fmt.Println("I am not unknowx")
		switch subcmd {
		case "LISTENING-PORT":
			conn.Write([]byte("+OK\r\n"))
		case "CAPA":
			conn.Write([]byte("+OK\r\n"))
		case "GETACK":
			_, err := conn.Write([]byte("*3\r\n$8\r\nreplconf\r\n$6\r\ngetack\r\n$1\r\n*\r\n"))
			if err != nil {
				fmt.Println("Failed to send ACK:")
			} else {
				fmt.Println("Sent REPLCONF ACK 0 to master")
			}
		}

	case "PSYNC":
		handlers.PSYNC(conn)

	default:
		conn.Write([]byte("-ERR unknown command\r\n"))

	}
}

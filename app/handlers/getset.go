package handlers

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

var redisKeyValueStore = make(map[string]interface{})
var redisKeyExpiryTime = make(map[string]time.Time)

func SET(cmdParser []interface{}, conn net.Conn) {

	key := fmt.Sprintf("%v", cmdParser[0])
	value := cmdParser[1]

	mu.Lock()
	redisKeyValueStore[key] = value

	if len(cmdParser) > 3 && strings.ToUpper(fmt.Sprintf("%v", cmdParser[2])) == "PX" {
		ms, _ := strconv.Atoi(fmt.Sprintf("%v", cmdParser[3]))
		redisKeyExpiryTime[key] = time.Now().Add(time.Duration(ms) * time.Millisecond)
	}
	fmt.Println(cmdParser...)
	mu.Unlock()
	conn.Write([]byte("+OK\r\n"))
}

func GET(cmd []interface{}, conn net.Conn) {
	key := fmt.Sprintf("%v", cmd[0])
	mu.Lock()

	expiry, ok := redisKeyExpiryTime[key]
	if ok && expiry.Before(time.Now()) {
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
}

func INCR(cmd []interface{}, conn net.Conn) {
	key := fmt.Sprintf("%v", cmd[0])

	_, ok := redisKeyValueStore[key]

	if !ok {
		redisKeyValueStore[key] = 1
		fmt.Fprintf(conn, ":%d\r\n", redisKeyValueStore[key])

	} else {
		switch redisKeyValueStore[key].(type) {
		case int:
			redisKeyValueStore[key] = redisKeyValueStore[key].(int) + 1
			fmt.Fprintf(conn, ":%d\r\n", redisKeyValueStore[key])

		default:
			fmt.Fprintf(conn, "-ERR value is not an integer or out of range\r\n")
		}

	}
}

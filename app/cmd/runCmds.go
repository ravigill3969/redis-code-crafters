package handlers

import (
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/app/utils"
)

var (
	mu                 sync.RWMutex
	redisKeyValueStore = make(map[string]utils.CacheItem)
	redisKeyTypeStore  = make(map[string]string)
	redisStreams       = make(map[string]*utils.Stream)
	redisListStore     = make(map[string][]string)

	xreadBlockMutex  = sync.Mutex{}
	xreadBlockSignal = sync.NewCond(&xreadBlockMutex)
)

func RunCmds(conn net.Conn, cmd []string) (string, bool) {
	if len(cmd) == 0 {
		return "", false
	}

	commandName := strings.ToUpper(cmd[0])
	args := cmd[1:]

	isWriteCmd := IsWriteCommand(commandName)

	switch commandName {
	case "PING":
		return "+PONG\r\n", false
	case "ECHO":
		if len(args) > 0 {
			return utils.ToRespStr(args[0]), false
		}
		return "", false
	case "INFO":
		return infoHandler(conn, args), false
	case "SET":
		return setHandler(conn, args), true
	case "GET":
		return getHandler(conn, args), false
	case "TYPE":
		return typeHandler(conn, args), false
	case "INCR":
		return incrHandler(conn, args), true
	case "XADD":
		return xaddHandler(conn, args), true
	case "XRANGE":
		return xrangeHandler(conn, args), false
	case "XREAD":
		return xreadHandler(conn, args), false
	case "LPUSH":
		return lpushHandler(conn, args), true
	case "RPUSH":
		return rpushHandler(conn, args), true
	case "LLEN":
		return llenHandler(conn, args), false
	case "LPOP":
		return lpopHandler(conn, args), true
	case "BLPOP":
		return blpopHandler(conn, args), true
	case "PSYNC":
		return psyncHandler(conn, args), false
	case "REPLCONF":
		return replconfHandler(conn, args), false
	default:
		return "-ERR unknown command\r\n", false
	}
}

func IsWriteCommand(commandName string) bool {
	switch strings.ToUpper(commandName) {
	case "SET", "INCR", "XADD", "LPUSH", "RPUSH", "LPOP", "BLPOP":
		return true
	}
	return false
}

// Each command handler from your original code should be refactored into a separate function here.
// For example:
func setHandler(conn net.Conn, args []string) string {
	if len(args) < 2 {
		return "-ERR wrong number of arguments for 'SET' command\r\n"
	}

	mu.Lock()
	defer mu.Unlock()

	key, value := args[0], args[1]
	expiry := int64(-1)

	if len(args) > 2 && strings.ToUpper(args[2]) == "PX" && len(args) > 3 {
		ms, err := strconv.Atoi(args[3])
		if err == nil {
			expiry = time.Now().UnixMilli() + int64(ms)
		}
	}

	redisKeyValueStore[key] = utils.CacheItem{
		Value:     value,
		ExpiresAt: expiry,
		ItemType:  "string",
	}
	return "+OK\r\n"
}

func getHandler(conn net.Conn, args []string) string {
	if len(args) < 1 {
		return "-ERR wrong number of arguments for 'GET' command\r\n"
	}

	mu.RLock()
	defer mu.RUnlock()

	key := args[0]
	item, exists := redisKeyValueStore[key]
	if !exists {
		return "$-1\r\n"
	}
	if item.ExpiresAt != -1 && time.Now().UnixMilli() >= item.ExpiresAt {
		delete(redisKeyValueStore, key)
		return "$-1\r\n"
	}

	return utils.ToRespStr(item.Value)
}

// ... and so on for all your other commands (INFO, PSYNC, etc.)

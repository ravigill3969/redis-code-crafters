package handlers

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

var redisKeyValueStore = make(map[string]interface{})
var redisKeyExpiryTime = make(map[string]time.Time)
var redisListStore = make(map[string][]string)

func SET(cmd []interface{}) bool {

	key := fmt.Sprintf("%v", cmd[0])
	value := cmd[1]

	mu.Lock()
	redisKeyValueStore[key] = value

	if len(cmd) > 3 && strings.ToUpper(fmt.Sprintf("%v", cmd[3])) == "PX" {
		ms, _ := strconv.Atoi(fmt.Sprintf("%v", cmd[4]))
		redisKeyExpiryTime[key] = time.Now().Add(time.Duration(ms) * time.Millisecond)
	}
	mu.Unlock()

	return true
}
func GET(cmd []interface{}) (string, bool) {
	key, ok := cmd[0].(string)
	if !ok {
		return "", false
	}

	mu.Lock()
	defer mu.Unlock()

	expiry, exists := redisKeyExpiryTime[key]
	if exists && expiry.Before(time.Now()) {
		delete(redisKeyExpiryTime, key)
		delete(redisKeyValueStore, key)
	}

	value, exists := redisKeyValueStore[key]
	if !exists {
		return "", false
	}

	strVal, ok := value.(string)
	if !ok {
		return "", false
	}

	return strVal, true
}

func TYPE(cmd []interface{}) bool {
	key := fmt.Sprintf("%v", cmd[0])

	_, ok := redisKeyValueStore[key]

	if ok {
		return true
	} else {
		return false
	}
}

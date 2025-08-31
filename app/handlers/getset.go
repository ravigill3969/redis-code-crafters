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
	if len(cmd) < 2 {
		return false // not enough arguments
	}

	key, ok := cmd[0].(string)
	if !ok {
		return false
	}

	value, ok := cmd[1].(string) // ensure value is a string
	if !ok {
		return false
	}

	mu.Lock()
	defer mu.Unlock()

	redisKeyValueStore[key] = value

	if len(cmd) > 3 {
		if pxStr, ok := cmd[3].(string); ok && strings.ToUpper(pxStr) == "PX" && len(cmd) > 4 {
			if ms, err := strconv.Atoi(fmt.Sprintf("%v", cmd[4])); err == nil {
				redisKeyExpiryTime[key] = time.Now().Add(time.Duration(ms) * time.Millisecond)
			}
		}
	}

	return true
}

func GET(cmd []interface{}) (string, bool) {
	key, ok := cmd[0].(string)
	if !ok {
		return "", false
	}

	mu.Lock()
	defer mu.Unlock()

	if expiry, exists := redisKeyExpiryTime[key]; exists && expiry.Before(time.Now()) {
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

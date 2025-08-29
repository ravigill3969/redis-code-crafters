package handlers

import (
	"fmt"
	"strconv"
	"sync"
)

var mu sync.RWMutex
var RedisListStore = map[string][]string{}

func RPUSH(cmd []interface{}) (int, error) {
	if len(cmd) < 2 {
		return 0, fmt.Errorf("wrong number of arguments")
	}

	key := fmt.Sprintf("%v", cmd[0])
	values := cmd[1:]

	mu.Lock()
	defer mu.Unlock()
	for _, v := range values {
		RedisListStore[key] = append(RedisListStore[key], fmt.Sprintf("%v", v))
	}

	return len(RedisListStore[key]), nil
}

func LRANGE(cmd []interface{}) ([]string, error) {
	if len(cmd) < 3 {
		return nil, fmt.Errorf("wrong number of arguments")
	}

	key := fmt.Sprintf("%v", cmd[0])
	start, _ := strconv.Atoi(fmt.Sprintf("%v", cmd[1]))
	end, _ := strconv.Atoi(fmt.Sprintf("%v", cmd[2]))

	mu.RLock()
	defer mu.RUnlock()
	list, ok := RedisListStore[key]
	if !ok {
		return []string{}, nil
	}

	return list[start : end+1], nil
}

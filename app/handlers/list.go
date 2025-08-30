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
	if !ok || len(list) == 0 {
		return []string{}, nil
	}

	length := len(list)

	if start < 0 {
		start = length + start
	}
	if end < 0 {
		end = length + end
	}

	// clamp bounds
	if start < 0 {
		start = 0
	}
	if end >= length {
		end = length - 1
	}
	if start > end {
		return []string{}, nil
	}

	return list[start : end+1], nil
}

func LPUSH(cmd []interface{}) (int, error) {
	if len(cmd) < 2 {
		return 0, fmt.Errorf("wrong number of arguments")
	}

	fmt.Println(cmd)

	key := fmt.Sprintf("%v", cmd[0])
	values := cmd[1:]

	mu.Lock()
	defer mu.Unlock()
	for _, v := range values {
		RedisListStore[key] = append([]string{fmt.Sprintf("%v", v)}, RedisListStore[key]...)
	}

	return len(RedisListStore[key]), nil

}

func LLEN(cmd []interface{}) int {
	key := cmd[0].(string)

	val, ok := RedisListStore[key]

	if !ok {
		return 0
	}

	return len(val)
}

func LPOP(cmd []interface{}) (string, bool) {
	key := cmd[0].(string)

	mu.RLock()
	defer mu.RUnlock()

	list, ok := RedisListStore[key]
	if !ok || len(list) == 0 {
		return "", false
	}

	first := list[0]
	RedisListStore[key] = list[1:]
	return first, true
}

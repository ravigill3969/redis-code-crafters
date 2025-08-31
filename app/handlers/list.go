package handlers

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

type ListWaiters struct {
	mu      sync.Mutex
	waiters map[string][]chan string
}

var listWaiters = ListWaiters{
	waiters: make(map[string][]chan string),
}

var mu sync.RWMutex
var RedisListStore = map[string][]string{}

func RPUSH(cmd []interface{}) (int, error) {
	if len(cmd) < 2 {
		return 0, fmt.Errorf("wrong number of arguments")
	}

	key := fmt.Sprintf("%v", cmd[0])
	values := cmd[1:]

	mu.Lock()
	for _, v := range values {
		RedisListStore[key] = append(RedisListStore[key], fmt.Sprintf("%v", v))
	}
	newLen := len(RedisListStore[key])
	mu.Unlock()

	listWaiters.mu.Lock()
	chans, ok := listWaiters.waiters[key]
	if ok && len(chans) > 0 {
		val := RedisListStore[key][0]
		RedisListStore[key] = RedisListStore[key][1:]

		ch := chans[0]
		listWaiters.waiters[key] = chans[1:]
		listWaiters.mu.Unlock()

		go func() { ch <- val }()
	} else {
		listWaiters.mu.Unlock()
	}

	return newLen, nil
}

func LRANGE(cmd []interface{}) ([]string, error) {
	fmt.Println("hit")
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

	length := len(list)

	if end < 0 {
		end = len(list) + end
	}

	if start < 0 {
		start = length + start
	}

	if start < 0 {
		start = 0
	}
	if end >= length {
		end = length - 1
	}

	fmt.Println(list)
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

func LPOP(cmd []interface{}) ([]string, bool) {
	if len(cmd) < 1 {
		return nil, false
	}

	key := fmt.Sprintf("%v", cmd[0])

	loop := 1
	if len(cmd) > 1 {
		if val, ok := cmd[1].(int); ok && val > 0 {
			loop = val
		}
	}

	mu.Lock()
	defer mu.Unlock()

	list, ok := RedisListStore[key]
	if !ok || len(list) == 0 {
		return nil, false
	}

	if loop > len(list) {
		loop = len(list)
	}

	res := list[:loop]
	RedisListStore[key] = list[loop:]
	return res, true
}

func BLPOP(cmd []interface{}) (string, bool) {
	mu.Lock()
	key := cmd[0].(string)
	timeout, _ := strconv.Atoi(fmt.Sprintf("%v", cmd[1]))

	values := RedisListStore[key]

	if len(values) > 0 {
		val := values[0]
		list := RedisListStore[key][1:]
		RedisListStore[key] = list
		mu.Unlock()
		return val, true
	}

	mu.Lock()
	if _, ok := RedisListStore[key]; !ok {
		RedisListStore[key] = []string{}
	}
	mu.Unlock()
	
	ch := make(chan string, 1)
	listWaiters.mu.Lock()
	listWaiters.waiters[key] = append(listWaiters.waiters[key], ch)
	listWaiters.mu.Unlock()

	if timeout == 0 {
		val := <-ch
		return val, true
	} else {
		select {
		case val := <-ch:
			return val, true
		case <-time.After(time.Duration(timeout) * time.Second):
			return "", false
		}
	}

}

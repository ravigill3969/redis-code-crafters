package handlers

import "fmt"

var RedisKeyArrayStore = map[string][]string{}

func RPUSH(cmd []string) (int, error) {
	fmt.Println(cmd)
	RedisKeyArrayStore[cmd[0]] = append(RedisKeyArrayStore[cmd[0]], cmd[1])

	fmt.Println(RedisKeyArrayStore[cmd[0]])

	return len(RedisKeyArrayStore[cmd[0]]), nil
}

package handlers

var RedisKeyArrayStore = map[string][]string{}

func RPUSH(cmd []string) (int, error) {
	key := cmd[0]
	values := cmd[1:]
	RedisKeyArrayStore[key] = append(RedisKeyArrayStore[key], values...)

	return len(RedisKeyArrayStore[key]), nil
}

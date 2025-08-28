package handlers

var RedisKeyArrayStore = map[string][]string{}

func RPUSH(cmd []string) (int, error) {
	RedisKeyArrayStore[cmd[0]] = append(RedisKeyArrayStore[cmd[0]], cmd[1])

	return len(RedisKeyArrayStore[cmd[0]]), nil
}

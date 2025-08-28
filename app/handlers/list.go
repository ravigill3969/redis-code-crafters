package handlers

var RedisKeyArrayStore = map[string][]string{}

func RPUSH(cmd []string) (int, error) {
	RedisKeyArrayStore[cmd[1]] = append(RedisKeyArrayStore[cmd[1]], cmd[2])

	return len(RedisKeyArrayStore[cmd[1]]), nil
}

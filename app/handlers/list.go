package handlers

import "strconv"

var RedisKeyArrayStore = map[string][]string{}

func RPUSH(cmd []interface) (int, error) {
	key := cmd[0]
	values := cmd[1:]
	RedisKeyArrayStore[key] = append(RedisKeyArrayStore[key], values...)

	return len(RedisKeyArrayStore[key]), nil
}

func LRANGE(cmd []string) ([]string, error) {
	key := cmd[0]
	startIndex := cmd[1]
	endIndex := cmd[2]

	var res []string

	value, ok := RedisKeyArrayStore[key]

	startIndexToInt, err := strconv.Atoi(startIndex)
	endIndexToInt, err := strconv.Atoi(endIndex)

	if err != nil {
		return res, err
	}

	if startIndexToInt > endIndexToInt || !ok || len(value) < startIndexToInt {
		return res, nil
	}

	for startIndexToInt < endIndexToInt {
		if startIndexToInt >= len(value) {
			break
		}
		res = append(res, value[startIndexToInt])
		startIndexToInt++
	}

	return res, nil
}

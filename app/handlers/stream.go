package handlers

import (
	"fmt"
	"strconv"
	"strings"
)

var redisStreams = map[string][]struct {
	ID     string
	Fields map[string]string
}{}

var redisStreamKeyWithTimeAndSequence = map[string]string{}

func XADD(cmd []interface{}) (string, error) {

	// cmd[0] = stream key
	// cmd[1] = entry ID
	// cmd[2:] = field-value pairs

	rstream := fmt.Sprintf("%v", cmd[0])
	id := fmt.Sprintf("%v", cmd[1])

	fields := map[string]string{}

	ok := isValidID(rstream, id)

	if id == "0-0" {
		return "", fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
	}

	if !ok {
		return "", fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}

	for i := 2; i < len(cmd); i += 2 {
		key := fmt.Sprintf("%v", cmd[i])
		if i+1 < len(cmd) {
			fields[key] = fmt.Sprintf("%v", cmd[i+1])
		}
	}

	entry := struct {
		ID     string
		Fields map[string]string
	}{
		ID:     id,
		Fields: fields,
	}

	redisStreams[rstream] = append(redisStreams[rstream], entry)

	return id, nil

}

func isValidID(streamKey string, newTimeAndSeq string) bool {
	fmt.Println("hit", streamKey, newTimeAndSeq)
	lastTimeAndSeq := redisStreamKeyWithTimeAndSequence[streamKey]

	if len(lastTimeAndSeq) == 0 {
		lastTimeAndSeq = "0-0"
	}

	redisStreamKeyWithTimeAndSequence[streamKey] = string(newTimeAndSeq)
	lastParts := strings.Split(lastTimeAndSeq, "-")
	newParts := strings.Split(newTimeAndSeq, "-")

	lastMs, _ := strconv.ParseInt(lastParts[0], 10, 64)
	lastSeq, _ := strconv.ParseInt(lastParts[1], 10, 64)

	newMs, _ := strconv.ParseInt(newParts[0], 10, 64)
	newSeq, _ := strconv.ParseInt(newParts[1], 10, 64)

	if newMs < lastMs {
		return false
	}
	if newMs == lastMs && newSeq <= lastSeq {
		return false
	}
	fmt.Println("retruning")

	return true
}

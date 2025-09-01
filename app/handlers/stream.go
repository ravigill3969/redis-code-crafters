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

var streamTimeAndSeq = map[string]string{}

var redisStreamKeyWithTimeAndSequence = map[string]string{}

func XADD(cmd []interface{}) (string, error) {

	rstream := fmt.Sprintf("%v", cmd[0])
	id := fmt.Sprintf("%v", cmd[1])

	fields := map[string]string{}

	check := true

	if strings.Split(id, "-")[1] == "*" {
		id = handleSeq(id, rstream)
		check = false
	} else if id == "0-0" {
		return "", fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
	}

	ok := isValidID(rstream, id)
	if check {
		if !ok {
			return "", fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
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

	return true
}

func handleSeq(seq string, key string) string {
	seqArray := strings.Split(seq, "-")

	lastTimeAndSeq, ok := redisStreamKeyWithTimeAndSequence[key]

	if !ok {
		redisStreamKeyWithTimeAndSequence[key] = fmt.Sprintf("%s-%s", seqArray[0], "1")
		return fmt.Sprintf("%s-%s", seqArray[0], "1")
	}

	return fmt.Sprintf("%s-%s", seqArray[0], strings.Split(lastTimeAndSeq, "-")[1])

}

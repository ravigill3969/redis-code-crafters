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

	// Handle auto-generated sequence
	if strings.HasSuffix(id, "-*") {
		id = handleSeq(id, rstream)
		check = false
	} else if id == "0-0" {
		return "", fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
	}

	// Validate ID if not auto-generated
	if check {
		if !isValidID(rstream, id) {
			return "", fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
	}

	// Build entry fields
	for i := 2; i < len(cmd); i += 2 {
		key := fmt.Sprintf("%v", cmd[i])
		if i+1 < len(cmd) {
			fields[key] = fmt.Sprintf("%v", cmd[i+1])
		}
	}

	// Save entry
	entry := struct {
		ID     string
		Fields map[string]string
	}{
		ID:     id,
		Fields: fields,
	}
	redisStreams[rstream] = append(redisStreams[rstream], entry)

	// Update last ID only after success
	redisStreamKeyWithTimeAndSequence[rstream] = id

	return id, nil
}

func isValidID(streamKey, newTimeAndSeq string) bool {
	lastTimeAndSeq := redisStreamKeyWithTimeAndSequence[streamKey]
	if len(lastTimeAndSeq) == 0 {
		lastTimeAndSeq = "0-0"
	}

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

	// Reject 0-0 or lower
	if newMs == 0 && newSeq == 0 {
		return false
	}

	return true
}
func handleSeq(seq, key string) string {
	seqArray := strings.Split(seq, "-")
	ms := seqArray[0]

	lastTimeAndSeq, ok := redisStreamKeyWithTimeAndSequence[key]
	if !ok {
		if ms == "0" {
			return "0-1"
		}
		return fmt.Sprintf("%s-%d", ms, 0)
	}

	lastParts := strings.Split(lastTimeAndSeq, "-")
	lastSeq, _ := strconv.ParseInt(lastParts[1], 10, 64)

	if ms == lastParts[0] {
		if lastParts[0] == "1" {

			return fmt.Sprintf("%s-%d", ms, lastSeq+1)
		}
	}

	return fmt.Sprintf("%s-%d", ms, 0)
}

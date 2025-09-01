package handlers

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

var streamTimeAndSeq = map[string]string{}
var redisStreamKeyWithTimeAndSequence = map[string]string{}
var redisStreams = map[string][]struct {
	ID     string
	Fields map[string]string
}{}

func XADD(cmd []interface{}) (string, error) {
	rstream := fmt.Sprintf("%v", cmd[0])
	id := fmt.Sprintf("%v", cmd[1])

	fields := map[string]string{}
	check := true

	if id == "*" {
		check = false
		id = handleTimeAndSeq(rstream)

	} else if strings.HasSuffix(id, "-*") {
		id = handleSeq(id, rstream)
		check = false
	} else if id == "0-0" {
		return "", fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
	}

	fmt.Println(id)
	fmt.Println(check)

	if check {
		if !isValidID(rstream, id) {
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

	redisStreamKeyWithTimeAndSequence[rstream] = id

	return id, nil
}

func isValidID(streamKey, newID string) bool {
	lastID := redisStreamKeyWithTimeAndSequence[streamKey]
	if lastID == "" {
		lastID = "0-0"
	}

	lastParts := strings.Split(lastID, "-")
	newParts := strings.Split(newID, "-")

	// Defensive check
	if len(lastParts) < 2 || len(newParts) < 2 {
		return false
	}

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
	if newMs == 0 && newSeq == 0 {
		return false
	}

	return true
}

func handleSeq(id, key string) string {
	ms := strings.Split(id, "-")[0]

	lastID := redisStreamKeyWithTimeAndSequence[key]
	if lastID == "" {
		if ms == "0" {
			return "0-1"
		}
		return fmt.Sprintf("%s-0", ms)
	}

	lastParts := strings.Split(lastID, "-")
	lastMs := lastParts[0]
	lastSeq, _ := strconv.ParseInt(lastParts[1], 10, 64)

	// If ms matches last entry → increment sequence
	if ms == lastMs {
		return fmt.Sprintf("%s-%d", ms, lastSeq+1)
	}

	// Otherwise, start fresh at 0
	return fmt.Sprintf("%s-0", ms)
}

func handleTimeAndSeq(key string) string {
	ms := time.Now().UnixMilli()

	lastID := redisStreamKeyWithTimeAndSequence[key]
	if lastID == "" {
		return fmt.Sprintf("%d-0", ms)
	}

	lastParts := strings.Split(lastID, "-")
	lastMs, _ := strconv.ParseInt(lastParts[0], 10, 64)
	lastSeq, _ := strconv.ParseInt(lastParts[1], 10, 64)

	if ms == lastMs {
		// Same ms → bump seq
		return fmt.Sprintf("%d-%d", ms, lastSeq+1)
	}

	// New ms → reset seq to 0
	return fmt.Sprintf("%d-0", ms)
}

func XRANGE(cmd []interface{}) {
	streamKey := cmd[0].(string)
	startSeq := cmd[1]
	endSeq := cmd[2]

	type Fields map[string]string

	res := []Fields{}

	values := redisStreams[streamKey]

	for _, v := range values {
		if strings.Split(v.ID, "-")[0] >= strconv.Itoa((startSeq.(int))) && strings.Split(v.ID, "-")[0] <= strconv.Itoa((endSeq.(int))) {
			res = append(res, v.Fields)
		}
	}

	fmt.Println(res)
}

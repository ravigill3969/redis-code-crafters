package handlers

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

var streamTimeAndSeq = map[string]string{}
var redisStreamKeyWithTimeAndSequence = map[string]string{}

type StreamEntry struct {
	ID     string
	Fields map[string]string
}

var redisStreams = map[string][]StreamEntry{}

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
func XRANGE(conn net.Conn, cmd []interface{}) {
	if len(cmd) < 3 {
		conn.Write([]byte("-ERR wrong number of arguments for XRANGE\r\n"))
		return
	}

	streamKey := cmd[0].(string)
	startID := cmd[1].(string)
	endID := cmd[2].(string)

	entries, ok := redisStreams[streamKey]
	if !ok || len(entries) == 0 {
		conn.Write([]byte("*0\r\n")) // empty RESP array
		return
	}

	res := []StreamEntry{}

	for _, e := range entries {
		if idInRange(e.ID, startID, endID) {
			res = append(res, e)
		}
	}

	// Write as RESP Array
	conn.Write([]byte(fmt.Sprintf("*%d\r\n", len(res))))
	for _, e := range res {
		// ID
		conn.Write([]byte(fmt.Sprintf("*2\r\n$%d\r\n%s\r\n", len(e.ID), e.ID)))

		// Fields as array
		fieldCount := len(e.Fields) * 2
		conn.Write([]byte(fmt.Sprintf("*%d\r\n", fieldCount)))
		for k, v := range e.Fields {
			conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(k), k)))
			conn.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(v), v)))
		}
	}
}

// Helper: checks if entryID is within startID and endID (inclusive)
func idInRange(entryID, startID, endID string) bool {
	eSplit := strings.Split(entryID, "-")
	sSplit := strings.Split(startID, "-")
	endSplit := strings.Split(endID, "-")

	eMs, _ := strconv.ParseInt(eSplit[0], 10, 64)
	eSeq, _ := strconv.ParseInt(eSplit[1], 10, 64)
	sMs, _ := strconv.ParseInt(sSplit[0], 10, 64)
	sSeq := int64(0)
	if len(sSplit) > 1 {
		sSeq, _ = strconv.ParseInt(sSplit[1], 10, 64)
	}
	endMs, _ := strconv.ParseInt(endSplit[0], 10, 64)
	endSeq := int64(999999)
	if len(endSplit) > 1 {
		endSeq, _ = strconv.ParseInt(endSplit[1], 10, 64)
	}

	if eMs < sMs || eMs > endMs {
		return false
	}
	if eMs == sMs && eSeq < sSeq {
		return false
	}
	if eMs == endMs && eSeq > endSeq {
		return false
	}
	return true
}

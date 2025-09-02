package handlers

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

var streamTimeAndSeq = map[string]string{}
var redisStreamKeyWithTimeAndSequence = map[string]string{}

type StreamEntry struct {
	ID     string
	Fields map[string]string
}

type ParentStreamEntry struct {
	res []StreamEntry
}

type Waiter struct {
	seq string
	ch  chan StreamEntry
}

type ListWaitersStream struct {
	mu      sync.Mutex
	waiters map[string][]Waiter
}

var redisStreamsMu sync.RWMutex
var listWaitersStream = ListWaitersStream{
	waiters: make(map[string][]Waiter),
}

var redisStreams = map[string][]StreamEntry{}

func XADD(cmd []interface{}) (string, error) {
	streamKey := fmt.Sprintf("%v", cmd[0])
	id := fmt.Sprintf("%v", cmd[1])

	fields := map[string]string{}
	check := true

	if id == "*" {
		check = false
		id = handleTimeAndSeq(streamKey)

	} else if strings.HasSuffix(id, "-*") {
		id = handleSeq(id, streamKey)
		check = false
	} else if id == "0-0" {
		return "", fmt.Errorf("ERR The ID specified in XADD must be greater than 0-0")
	}

	if check {
		if !isValidID(streamKey, id) {
			return "", fmt.Errorf("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
	}

	for i := 2; i < len(cmd); i += 2 {
		key := fmt.Sprintf("%v", cmd[i])
		if i+1 < len(cmd) {
			fields[key] = fmt.Sprintf("%v", cmd[i+1])
		}
	}

	entry := StreamEntry{
		ID:     id,
		Fields: fields,
	}

	redisStreamsMu.Lock()
	redisStreams[streamKey] = append(redisStreams[streamKey], entry)
	redisStreamsMu.Unlock()

	redisStreamKeyWithTimeAndSequence[streamKey] = id

	listWaitersStream.mu.Lock()
	waiters := listWaitersStream.waiters[streamKey]
	var remaining []Waiter

	for _, w := range waiters {
		if xreadIsValidId(w.seq, entry.ID) {
			select {
			case w.ch <- entry:
			default: 
			}
		} else {
			remaining = append(remaining, w)
		}
	}

	listWaitersStream.waiters[streamKey] = remaining
	listWaitersStream.mu.Unlock()

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
	fmt.Println(cmd)
	streamKey := fmt.Sprintf("%s", cmd[0])
	startSeq := fmt.Sprintf("%s", cmd[1])
	endSeq := fmt.Sprintf("%s", cmd[2])

	if endSeq == "+" {
		endSeq = "999999999999-999999999"
	}

	entries, ok := redisStreams[streamKey]

	if !ok || len(entries) == 0 {
		conn.Write([]byte("No data availabe"))
		return
	}

	var res []StreamEntry

	for _, e := range entries {
		if xrangeIsValidId(startSeq, endSeq, e.ID) {
			res = append(res, e)
		}
	}

	var s strings.Builder

	s.Write([]byte(fmt.Sprintf("*%d\r\n", len(res))))
	for _, c := range res {
		s.Write([]byte(fmt.Sprintf("*%d\r\n", 2)))

		s.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(c.ID), c.ID)))
		s.Write([]byte(fmt.Sprintf("*%d\r\n", len(c.Fields)*2)))

		for key := range c.Fields {
			fmt.Println(key)
			val := c.Fields[key]
			fmt.Println(val)
			s.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(key), key))
			s.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(val), val))
		}

	}

	conn.Write([]byte(s.String()))

}

func xrangeIsValidId(startSeq, endSeq, loopId string) bool {

	startSplit := strings.Split(startSeq, "-")
	endSplit := strings.Split(endSeq, "-")
	loopSplit := strings.Split(loopId, "-")

	startMS, _ := strconv.ParseInt(startSplit[0], 10, 64)
	endMS, _ := strconv.ParseInt(endSplit[0], 10, 64)
	loopMS, _ := strconv.ParseInt(loopSplit[0], 10, 64)

	startSequ := int64(0)
	if len(startSplit) > 1 {
		startSequ, _ = strconv.ParseInt(startSplit[1], 10, 64)
	}
	endSequ := int64(0)
	if len(endSplit) > 1 {
		endSequ, _ = strconv.ParseInt(endSplit[1], 10, 64)
	}
	loopSequ := int64(0)
	if len(loopSplit) > 1 {
		loopSequ, _ = strconv.ParseInt(loopSplit[1], 10, 64)
	}

	if loopMS < startMS || (loopMS == startMS && loopSequ < startSequ) {
		return false
	}

	if loopMS > endMS || (loopMS == endMS && loopSequ > endSequ) {
		return false
	}

	return true
}

func xreadIsValidId(startSeq, loopId string) bool {
	startSplit := strings.Split(startSeq, "-")
	loopSplit := strings.Split(loopId, "-")

	startMS, _ := strconv.ParseInt(startSplit[0], 10, 64)
	loopMS, _ := strconv.ParseInt(loopSplit[0], 10, 64)

	startSequ := int64(0)
	if len(startSplit) == 2 {
		startSequ, _ = strconv.ParseInt(startSplit[1], 10, 64)
	}

	loopSequ := int64(0)

	if len(loopSplit) == 2 {
		loopSequ, _ = strconv.ParseInt(loopSplit[1], 10, 64)
	}

	if loopMS < startMS || (loopSequ < startSequ && loopMS == startMS) {
		return false
	}

	return true
}

func XREAD(conn net.Conn, cmdOrg []interface{}) {

	// [block 1000 streams mango 0-1]

	blockStr := fmt.Sprintf("%s", cmdOrg[0])

	if blockStr == "block" {
		t, _ := strconv.ParseInt(fmt.Sprintf("%s", cmdOrg[1]), 10, 64)

		handleBlockStream(conn, cmdOrg[3:], t)
		return
	}

	cmd := cmdOrg[1:]

	namePointer := 0
	seqPointer := len(cmd) / 2

	var parentRes []ParentStreamEntry

	for namePointer < len(cmd)/2 {
		streamKey := fmt.Sprintf("%s", cmd[namePointer])
		entries := redisStreams[streamKey]

		var childRes []StreamEntry
		for _, e := range entries {
			if xreadIsValidId(fmt.Sprintf("%s", cmd[seqPointer]), e.ID) {
				childRes = append(childRes, e)
			}
		}
		parentRes = append(parentRes, ParentStreamEntry{res: childRes})

		namePointer++
		seqPointer++
	}

	var s strings.Builder

	s.WriteString(fmt.Sprintf("*%d\r\n", len(parentRes)))

	for i, e := range parentRes {
		streamName := fmt.Sprintf("%s", cmd[i])

		s.WriteString("*2\r\n")

		s.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(streamName), streamName))

		s.WriteString(fmt.Sprintf("*%d\r\n", len(e.res)))

		for _, c := range e.res {
			// entry = [id, fields]
			s.WriteString("*2\r\n")
			s.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(c.ID), c.ID))

			// fields
			s.WriteString(fmt.Sprintf("*%d\r\n", len(c.Fields)*2))
			for k, v := range c.Fields {
				s.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(k), k))
				s.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(v), v))
			}
		}
	}

	conn.Write([]byte(s.String()))
}

func handleBlockStream(conn net.Conn, cmd []interface{}, blockMs int64) {
	streamKey := fmt.Sprintf("%s", cmd[0])
	seq := fmt.Sprintf("%s", cmd[1]) // last seen ID

	// register a waiter
	ch := make(chan StreamEntry, 1)

	listWaitersStream.mu.Lock()
	w := Waiter{seq: fmt.Sprintf("%s", cmd[len(cmd)-1]), ch: make(chan StreamEntry, 1)}
	listWaitersStream.waiters[streamKey] = append(listWaitersStream.waiters[streamKey], w)
	listWaitersStream.mu.Unlock()

	// make sure stream exists
	if _, ok := redisStreams[streamKey]; !ok {
		redisStreams[streamKey] = []StreamEntry{}
	}

	var entry StreamEntry
	var ok bool

	if blockMs == 0 {
		// block until an entry comes
		entry, ok = <-ch
	} else {
		// block with given time
		select {
		case entry, ok = <-ch:
		case <-time.After(time.Duration(blockMs) * time.Millisecond):
			conn.Write([]byte("*-1\r\n"))
			return
		}
	}

	if !ok {
		conn.Write([]byte("*-1\r\n"))
		return
	}

	if !xreadIsValidId(seq, entry.ID) {
		conn.Write([]byte("*-1\r\n"))
		return
	}

	var s strings.Builder
	s.WriteString("*1\r\n")
	s.WriteString("*2\r\n")
	s.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(streamKey), streamKey))
	s.WriteString("*1\r\n")
	s.WriteString("*2\r\n")
	s.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(entry.ID), entry.ID))

	s.WriteString(fmt.Sprintf("*%d\r\n", len(entry.Fields)*2))
	for k, v := range entry.Fields {
		s.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(k), k))
		s.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(v), v))
	}

	conn.Write([]byte(s.String()))
}

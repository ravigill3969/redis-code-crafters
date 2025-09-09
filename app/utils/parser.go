package utils

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const nullRespStr = "$-1\r\n"

// CacheItem holds key-value data with an optional expiration time.
type CacheItem struct {
	Value     string
	ExpiresAt int64
	ItemType  string
}

// StreamEntry represents an entry in a Redis Stream.
type StreamEntry struct {
	Timestamp      int64
	SequenceNumber int
	Values         map[string]string
}

// Stream holds stream data for a given key.
type Stream struct {
	LastMillisecondsTime int64
	LastSequenceNumber   int
	Entries              []StreamEntry
}

// ToRespStr formats a string into a RESP Bulk String.
func ToRespStr(raw string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(raw), raw)
}

// ToRespArr formats a slice of strings into a RESP Array.
func ToRespArr(strs ...string) string {
	respArr := fmt.Sprintf("*%d\r\n", len(strs))
	for _, str := range strs {
		respArr += ToRespStr(str)
	}
	return respArr
}

// GenerateReplId generates a random hex string for the replication ID.
func GenerateReplId() string {
	bytes := make([]byte, 20)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// ParseRespCommand correctly parses a RESP command from a bufio.Reader.
func ParseRespCommand(reader *bufio.Reader) (rawCommand string, commandName string, args []string, err error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", "", nil, err
	}
	rawCommand += line

	if line[0] != '*' {
		return "", strings.ToLower(strings.TrimSpace(line)), nil, nil
	}

	numArgsStr := strings.TrimSpace(line[1:])
	numArgs, err := strconv.Atoi(numArgsStr)
	if err != nil {
		return "", "", nil, fmt.Errorf("invalid number of arguments: %w", err)
	}

	args = make([]string, numArgs)
	for i := 0; i < numArgs; i++ {
		// Read length header
		line, err = reader.ReadString('\n')
		if err != nil {
			return "", "", nil, err
		}
		rawCommand += line
		if line[0] != '$' {
			return "", "", nil, fmt.Errorf("expected bulk string, got: %q", line)
		}

		// Read actual value
		length, _ := strconv.Atoi(strings.TrimSpace(line[1:]))
		value := make([]byte, length)
		_, err = io.ReadFull(reader, value)
		if err != nil {
			return "", "", nil, err
		}
		rawCommand += string(value)
		rawCommand += "\r\n"
		args[i] = string(value)

		// Read trailing CRLF
		crlf := make([]byte, 2)
		_, err = io.ReadFull(reader, crlf)
		if err != nil {
			return "", "", nil, err
		}
	}

	if len(args) == 0 {
		return rawCommand, "", nil, nil
	}

	commandName = strings.ToLower(args[0])
	return rawCommand, commandName, args[1:], nil
}

// Your original functions InterfaceSliceToStringSlice, TokenizeRESP, and ParseRESP are no longer needed
// due to the new, correct parsing logic.

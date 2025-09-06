package utils

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

func EncodeAsRESPArray(cmd []string) string {
	s := fmt.Sprintf("*%d\r\n", len(cmd))
	for _, arg := range cmd {
		s += fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
	}
	return s
}

func InterfaceSliceToStringSlice(cmd []interface{}) []string {
	strCmd := make([]string, len(cmd))
	for i, arg := range cmd {
		switch v := arg.(type) {
		case string:
			strCmd[i] = v
		case []byte:
			strCmd[i] = string(v)
		default:
			strCmd[i] = fmt.Sprintf("%v", arg) // fallback
		}
	}

	return strCmd
}

func TokenizeRESP(raw string) []string {

	clean := strings.ReplaceAll(raw, "\r\n", "\n")
	lines := strings.Split(clean, "\n")
	tokens := []string{}
	for _, line := range lines {
		if line != "" {
			tokens = append(tokens, line)
		}
	}

	return tokens
}

func ParseRESP(raw string) []interface{} {
	lines := TokenizeRESP(raw)
	cmd := []interface{}{}

	for _, t := range lines {
		if t == "" {
			continue
		}

		switch t[0] {
		case '*':
			if len(t) == 1 {
				cmd = append(cmd, t)
			}
		case '$':
			if len(t) == 1 {
				cmd = append(cmd, t)
			}
		default:
			if i, err := strconv.Atoi(t); err == nil {
				cmd = append(cmd, i)
			} else {
				cmd = append(cmd, t)
			}
		}
	}

	fmt.Println(cmd, "inside parse cmds")

	return cmd

}

// ParseRESPWithOffset parses a single RESP command from raw bytes
// Returns the parsed command as []interface{} and the number of bytes consumed
func ParseRESPWithOffset(data []byte) ([]interface{}, int) {
	if len(data) == 0 {
		return nil, 0
	}

	switch data[0] {
	case '*': // Array
		lines := bytes.SplitN(data, []byte("\r\n"), 2)
		if len(lines) < 2 {
			return nil, 0
		}

		numElements, err := strconv.Atoi(string(lines[0][1:]))
		if err != nil || numElements <= 0 {
			return nil, 0
		}

		remaining := lines[1]
		cmd := []interface{}{}
		totalConsumed := len(lines[0]) + 2 // '*<num>\r\n'

		for i := 0; i < numElements; i++ {
			if len(remaining) == 0 {
				return nil, 0
			}

			if remaining[0] != '$' {
				return nil, 0
			}

			// Read bulk string length
			idx := bytes.Index(remaining, []byte("\r\n"))
			if idx == -1 {
				return nil, 0
			}

			bulkLen, err := strconv.Atoi(string(remaining[1:idx]))
			if err != nil {
				return nil, 0
			}

			if len(remaining) < idx+2+bulkLen+2 {
				// Not enough data yet
				return nil, 0
			}

			value := remaining[idx+2 : idx+2+bulkLen]
			cmd = append(cmd, string(value))

			// Move to next element
			remaining = remaining[idx+2+bulkLen+2:]
			totalConsumed += idx + 2 + bulkLen + 2
		}

		return cmd, totalConsumed

	case '+': // Simple string
		idx := bytes.Index(data, []byte("\r\n"))
		if idx == -1 {
			return nil, 0
		}
		return []interface{}{string(data[1:idx])}, idx + 2

	case '-': // Error
		idx := bytes.Index(data, []byte("\r\n"))
		if idx == -1 {
			return nil, 0
		}
		return []interface{}{string(data[1:idx])}, idx + 2

	case ':': // Integer
		idx := bytes.Index(data, []byte("\r\n"))
		if idx == -1 {
			return nil, 0
		}
		val, err := strconv.Atoi(string(data[1:idx]))
		if err != nil {
			return nil, 0
		}
		return []interface{}{val}, idx + 2

	case '$': // Bulk string
		idx := bytes.Index(data, []byte("\r\n"))
		if idx == -1 {
			return nil, 0
		}
		length, err := strconv.Atoi(string(data[1:idx]))
		if err != nil {
			return nil, 0
		}
		if length == -1 {
			return []interface{}{nil}, idx + 2
		}
		if len(data) < idx+2+length+2 {
			return nil, 0
		}
		value := data[idx+2 : idx+2+length]
		return []interface{}{string(value)}, idx + 2 + length + 2
	}

	return nil, 0
}

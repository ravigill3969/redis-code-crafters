package utils

import (
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

	return cmd

}

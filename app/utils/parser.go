package utils

import (
    "bytes"
    "fmt"
    "io"
    "strconv"
    "strings"
)

// EncodeAsRESPArray converts a slice of strings into a RESP array string.
func EncodeAsRESPArray(cmd []string) string {
    s := fmt.Sprintf("*%d\r\n", len(cmd))
    for _, arg := range cmd {
        s += fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
    }
    return s
}

// InterfaceSliceToStringSlice converts a slice of interfaces to a slice of strings.
func InterfaceSliceToStringSlice(cmd []interface{}) []string {
    strCmd := make([]string, len(cmd))
    for i, arg := range cmd {
        strCmd[i] = fmt.Sprintf("%v", arg)
    }
    return strCmd
}

// ParseRESP reads from an io.Reader and returns a slice of parsed commands.
func ParseRESP(reader *bytes.Buffer) ([][]interface{}, error) {
    var commands [][]interface{}
    for {
        prefix, err := reader.ReadByte()
        if err != nil {
            return commands, io.EOF
        }
        
        switch prefix {
        case '*': // Array
            line, err := reader.ReadString('\n')
            if err != nil {
                return commands, io.EOF
            }
            arrayLen, err := strconv.Atoi(strings.TrimSpace(line))
            if err != nil {
                return commands, fmt.Errorf("invalid array length: %w", err)
            }
            if arrayLen == -1 {
                continue
            }
            
            var cmd []interface{}
            for i := 0; i < arrayLen; i++ {
                argPrefix, err := reader.ReadByte()
                if err != nil {
                    return commands, io.EOF
                }
                if argPrefix != '$' {
                    return commands, fmt.Errorf("expected bulk string for command argument, got: %c", argPrefix)
                }

                argLenStr, err := reader.ReadString('\n')
                if err != nil {
                    return commands, io.EOF
                }
                argLen, err := strconv.Atoi(strings.TrimSpace(argLenStr))
                if err != nil {
                    return commands, fmt.Errorf("invalid bulk string length: %w", err)
                }
                
                if argLen == -1 {
                    cmd = append(cmd, nil)
                    continue
                }

                bulk := make([]byte, argLen+2)
                if _, err := io.ReadFull(reader, bulk); err != nil {
                    return commands, io.EOF
                }
                cmd = append(cmd, string(bulk[:argLen]))
            }
            commands = append(commands, cmd)
        
        default:
            return commands, fmt.Errorf("unknown RESP prefix: %c", prefix)
        }
    }
}

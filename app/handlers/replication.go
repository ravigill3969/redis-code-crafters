package handlers

import (
	"fmt"
	"net"
	"os"
)

func INFO(conn net.Conn, cmd []interface{}) {
	role := "master"

	// Check CLI args for replica flag
	for i := 0; i < len(os.Args); i++ {
		if os.Args[i] == "--replicaof" && i+1 < len(os.Args) {
			role = "slave"
			i++ // skip the host argument
		}
	}

	masterReplId := "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	masterReplOffset := "0"

	// Build plain INFO replication body
	response := "role:" + role + "\r\n" +
		"master_replid:" + masterReplId + "\r\n" +
		"master_repl_offset:" + masterReplOffset + "\r\n"

	// Wrap in RESP Bulk String: $<len>\r\n<response>\r\n
	bulk := fmt.Sprintf("$%d\r\n%s\r\n", len(response), response)

	fmt.Fprint(conn, bulk)
}

func NewBulkString(msg string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(msg), msg)
}

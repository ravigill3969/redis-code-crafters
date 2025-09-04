package handlers

import (
	"fmt"
	"net"
	"os"
)

func INFO(conn net.Conn, cmd []interface{}) {
	role := "master"

	for i := range os.Args {
		if os.Args[i] == "--replicaof" && i+1 < len(os.Args) {
			role = "slave"
			i++
		}
	}

	master_replid := "master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	master_repl_offset := "master_repl_offset:0"

	r := "role:" + role

	response := r + "\r\n" +
		"master_replid:" + master_replid + "\r\n" +
		"master_repl_offset:" + master_repl_offset + "\r\n"

	fmt.Fprint(conn, response)
}

func NewBulkString(msg string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(msg), msg)
}

package handlers

import (
	"encoding/hex"
	"fmt"
	"net"
	"os"
)

func INFO(conn net.Conn, cmd []interface{}) {
	role := "master"

	for i := 0; i < len(os.Args); i++ {
		if os.Args[i] == "--replicaof" && i+1 < len(os.Args) {
			role = "slave"
			i++
		}
	}

	masterReplId := "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	masterReplOffset := "0"

	response := "role:" + role + "\r\n" +
		"master_replid:" + masterReplId + "\r\n" +
		"master_repl_offset:" + masterReplOffset + "\r\n"

	bulk := fmt.Sprintf("$%d\r\n%s\r\n", len(response), response)

	fmt.Fprint(conn, bulk)
}

func PSYNC(conn net.Conn) {
	// Send FULLRESYNC line
	conn.Write([]byte("+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n"))

	// RDB content as binary
	RDBcontent, _ := hex.DecodeString(
		"524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2",
	)

	// Write RESP bulk string header
	fmt.Fprintf(conn, "$%d\r\n", len(RDBcontent))

	// Write raw RDB bytes (don’t cast to string)
	conn.Write(RDBcontent)

	// Write trailing CRLF
	conn.Write([]byte("\r\n"))
}

package handlers

import (
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"strconv"
)

// MasterReplOffset is a pointer to the global offset defined in main.go
var MasterReplOffset *int64

// INFO handles the INFO command, providing server information like role and replication details.
func INFO(conn net.Conn, cmd []interface{}) {
	role := "master"

	for i := 0; i < len(os.Args); i++ {
		if os.Args[i] == "--replicaof" && i+1 < len(os.Args) {
			role = "slave"
			i++
		}
	}

	masterReplId := "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
	masterReplOffsetStr := "0"
	if MasterReplOffset != nil {
		masterReplOffsetStr = strconv.FormatInt(*MasterReplOffset, 10)
	}

	response := "role:" + role + "\r\n" +
		"master_replid:" + masterReplId + "\r\n" +
		"master_repl_offset:" + masterReplOffsetStr + "\r\n"

	bulk := fmt.Sprintf("$%d\r\n%s\r\n", len(response), response)
	fmt.Fprint(conn, bulk)
}

// PSYNC handles the PSYNC command, initiating the full synchronization process.
func PSYNC(conn net.Conn) {
	conn.Write([]byte("+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n"))

	rdbContent, _ := hex.DecodeString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")

	// Send the RDB file as a RESP bulk string
	conn.Write([]byte(fmt.Sprintf("$%d\r\n", len(rdbContent))))
	conn.Write(rdbContent)
}

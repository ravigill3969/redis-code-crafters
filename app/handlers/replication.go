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

	r := "role" + role

	conn.Write([]byte(fmt.Sprintf("$%d\r\n:%s\r\n", len(r), r)))
}

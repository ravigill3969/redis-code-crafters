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

	conn.Write([]byte(fmt.Sprintf("$11\r\nrole:%s\r\n", role)))
}

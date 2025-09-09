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
		if os.Args[i] == "--replicaof" {
			role = "slave"
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
	conn.Write([]byte("+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n"))
	RDBcontent, _ := hex.DecodeString("524544495330303131fa...")
	fmt.Fprintf(conn, "$%v\r\n%v", len(RDBcontent), string(RDBcontent))
}

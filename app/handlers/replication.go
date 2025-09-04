package handlers

import (
	"fmt"
	"net"
	"os"
	"time"
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
	fmt.Fprintf(conn, "+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n")

	emptyRDB := []byte{
		0x52, 0x45, 0x44, 0x49, 0x53, 0x00,
		0x00,
		0xFF,
	}

	time.Sleep(1000)
	fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(emptyRDB), emptyRDB)

}

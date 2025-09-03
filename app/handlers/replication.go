package handlers

import "net"

func INFO(conn net.Conn, cmd []interface{}) {
	conn.Write([]byte("$11\r\nrole:master\r\n"))
}

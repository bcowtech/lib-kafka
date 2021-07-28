package internal

import (
	"log"
	"net"
	"time"
)

func Ping(addresses []string, timeout time.Duration) (err error) {
	for _, address := range addresses {
		var conn net.Conn
		conn, err = net.DialTimeout("tcp", address, timeout)
		if err != nil {
			continue
		}
		if conn != nil {
			defer conn.Close()
			log.Printf("%% Notice: tcp %s opened\n", address)
			return nil
		}
	}
	return nil
}

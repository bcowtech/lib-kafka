package internal

import (
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
			return nil
		}
	}
	return err
}

package ipaddress

import (
	"errors"
	"net"
)

// ErrNoAddressFound where loopback address are not considered valid.
var ErrNoAddressFound = errors.New("no address found")

// Find a non-loopback ip address.
func Find() (net.IP, error) {
	extract := func(ip net.IP) net.IP {
		if ip.IsLoopback() {
			return nil
		}
		if ip.To4() != nil {
			return ip.To4()
		}
		return ip
	}

	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}

	var ip net.IP
	for _, i := range interfaces {
		addrs, err := i.Addrs()
		if err != nil {
			return nil, err
		}
		for _, addr := range addrs {
			switch addr := addr.(type) {
			case *net.IPNet:
				ip = extract(addr.IP)
			case *net.IPAddr:
				ip = extract(addr.IP)
			}
		}
		if ip != nil {
			break
		}
	}
	if ip == nil {
		return nil, ErrNoAddressFound
	}
	return ip, nil
}
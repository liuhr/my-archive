package util

import (
	"net"
	"net/url"
	"github.com/outbrain/golib/log"
	"regexp"
	"os/exec"
	"strings"
	"strconv"
)

const ErrorFloat  = 0.0

func GetLocalIp() (string,error){
	var ip string
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ip, err
	}
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				ip=ipnet.IP.String()
				break
			}
		}
	}
	return ip, nil
}

func IsUseTcpHostPort(host string, port string) bool {
	remote := host + ":" + port
	tcpAddr, _ := net.ResolveTCPAddr("tcp4", remote)
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		//fmt.Printf("no==%s:%s\r\n", host, port)
		return false

	}
	defer conn.Close()
	//fmt.Printf("ok==%s:%s\r\n", host, port)
	return true
}

func IsUseTcpUri(uri string) bool {

	u,err:=url.Parse(uri)
	if err != nil {
		log.Errorf("url:%s parse error:%s",uri,err.Error())
		return false
	}
	tcpAddr, _ := net.ResolveTCPAddr(u.Scheme,u.Host)
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		//fmt.Printf("no==%s:%s\r\n", host, port)
		return false

	}
	defer conn.Close()
	//fmt.Printf("ok==%s:%s\r\n", host, port)
	return true
}


func IsValidIp(ip string) bool {
	ValidIpAddressRegex := "^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$"
	ipPattern:=regexp.MustCompile(ValidIpAddressRegex)
	return ipPattern.MatchString(ip)
}


func PingIpDelay(ip string) float64 {
	out, err := exec.Command("ping", "-c 3", "-W 3", "-n", ip).Output()
	if err != nil {
		log.Errorf("ping ip:%s error:%s", ip, err.Error())
	}
	if strings.Contains(string(out), "Destination Host Unreachable") {
		log.Errorf("ping ip:%s,Destination Host Unreachable", ip)
		return ErrorFloat
	}
	latencyPattern := regexp.MustCompile(`(round-trip|rtt)\s+\S+\s*=\s*([0-9.]+)/([0-9.]+)/([0-9.]+)/([0-9.]+)\s*ms`)
	matches := latencyPattern.FindAllStringSubmatch(string(out), -1)
	var avg float64
	for _, item := range matches {
		if len(item) >= 4 {
			avg, _ = strconv.ParseFloat(strings.TrimSpace(item[3]), 64)
			break
		}

	}
	return avg
}

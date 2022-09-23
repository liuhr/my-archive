package util

import (
	"regexp"
)

func IsIP(ip string) (b bool) {

	m, _ := regexp.MatchString("^[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}$", ip)

	if  !m {
		return false
	}
	return true
}



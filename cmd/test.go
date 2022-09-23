package main

import (
	"my-archive/util"
	"fmt"
)

func main() {
	s:="123456"
	s1,_:=util.Encrypt(s)
	fmt.Println(s1)
	fmt.Println(util.Decrypt(s1))
}

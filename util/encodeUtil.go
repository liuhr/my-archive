package util

import (
	"github.com/outbrain/golib/log"
	"golang.org/x/text/transform"
	"golang.org/x/text/encoding/htmlindex"
)

func Encoder(encoding string,s string) (string) {

	encode,err:=htmlindex.Get(encoding)
	if err!=nil {
		log.Infof("不存在指定的编码集:%s,返回原始字符串",encoding)
		return s
	}
	result,_,err:=transform.String(encode.NewEncoder(),s)
	if err!=nil {
		log.Infof("Encoder转换编码错误,返回原始字符串",encoding)
		return s
	}
	return result
}

func Decoder(encoding string,s string) (string) {
	encode,err:=htmlindex.Get(encoding)
	if err!=nil {
		log.Infof("不存在指定的编码集:%s,返回原始字符串",encoding)
		return s
	}
	result,_,err:=transform.String(encode.NewDecoder(),s)
	if err!=nil {
		log.Infof("Decoder转换编码错误,返回原始字符串",encoding)
		return s
	}
	return result
}

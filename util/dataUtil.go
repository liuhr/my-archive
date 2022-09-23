package util

import (
	"strconv"
	"github.com/outbrain/golib/log"
	"regexp"
	"fmt"
	"strings"
	"hash/crc32"
	"bufio"
	"os"
	"sort"
	"reflect"
	"bytes"
)

const (
	NEWLINE="\n"
	KEY_VALUE_SPLIT=":"
	PRINT_PAD_LENGTH=64
)

func StringMapAdd(target map[string]string, source map[string]string) {
	for k, v := range source {
		if _, ok := target[k]; ok {
			log.Errorf("key conflict:", k)
		}
		target[k] = v
	}
}

func IntMapAdd(target map[string]string, source map[string]int64) {
	for k, v := range source {
		if _, ok := target[k]; ok {
			log.Errorf("key conflict:", k)
		}
		target[k] = strconv.FormatInt(v, 10)
	}
}

func ConvStrToInt64(s string) int64 {
	if s=="" || s=="NULL" {
		return 0
	}
	value := regexp.MustCompile("\\d+").FindString(s)
	i, err := strconv.ParseInt(value, 10, 64)
	if nil != err {
		log.Errorf("convStrToInt64 err: parse(%v) to int64 err:%v\n", s, err)
		return 0
	}
	return i
}

func ConvStrToInt(s string) int {
	if s=="" || s=="NULL" {
		return 0
	}
	i, err := strconv.Atoi(s)
	if nil != err {
		log.Errorf("ConvStrToInt err: parse(%v) to int err:%v\n", s, err)
		return 0
	}
	return i
}

func ConvStrToUint(s string) uint {
	if s=="" || s=="NULL" {
		return 0
	}
	i, err := strconv.ParseUint(s,10,32)
	if nil != err {
		log.Errorf("ConvStrToInt err: parse(%v) to uint err:%v\n", s, err)
		return 0
	}
	return uint(i)
}

func ConvStrToUint64(s string) uint64 {
	if s=="" || s=="NULL" {
		return 0
	}
	i, err := strconv.ParseUint(s,10,64)
	if nil != err {
		log.Errorf("ConvStrToInt err: parse(%v) to uint64 err:%v\n", s, err)
		return 0
	}
	return i
}

func ConvStrToFloat(s string) float64 {
	if s=="" || s=="NULL" {
		return 0
	}
	i, err := strconv.ParseFloat(s,64)
	if nil != err {
		log.Errorf("ConvStrToInt err: parse(%v) to int err:%v\n", s, err)
		return 0
	}
	return i
}

func ConvStrToBool(s string) bool {
	if s=="" || s=="NULL" {
		return false
	}
	i, err := strconv.ParseBool(s)
	if nil != err {
		log.Errorf("ConvStrToBool err: parse(%v) to bool err:%v\n", s, err)
		return false
	}
	return i
}

func CollectStringToMap(s string) map[string]string{
	result := make(map[string]string)
	for _,kv:=range strings.Split(s,NEWLINE) {
		kv:=strings.Split(kv,KEY_VALUE_SPLIT)
		if len(kv)==2 && strings.TrimSpace(kv[0])!="" && strings.TrimSpace(kv[1])!="" {
			result[strings.TrimSpace(kv[0])]=strings.TrimSpace(kv[1])
		}
	}
	return result
}

func CollectAllRowsToArray(keyColName string, values []map[string]string) []string {
	var result []string
	for _, mp := range values {
		mp = ChangeKeyCase(mp)
		result=append(result,mp[keyColName])
	}
	return result
}



func ChangeKeyCase(m map[string]string) map[string]string {
	lowerMap := make(map[string]string)
	for k, v := range m {
		lowerMap[strings.ToLower(k)] = v
	}
	return lowerMap
}


func CollectAllRowsToMap(keyColName string, valueColName string, values []map[string]string) map[string]string {
	result := make(map[string]string)
	for _, mp := range values {
		result[mp[keyColName]] = mp[valueColName]
	}
	return result
}

func CollectAllRowsToPrefixKeyMap(prefixKey string,keyColName string, valueColName string, values []map[string]string) map[string]string {
	result := make(map[string]string)
	for _, mp := range values {
		mp = ChangeKeyCase(mp)
		result[prefixKey+mp[keyColName]] = mp[valueColName]
	}
	return result
}



func CollectFirstRowAsMapValue(key string, valueColName string, values []map[string]string) map[string]string {
	result := make(map[string]string)
	queryResult := values
	if 0 == len(queryResult) {
		log.Info("collectFirstRowAsMapValue:Got nothing from query: ")
		return result
	}
	mp := ChangeKeyCase(queryResult[0])
	if _, ok := mp[valueColName]; !ok {
		log.Info("collectFirstRowAsMapValue:Couldn't get %s from %s\n", valueColName)
		return result
	}
	result[key] = mp[valueColName]
	return result
}

func CollectAllRowsAsMapValue(preKey string, valueColName string, values []map[string]string) map[string]string {
	result := make(map[string]string)
	for i, mp := range values {
		mp = ChangeKeyCase(mp)
		if _, ok := mp[valueColName]; !ok {
			log.Info("collectAllRowsAsMapValue:Couldn't get %s from %s\n", valueColName)
			return result
		}
		result[preKey+strconv.Itoa(i)] = mp[valueColName]
	}
	return result
}

func CollectRowsAsMapValue(preKey string, valueColName string, lines int, values []map[string]string) map[string]string {
	result := make(map[string]string)
	line := 0
	resultMap := values
	for i, mp := range resultMap {
		if i >= lines {
			break
		}
		mp = ChangeKeyCase(mp)
		if _, ok := mp[valueColName]; !ok {
			log.Info("collectRowsAsMapValue: Couldn't get %s from %s\n", valueColName)
			return result
		}
		result[fmt.Sprintf("%s%02d", preKey, i)] = mp[valueColName]
		line++
	}

	for line < lines {
		result[fmt.Sprintf("%s%02d", preKey, line)] = "0"
		line++
	}
	return result
}


func GetCrc32(s string) uint32{
	return  crc32.ChecksumIEEE([]byte(s))
}

func ChangeNullToBlack(s string) string {
	if s=="NULL" {
		s=""
	}
	return s
}



func GetUserPassword(msg string) string {
	fmt.Printf("请输入%s连接数据库的密码:",msg)
	reader := bufio.NewReader(os.Stdin)
	text, _ := reader.ReadString('\n')
	return strings.TrimSpace(text)
}


func PrintFormat(key,value string,length int) string{
	stringLength:=fmt.Sprintf("%% %ds",length)
	space := fmt.Sprintf(stringLength,"")
	stringLength=fmt.Sprintf("%%.%ds",length)
	key=fmt.Sprintf(stringLength, key+":"+space)
	return fmt.Sprintf("%s %s\n",key,value)
}


func ConvStrToArray(listString string) []int{
	var list []int
	for _,listArray:=range strings.Split(listString,",") {
		if strings.Contains(listArray,"-") {
			rangeList:=strings.Split(listArray,"-")
			if len(rangeList)==2 {
				lower:=ConvStrToInt(rangeList[0])
				upper:=ConvStrToInt(rangeList[1])
				list=append(list,MakeIntArray(lower,upper)...)
			}
		} else {
			list=append(list,ConvStrToInt(listArray))
		}
	}
	sort.Ints(list)
	return list
}

func MakeIntArray(lower int,upper int) []int {
	var list []int
	for i:=lower;i<=upper;i++ {
		list=append(list,i)
	}
	return list
}


// 判断obj是否在target中，target支持的类型arrary,slice,map
func Contain(obj interface{}, target interface{}) (bool, error) {
	targetValue := reflect.ValueOf(target)
	switch reflect.TypeOf(target).Kind() {
	case reflect.Slice, reflect.Array:
		for i := 0; i < targetValue.Len(); i++ {
			if targetValue.Index(i).Interface() == obj {
				return true, nil
			}
		}
	case reflect.Map:
		if targetValue.MapIndex(reflect.ValueOf(obj)).IsValid() {
			return true, nil
		}
	}

	return false, fmt.Errorf("not in array")
}

func Join(a []interface{}, deli string) string {
	if len(a) == 0 {
		return ""
	}
	if len(a) == 1 {
		return fmt.Sprintf("%v", a[0])
	}

	buffer := &bytes.Buffer{}
	buffer.WriteString(fmt.Sprintf("%v", a[0]))
	for i := 1; i < len(a); i++ {
		buffer.WriteString(deli)
		buffer.WriteString(fmt.Sprintf("%v", a[i]))
	}
	return buffer.String()
}

func ConvSliceToInterface(arg interface{}) (out []interface{}, ok bool) {
	slice, success := takeArg(arg, reflect.Slice)
	if !success {
		ok = false
		return
	}
	c := slice.Len()
	out = make([]interface{}, c)
	for i := 0; i < c; i++ {
		out[i] = slice.Index(i).Interface()
	}
	return out, true
}

func takeArg(arg interface{}, kind reflect.Kind) (val reflect.Value, ok bool) {
	val = reflect.ValueOf(arg)
	if val.Kind() == kind {
		ok = true
	}
	return
}
package szBinary

import (
	"strconv"
	"strings"
	"time"
	"unsafe"
)

func Str2bytes(s string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	h := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&h))
}

func Bytes2str(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func LocalTimeStamp() (timestamp uint64) {
	timestamp,_ = strconv.ParseUint(strings.ReplaceAll(time.Now().Format("20060102150405.000"), ".", ""), 10, 64)
	return
}

func LocalDate() (date uint32) {
	d,_ := strconv.ParseUint(time.Now().Format("20060102"), 10, 32)
	date = uint32(d)
	return
}
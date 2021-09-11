package szBinary

import (
	"bytes"
	"encoding/binary"
)

func logOut(text string) []byte {
	// SessionStatus + text
	buff := make([]byte, 4 + 200)
	// 101 means SessionStatus=others
	binary.BigEndian.PutUint32(buff[0:4], 101)
	// text
	textBuff := bytes.Repeat([]byte{' '}, 200)
	copy(textBuff, Str2bytes(text))
	copy(buff[4:], textBuff)
	return buff
}
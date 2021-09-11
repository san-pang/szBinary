package szBinary

import "encoding/binary"

func logOn(senderCompID, targetCompID string, heartBtInt uint32, password, defaultApplVerID string) []byte {
	// senderCompID + targetCompID + heartBtInt + password + defaultApplVerID
	buff := make([]byte, 20 + 20 + 4 + 16 + 32)
	// senderCompID
	copy(buff[0:20], Str2bytes(senderCompID))
	// targetCompID
	copy(buff[20:40], Str2bytes(targetCompID))
	// heartBtInt
	binary.BigEndian.PutUint32(buff[40:44], heartBtInt)
	// password
	copy(buff[44:60], Str2bytes(password))
	// defaultApplVerID
	copy(buff[60:92], Str2bytes(defaultApplVerID))
	return buff
}
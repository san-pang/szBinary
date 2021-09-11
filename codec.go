package szBinary

import (
	"encoding/binary"
	"github.com/panjf2000/gnet"
)

const (
	headerLen = 4
	bodyLen = 4
	checksumLen = 4
)

type szBinaryCodec struct {
}

func newSzBinaryCodec() *szBinaryCodec {
	return &szBinaryCodec{}
}

// Encode ...
func (cc *szBinaryCodec) Encode(c gnet.Conn, buf []byte) (out []byte, err error) {
	return buf, nil
}

type innerBuffer []byte

func (in *innerBuffer) readN(n int) (buf []byte, err error) {
	if n == 0 {
		return nil, nil
	}

	if n < 0 {
		return nil, errNegativeLength
	} else if n > len(*in) {
		return nil, errBuffLenthExceed
	}
	buf = (*in)[:n]
	*in = (*in)[n:]
	return
}

// Decode ...
func (cc *szBinaryCodec) Decode(c gnet.Conn) ([]byte, error) {
	var in innerBuffer
	in = c.Read()
	headerBuff, err := in.readN(headerLen)
	if err != nil {
		return nil, errUnexpectedEOF
	}

	lenBuf, bodyLength, err := cc.getBody(&in)
	if err != nil {
		return nil, err
	}

	bodyBuff, err := in.readN(bodyLength)
	if err != nil {
		return nil, errUnexpectedEOF
	}

	checksumBuff, err := in.readN(checksumLen)
	if err != nil {
		return nil, errUnexpectedEOF
	}

	fullMessage := make([]byte, headerLen + bodyLen + bodyLength + checksumLen)
	copy(fullMessage, headerBuff)
	copy(fullMessage[headerLen:], lenBuf)
	copy(fullMessage[headerLen + bodyLen:], bodyBuff)
	copy(fullMessage[headerLen + bodyLen + bodyLength:], checksumBuff)
	c.ShiftN(len(fullMessage))
	return fullMessage, nil
}

func (cc *szBinaryCodec) getBody(in *innerBuffer) ([]byte, int, error) {
	lenBuf, err := in.readN(bodyLen)
	if err != nil {
			return nil, 0, errUnexpectedEOF
	}
	return lenBuf, int(binary.BigEndian.Uint32(lenBuf)), nil
}
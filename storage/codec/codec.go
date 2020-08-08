package codec

import (
	"encoding/binary"
	"errors"
	"github.com/tikv/client-go/codec"
)

// MvccEncode returns the encoded key.
func MvccEncode(key []byte, ts uint64) []byte {
	//TODO: should we encode key to memory comparable
	b := codec.EncodeBytes(key)
	ret := codec.EncodeUintDesc(b, ts)
	return ret
}

func MvccDecode(b []byte) ([]byte, uint64, error) {
	if len(b) < 8 {
		return nil, 0, errors.New("insufficient bytes to decode value")
	}

	data := b[len(b)-8:]
	ts := binary.BigEndian.Uint64(data)
	b = b[:len(b)-8]
	var err error
	_, b, err = codec.DecodeBytes(b)
	return b, ^ts, err
}
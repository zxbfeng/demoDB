package demodb

import "encoding/binary"

const (
	SET uint16 = 0
	DEL uint16 = 1
)

const (
	entryHeaderSize = 10
)

type entry struct {
	key     []byte
	val     []byte
	keySize uint32
	valSize uint32
	mark    uint16
}

func newEntry(key []byte, val []byte, mark uint16) *entry {
	e := &entry{
		key:     key,
		val:     val,
		keySize: uint32(len(key)),
		valSize: uint32(len(val)),
		mark:    mark,
	}
	return e
}

func (e *entry) getEntrySize() int {
	return int(e.keySize) + int(e.valSize) + entryHeaderSize
}

// encode entry 编码
func (e *entry) encode() []byte {
	buf := make([]byte, e.getEntrySize())
	binary.BigEndian.PutUint32(buf[0:4], e.keySize)
	binary.BigEndian.PutUint32(buf[4:8], e.valSize)
	binary.BigEndian.PutUint16(buf[8:10], e.mark)
	copy(buf[entryHeaderSize:entryHeaderSize+e.keySize], e.key)
	copy(buf[entryHeaderSize+e.keySize:], e.val)
	return buf
}

func decode(data []byte) (*entry, error) {
	return nil, nil
}

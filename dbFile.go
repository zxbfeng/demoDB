package demodb

import (
	"encoding/binary"
	"fmt"
	"os"
)

const (
	fileName      = "demo.db"
	mergeFileName = "mergeDemo.db"
	fileMode      = 0644
)

type dbFile struct {
	file   *os.File
	offset int
}

func getFileName(dbPath string) string {
	return fmt.Sprintf("%s%s%s", dbPath, string(os.PathSeparator), fileName)
}

func getMergeFileName(dbPath string) string {
	return fmt.Sprintf("%s%s%s", dbPath, string(os.PathSeparator), mergeFileName)
}

// NewDBFile 创建文件句柄
func NewDBFile(dbPath string) (*dbFile, error) {
	return newIternal(getFileName(dbPath))
}

func NewMergeFile(dbPath string) (*dbFile, error) {
	return newIternal(getMergeFileName(dbPath))
}

func newIternal(fileName string) (*dbFile, error) {
	df := &dbFile{}
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, fileMode)
	if err != nil {
		return nil, err
	}

	stat, err := os.Stat(fileName)
	if err != nil {
		return nil, err
	}
	df.file = file
	df.offset = int(stat.Size())
	return df, nil
}

func (df *dbFile) Read(offset int) (*entry, error) {
	// 首先读取header
	buf := make([]byte, entryHeaderSize)
	_, err := df.file.ReadAt(buf, int64(offset))
	if err != nil {
		return nil, err
	}

	e := &entry{
		keySize: (binary.BigEndian.Uint32(buf[0:4])),
		valSize: (binary.BigEndian.Uint32(buf[4:8])),
		mark:    (binary.BigEndian.Uint16(buf[8:10])),
	}

	// 读取内容
	offset += entryHeaderSize
	if e.keySize > 0 {
		e.key = make([]byte, e.keySize)
		df.file.ReadAt(e.key, int64(offset))
	}

	offset += int(e.keySize)
	if e.valSize > 0 {
		e.val = make([]byte, e.valSize)
		df.file.ReadAt(e.val, int64(offset))
	}
	return e, nil
}

func (df *dbFile) Write(e *entry) error {
	data := e.encode()
	_, err := df.file.WriteAt(data, int64(df.offset))
	if err != nil {
		return err
	}
	df.offset += e.getEntrySize()
	return nil
}

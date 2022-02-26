package demodb

import (
	"fmt"
	"io"
	"log"
	"os"
)

type Demodb struct {
	file    *dbFile
	indexes map[string]int
	dbPath  string
}

func Open(dbPath string) (*Demodb, error) {
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		if err = os.MkdirAll(dbPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	dbFile, err := NewDBFile(dbPath)
	if err != nil {
		return nil, err
	}

	db := &Demodb{
		file:    dbFile,
		dbPath:  dbPath,
		indexes: make(map[string]int),
	}
	log.Printf("loadDbMemoryIndexes.....")
	err = db.loadDbMemoryIndexes()
	if err != nil {
		return nil, err
	}
	log.Printf("indexes size:%d\n", len(db.indexes))
	return db, nil
}

// loadDbMemoryIndexes 加载db内容到内存构建索引
func (db *Demodb) loadDbMemoryIndexes() error {
	if db.file == nil {
		return fmt.Errorf("the under file is nil")
	}
	var offset int
	for {
		e, err := db.file.Read(offset)
		if err != nil {
			if io.EOF == err {
				break
			}
			return err
		}

		db.indexes[string(e.key)] = offset
		if e.mark == DEL {
			delete(db.indexes, string(e.key))
		}
		offset += e.getEntrySize()
		log.Printf("offset:%d, key:%s val:%s \n", offset, e.key, e.val)
	}
	return nil
}

// Get 获取key的内容
func (db *Demodb) Get(key []byte) ([]byte, error) {
	v, ok := db.indexes[string(key)]
	if !ok {
		return nil, fmt.Errorf("nil")
	}

	e, err := db.file.Read(v)
	if err != nil {
		return nil, err
	}
	return e.val, nil
}

func (db *Demodb) Set(key []byte, val []byte) error {
	currentOff := db.file.offset
	e := newEntry(key, val, SET)
	err := db.file.Write(e)
	if err != nil {
		return fmt.Errorf("write data to file failed")
	}
	// 更新内存索引
	db.indexes[string(key)] = currentOff
	// log.Printf("set data entry:%s, data len:%d, key len:%d val len:%d \n", data, len(data), len(key), len(val))
	return nil
}

func (db *Demodb) Del(key []byte) error {
	e := newEntry(key, nil, DEL)
	err := db.file.Write(e)
	if err != nil {
		return fmt.Errorf("write delete data to file failed")
	}

	delete(db.indexes, string(key))
	return nil
}

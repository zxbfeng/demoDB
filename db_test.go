package demodb

import (
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func TestOpen(t *testing.T) {
	db, err := Open("/tmp/demodb")
	if err != nil {
		t.Error(err)
	}
	t.Log(db)
}

func TestSet(t *testing.T) {
	db, err := Open("/tmp/demodb")
	if err != nil {
		t.Error(err)
	}

	rand.Seed(time.Now().UnixNano())
	keyPrefix := "test_key_"
	valPrefix := "test_val_"

	for i := 0; i < 10; i++ {
		key := []byte(keyPrefix + strconv.Itoa(i%5))
		val := []byte(valPrefix + strconv.FormatInt(rand.Int63(), 10))
		err = db.Set(key, val)
	}

	if err != nil {
		t.Log(err)
	}
}

func TestGet(t *testing.T) {
	db, err := Open("/tmp/demodb")
	if err != nil {
		t.Error(err)
	}

	getVal := func(key []byte) {
		val, err := db.Get(key)
		if err != nil {
			t.Error("read val err: ", err)
		} else {
			t.Logf("key = %s, val = %s\n", string(key), string(val))
		}
	}

	getVal([]byte("test_key_0"))
	getVal([]byte("test_key_1"))
	getVal([]byte("test_key_2"))
	getVal([]byte("test_key_3"))
	getVal([]byte("test_key_4"))
}

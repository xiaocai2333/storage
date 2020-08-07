package storage

type Key =  []byte
type Value = []byte

type Store interface {
	Get(key Key, timestamp uint64) (Value, error)
	BatchGet(keys []Key, timestamp uint64) ([]Value, error)
	Set(key Key, v Value,timestamp uint64) error
	BatchSet(keys []Key, v []Value, timestamp uint64) error
	Delete(key Key, timestamp uint64) error
	BatchDelete(keys []Key, timestamp uint64) error
	Scan(start Key, end Key, limit uint32, timestamp uint64) ([]Key, []Value, error)
	ReverseScan(start Key, end Key, limit uint32, timestamp uint64) ([]Key, []Value, error)
}
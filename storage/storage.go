package storage

import "context"

type Key = []byte
type Value = []byte

type Store interface {
	Get(ctx context.Context, key Key, timestamp uint64) (Value, error)
	BatchGet(ctx context.Context, keys []Key, timestamp uint64) ([]Value, error)
	Set(ctx context.Context, key Key, v Value, timestamp uint64) error
	BatchSet(ctx context.Context, keys []Key, v []Value, timestamp uint64) error
	Delete(ctx context.Context, key Key, timestamp uint64) error
	BatchDelete(ctx context.Context, keys []Key, timestamp uint64) error
}

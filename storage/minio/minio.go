package minio

import (
	"context"
	"fmt"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"io"
	"os"
	"syscall"
	. "test-minio/storage"
	"text/template/parse"
)

var bucketName = "minio-test"


type minioStore struct {
	client *minio.Client

}

func Init(endpoint string, accessKeyID string, secretAccessKey string, useSSL bool) (*minioStore, error) {

	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})

	if err != nil {
		return nil, err
	}
	return &minioStore{
		client: minioClient,
	}, nil
}

func (s *minioStore) Get(ctx context.Context, key Key, timestamp uint64) (Value, error) {
	minioKey := string(key) + string(timestamp)

	objects := s.client.ListObjects(ctx, bucketName, minio.ListObjectsOptions{Prefix: minioKey, Recursive: false})

	for i := 0; i < len(objects); i++ {
		object := objects[i]
		if object == nil {

		}
	}

	object, err := s.client.GetObject(ctx, bucketName, string(key), minio.GetObjectOptions{})
	if err != nil {
		panic(err)
		return nil, err
	}

	size := 256 * 1024
	buf := make([]byte, size)
	n, err := object.Read(buf)
	if err != nil {
		panic(err.Error())
		return nil, err
	}
	return Value(buf[:n]), err
}

func (s *minioStore) BatchGet(ctx context.Context, keys []Key, timestamp uint64) ([]Value, []error) {


}

func Set(ctx context.Context, key Key, v Value, timestamp uint64) error {

}

func BatchSet(ctx context.Context, keys []Key, v []Value, timestamp uint64) error {

}

func Delete(ctx context.Context, key Key, timestamp uint64) error {

}

func BatchDelete(ctx context.Context, keys []Key, timestamp uint64) error {

}

func Scan(ctx context.Context, start Key, end Key, limit uint32, timestamp uint64) ([]Key, []Value, error) {

}

func ReverseScan(ctx context.Context, start Key, end Key, limit uint32, timestamp uint64) ([]Key, []Value, error) {

}

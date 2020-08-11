package minio

import (
	"bytes"
	"context"
	"fmt"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"io"
	"strings"
	. "test-minio/storage"
	"test-minio/storage/codec"
)

var bucketName = "zcbucket"

type minioStore struct {
	client *minio.Client
}

func New(endpoint string, accessKeyID string, secretAccessKey string, useSSL bool) (*minioStore, error) {

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
	minioKey := codec.MvccEncode(key, timestamp)

	objects := s.client.ListObjects(ctx, bucketName, minio.ListObjectsOptions{Prefix: string(key)})
	for objectInfo := range objects {
		if objectInfo.Err != nil {
			fmt.Println(objectInfo.Err)
			return nil, objectInfo.Err
		}

		splitKey := strings.Split(objectInfo.Key, "_")

		if string(key) == strings.Join(splitKey[:len(splitKey)-1], "_") {
			if minioKey <= objectInfo.Key {
				object, err := s.client.GetObject(ctx, bucketName, objectInfo.Key, minio.GetObjectOptions{})
				if err != nil {
					fmt.Println(err)
					return nil, err
				}

				size := 256 * 1024
				buf := make([]byte, size)
				n, err := object.Read(buf)
				if err != nil && err != io.EOF {
					fmt.Println(err)
					return nil, err
				}

				return buf[:n], err
			}
		}
	}

	return nil, nil
}

func (s *minioStore) BatchGet(ctx context.Context, keys []Key, timestamp uint64) ([]Value, []error) {
	var values []Value
	var errs []error

	for i := 0; i < len(keys); i++ {
		object, err := s.Get(ctx, keys[i], timestamp)
		if err != nil && err != io.EOF {
			fmt.Println(err)
			values = append(values, nil)
			errs = append(errs, err)
		} else {
			values = append(values, object)
			errs = append(errs, nil)
		}
	}

	return values, errs
}

func (s *minioStore) Set(ctx context.Context, key Key, v Value, timestamp uint64) error {
	minioKey := codec.MvccEncode(key, timestamp)

	reader := bytes.NewReader(v)
	uploadInfo, err := s.client.PutObject(ctx, bucketName, minioKey, reader, int64(len(v)), minio.PutObjectOptions{})

	if err != nil {
		return err
	}

	fmt.Println(uploadInfo)
	return err
}

func (s *minioStore) BatchSet(ctx context.Context, keys []Key, values []Value, timestamp uint64) error {

	for i := 0; i < len(keys); i++ {
		err := s.Set(ctx, keys[i], values[i], timestamp)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *minioStore) Delete(ctx context.Context, key Key, timestamp uint64) error {
	minioKey := codec.MvccEncode(key, timestamp)

	objects := s.client.ListObjects(ctx, bucketName, minio.ListObjectsOptions{Prefix: string(key)})

	for objectInfo := range objects {
		if objectInfo.Err != nil {
			fmt.Println(objectInfo.Err)
			return objectInfo.Err
		}
		splitKey := strings.Split(objectInfo.Key, "_")
		if string(key) == strings.Join(splitKey[:len(splitKey)-1], "_") {
			if minioKey <= objectInfo.Key {
				err := s.client.RemoveObject(ctx, bucketName, objectInfo.Key, minio.RemoveObjectOptions{})
				if err != nil {
					fmt.Println(err)
					return err
				}
			}
		}
	}

	return nil
}

func (s *minioStore) BatchDelete(ctx context.Context, keys []Key, timestamp uint64) error {

	for i := 0; i < len(keys); i++ {
		err := s.Delete(ctx, keys[i], timestamp)
		if err != nil {
			fmt.Println(err)
			return err
		}
	}

	return nil
}

//func Scan(ctx context.Context, start Key, end Key, limit uint32, timestamp uint64) ([]Key, []Value, error) {
//
//}
//
//func ReverseScan(ctx context.Context, start Key, end Key, limit uint32, timestamp uint64) ([]Key, []Value, error) {
//
//}

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

//TODO: How to define bucketName?
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

	for objectInfo := range s.listObjectsKeys(ctx, key, timestamp){
		if objectInfo.Err != nil {
			fmt.Println(objectInfo.Err)
			return nil, objectInfo.Err
		}
		object, err := s.client.GetObject(ctx, bucketName, objectInfo.Key, minio.GetObjectOptions{})

		if err != nil {
			fmt.Println(err)
			return nil, err
		}

		buf := make([]byte, objectInfo.Size)
		n, err := object.Read(buf)
		if err != nil && err != io.EOF {
			fmt.Println(err)
			return nil, err
		}
		return buf[:n], err
	}

	return nil, nil
}

func (s *minioStore) BatchGet(ctx context.Context, keys []Key, timestamp uint64) ([]Value, error) {
	var values []Value

	for i := 0; i < len(keys); i++ {
		object, err := s.Get(ctx, keys[i], timestamp)
		if err != nil && err != io.EOF {
			fmt.Println(err)
			values = append(values, nil)
			return nil, err
		} else {
			values = append(values, object)
		}
	}

	return values, nil
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
	objectsCh := make(chan minio.ObjectInfo)

	go func() {
		defer close(objectsCh)
		for object := range s.listObjectsKeys(ctx, key, timestamp) {
			objectsCh <- object
		}
	}()

	opts := minio.RemoveObjectsOptions{
		GovernanceBypass: true,
	}

	for rErr := range s.client.RemoveObjects(ctx, bucketName, objectsCh, opts) {
		if rErr.Err != nil {
			return rErr.Err
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

func (s *minioStore) listObjectsKeys(ctx context.Context, key Key, timestamp uint64) <-chan minio.ObjectInfo {
	minioKey := codec.MvccEncode(key, timestamp)

	objectsCh := make(chan minio.ObjectInfo)

	go func() {
		defer close(objectsCh)
		for object := range s.client.ListObjects(ctx, bucketName, minio.ListObjectsOptions{Prefix: string(key)}){
			if object.Err != nil {
				fmt.Println(object.Err)
				objectsCh <- minio.ObjectInfo{

				}
			}
			splitKey := strings.Split(object.Key, "_")
			if string(key) == strings.Join(splitKey[:len(splitKey)-1], "_") {
				if minioKey <= object.Key {
					objectsCh <- object
				}
			}
		}
	}()

	return objectsCh
}

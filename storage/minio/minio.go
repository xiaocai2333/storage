package minio

import (
	"fmt"
	"github.com/minio/minio-go/v7"
	"log"
	"strconv"
	. "stroage/storage/storage"
)

type minioStore struct {
	client *minio.Client
}

func Init (endpoint string, accessKeyID string, secretAccessKey string, useSSL bool) (*minioStore, error) {
	minioClient, err := minio.New(endpoint, accessKeyID, secretAccessKey, useSSL)
	if err != nil {
		return nil, err
	}
	return &minioStore{
		client: minioClient,
	}, nil
}


func Get (key Key, timestam uint64) (Value, error) {
	key = string(key)
	fmt.Println(key)
}


func get(key Key, timestamp uint64) (Value, error) {
	minioClient = init()
}


func BatchGet(keys []Key, timestamp uint64) ([]Value, []error) {

}


func Set(key Key, v Value,timestamp uint64) error {

}


func BatchSet(keys []Key, v []Value, timestamp uint64) error {

}


func Delete(key Key, timestamp uint64) error {

}


func BatchDelete(keys []Key, timestamp uint64) error {

}


func Scan(start Key, end Key, limit uint32, timestamp uint64) ([]Key, []Value, error) {

}


func ReverseScan(start Key, end Key, limit uint32, timestamp uint64) ([]Key, []Value, error) {

}
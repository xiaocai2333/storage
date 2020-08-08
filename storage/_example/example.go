package main

import (
	"context"
	"fmt"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"io"
	"os"

	//. "test-minio/storage"
	//"test-minio/storage/minio"
	//. "test-minio/storage/minio/minio"
	//"time"
)

type Reader interface {
	Read(p []byte) (n int, err error)
}

type LimitedReader struct {
	R Reader // underlying reader
	N int64  // max bytes remaining
}

func main() {
	endpoint := "play.min.io"
	accessKeyID := "Q3AM3UQ867SPQQA43P2F"
	secretAccessKey := "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
	useSSL := true
	ctx := context.Background()
	//client, err := minio.Init(endpoint, accessKeyID, secretAccessKey, useSSL)
	//
	//if err != nil {
	//	panic(err.Error())
	//}
	//
	//key := Key("milvus")
	//timestamp := time.Now().Unix()
	//client.Get(ctx, key, timestamp)

	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})

	if err != nil {
		panic(err.Error())
		fmt.Println("hahahaha")
		return
	}

	bucketName := "zcbucket"
	objectName := "bar11"

	object, err := minioClient.GetObject(ctx, bucketName, objectName, minio.GetObjectOptions{})
	fmt.Println("value = ")
	fmt.Println(object.Stat())
	localFile, err := os.Create("/tmp/local-file.txt")

	size := 256 * 1024
	buf := make([]byte, size)
	n, err := object.Read(buf)
	fmt.Println("n = ", n)
	fmt.Println("buf = ", buf[:n])
	if _, err = io.Copy(localFile, object); err != nil{
		fmt.Println(err)
		return
	}
}
package main

import (
	"context"
	"fmt"
	"test-minio/storage/minio"
)

func main() {
	endpoint := "play.min.io"
	accessKeyID := "Q3AM3UQ867SPQQA43P2F"
	secretAccessKey := "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
	useSSL := true
	ctx := context.Background()

	//初始化minio
	client, err := minio.New(endpoint, accessKeyID, secretAccessKey, useSSL)

	if err != nil {
		fmt.Println(err)
	}

	//put对象
	key := []byte("letter")

	err = client.Set(ctx, key, []byte("abcdefghijklmnoopqrstuvwxyz"), 1234567)
	fmt.Println(err)
	err = client.Set(ctx, []byte("letterr"), []byte("asjkfghkusguisdhjfbsukdfhhjskxjf"), 1234667)
	fmt.Println(err)
	err = client.Set(ctx, key, []byte("123472146716490asjfugasf"), 1234767)
	fmt.Println(err)


	//读取对象
	objectName := "letter"
	key = []byte(objectName)
	object, _ := client.Get(ctx, key, 1234680)
	fmt.Println(string(object))
	object, _ = client.Get(ctx, key, 1234567)
	fmt.Println(string(object))
	object, _ = client.Get(ctx, key, 1234800)
	fmt.Println(string(object))
	object, _ = client.Get(ctx, []byte("letterr"), 1234667)
	fmt.Println(string(object))

	//删除对象
	err = client.Delete(ctx, key, 1234700)
	fmt.Println(err)
	object, _ = client.Get(ctx, key, 1234700)
	fmt.Println(string(object))
	object, _ = client.Get(ctx, key, 1234800)
	fmt.Println(string(object))

	return
}
package main

import (
	"context"
	"fmt"
	"test-minio/storage/minio"
)

func main() {
	//endPoint := "play.min.io"
	//accessKeyID := "Q3AM3UQ867SPQQA43P2F"
	//secretAccessKey := "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"

	endPoint := "127.0.0.1:9000"
	accessKeyID := "testminio"
	secretAccessKey := "testminio"

	//useSSL := true
	ctx := context.Background()

	//初始化minio
	client, err := minio.New(ctx, endPoint, accessKeyID, secretAccessKey, false)

	if err != nil {
		fmt.Println(err)
	}

	//put对象
	//key := []byte("letter")
	//
	//for i := 0; i < 100; i++ {
	//	err = client.Set(ctx, key, []byte("abcdefghijklmnopqrstuvwxyz"), 1234567 + uint64(i))
	//	fmt.Println(err)
	//}
	//
	////读取对象
	//objectName := "letter"
	//key = []byte(objectName)
	//for i := 0; i < 100; i++ {
	//	object, _ := client.Get(ctx, key, 1234568 + uint64(i))
	//	fmt.Println(string(object))
	//}
	//
	////删除对象
	//err = client.Delete(ctx, key, 1234667)
	//fmt.Println(err)

	keys := [][]byte{[]byte("foo"), []byte("bar")}
	values := [][]byte{[]byte("The key is foo!"), []byte("The key is bar!")}
	err = client.BatchSet(ctx, keys, values, 555555)


	return
}
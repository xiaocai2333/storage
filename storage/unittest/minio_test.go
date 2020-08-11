package unittest

import (
	"context"
	"github.com/stretchr/testify/assert"
	"test-minio/storage/minio"
	"testing"
)

var endpoint = "play.min.io"
var accessKeyID = "Q3AM3UQ867SPQQA43P2F"
var secretAccessKey = "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
var useSSL = true
var ctx = context.Background()
var client, err = minio.New(endpoint, accessKeyID, secretAccessKey, useSSL)

func TestPut(t *testing.T) {
	err = client.Set(ctx, []byte("bar"), []byte("abcdefghijklmnoopqrstuvwxyz"), 1234567)
	assert.Nil(t, err)
	err = client.Set(ctx, []byte("bar"), []byte("djhfkjsbdfbsdughorsgsdjhgoisdgh"), 1235567)
	assert.Nil(t, err)
	err = client.Set(ctx, []byte("bar"), []byte("123854676ershdgfsgdfk,sdhfg;sdi8"), 1236567)
	assert.Nil(t, err)
	err = client.Set(ctx, []byte("bar_1"), []byte("testkeybarorbar_1"), 1236567)
	assert.Nil(t, err)
}

func TestGet(t *testing.T) {
	object, _ := client.Get(ctx, []byte("bar"), 1234999)
	assert.Equal(t, "abcdefghijklmnoopqrstuvwxyz", string(object))
	object, _ = client.Get(ctx, []byte("bar"), 1235999)
	assert.Equal(t, "djhfkjsbdfbsdughorsgsdjhgoisdgh", string(object))
	object, _ = client.Get(ctx, []byte("bar"), 1236567)
	assert.Equal(t, "123854676ershdgfsgdfk,sdhfg;sdi8", string(object))
	object, _ = client.Get(ctx, []byte("bar_1"), 1236800)
	assert.Equal(t, "testkeybarorbar_1", string(object))
}

func TestDelete(t *testing.T){
	err = client.Delete(ctx, []byte("bar"), 1237000)
	assert.Nil(t, err)
	object, _ := client.Get(ctx, []byte("bar"), 1237000)
	assert.Nil(t, object)
	err = client.Delete(ctx, []byte("bar_1"), 1237000)
	assert.Nil(t, err)
}

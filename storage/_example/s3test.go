package main

import (
	"context"
	. "test-minio/storage"
	"test-minio/storage/minio"

	//	"crypto/hmac"
	"crypto/md5"
	//	"crypto/sha1"
	//	"crypto/tls"
	//	"encoding/base64"
	"flag"
	"fmt"
	"code.cloudfoundry.org/bytefmt"
	//	"github.com/aws/aws-sdk-go/aws"
	//	"github.com/aws/aws-sdk-go/aws/credentials"
	//	"github.com/pivotal-golang/bytefmt"
	//	"io"
	"io/ioutil"
	"log"
	"math/rand"
	//	"net"
	"net/http"
	"os"
	//	"sort"
	//	"strconv"
	//	"strings"
	//	"sync"
	"sync/atomic"
	"time"
)

type DummyWriteBuffer struct {
}

func (fw DummyWriteBuffer) WriteAt(p []byte, offset int64) (n int, err error) {
	// ignore 'offset' because we forced sequential downloads
	return ioutil.Discard.Write(p)
}


// Global variables
var bucket string
var already_upload int
var duration_secs, threads int
var object_size uint64
var object_data []byte
var running_threads, upload_count, download_count, upload_slowdown_count, download_slowdown_count int32
var endtime, upload_finish, download_finish time.Time

var skip_download int
var skip_upload int

var endPoint = "127.0.0.1:9000"
var accessKeyID = "testminio"
var secretAccessKey = "testminio"
var useSSL = false
var ctx = context.Background()
var client, err = minio.New(ctx, endPoint, accessKeyID, secretAccessKey, useSSL)

func logit(msg string) {
	fmt.Println(msg)
	logfile, _ := os.OpenFile("benchmark.log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if logfile != nil {
		logfile.WriteString(time.Now().Format(http.TimeFormat) + ": " + msg + "\n")
		logfile.Close()
	}
}

//func _getFile(sess *session.Session) {
//	atomic.AddInt32(&download_count, 1)
//	max_num := upload_count - upload_slowdown_count
//	objnum := rand.Int31n(max_num) + 1
//	filename := fmt.Sprintf("Object-%d", objnum)
//	//fmt.Println("run getFile ", filename)
//	downloader := s3manager.NewDownloader(sess)
//	//buff := &aws.WriteAtBuffer{}
//	buff := &DummyWriteBuffer{}
//	_, err := downloader.Download(buff,
//		&s3.GetObjectInput{
//			Bucket: &bucket,
//			Key:    &filename,
//		})
//
//	if err != nil {
//		atomic.AddInt32(&download_slowdown_count, 1)
//		//fmt.Println(err)
//	}
//	//fmt.Println("run getFile:%s done", filename)
//}

//func runGetFile(thread_num int) {
//	sess := session.Must(session.NewSessionWithOptions(session.Options{
//		SharedConfigState: session.SharedConfigEnable,
//	}))
//
//	for time.Now().Before(endtime) {
//		//atomic.AddInt32(&download_count, 1)
//		//time.Sleep(time.Millisecond * 20)
//		_getFile(sess)
//		//fmt.Println("run getFile")
//	}
//
//	// Remember last done time
//	download_finish = time.Now()
//	// One less thread
//	atomic.AddInt32(&running_threads, -1)
//}

func _putFile() {
	objnum := atomic.AddInt32(&upload_count, 1)
	filename := fmt.Sprintf("Object-%d", objnum)
	//fmt.Println("run putFile, ", filename)
	//file:= bytes.NewReader(object_data)


	err := client.Set(ctx, Key(filename), object_data, 0)
	if err != nil {
		atomic.AddInt32(&upload_slowdown_count, 1)
		//fmt.Println(err)
	}

}

func runPutFile(thread_num int) {

	for time.Now().Before(endtime) {
		//atomic.AddInt32(&upload_count, 1)
		//time.Sleep(time.Millisecond * 20)
		_putFile()
		//fmt.Println("run putFile")
	}

	// Remember last done time
	upload_finish = time.Now()
	// One less thread
	atomic.AddInt32(&running_threads, -1)
}

func main() {
	// Hello

	// Parse command line
	myflag := flag.NewFlagSet("myflag", flag.ExitOnError)
	myflag.StringVar(&bucket, "b", "zilliz-hz01", "Bucket for testing")
	myflag.IntVar(&duration_secs, "d", 2, "Duration of each test in seconds")
	myflag.IntVar(&threads, "t", 500, "Number of threads to run")
	myflag.IntVar(&already_upload, "upload_count", 0, "uploaded number")
	myflag.IntVar(&skip_upload, "skip_upload", 0, " 1 skip upload, 0 not skip")
	myflag.IntVar(&skip_download, "skip_download", 0, " 1 skip download, 0 not skip")

	var sizeArg string
	myflag.StringVar(&sizeArg, "z", "0.01K", "Size of objects in bytes with postfix K, M, and G")
	if err := myflag.Parse(os.Args[1:]); err != nil {
		os.Exit(1)
	}

	// Check the arguments
	var err error
	if object_size, err = bytefmt.ToBytes(sizeArg); err != nil {
		log.Fatalf("Invalid -z argument for object size: %v", err)
	}

	logit(fmt.Sprintf("Parameters: bucket=%s, duration=%d, threads=%d, size=%s",
		bucket, duration_secs, threads, sizeArg))

	// Initialize data for the bucket
	object_data = make([]byte, object_size)
	rand.Read(object_data)
	hasher := md5.New()
	hasher.Write(object_data)

	// reset counters
	upload_count = 0
	upload_slowdown_count = 0
	download_count = 0
	download_slowdown_count = 0

	running_threads = int32(threads)

	test_upload := 1
	// Run the upload case
	if skip_upload == 0 && test_upload == 1 {
		starttime := time.Now()
		endtime = starttime.Add(time.Second * time.Duration(duration_secs))

		for n := 1; n <= threads; n++ {
			go runPutFile(n)
		}

		// Wait for it to finish
		for atomic.LoadInt32(&running_threads) > 0 {
			time.Sleep(time.Millisecond)
		}
		upload_time := upload_finish.Sub(starttime).Seconds()

		bps := float64(uint64(upload_count)*object_size) / upload_time
		logit(fmt.Sprintf("PUT time %.1f secs, objects = %d, speed = %sB/sec, %.1f operations/sec. Slowdowns = %d",
			upload_time, upload_count, bytefmt.ByteSize(uint64(bps)), float64(upload_count)/upload_time, upload_slowdown_count))
	} else {
		if already_upload > 0 && upload_count <= 0 {
			upload_count  = int32(already_upload)
		}

	}

	fmt.Println(" upload_count :", upload_count)
	fmt.Println(" skip_download :", skip_download)
	fmt.Println(" skip_upload :", skip_upload)

	// Run the download case
	//test_download := 1
	//running_threads = int32(threads)
	//if skip_download == 0 && test_download == 1 {
	//
	//	starttime := time.Now()
	//	endtime = starttime.Add(time.Second * time.Duration(duration_secs))
	//	for n := 1; n <= threads; n++ {
	//		go runGetFile(n)
	//	}
	//
	//	// Wait for it to finish
	//	for atomic.LoadInt32(&running_threads) > 0 {
	//		time.Sleep(time.Millisecond)
	//	}
	//	download_time := download_finish.Sub(starttime).Seconds()
	//
	//	bps := float64(uint64(download_count)*object_size) / download_time
	//	logit(fmt.Sprintf("GET time %.1f secs, objects = %d, speed = %sB/sec, %.1f operations/sec. Slowdowns = %d",
	//		download_time, download_count, bytefmt.ByteSize(uint64(bps)), float64(download_count)/download_time, download_slowdown_count))
	//}

	// All done
}
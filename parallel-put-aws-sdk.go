/*
 * Minio Cloud Storage (C) 2017 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// Uploads all the inputs objects in parallel, upon any error this function panics.
func parallelUploadsAwsSdk(objectNames []string, data []byte, conc int) {

	var wg sync.WaitGroup
	inputs := make(chan string)

	// Start one go routine per CPU
	for i := 0; i < conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			uploadWorkerAwsSdk(inputs, data)
		}()
	}

	// Push objects onto input channel
	go func() {

		for _, objectName := range objectNames {
			inputs <- objectName
		}

		// Close input channel
		close(inputs)
	}()

	// Wait for workers to complete
	wg.Wait()
}

// Worker routine for uploading an object
func uploadWorkerAwsSdk(inputs <-chan string, data []byte) {

	endpoints := []string{"10.0.1.1:9000", "10.0.1.2:9000", "10.0.1.3:9000", "10.0.1.4:9000"}
	clients := make([]*session.Session, 4)

	for i := 0; i < 4; i++ {

		credsUp := credentials.NewStaticCredentials(accessKey, secretKey, "")
		sessUp := session.New(aws.NewConfig().
			WithCredentials(credsUp).
			WithRegion("us-east-1").
			WithEndpoint(endpoints[i]).
			WithDisableSSL(true).
			WithS3ForcePathStyle(true))

		clients[i] = sessUp
	}

	i := 0
	for name := range inputs {
		if _, err := uploadBlobAwsSdk(clients[i&3], data, name); err != nil {
			panic(err)
		}
		i++
	}
}

// uploadBlob does an upload to the server
func uploadBlobAwsSdk(session *session.Session, data []byte, key string) (n int64, err error) {

	uploader := s3manager.NewUploader(session, func(u *s3manager.Uploader) {
		u.PartSize = 256 * 1024 * 1024 // 256MB per part
	})
	r := bytes.NewReader(data)
	//defer r.Close()
	_, err = uploader.Upload(&s3manager.UploadInput{
		Body:   r,
		Bucket: aws.String("upload"),
		Key:    aws.String(key),
	})

	return
}

/*
func main() {
	flag.Parse()

	endpoint = os.Getenv("ENDPOINT")
	accessKey = os.Getenv("ACCESSKEY")
	secretKey = os.Getenv("SECRETKEY")
	if accessKey == "" || secretKey == "" {
		log.Fatalln("ENDPOINT, ACCESSKEY and SECRETKEY must be set")
	}

	totalstr := os.Getenv("TOTAL")
	total, err := strconv.Atoi(totalstr)
	if err != nil {
		log.Fatalln(err)
	}

	concurrency := os.Getenv("CONCURRENCY")
	conc, err := strconv.Atoi(concurrency)
	if err != nil {
		log.Fatalln(err)
	}
	concurrencyEnd := os.Getenv("CONCURRENCY_END")
	concEnd, err := strconv.Atoi(concurrencyEnd)
	if err != nil {
		concEnd = conc
	}
	concurrencyIncr := os.Getenv("CONCURRENCY_INCR")
	concIncr, err := strconv.Atoi(concurrencyIncr)
	if err != nil {
		concIncr = 1
	}

	for c := conc; c <= concEnd; c += concIncr {
		if c > conc {
			fmt.Println("Sleeping...")
			time.Sleep(15 * time.Second)
		}
		fmt.Println("Testing with concurrency", c)
		run(total, c)
	}
}
*/

func runAwsSdk(total, concurrency int) {

	utime := time.Now().UnixNano()
	var objectNames []string
	for i := 0; i < total; i++ {
		objectNames = append(objectNames, fmt.Sprintf("object-%d-%d", utime, i+1))
	}

	var data = bytes.Repeat([]byte("a"), *objectSize)

	start := time.Now().UTC()
	parallelUploads(objectNames, data, concurrency)

	totalSize := total * *objectSize
	elapsed := time.Since(start)
	fmt.Println("Elapsed time :", elapsed)
	seconds := float64(elapsed) / float64(time.Second)
	objsPerSec := float64(total) / seconds
	mbytePerSec := float64(totalSize) / seconds / 1024 / 1024
	fmt.Printf("Speed        : %4.0f objs/sec\n", objsPerSec)
	fmt.Printf("Bandwidth    : %4.0f MByte/sec\n", mbytePerSec)
	fmt.Printf("%d, %f, %f, %f\n", concurrency, seconds, objsPerSec, mbytePerSec)
}

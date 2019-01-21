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
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/minio/minio-go"
)

// Change this value to test with a different object size.
const defaultObjectSize = /*3 * 48*/ 120 * 1024 * 1024

// Uploads all the inputs objects in parallel, upon any error this function panics.
func parallelUploads(objectNames []string, data []byte, conc int) {

	// endpoints := []string{"10.0.1.1:9000", "10.0.1.2:9000", "10.0.1.3:9000", "10.0.1.4:9000"}
	// endpoints := []string{"10.0.1.2:9001", "10.0.1.3:9001",  "10.0.1.4:9001"}
	endpoints := []string{"10.0.2.1:9001", "10.0.1.2:9001", "10.0.2.3:9001", "10.0.1.4:9001"}
	fmt.Println("Hitting servers", endpoints)

	var wg sync.WaitGroup
	inputs := make(chan string)

	// Start one go routine per CPU
	for i := 0; i < conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			uploadWorker(endpoints, inputs, data)
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
func uploadWorker(endpoints []string, inputs <-chan string, data []byte) {

	clients := make([]*minio.Client, len(endpoints))

	for i := range clients {
		s3Client, err := minio.New(endpoints[i], accessKey, secretKey, false)
		if err != nil {
			log.Fatalln("Failed to create client", err)
		}
		clients[i] = s3Client
	}

	i := rand.Intn(100) // Just start at a random number
	for name := range inputs {
		if _, err := uploadBlob(clients[i%len(clients)], data, name); err != nil {
			panic(err)
		}
		i++
	}
}

// uploadBlob does an upload to the server
func uploadBlob(s3Client *minio.Client, data []byte, objectName string) (n int64, err error) {

	options := minio.PutObjectOptions{}
	//options.NumThreads = 8
	//options.ServerSideEncryption = ...

	n, err = s3Client.PutObject("upload", objectName, bytes.NewReader(data), int64(len(data)), options)
	if err != nil {
		fmt.Println(err)
		return
	}

	return
}

/*
func main() {
	flag.Parse()

	rand.Seed(time.Now().UTC().UnixNano())
	fmt.Printf("Object size  : %4d MB\n", *objectSize/1024/1024)

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

func run(total, concurrency int) {

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

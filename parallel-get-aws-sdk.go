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
	"os"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// Downloads all object names in parallel.
func parallelDownloadsAwsSdk(objectNames []string, conc int) {

	var wg sync.WaitGroup
	inputs := make(chan string)

	// Start one go routine per CPU
	for i := 0; i < conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			downloadWorkerAwsSdk(inputs)
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

// Worker routine for downloading an object
func downloadWorkerAwsSdk(inputs <-chan string) {

	for name := range inputs {
		if err := downloadBlobAwsSdk(name); err != nil {
			panic(err)
		}
	}
}

// downloadBlob does an upload to the S3/Minio server
func downloadBlobAwsSdk(objectName string) error {
	credsUp := credentials.NewStaticCredentials(os.Getenv("ACCESSKEY"), os.Getenv("SECRETKEY"), "")
	sessUp := session.New(aws.NewConfig().
		WithCredentials(credsUp).
		WithRegion("us-east-1").
		WithEndpoint(os.Getenv("ENDPOINT")).
		WithS3ForcePathStyle(true))

	downloader := s3manager.NewDownloader(sessUp, func(u *s3manager.Downloader) {
		u.PartSize = 256 * 1024 * 1024 // 256MB per part
	})

	var err error
	_, err = downloader.Download(Discard, &s3.GetObjectInput{
		Bucket: aws.String(os.Getenv("BUCKET")),
		Key:    aws.String(objectName),
	})

	return err
}

/*
func main() {

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

	var objectNames []string
	for i := 0; i < total; i++ {
		objectNames = append(objectNames, fmt.Sprintf("test240mb.obj_%d", i+1))
	}

	start := time.Now().UTC()
	parallelDownloads(objectNames, conc)
	totalSize := total * 251658240
	elapsed := time.Since(start)
	fmt.Println("Elapsed time :", elapsed)
	seconds := float64(elapsed) / float64(time.Second)
	fmt.Printf("Speed        : %4.0f objs/sec\n", float64(total)/seconds)
	fmt.Printf("Bandwidth    : %4.1f GB/sec\n", float64(totalSize)/seconds/1024/1024/1024)
}
*/

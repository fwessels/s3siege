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
	"hash/crc32"
	"io"
	"log"
	"sync"

	"github.com/minio/minio-go"
)

func hashOrder(key string, cardinality int) []int {
	if cardinality <= 0 {
		// Returns an empty int slice for cardinality < 0.
		return nil
	}

	nums := make([]int, cardinality)
	keyCrc := crc32.Checksum([]byte(key), crc32.IEEETable)

	start := int(keyCrc % uint32(cardinality))
	for i := 1; i <= cardinality; i++ {
		nums[i-1] = 1 + ((start + i) % cardinality)
	}
	return nums
}

type devNull int

func (devNull) WriteAt(p []byte, off int64) (int, error) {
	return len(p), nil
}

// Discard is an io.WriterAt on which
// all WriteAt calls succeed without
// doing anything.
var Discard io.WriterAt = devNull(0)

// Downloads all object names in parallel.
func parallelDownloads(objects []ObjectWithRange, conc int) (total, parts int64) {

	var wg sync.WaitGroup
	inputs := make(chan ObjectWithRange)
	outputs := make(chan int)

	// Start one go routine per CPU
	for i := 0; i < conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			downloadWorker(inputs, outputs)
		}()
	}

	// Push objects onto input channel
	go func() {
		for _, owr := range objects {
			inputs <- owr
		}

		// Close input channel
		close(inputs)
	}()

	// Wait for workers to complete
	go func() {
		wg.Wait()
		close(outputs) // Close output channel
	}()

	for n := range outputs {
		total += int64(n)
		parts++
	}

	return
}

// Worker routine for downloading an object
func downloadWorker(inputs <-chan ObjectWithRange, outputs chan<- int) {

	var chunk []byte

	endpoint := owr.server

	minioClient, err := minio.New(endpoint, accessKey, secretKey, false)
	if err != nil {
		log.Fatalln("Failed to create client", err)
	}

	for owr := range inputs {
		if chunk == nil {
			// Allocate buffer
			chunk = make([]byte, owr.end-owr.start)
		}
		if n, err := downloadBlob(minioClient, owr, chunk); err != nil {
			panic(err)
		} else {
			outputs <- n
		}
	}
}

// downloadBlob does an upload to the S3/Minio server
func downloadBlob(minioClient *minio.Client, owr ObjectWithRange, chunk []byte) (n int, err error) {

	options := minio.GetObjectOptions{}
	options.SetRange(int64(owr.start), int64(owr.end))

	object, err := minioClient.GetObject(bucket, owr.object, options)
	if err != nil {
		log.Fatalln(err)
	}

	n, err = io.ReadFull(object, chunk)
	object.Close()
	if err != nil {
		log.Fatalln(err)
	}
	//fmt.Println(chunk[0], chunk[len(chunk)-1])
	if chunk[0] != owr.sanityCheck {
		log.Fatalln("SANITY CHECK MISMATCH FOR FIRST BYTE: got", chunk[0], "expected", owr.sanityCheck)

	} else if chunk[len(chunk)-1] != owr.sanityCheck {
		log.Fatalln("SANITY CHECK MISMATCH FOR LAST BYTE: got", chunk[len(chunk)-1], "expected", owr.sanityCheck)
	}

	return
}

type ObjectWithRange struct {
	object      string
	server      string
	start       int
	end         int
	sanityCheck byte
}

const data = 12
const parity = 4
const partlength = 0x1400000

func getLocalServer(p int, order []int, servers []string) string {

	for i, o := range order {
		if p == o {
			return servers[i]
		}
	}
	return ""
}

/*
var (
	accessKey string
	secretKey string
	bucket    string
)
*/

func executeGet(total, conc int) {

	servers := [16]string{"10.0.1.1:9000", "10.0.1.2:9000", "10.0.1.3:9000", "10.0.1.4:9000", "10.0.1.1:9000", "10.0.1.2:9000", "10.0.1.3:9000", "10.0.1.4:9000", "10.0.1.1:9000", "10.0.1.2:9000", "10.0.1.3:9000", "10.0.1.4:9000", "10.0.1.1:9000", "10.0.1.2:9000", "10.0.1.3:9000", "10.0.1.4:9000"}

	var objects []ObjectWithRange
	for i := 1; i <= total; i++ {
		object := fmt.Sprintf("test240mb.obj_%d", i)
		order := hashOrder(object, data+parity)
		for p := 1; p <= data; p++ {
			localServer := getLocalServer(p, order, servers[:])
			if true { // localServer == "10.0.1.2:9000" {
				owr := ObjectWithRange{object, localServer, (p - 1) * partlength, p * partlength, byte(p) + 64}
				objects = append(objects, owr)
			}
		}
	}

	start := time.Now().UTC()
	totalBytes, totalParts := parallelDownloads(objects, conc)
	fmt.Println("totalBytes", totalBytes, "totalParts", totalParts)
	elapsed := time.Since(start)
	fmt.Println("Elapsed time :", elapsed)
	seconds := float64(elapsed) / float64(time.Second)
	fmt.Printf("Speed        : %4.1f objs/sec\n", float64(total)/seconds)
	fmt.Printf("             : %4.1f parts/sec\n", float64(totalParts)/seconds)
	fmt.Printf("Bandwidth    : %4.1f GB/sec\n", float64(totalBytes)/seconds/1024/1024/1024)
}
